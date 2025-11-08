package commands

import (
	"fmt"
	"math/rand"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotsDeleteCmd = &cobra.Command{
	Use:   "snapshotsdelete",
	Short: "Delete snapshots",
	Long:  `Delete snapshots`,
	RunE:  runSnapshotsDelete,
}

func init() {
	addFlags(snapshotsDeleteCmd)
}

func runSnapshotsDelete(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	s3Config := cfg.GetOsctlIndicesS3SnapshotsConfig()
	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	logger.Info(fmt.Sprintf("Starting snapshot deletion indicesCount=%d allDays=%d unknownDays=%d", len(indicesConfig), s3Config.UnitCount.All, s3Config.UnitCount.Unknown))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return err
	}

	allSnapshots, err := client.GetSnapshots(cfg.GetSnapshotRepo(), "*")
	if err != nil {
		return fmt.Errorf("failed to get all snapshots: %v", err)
	}

	var names []string
	for _, s := range allSnapshots {
		names = append(names, s.Snapshot)
	}
	if len(names) > 0 {
		logger.Info(fmt.Sprintf("Found snapshots %s", strings.Join(names, ", ")))
	} else {
		logger.Info("Found snapshots none")
	}

	repoToSnapshots := map[string][]string{}
	var snapshotsToDelete []string
	var unknownSnapshots []string
	var danglingSnapshots []opensearch.Snapshot

	for _, snapshot := range allSnapshots {
		snapshotName := snapshot.Snapshot

		indexConfig := utils.FindMatchingSnapshotConfig(snapshotName, indicesConfig)

		if indexConfig != nil && indexConfig.Repository != "" {
			indexConfig = nil
		}

		if indexConfig == nil {
			if utils.HasDateInName(snapshotName, cfg.GetDateFormat()) {
				unknownSnapshots = append(unknownSnapshots, snapshotName)
			} else {
				danglingSnapshots = append(danglingSnapshots, snapshot)
			}
		} else if indexConfig.Snapshot {
			daysCount := s3Config.UnitCount.All
			if indexConfig.SnapshotCountS3 > 0 {
				daysCount = indexConfig.SnapshotCountS3
			}
			if utils.IsOlderThanCutoff(snapshotName, utils.FormatDate(time.Now().AddDate(0, 0, -daysCount), cfg.GetDateFormat()), cfg.GetDateFormat()) {
				snapshotsToDelete = append(snapshotsToDelete, snapshotName)
			}
		}
	}

	if unknownConfig.Snapshot && s3Config.UnitCount.Unknown > 0 {
		for _, snapshotName := range unknownSnapshots {
			if utils.IsOlderThanCutoff(snapshotName, utils.FormatDate(time.Now().AddDate(0, 0, -s3Config.UnitCount.Unknown), cfg.GetDateFormat()), cfg.GetDateFormat()) {
				snapshotsToDelete = append(snapshotsToDelete, snapshotName)
			}
		}
		for _, snapshot := range danglingSnapshots {
			logger.Info(fmt.Sprintf("Dangling snapshot (default repo) snapshot=%s", snapshot.Snapshot))
		}
	}

	repoSet := map[string]bool{}
	for _, ic := range indicesConfig {
		if ic.Repository != "" && ic.Snapshot {
			repoSet[ic.Repository] = true
		}
	}
	for repo := range repoSet {
		rsnaps, err := client.GetSnapshots(repo, "*")
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get repo snapshots repo=%s error=%v", repo, err))
			continue
		}
		if len(rsnaps) > 0 {
			rnames := make([]string, 0, len(rsnaps))
			for _, s := range rsnaps {
				rnames = append(rnames, s.Snapshot)
			}
			logger.Info(fmt.Sprintf("Found snapshots in repo=%s %s", repo, strings.Join(rnames, ", ")))
		} else {
			logger.Info(fmt.Sprintf("Found snapshots in repo=%s none", repo))
		}
		for _, s := range rsnaps {
			name := s.Snapshot
			ic := utils.FindMatchingSnapshotConfig(name, indicesConfig)
			if ic == nil {
				logger.Info(fmt.Sprintf("Dangling snapshot (repo=%s) snapshot=%s (no matching config)", repo, name))
				continue
			}
			if ic.Repository != repo {
				logger.Info(fmt.Sprintf("Dangling snapshot (repo=%s) snapshot=%s (belongs to repo=%s)", repo, name, ic.Repository))
				continue
			}
			if !ic.Snapshot {
				logger.Info(fmt.Sprintf("Dangling snapshot (repo=%s) snapshot=%s (snapshot disabled in config)", repo, name))
				continue
			}
			if !utils.HasDateInName(name, cfg.GetDateFormat()) {
				logger.Info(fmt.Sprintf("Skip repo snapshot without date repo=%s snapshot=%s", repo, name))
				continue
			}
			daysCount := s3Config.UnitCount.All
			if ic.SnapshotCountS3 > 0 {
				daysCount = ic.SnapshotCountS3
			}
			if utils.IsOlderThanCutoff(name, utils.FormatDate(time.Now().AddDate(0, 0, -daysCount), cfg.GetDateFormat()), cfg.GetDateFormat()) {
				repoToSnapshots[repo] = append(repoToSnapshots[repo], name)
			}
		}
	}

	var successfulDeletions []string
	var failedDeletions []string

	if len(snapshotsToDelete) > 0 {
		if !cfg.GetDryRun() {
			randomWaitSeconds := rand.Intn(291) + 10
			randomWaitDuration := time.Duration(randomWaitSeconds) * time.Second
			logger.Info(fmt.Sprintf("Waiting %d seconds before starting snapshot deletion to distribute load", randomWaitSeconds))
			time.Sleep(randomWaitDuration)
		}

		logger.Info(fmt.Sprintf("Snapshots to delete %s", strings.Join(snapshotsToDelete, ", ")))
		logger.Info(fmt.Sprintf("Deleting snapshots count=%d", len(snapshotsToDelete)))
		successful, failed, err := utils.BatchDeleteSnapshots(client, snapshotsToDelete, cfg.GetSnapshotRepo(), cfg.GetDryRun(), logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete snapshots error=%v", err))
		}
		successfulDeletions = append(successfulDeletions, successful...)
		failedDeletions = append(failedDeletions, failed...)
	} else {
		logger.Info("No snapshots for deletion in default repo")
	}

	if len(repoToSnapshots) > 0 {
		for repo, names := range repoToSnapshots {
			if len(names) == 0 {
				continue
			}
			logger.Info(fmt.Sprintf("Snapshots to delete (repo=%s) %s", repo, strings.Join(names, ", ")))
			successful, failed, _ := utils.BatchDeleteSnapshots(client, names, repo, cfg.GetDryRun(), logger)
			for _, name := range successful {
				successfulDeletions = append(successfulDeletions, fmt.Sprintf("%s (repo=%s)", name, repo))
			}
			for _, name := range failed {
				failedDeletions = append(failedDeletions, fmt.Sprintf("%s (repo=%s)", name, repo))
			}
		}
	} else {
		logger.Info("No snapshots for deletion in custom repos")
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("SNAPSHOT DELETION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulDeletions) > 0 {
			logger.Info(fmt.Sprintf("Successfully deleted: %d snapshots", len(successfulDeletions)))
			for _, name := range successfulDeletions {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		}
		if len(failedDeletions) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Failed to delete: %d snapshots", len(failedDeletions)))
			for _, name := range failedDeletions {
				logger.Info(fmt.Sprintf("  ✗ %s", name))
			}
		}
		if len(successfulDeletions) == 0 && len(failedDeletions) == 0 {
			logger.Info("No snapshots were deleted")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info("Snapshot deletion completed")
	return nil
}
