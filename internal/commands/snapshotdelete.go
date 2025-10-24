package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"regexp"
	"time"

	"github.com/spf13/cobra"
)

var snapshotDeleteCmd = &cobra.Command{
	Use:   "snapshotdelete",
	Short: "Delete snapshots",
	Long:  `Delete snapshots`,
	RunE:  runSnapshotDelete,
}

func init() {
	snapshotDeleteCmd.Flags().String("repo", "", "Snapshot repository name")
	snapshotDeleteCmd.Flags().String("index", "", "Index name pattern for snapshots")
	snapshotDeleteCmd.Flags().Int("days", 180, "Number of days to keep snapshots")
	snapshotDeleteCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for snapshot names")
	snapshotDeleteCmd.Flags().Bool("dangle-snapshots", false, "Delete dangling snapshots")
	snapshotDeleteCmd.Flags().StringSlice("exclude-list", []string{}, "List of snapshots to exclude from dangling deletion")
	snapshotDeleteCmd.Flags().String("kind", "prefix", "Matching kind: prefix or regex")
	snapshotDeleteCmd.Flags().Bool("dry-run", false, "Show what would be deleted without actually deleting")

	addCommonFlags(snapshotDeleteCmd)
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	repo, _ := cmd.Flags().GetString("repo")
	index, _ := cmd.Flags().GetString("index")
	days, _ := cmd.Flags().GetInt("days")
	dateFormat, _ := cmd.Flags().GetString("date-format")
	dangleSnapshots, _ := cmd.Flags().GetBool("dangle-snapshots")
	excludeList, _ := cmd.Flags().GetStringSlice("exclude-list")
	kind, _ := cmd.Flags().GetString("kind")
	dryRun, _ := cmd.Flags().GetBool("dry-run")

	if repo == "" {
		return fmt.Errorf("repo parameter is required")
	}

	if index == "" && !dangleSnapshots {
		return fmt.Errorf("index parameter is required or use --dangle-snapshots flag")
	}

	if dangleSnapshots && len(excludeList) == 0 {
		return fmt.Errorf("exclude-list parameter is required for dangling snapshots")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), dateFormat)
	logger.Info("Starting snapshots deletion", "repo", repo, "index", index, "dangleSnapshots", dangleSnapshots, "days", days, "cutoffDate", cutoffDate, "kind", kind)

	var allSnapshots []opensearch.Snapshot

	if dangleSnapshots || index == "unknown" {
		logger.Info("Getting unknown snapshots")
		allSnapshots, err = client.GetSnapshots(repo, "unknown*")
	} else if kind == "regex" {
		logger.Info("Getting all snapshots for regex matching")
		allSnapshots, err = client.GetSnapshots(repo, "*")
	} else {
		pattern := index + "*"
		logger.Info("Getting snapshots with prefix pattern", "pattern", pattern)
		allSnapshots, err = client.GetSnapshots(repo, pattern)
	}

	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	logger.Info("Retrieved snapshots from OpenSearch", "count", len(allSnapshots))
	if len(allSnapshots) > 0 {
		sampleSnapshots := make([]string, 0, utils.Min(5, len(allSnapshots)))
		for i := 0; i < utils.Min(5, len(allSnapshots)); i++ {
			sampleSnapshots = append(sampleSnapshots, allSnapshots[i].Snapshot)
		}
		logger.Info("Sample snapshots", "snapshots", sampleSnapshots)
	}

	var snapshotsToDelete []string
	for _, snapshot := range allSnapshots {
		if shouldDeleteSnapshot(snapshot.Snapshot, index, dangleSnapshots, excludeList, kind, cutoffDate, dateFormat) {
			snapshotsToDelete = append(snapshotsToDelete, snapshot.Snapshot)
		}
	}

	if len(snapshotsToDelete) == 0 {
		logger.Info("No snapshots found for deletion")
		return nil
	}

	if dryRun {
		logger.Info("DRY RUN: Would delete snapshots", "count", len(snapshotsToDelete))
		for _, snapshot := range snapshotsToDelete {
			logger.Info("DRY RUN: Would delete snapshot", "snapshot", snapshot)
		}
		return nil
	}

	logger.Info("Found snapshots for deletion", "count", len(snapshotsToDelete))

	for _, snapshot := range snapshotsToDelete {
		if err := client.DeleteSnapshot(repo, snapshot); err != nil {
			logger.Error("Failed to delete snapshot", "snapshot", snapshot, "error", err)
			continue
		}

		logger.Info("Deleted snapshot", "snapshot", snapshot)
	}

	logger.Info("Snapshots deletion completed", "processed", len(snapshotsToDelete))
	return nil
}

func shouldDeleteSnapshot(snapshot, targetIndex string, dangleSnapshots bool, excludeList []string, kind, cutoffDate, dateFormat string) bool {
	if dangleSnapshots {
		return isDanglingSnapshot(snapshot, excludeList) && utils.IsOlderThanCutoff(snapshot, cutoffDate, dateFormat)
	}

	if !isSnapshotMatching(snapshot, targetIndex, kind) {
		return false
	}

	return utils.IsOlderThanCutoff(snapshot, cutoffDate, dateFormat)
}

func isSnapshotMatching(snapshot, targetIndex string, kind string) bool {
	if kind == "regex" {
		matched, err := regexp.MatchString(targetIndex, snapshot)
		return err == nil && matched
	}

	// prefix matching (default)
	if len(snapshot) < len(targetIndex) {
		return false
	}
	return snapshot[:len(targetIndex)] == targetIndex
}

func isDanglingSnapshot(snapshot string, excludeList []string) bool {
	for _, exclude := range excludeList {
		if isSnapshotMatching(snapshot, exclude, "prefix") {
			return false
		}
	}

	return true
}
