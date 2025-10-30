package commands

import (
	"fmt"
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

	client, err := opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	allSnapshots, err := client.GetSnapshots(cfg.SnapshotRepo, "*")
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

	var snapshotsToDelete []string
	var unknownSnapshots []string
	var danglingSnapshots []opensearch.Snapshot

	for _, snapshot := range allSnapshots {
		snapshotName := snapshot.Snapshot

		indexConfig := utils.FindMatchingSnapshotConfig(snapshotName, indicesConfig)

		if indexConfig == nil {
			if utils.HasDateInName(snapshotName, cfg.DateFormat) {
				unknownSnapshots = append(unknownSnapshots, snapshotName)
			} else {
				danglingSnapshots = append(danglingSnapshots, snapshot)
			}
		} else if indexConfig.Snapshot {
			daysCount := s3Config.UnitCount.All
			if indexConfig.SnapshotCountS3 > 0 {
				daysCount = indexConfig.SnapshotCountS3
			}
			if utils.IsOlderThanCutoff(snapshotName, utils.FormatDate(time.Now().AddDate(0, 0, -daysCount), cfg.DateFormat), cfg.DateFormat) {
				snapshotsToDelete = append(snapshotsToDelete, snapshotName)
			}
		}
	}

	unknownSnapshots = utils.FilterUnknownSnapshots(unknownSnapshots)
	if unknownConfig.Snapshot && s3Config.UnitCount.Unknown > 0 {
		for _, snapshotName := range unknownSnapshots {
			if utils.IsOlderThanCutoff(snapshotName, utils.FormatDate(time.Now().AddDate(0, 0, -s3Config.UnitCount.Unknown), cfg.DateFormat), cfg.DateFormat) {
				snapshotsToDelete = append(snapshotsToDelete, snapshotName)
			}
		}
	}

	if unknownConfig.Snapshot && s3Config.UnitCount.Unknown > 0 {
		cutoffTime := time.Now().AddDate(0, 0, -s3Config.UnitCount.Unknown)
		for _, snapshot := range danglingSnapshots {
			if snapshot.StartTimeInMillis > 0 {
				snapshotTime := time.Unix(0, snapshot.StartTimeInMillis*int64(time.Millisecond))
				if snapshotTime.Before(cutoffTime) || snapshotTime.Equal(cutoffTime) {
					snapshotsToDelete = append(snapshotsToDelete, snapshot.Snapshot)
				}
			}
		}
	}

	if len(snapshotsToDelete) > 0 {
		logger.Info(fmt.Sprintf("Snapshots to delete %s", strings.Join(snapshotsToDelete, ", ")))
		logger.Info(fmt.Sprintf("Deleting snapshots count=%d", len(snapshotsToDelete)))
		err := utils.DeleteSnapshotsBatch(client, snapshotsToDelete, cfg.SnapshotRepo, cfg.GetDryRun(), logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete snapshots error=%v", err))
		}
	} else {
		logger.Info("No snapshots for deletion")
	}

	logger.Info("Snapshot deletion completed")
	return nil
}
