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

var indicesDeleteCmd = &cobra.Command{
	Use:   "indicesdelete",
	Short: "Delete indices",
	Long:  `Delete indices`,
	RunE:  runIndicesDelete,
}

func init() {
	addFlags(indicesDeleteCmd)
}

func runIndicesDelete(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()
	s3Config := cfg.GetOsctlIndicesS3SnapshotsConfig()
	checkSnapshots := cfg.GetIndicesDeleteCheckSnapshots()

	logger.Info(fmt.Sprintf("Starting indices deletion indicesCount=%d unknownDays=%d checkSnapshots=%t", len(indicesConfig), unknownConfig.DaysCount, checkSnapshots))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	allIndices, err := client.GetIndicesWithFields("*", "index,cd", "index:asc")
	if err != nil {
		return fmt.Errorf("failed to get all indices: %v", err)
	}
	allIndexNames := utils.IndexInfosToNames(allIndices)
	if len(allIndexNames) > 0 {
		logger.Info(fmt.Sprintf("Found indices %s", strings.Join(allIndexNames, ", ")))
	} else {
		logger.Info("Found indices none")
	}

	var indicesOlderThanRetentionPeriod []string
	var indicesRequiringSnapshotCheck []string
	var unknownIndices []string
	var indicesWithoutDateForLog []string

	for _, idx := range allIndices {
		indexName := idx.Index

		if strings.HasPrefix(indexName, ".") || (cfg.GetExtractedPattern() != "" && strings.HasPrefix(indexName, cfg.GetExtractedPattern())) {
			continue
		}

		indexConfig := utils.FindMatchingIndexConfig(indexName, indicesConfig)
		hasDateInName := utils.HasDateInName(indexName, cfg.GetDateFormat())

		if indexConfig == nil {
			if hasDateInName {
				unknownIndices = append(unknownIndices, indexName)
			} else {
				indicesWithoutDateForLog = append(indicesWithoutDateForLog, indexName)
			}
		} else {
			if hasDateInName {
				cutoffDateDaysCount := utils.FormatDate(time.Now().AddDate(0, 0, -indexConfig.DaysCount), cfg.GetDateFormat())
				if utils.IsOlderThanCutoff(indexName, cutoffDateDaysCount, cfg.GetDateFormat()) {
					indicesOlderThanRetentionPeriod = append(indicesOlderThanRetentionPeriod, indexName)

					if indexConfig.Snapshot {
						s3daysCount := s3Config.UnitCount.All
						if indexConfig.SnapshotCountS3 > 0 {
							s3daysCount = indexConfig.SnapshotCountS3
						}
						cutoffDateS3 := utils.FormatDate(time.Now().AddDate(0, 0, -s3daysCount), cfg.GetDateFormat())

						if !utils.IsOlderThanCutoff(indexName, cutoffDateS3, cfg.GetDateFormat()) {
							indicesRequiringSnapshotCheck = append(indicesRequiringSnapshotCheck, indexName)
						}
					}
				}
			} else {
				indicesWithoutDateForLog = append(indicesWithoutDateForLog, indexName)
			}
		}
	}

	unknownIndices = utils.FilterUnknownIndices(unknownIndices)
	if unknownConfig.DaysCount > 0 {
		for _, indexName := range unknownIndices {
			cutoffDateDaysCount := utils.FormatDate(time.Now().AddDate(0, 0, -unknownConfig.DaysCount), cfg.GetDateFormat())
			if utils.IsOlderThanCutoff(indexName, cutoffDateDaysCount, cfg.GetDateFormat()) {
				indicesOlderThanRetentionPeriod = append(indicesOlderThanRetentionPeriod, indexName)

				if unknownConfig.Snapshot {
					cutoffDateS3 := utils.FormatDate(time.Now().AddDate(0, 0, -s3Config.UnitCount.Unknown), cfg.GetDateFormat())

					if !utils.IsOlderThanCutoff(indexName, cutoffDateS3, cfg.GetDateFormat()) {
						indicesRequiringSnapshotCheck = append(indicesRequiringSnapshotCheck, indexName)
					}
				}
			}
		}
	}

	if len(indicesWithoutDateForLog) > 0 {
		logger.Info(fmt.Sprintf("Indices skipped (no date in name) count=%d list=%s", len(indicesWithoutDateForLog), strings.Join(indicesWithoutDateForLog, ", ")))
	}

	if len(indicesOlderThanRetentionPeriod) > 0 {
		logger.Info(fmt.Sprintf("Indices older than retention period (days_count) count=%d list=%s", len(indicesOlderThanRetentionPeriod), strings.Join(indicesOlderThanRetentionPeriod, ", ")))
	} else {
		logger.Info("Indices older than retention period (days_count): none")
	}

	if len(indicesRequiringSnapshotCheck) > 0 {
		logger.Info(fmt.Sprintf("Indices requiring snapshot check (older than days_count but not older than snapshot_count_s3) count=%d list=%s", len(indicesRequiringSnapshotCheck), strings.Join(indicesRequiringSnapshotCheck, ", ")))
	} else {
		logger.Info("Indices requiring snapshot check: none")
	}

	var snapshots []opensearch.Snapshot
	var indicesWithoutSnapshot []string
	var indicesToDeleteFinal []string

	if len(indicesRequiringSnapshotCheck) > 0 {
		snapRepo := cfg.GetSnapshotRepo()
		if checkSnapshots {
			if snapRepo == "" {
				return fmt.Errorf("snap-repo is required when indicesdelete-check-snapshots is true")
			}

			logger.Info(fmt.Sprintf("Getting all snapshots from repository repo=%s", snapRepo))
			snapshots, err = utils.GetSnapshotsIgnore404(client, snapRepo, "*")
			if err != nil {
				return fmt.Errorf("failed to get snapshots: %v", err)
			}
			if snapshots == nil {
				snapshots = []opensearch.Snapshot{}
			}

			var snapshotNames []string
			for _, s := range snapshots {
				if s.State == "SUCCESS" {
					snapshotNames = append(snapshotNames, s.Snapshot)
				}
			}
			if len(snapshotNames) > 0 {
				logger.Info(fmt.Sprintf("Found successful snapshots count=%d", len(snapshotNames)))
			} else {
				logger.Info("Found snapshots none")
			}

			for _, indexName := range indicesRequiringSnapshotCheck {
				hasSnapshot := utils.HasValidSnapshot(indexName, snapshots)
				if hasSnapshot {
					logger.Info(fmt.Sprintf("Index has valid snapshot index=%s", indexName))
					indicesToDeleteFinal = append(indicesToDeleteFinal, indexName)
				} else {
					logger.Warn(fmt.Sprintf("Index has no valid snapshot, skipping deletion index=%s", indexName))
					indicesWithoutSnapshot = append(indicesWithoutSnapshot, indexName)
				}
			}

			snapshotCheckSet := make(map[string]bool)
			for _, idx := range indicesRequiringSnapshotCheck {
				snapshotCheckSet[idx] = true
			}

			for _, indexName := range indicesOlderThanRetentionPeriod {
				if !snapshotCheckSet[indexName] {
					indicesToDeleteFinal = append(indicesToDeleteFinal, indexName)
				}
			}
		} else {
			indicesToDeleteFinal = indicesOlderThanRetentionPeriod
		}
	} else {
		indicesToDeleteFinal = indicesOlderThanRetentionPeriod
	}

	if len(indicesWithoutSnapshot) > 0 {
		logger.Warn(fmt.Sprintf("Indices skipped (no valid snapshot) count=%d list=%s", len(indicesWithoutSnapshot), strings.Join(indicesWithoutSnapshot, ", ")))
	}

	var successfulDeletions []string
	var failedDeletions []string

	if len(indicesToDeleteFinal) > 0 {
		logger.Info(fmt.Sprintf("Indices to delete (final list) count=%d list=%s", len(indicesToDeleteFinal), strings.Join(indicesToDeleteFinal, ", ")))
		logger.Info(fmt.Sprintf("Deleting indices count=%d", len(indicesToDeleteFinal)))
		successful, failed, err := utils.BatchDeleteIndices(client, indicesToDeleteFinal, cfg.GetDryRun(), logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete indices error=%v", err))
		}
		successfulDeletions = successful
		failedDeletions = failed
	} else {
		logger.Info("No indices for deletion")
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("INDICES DELETION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulDeletions) > 0 {
			logger.Info(fmt.Sprintf("Successfully deleted: %d indices", len(successfulDeletions)))
			for _, name := range successfulDeletions {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		}
		if len(failedDeletions) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Failed to delete: %d indices", len(failedDeletions)))
			for _, name := range failedDeletions {
				logger.Info(fmt.Sprintf("  ✗ %s", name))
			}
		}
		if len(indicesWithoutSnapshot) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Skipped (no valid snapshot): %d indices", len(indicesWithoutSnapshot)))
			for _, name := range indicesWithoutSnapshot {
				logger.Info(fmt.Sprintf("  - %s", name))
			}
		}
		if len(successfulDeletions) == 0 && len(failedDeletions) == 0 && len(indicesWithoutSnapshot) == 0 {
			logger.Info("No indices were deleted")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info("Indices deletion completed")
	return nil
}
