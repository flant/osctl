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

var retentionCmd = &cobra.Command{
	Use:   "retention",
	Short: "Manage disk space by deleting old indices",
	Long: `Manage disk space by deleting old indices when disk utilization exceeds threshold.
Only deletes indices that have valid snapshots in the repository.`,
	RunE: runRetention,
}

func init() {
	addFlags(retentionCmd)
}

func runRetention(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()

	threshold := cfg.GetRetentionThreshold()
	retentionDaysCount := cfg.GetRetentionDaysCount()
	checkSnapshots := cfg.GetRetentionCheckSnapshots()
	checkNodesDown := cfg.GetRetentionCheckNodesDown()
	snapRepo := cfg.GetSnapshotRepo()
	dateFormat := cfg.GetDateFormat()
	kubeNamespace := cfg.GetKubeNamespace()

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	if retentionDaysCount < 2 {
		return fmt.Errorf("retention-days-count must be at least 2 days, got %d", retentionDaysCount)
	}

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting retention process threshold=%.2f retentionDaysCount=%d checkSnapshots=%t checkNodesDown=%t snapRepo=%s dryRun=%t", threshold, retentionDaysCount, checkSnapshots, checkNodesDown, snapRepo, cfg.GetDryRun()))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	logger.Info("Getting average disk utilization")
	avgUtil, err := utils.GetAverageUtilization(client, logger, true)
	if err != nil {
		return fmt.Errorf("failed to get utilization: %v", err)
	}
	logger.Info(fmt.Sprintf("Current disk utilization utilization=%d threshold=%.2f", avgUtil, threshold))

	var nodesDiff int
	nodesDiff, err = utils.CheckNodesDown(client, logger, checkNodesDown, kubeNamespace, true)
	if err != nil {
		if checkNodesDown {
			return fmt.Errorf("failed to check nodes: %v", err)
		} else {
			logger.Warn(fmt.Sprintf("Failed to check nodes (check disabled) error=%v", err))
		}
	}

	if checkNodesDown && nodesDiff != 0 {
		logger.Info(fmt.Sprintf("Cannot run retention: nodes are down (difference=%d)", nodesDiff))
		return nil
	}

	if float64(avgUtil) <= threshold {
		logger.Info("Utilization below threshold, nothing to do")
		return nil
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -retentionDaysCount), dateFormat)
	logger.Info(fmt.Sprintf("Cutoff date for retention cutoffDate=%s retentionDaysCount=%d", cutoffDate, retentionDaysCount))

	allIndices, err := client.GetIndicesWithFields("*", "index,ss", "ss:desc")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	if len(allIndices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	filteredIndices := make([]opensearch.IndexInfo, 0)
	goFormat := utils.ConvertDateFormat(dateFormat)

	for _, idx := range allIndices {
		indexName := idx.Index

		if utils.ShouldSkipIndexRetention(indexName) {
			continue
		}

		hasDate := utils.HasDateInName(indexName, dateFormat)
		if !hasDate {
			continue
		}

		extractedDate := utils.ExtractDateFromIndex(indexName, dateFormat)
		if extractedDate == "" {
			continue
		}

		indexTime, err := time.Parse(goFormat, extractedDate)
		if err == nil {
			if indexTime.After(time.Now()) {
				continue
			}
		}

		if utils.IsOlderThanCutoff(indexName, cutoffDate, dateFormat) {
			filteredIndices = append(filteredIndices, idx)
		} else {
			logger.Info(fmt.Sprintf("Skipping index: newer than cutoff date index=%s cutoffDate=%s", indexName, cutoffDate))
		}
	}

	if len(filteredIndices) == 0 {
		logger.Info("No indices older than cutoff date to process")
		return nil
	}

	found := utils.IndexInfosToNames(filteredIndices)
	logger.Info(fmt.Sprintf("Found indices to evaluate count=%d indices=%s", len(filteredIndices), strings.Join(found, ", ")))

	var snapshots []opensearch.Snapshot
	if checkSnapshots {
		snapshots, err = utils.GetSnapshotsIgnore404(client, snapRepo, "*")
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %v", err)
		}
		if snapshots == nil {
			snapshots = []opensearch.Snapshot{}
		}
		logger.Info(fmt.Sprintf("Found snapshots count=%d", len(snapshots)))
	}

	var indicesToDelete []opensearch.IndexInfo
	for _, idx := range filteredIndices {
		if float64(avgUtil) <= threshold {
			break
		}

		if checkSnapshots {
			if !utils.HasValidSnapshot(idx.Index, snapshots) {
				logger.Warn(fmt.Sprintf("No valid snapshots found index=%s", idx.Index))
				continue
			}
			logger.Info(fmt.Sprintf("Valid snapshot found index=%s", idx.Index))
		}

		indicesToDelete = append(indicesToDelete, idx)
	}

	if cfg.GetDryRun() {
		logger.Info("DRY RUN: Indices that would be deleted")
		logger.Info("=" + strings.Repeat("=", 50))
		count := 0
		for _, idx := range indicesToDelete {
			if count >= 5 {
				break
			}
			logger.Info(fmt.Sprintf("%d. %s (size: %s)", count+1, idx.Index, idx.Size))
			count++
		}
		if len(indicesToDelete) > 5 {
			logger.Info(fmt.Sprintf("... and %d more indices", len(indicesToDelete)-5))
		}
		logger.Info(fmt.Sprintf("DRY RUN: Would delete %d indices", len(indicesToDelete)))
		return nil
	}

	var successfulDeletions []string
	var failedDeletions []string

	if len(indicesToDelete) > 0 {
		delNames := utils.IndexInfosToNames(indicesToDelete)
		logger.Info(fmt.Sprintf("Indices selected for deletion %s", strings.Join(delNames, ", ")))
	}
	for _, idx := range indicesToDelete {
		if err := client.DeleteIndex(idx.Index); err != nil {
			logger.Error(fmt.Sprintf("Failed to delete index index=%s error=%v", idx.Index, err))
			failedDeletions = append(failedDeletions, idx.Index)
			continue
		}

		logger.Info(fmt.Sprintf("Deleted index index=%s", idx.Index))
		successfulDeletions = append(successfulDeletions, idx.Index)

		time.Sleep(15 * time.Second)

		avgUtil, err = utils.GetAverageUtilization(client, logger, false)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get utilization after deletion error=%v", err))
			break
		}
		logger.Info(fmt.Sprintf("Current disk utilization after deletion utilization=%d threshold=%.2f", avgUtil, threshold))

		nodesDiff, err = utils.CheckNodesDown(client, logger, checkNodesDown, kubeNamespace, false)
		if err != nil {
			if checkNodesDown {
				logger.Error(fmt.Sprintf("Failed to check nodes after deletion error=%v", err))
				break
			} else {
				logger.Warn(fmt.Sprintf("Failed to check nodes after deletion (check disabled) error=%v", err))
			}
		}

		if checkNodesDown && nodesDiff != 0 {
			logger.Info(fmt.Sprintf("Cannot continue retention: nodes are down (difference=%d)", nodesDiff))
			break
		}

		if float64(avgUtil) <= threshold {
			logger.Info("Utilization below threshold after deletion, stopping")
			break
		}
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("RETENTION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		logger.Info(fmt.Sprintf("Final disk utilization: %d%%", avgUtil))
		if len(successfulDeletions) > 0 {
			logger.Info("")
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
		if len(successfulDeletions) == 0 && len(failedDeletions) == 0 {
			logger.Info("No indices were deleted")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info(fmt.Sprintf("Retention completed finalUtilization=%d", avgUtil))
	return nil
}
