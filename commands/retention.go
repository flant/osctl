package commands

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"strconv"
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
	snapRepo := cfg.GetSnapshotRepo()
	dateFormat := cfg.GetDateFormat()

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting retention process threshold=%.2f snapRepo=%s dryRun=%t", threshold, snapRepo, cfg.GetDryRun()))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	logger.Info("Getting average disk utilization")
	avgUtil, err := getAverageUtilization(client)
	if err != nil {
		return fmt.Errorf("failed to get utilization: %v", err)
	}
	logger.Info(fmt.Sprintf("Current disk utilization utilization=%d threshold=%.2f", avgUtil, threshold))

	if float64(avgUtil) <= threshold {
		logger.Info("Utilization below threshold, nothing to do")
		return nil
	}

	today := utils.FormatDate(time.Now(), dateFormat)
	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), dateFormat)

	pattern := fmt.Sprintf("*,-.*,-*%s,-*%s,-extracted_*", today, yesterday)
	indices, err := client.GetIndicesWithFields(pattern, "index,ss", "ss:desc")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	if len(indices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	found := utils.IndexInfosToNames(indices)
	logger.Info(fmt.Sprintf("Found indices to evaluate %s", strings.Join(found, ", ")))

	snapshots, err := client.GetSnapshots(snapRepo, "*")
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	var indicesToDelete []opensearch.IndexInfo
	for _, idx := range indices {
		if float64(avgUtil) <= threshold {
			break
		}

		if !utils.HasValidSnapshot(idx.Index, snapshots) {
			logger.Warn(fmt.Sprintf("No valid snapshots found index=%s", idx.Index))
			continue
		}

		logger.Info(fmt.Sprintf("Valid snapshot found index=%s", idx.Index))

		indicesToDelete = append(indicesToDelete, idx)
	}

	if cfg.GetDryRun() {
		fmt.Println("\nDRY RUN: Indices that would be deleted")
		fmt.Println("=" + strings.Repeat("=", 50))
		count := 0
		for _, idx := range indicesToDelete {
			if count >= 5 {
				break
			}
			fmt.Printf("%d. %s (size: %s)\n", count+1, idx.Index, idx.Size)
			count++
		}
		if len(indicesToDelete) > 5 {
			fmt.Printf("... and %d more indices\n", len(indicesToDelete)-5)
		}
		fmt.Printf("\nDRY RUN: Would delete %d indices\n", len(indicesToDelete))
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

		avgUtil, err = getAverageUtilization(client)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get utilization after deletion error=%v", err))
			break
		}

		logger.Info(fmt.Sprintf("Updated utilization utilization=%d", avgUtil))

		if float64(avgUtil) <= threshold {
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

func getAverageUtilization(client *opensearch.Client) (int, error) {
	allocation, err := client.GetAllocation()
	if err != nil {
		return 0, err
	}

	if len(allocation) == 0 {
		return 0, fmt.Errorf("no allocation data")
	}

	sum := 0
	for _, alloc := range allocation {
		if percent, err := strconv.Atoi(alloc.DiskPercent); err == nil {
			sum += percent
		}
	}

	return sum / len(allocation), nil
}
