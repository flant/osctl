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
	cfg := config.GetCommandConfig(cmd)

	threshold := cfg.GetRetentionThreshold()
	snapRepo := cfg.SnapshotRepo
	dateFormat := cfg.DateFormat
	dryRun := cfg.GetDryRun()

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting retention process threshold=%.2f snapRepo=%s dryRun=%t", threshold, snapRepo, dryRun))

	client, err := utils.NewOSClientFromCommandConfig(cfg)
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
	indices, err := client.GetIndicesWithFields(pattern, "i,ss", "ss:desc")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	if len(indices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	var found []string
	for _, idx := range indices {
		found = append(found, idx.Index)
	}
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

		indicesToDelete = append(indicesToDelete, idx)
	}

	if dryRun {
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

	if len(indicesToDelete) > 0 {
		var delNames []string
		for _, idx := range indicesToDelete {
			delNames = append(delNames, idx.Index)
		}
		logger.Info(fmt.Sprintf("Indices selected for deletion %s", strings.Join(delNames, ", ")))
	}
	for _, idx := range indicesToDelete {
		if err := client.DeleteIndex(idx.Index); err != nil {
			logger.Error(fmt.Sprintf("Failed to delete index index=%s error=%v", idx.Index, err))
			continue
		}

		logger.Info(fmt.Sprintf("Deleted index index=%s", idx.Index))

		time.Sleep(1 * time.Second)

		avgUtil, err = getAverageUtilization(client)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get utilization after deletion error=%v", err))
			break
		}

		logger.Info(fmt.Sprintf("Updated utilization utilization=%d", avgUtil))
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
