package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
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
	retentionCmd.Flags().Int("threshold", 75, "Disk usage threshold percentage")
	retentionCmd.Flags().String("snap-repo", "", "Snapshot repository name")
	retentionCmd.Flags().String("endpoint", "opendistro", "OpenSearch endpoint")

	addCommonFlags(retentionCmd)
}

func runRetention(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	threshold, _ := cmd.Flags().GetInt("threshold")
	snapRepo, _ := cmd.Flags().GetString("snap-repo")
	dateFormat, _ := cmd.Flags().GetString("date-format")

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	avgUtil, err := getAverageUtilization(client)
	if err != nil {
		return fmt.Errorf("failed to get utilization: %v", err)
	}

	logger.Info("Current utilization", "utilization", avgUtil, "threshold", threshold)

	if avgUtil <= threshold {
		logger.Info("Utilization below threshold, nothing to do")
		return nil
	}

	today := utils.FormatDate(time.Now(), dateFormat)
	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), dateFormat)

	pattern := fmt.Sprintf("*,-.*,-*%s,-*%s,-extracted_*", today, yesterday)
	indices, err := client.GetIndicesWithSize(pattern)
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	if len(indices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	sortIndicesBySize(indices)

	for _, idx := range indices {
		if avgUtil <= threshold {
			break
		}

		if !hasValidSnapshots(idx.Index, snapRepo, dateFormat, client) {
			logger.Warn("No valid snapshots found", "index", idx.Index)
			continue
		}

		if err := client.DeleteIndex(idx.Index); err != nil {
			logger.Error("Failed to delete index", "index", idx.Index, "error", err)
			continue
		}

		logger.Info("Deleted index", "index", idx.Index)

		time.Sleep(1 * time.Second)

		avgUtil, err = getAverageUtilization(client)
		if err != nil {
			logger.Error("Failed to get utilization after deletion", "error", err)
			break
		}

		logger.Info("Updated utilization", "utilization", avgUtil)
	}

	logger.Info("Retention completed", "finalUtilization", avgUtil)
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

func sortIndicesBySize(indices []opensearch.IndexInfo) {
	for i := 0; i < len(indices)-1; i++ {
		for j := i + 1; j < len(indices); j++ {
			if parseSize(indices[i].Size) < parseSize(indices[j].Size) {
				indices[i], indices[j] = indices[j], indices[i]
			}
		}
	}
}

func parseSize(sizeStr string) int64 {
	if sizeStr == "" {
		return 0
	}

	sizeStr = strings.TrimSpace(sizeStr)

	multiplier := int64(1)
	if strings.HasSuffix(sizeStr, "gb") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "gb")
	} else if strings.HasSuffix(sizeStr, "mb") {
		multiplier = 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "mb")
	} else if strings.HasSuffix(sizeStr, "kb") {
		multiplier = 1024
		sizeStr = strings.TrimSuffix(sizeStr, "kb")
	}

	if size, err := strconv.ParseFloat(sizeStr, 64); err == nil {
		return int64(size * float64(multiplier))
	}

	return 0
}

func hasValidSnapshots(index, snapRepo, dateFormat string, client *opensearch.Client) bool {
	dateStr := extractDateFromIndex(index)
	if dateStr == "" {
		return false
	}

	nextDay := getNextDay(dateStr, dateFormat)
	snapshots, err := client.GetSnapshots(snapRepo, fmt.Sprintf("*%s*", nextDay))
	if err != nil {
		return false
	}

	return hasIndexInSnapshots(index, snapshots)
}

func extractDateFromIndex(index string) string {
	parts := strings.Split(index, "-")
	for _, part := range parts {
		if len(part) == 10 && (strings.Count(part, ".") == 2 || strings.Count(part, "-") == 2) {
			return part
		}
	}
	return ""
}

func getNextDay(dateStr, dateFormat string) string {
	goFormat := utils.ConvertDateFormat(dateFormat)
	if t, err := time.Parse(goFormat, dateStr); err == nil {
		return t.AddDate(0, 0, 1).Format(goFormat)
	}
	return ""
}

func hasIndexInSnapshots(index string, snapshots []opensearch.Snapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot.State != "SUCCESS" {
			continue
		}
		for _, snapshotIndex := range snapshot.Indices {
			if snapshotIndex == index {
				return true
			}
		}
	}
	return false
}
