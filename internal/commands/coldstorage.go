package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var coldStorageCmd = &cobra.Command{
	Use:   "coldstorage",
	Short: "Migrate indices to cold storage",
	Long: `Migrate indices to cold storage nodes based on age criteria.
Sets replicas to 0 and moves indices to cold storage nodes.`,
	RunE: runColdStorage,
}

func init() {
	coldStorageCmd.Flags().Int("hot-count", 30, "Number of days to keep indices in hot storage")
	coldStorageCmd.Flags().String("cold-attribute", "temp", "Node attribute for cold storage")
	coldStorageCmd.Flags().String("hot-attribute", "hot", "Node attribute for hot storage")

	addCommonFlags(coldStorageCmd)
}

func runColdStorage(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	hotCount, _ := cmd.Flags().GetInt("hot-count")
	coldAttribute, _ := cmd.Flags().GetString("cold-attribute")
	dateFormat, _ := cmd.Flags().GetString("date-format")

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -hotCount), dateFormat)

	allIndices, err := client.GetIndices("*")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	var coldIndices []string
	for _, index := range allIndices {
		if shouldMoveToColdStorage(index, cutoffDate, dateFormat) {
			coldIndices = append(coldIndices, index)
		}
	}

	if len(coldIndices) == 0 {
		logger.Info("No indices found for cold storage migration")
		return nil
	}

	logger.Info("Found indices for cold storage migration", "count", len(coldIndices))

	for _, index := range coldIndices {
		if err := client.SetColdStorage(index, coldAttribute); err != nil {
			logger.Error("Failed to migrate to cold storage", "index", index, "error", err)
			continue
		}

		logger.Info("Migrated to cold storage", "index", index)
	}

	logger.Info("Cold storage migration completed", "processed", len(coldIndices))
	return nil
}

func shouldMoveToColdStorage(index, cutoffDate, dateFormat string) bool {
	return utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
}
