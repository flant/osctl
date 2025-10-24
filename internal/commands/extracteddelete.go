package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var extractedDeleteCmd = &cobra.Command{
	Use:   "extracteddelete",
	Short: "Delete extracted indices",
	Long:  `Delete extracted indices that are no longer needed.`,
	RunE:  runExtractedDelete,
}

func init() {
	extractedDeleteCmd.Flags().Int("days", 2, "Number of days to keep extracted indices")
	extractedDeleteCmd.Flags().String("date-format", "%d-%m-%Y", "Date format for extracted indices")

	addCommonFlags(extractedDeleteCmd)
}

func runExtractedDelete(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	days, _ := cmd.Flags().GetInt("days")
	dateFormat, _ := cmd.Flags().GetString("date-format")

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), dateFormat)

	allIndices, err := client.GetIndices("*")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	var extractedIndices []string
	for _, index := range allIndices {
		if shouldDeleteExtractedIndex(index, cutoffDate, dateFormat) {
			extractedIndices = append(extractedIndices, index)
		}
	}

	if len(extractedIndices) == 0 {
		logger.Info("No extracted indices found for deletion")
		return nil
	}

	logger.Info("Found extracted indices for deletion", "count", len(extractedIndices))

	for _, index := range extractedIndices {
		if err := client.DeleteIndex(index); err != nil {
			logger.Error("Failed to delete extracted index", "index", index, "error", err)
			continue
		}

		logger.Info("Deleted extracted index", "index", index)
	}

	logger.Info("Extracted indices deletion completed", "processed", len(extractedIndices))
	return nil
}

func shouldDeleteExtractedIndex(index, cutoffDate, dateFormat string) bool {
	if !isExtractedIndex(index) {
		return false
	}

	return utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
}

func isExtractedIndex(index string) bool {
	return len(index) >= 9 && index[:9] == "extracted"
}
