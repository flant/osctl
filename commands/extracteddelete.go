package commands

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
	"strings"
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
	addFlags(extractedDeleteCmd)
}

func runExtractedDelete(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()

	days := cfg.GetExtractedDays()
	dateFormat := cfg.GetRecovererDateFormat()
	dryRun := cfg.GetDryRun()

	logger := logging.NewLogger()
	client, err := utils.NewOSClientFromCommandConfigWithError(cfg, cfg.GetOpenSearchRecovererURL())
	if err != nil {
		return err
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), dateFormat)
	logger.Info(fmt.Sprintf("Starting extracted indices deletion days=%d cutoffDate=%s dryRun=%t", days, cutoffDate, dryRun))

	pattern := cfg.GetExtractedPattern()
	if pattern == "" {
		pattern = "extracted_"
	}
	allIndices, err := client.GetIndicesWithFields(pattern+"*", "index")
	if err != nil {
		return fmt.Errorf("failed to get extracted indices: %v", err)
	}
	logger.Info(fmt.Sprintf("Retrieved extracted indices from OpenSearch count=%d", len(allIndices)))

	names := utils.IndexInfosToNames(allIndices)
	if len(names) > 0 {
		logger.Info(fmt.Sprintf("Found extracted indices %s", strings.Join(names, ", ")))
	} else {
		logger.Info("Found extracted indices none")
	}

	var extractedIndices []string
	for _, index := range allIndices {
		if shouldDeleteExtractedIndex(index.Index, cutoffDate, dateFormat) {
			extractedIndices = append(extractedIndices, index.Index)
		}
	}

	if len(extractedIndices) == 0 {
		logger.Info("No extracted indices found for deletion")
		return nil
	}

	logger.Info(fmt.Sprintf("Found extracted indices for deletion count=%d", len(extractedIndices)))
	logger.Info(fmt.Sprintf("Extracted indices to delete %s", strings.Join(extractedIndices, ", ")))

	if dryRun {
		logger.Info(fmt.Sprintf("DRY RUN: Would delete extracted indices indices=%v", extractedIndices))
		return nil
	}

	for _, index := range extractedIndices {
		logger.Info(fmt.Sprintf("Deleting extracted index index=%s", index))
		if err := client.DeleteIndex(index); err != nil {
			logger.Error(fmt.Sprintf("Failed to delete extracted index index=%s error=%v", index, err))
			continue
		}

		logger.Info(fmt.Sprintf("Deleted extracted index index=%s", index))
	}

	logger.Info(fmt.Sprintf("Extracted indices deletion completed processed=%d", len(extractedIndices)))
	return nil
}

func shouldDeleteExtractedIndex(index, cutoffDate, dateFormat string) bool {
	return utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
}
