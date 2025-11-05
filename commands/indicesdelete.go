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
	cmdCfg := config.GetCommandConfig(cmd)
	logger := logging.NewLogger()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	logger.Info(fmt.Sprintf("Starting indices deletion indicesCount=%d unknownDays=%d", len(indicesConfig), unknownConfig.DaysCount))

	client, err := utils.NewOSClientFromCommandConfig(cmdCfg)
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

	var indicesToDelete []string
	var unknownIndices []string
	var indicesWithoutDateForLog []string

	for _, idx := range allIndices {
		indexName := idx.Index

		if strings.HasPrefix(indexName, ".") || (cfg.ExtractedPattern != "" && strings.HasPrefix(indexName, cfg.ExtractedPattern)) {
			continue
		}

		indexConfig := utils.FindMatchingIndexConfig(indexName, indicesConfig)
		hasDateInName := utils.HasDateInName(indexName, cfg.DateFormat)

		if indexConfig == nil {
			if hasDateInName {
				unknownIndices = append(unknownIndices, indexName)
			} else {
				indicesWithoutDateForLog = append(indicesWithoutDateForLog, indexName)
			}
		} else {
			if hasDateInName {
				if utils.IsOlderThanCutoff(indexName, utils.FormatDate(time.Now().AddDate(0, 0, -indexConfig.DaysCount), cfg.DateFormat), cfg.DateFormat) {
					indicesToDelete = append(indicesToDelete, indexName)
				}
			} else {
				indicesWithoutDateForLog = append(indicesWithoutDateForLog, indexName)
			}
		}
	}

	unknownIndices = utils.FilterUnknownIndices(unknownIndices)
	if unknownConfig.DaysCount > 0 {
		for _, indexName := range unknownIndices {
			if utils.IsOlderThanCutoff(indexName, utils.FormatDate(time.Now().AddDate(0, 0, -unknownConfig.DaysCount), cfg.DateFormat), cfg.DateFormat) {
				indicesToDelete = append(indicesToDelete, indexName)
			}
		}
	}

	if len(indicesWithoutDateForLog) > 0 {
		logger.Info(fmt.Sprintf("Indices skipped (no date in name) count=%d list=%s", len(indicesWithoutDateForLog), strings.Join(indicesWithoutDateForLog, ", ")))
	}

	if len(indicesToDelete) > 0 {
		logger.Info(fmt.Sprintf("Indices to delete %s", strings.Join(indicesToDelete, ", ")))
		logger.Info(fmt.Sprintf("Deleting indices count=%d", len(indicesToDelete)))
		err := utils.DeleteIndicesBatch(client, indicesToDelete, cfg.GetDryRun(), logger)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete indices error=%v", err))
		}
	} else {
		logger.Info("No indices for deletion")
	}

	logger.Info("Indices deletion completed")
	return nil
}
