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

var coldStorageCmd = &cobra.Command{
	Use:   "coldstorage",
	Short: "Migrate indices to cold storage",
	Long: `Migrate indices to cold storage nodes based on age criteria.
Sets replicas to 0 and moves indices to cold storage nodes.`,
	RunE: runColdStorage,
}

func init() {
	addFlags(coldStorageCmd)
}

func runColdStorage(cmd *cobra.Command, args []string) error {
	cfg := config.GetCommandConfig(cmd)

	hotCount := cfg.GetHotCount()
	coldAttribute := cfg.ColdAttribute
	dateFormat := cfg.DateFormat
	dryRun := cfg.GetDryRun()

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting cold storage migration hotCount=%d coldAttribute=%s dryRun=%t", hotCount, coldAttribute, dryRun))

	client, err := opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -hotCount), dateFormat)

	allIndices, err := client.GetIndicesWithFields("*", "index")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	var allNames []string
	for _, index := range allIndices {
		allNames = append(allNames, index.Index)
	}
	if len(allNames) > 0 {
		logger.Info(fmt.Sprintf("Found indices %s", strings.Join(allNames, ", ")))
	} else {
		logger.Info("Found indices none")
	}

	var coldIndices []string
	for _, index := range allIndices {
		if shouldMoveToColdStorage(index.Index, cutoffDate, dateFormat) {
			coldIndices = append(coldIndices, index.Index)
		}
	}

	if len(coldIndices) == 0 {
		logger.Info("No indices found for cold storage migration")
		return nil
	}

	logger.Info(fmt.Sprintf("Found indices for cold storage migration count=%d", len(coldIndices)))
	if len(coldIndices) > 0 {
		logger.Info(fmt.Sprintf("Cold storage candidates %s", strings.Join(coldIndices, ", ")))
	}

	for _, index := range coldIndices {
		if dryRun {
			logger.Info(fmt.Sprintf("DRY RUN: Would migrate to cold storage index=%s attribute=%s", index, coldAttribute))
			continue
		}

		if err := client.SetColdStorage(index, coldAttribute); err != nil {
			logger.Error(fmt.Sprintf("Failed to migrate to cold storage index=%s error=%v", index, err))
			continue
		}

		logger.Info(fmt.Sprintf("Migrated to cold storage index=%s", index))
	}

	logger.Info(fmt.Sprintf("Cold storage migration completed processed=%d", len(coldIndices)))
	return nil
}

func shouldMoveToColdStorage(index, cutoffDate, dateFormat string) bool {
	return utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
}
