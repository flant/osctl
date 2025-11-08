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
	cfg := config.GetConfig()

	hotCount := cfg.GetHotCount()
	coldAttribute := cfg.GetColdAttribute()
	dateFormat := cfg.GetDateFormat()
	dryRun := cfg.GetDryRun()

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting cold storage migration hotCount=%d coldAttribute=%s dryRun=%t", hotCount, coldAttribute, dryRun))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -hotCount), dateFormat)

	allIndices, err := client.GetIndicesWithFields("*", "index")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	allNames := utils.IndexInfosToNames(allIndices)
	if len(allNames) > 0 {
		logger.Info(fmt.Sprintf("Found indices %s", strings.Join(allNames, ", ")))
	} else {
		logger.Info("Found indices none")
	}

	var candidates []string
	for _, index := range allIndices {
		if utils.IsOlderThanCutoff(index.Index, cutoffDate, dateFormat) {
			candidates = append(candidates, index.Index)
		}
	}

	if len(candidates) == 0 {
		logger.Info("No indices found for cold storage migration")
		return nil
	}

	var coldIndices []string
	var alreadyCold []string
	for _, idx := range candidates {
		req, err := client.GetIndexColdRequirement(idx)
		if err != nil {
			logger.Error(fmt.Sprintf("Skip index due to read settings error index=%s error=%v", idx, err))
			continue
		}
		if req == coldAttribute {
			logger.Info(fmt.Sprintf("Already in cold: index=%s attr=%s", idx, req))
			alreadyCold = append(alreadyCold, idx)
			continue
		}
		logger.Info(fmt.Sprintf("Candidate for cold storage: index=%s current_attr=%s target_attr=%s", idx, req, coldAttribute))
		coldIndices = append(coldIndices, idx)
	}

	if len(alreadyCold) > 0 {
		logger.Info(fmt.Sprintf("Skip already in cold count=%d list=%s", len(alreadyCold), strings.Join(alreadyCold, ", ")))
	}
	if len(coldIndices) == 0 {
		logger.Info("No indices require cold storage migration")
		return nil
	}

	logger.Info(fmt.Sprintf("Found indices for cold storage migration count=%d", len(coldIndices)))
	if len(coldIndices) > 0 {
		logger.Info(fmt.Sprintf("Cold storage candidates %s", strings.Join(coldIndices, ", ")))
	}

	var successfulMigrations []string
	var failedMigrations []string

	for _, index := range coldIndices {
		if dryRun {
			logger.Info(fmt.Sprintf("DRY RUN: Would migrate to cold storage index=%s attribute=%s", index, coldAttribute))
			successfulMigrations = append(successfulMigrations, index)
			continue
		}

		if err := client.SetColdStorage(index, coldAttribute); err != nil {
			logger.Error(fmt.Sprintf("Failed to migrate to cold storage index=%s error=%v", index, err))
			failedMigrations = append(failedMigrations, index)
			continue
		}

		logger.Info(fmt.Sprintf("Migrated to cold storage index=%s", index))
		successfulMigrations = append(successfulMigrations, index)
	}

	if !dryRun {
		logger.Info("\n" + strings.Repeat("=", 60))
		logger.Info("COLD STORAGE MIGRATION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulMigrations) > 0 {
			logger.Info(fmt.Sprintf("Successfully migrated to cold: %d indices", len(successfulMigrations)))
			for _, name := range successfulMigrations {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		}
		if len(failedMigrations) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Failed to migrate: %d indices", len(failedMigrations)))
			for _, name := range failedMigrations {
				logger.Info(fmt.Sprintf("  ✗ %s", name))
			}
		}
		if len(alreadyCold) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Already in cold: %d indices", len(alreadyCold)))
			for _, name := range alreadyCold {
				logger.Info(fmt.Sprintf("  - %s", name))
			}
		}
		if len(successfulMigrations) == 0 && len(failedMigrations) == 0 && len(alreadyCold) == 0 {
			logger.Info("No indices were migrated to cold storage")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info(fmt.Sprintf("Cold storage migration completed processed=%d skipped_already_cold=%d", len(coldIndices), len(alreadyCold)))
	return nil
}
