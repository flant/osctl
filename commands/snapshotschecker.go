package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotsCheckerCmd = &cobra.Command{
	Use:   "snapshotschecker",
	Short: "Check for missing snapshots and send alerts",
	Long: `Check for missing snapshots of indices and send alerts to Madison.
Supports both whitelist and exclude list modes.`,
	RunE: runSnapshotsChecker,
}

func init() {
	addFlags(snapshotsCheckerCmd)
}

func runSnapshotsChecker(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	if cfg.IsFullPrefixSnapshots() {
		return runSnapshotsCheckerFullPrefix(cmd, args)
	}
	logger := logging.NewLogger()

	logger.Info("Starting snapshot checking")

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices config: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	today := utils.FormatDate(time.Now(), cfg.GetDateFormat())
	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), cfg.GetDateFormat())

	var madisonClient *alerts.Client
	if cfg.GetMadisonKey() != "" && cfg.GetOSDURL() != "" && cfg.GetMadisonURL() != "" {
		madisonClient = alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	}
	if !cfg.GetDryRun() {
		err = client.VerifyRepository(cfg.GetSnapshotRepo())
		if err != nil {
			if madisonClient != nil {
				madisonClient.SendMadisonVerifyRepoPermissionFailedAlert(cfg.GetSnapshotRepo(), cfg.GetKubeNamespace())
			}
			return fmt.Errorf("Checking permissions for repo %s was failed with %s", cfg.GetSnapshotRepo(), err)
		}
	}

	logger.Info(fmt.Sprintf("Getting all indices excluding today and yesterday today=%s yesterday=%s", today, yesterday))

	allIndices, err := client.GetIndicesWithFields("*", "index")
	if err != nil {
		return fmt.Errorf("failed to get all indices: %v", err)
	}

	var indicesToProcess []string
	for _, idx := range allIndices {
		indexName := idx.Index
		if utils.ShouldSkipIndex(indexName) {
			continue
		}

		hasDate := utils.HasDateInName(indexName, cfg.GetDateFormat())
		if !hasDate {
			continue
		}

		extractedDate := utils.ExtractDateFromIndex(indexName, cfg.GetDateFormat())
		if extractedDate == "" {
			continue
		}

		if extractedDate == today || extractedDate == yesterday {
			continue
		}

		goFormat := utils.ConvertDateFormat(cfg.GetDateFormat())
		indexTime, err := time.Parse(goFormat, extractedDate)
		if err == nil {
			if indexTime.After(time.Now()) {
				continue
			}
		}

		indicesToProcess = append(indicesToProcess, indexName)
	}

	logger.Info(fmt.Sprintf("Found indices to process count=%d", len(indicesToProcess)))
	if len(indicesToProcess) > 0 {
		logger.Info(fmt.Sprintf("Indices to process %s", strings.Join(indicesToProcess, ", ")))
	}

	if len(indicesToProcess) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	logger.Info(fmt.Sprintf("Getting all snapshots from repository repo=%s", cfg.GetSnapshotRepo()))
	allSnapshots, err := utils.GetSnapshotsIgnore404(client, cfg.GetSnapshotRepo(), "*")
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}
	if allSnapshots == nil {
		allSnapshots = []opensearch.Snapshot{}
	}

	var snapshotNames []string
	for _, s := range allSnapshots {
		if s.State == "SUCCESS" {
			snapshotNames = append(snapshotNames, s.Snapshot)
		}
	}
	if len(snapshotNames) > 0 {
		logger.Info(fmt.Sprintf("Found successful snapshots count=%d", len(snapshotNames)))
	} else {
		logger.Info("Found snapshots none")
	}

	var missingSnapshotIndicesList []string

	for _, indexName := range indicesToProcess {
		indexConfig := utils.FindMatchingIndexConfig(indexName, indicesConfig)
		shouldHaveSnapshot := false

		if indexConfig != nil {
			if !indexConfig.Snapshot || indexConfig.ManualSnapshot {
				continue
			}

			shouldHaveSnapshot = true

		} else {
			if unknownConfig.Snapshot && !unknownConfig.ManualSnapshot {
				shouldHaveSnapshot = true
			}
		}

		if shouldHaveSnapshot {
			if !utils.HasValidSnapshot(indexName, allSnapshots) {
				missingSnapshotIndicesList = append(missingSnapshotIndicesList, indexName)
			}
		}
	}

	if len(missingSnapshotIndicesList) > 0 {
		logger.Warn(fmt.Sprintf("Missing snapshots found count=%d", len(missingSnapshotIndicesList)))
		logger.Warn(fmt.Sprintf("Missing snapshots list %s", strings.Join(missingSnapshotIndicesList, ", ")))
		if cfg.GetDryRun() {
			logger.Info("DRY RUN: Would send Madison alert for missing snapshots")
		} else {
			madisonClient := alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
			response, err := madisonClient.SendMadisonSnapshotMissingAlert(missingSnapshotIndicesList, cfg.GetSnapshotRepo(), cfg.GetKubeNamespace(), today)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
				return fmt.Errorf("failed to send Madison alert: %v", err)
			}
			logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotMissing response=%s", response))
		}
	} else {
		logger.Info("All snapshots are present")
	}

	logger.Info("Snapshot checking completed")
	return nil
}
