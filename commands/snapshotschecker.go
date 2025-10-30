package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
	"strings"

	"github.com/spf13/cobra"
)

var snapshotCheckerCmd = &cobra.Command{
	Use:   "snapshotchecker",
	Short: "Check for missing snapshots and send alerts",
	Long: `Check for missing snapshots of indices and send alerts to Madison.
Supports both whitelist and exclude list modes.`,
	RunE: runSnapshotChecker,
}

func init() {
	addFlags(snapshotCheckerCmd)
}

func runSnapshotChecker(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	cmdCfg := config.GetCommandConfig(cmd)
	logger := logging.NewLogger()

	logger.Info("Starting snapshot checking")

	client, err := utils.NewOSClientFromCommandConfig(cmdCfg)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices config: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	yesterday := utils.GetYesterdayFormatted(cfg.DateFormat)
	dayBeforeYesterday := utils.GetDayBeforeYesterdayFormatted(cfg.DateFormat)

	logger.Info(fmt.Sprintf("Getting indices for date date=%s", dayBeforeYesterday))
	allIndices, err := client.GetIndicesWithFields("*"+dayBeforeYesterday+"*", "index")
	if err != nil {
		return fmt.Errorf("failed to get indices for date: %v", err)
	}

	if len(allIndices) == 0 {
		logger.Info(fmt.Sprintf("No indices found for date date=%s", dayBeforeYesterday))
		return nil
	}

	var indexNamesList []string
	for _, idx := range allIndices {
		indexNamesList = append(indexNamesList, idx.Index)
	}
	logger.Info(fmt.Sprintf("Found indices %s", strings.Join(indexNamesList, ", ")))

	var expectedIndicesList []string
	for _, idx := range allIndices {
		indexName := idx.Index

		shouldSnapshot := false

		for _, indexConfig := range indicesConfig {
			if utils.MatchesIndex(indexName, indexConfig) && indexConfig.Snapshot {
				shouldSnapshot = true
				break
			}
		}

		if !shouldSnapshot && unknownConfig.Snapshot {
			shouldSnapshot = true
		}

		if shouldSnapshot {
			expectedIndicesList = append(expectedIndicesList, indexName)
		}
	}

	logger.Info(fmt.Sprintf("Getting snapshots for date date=%s", yesterday))
	allSnapshots, err := client.GetSnapshots(cfg.SnapshotRepo, "*"+yesterday+"*")
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	var snapshotNames []string
	for _, s := range allSnapshots {
		snapshotNames = append(snapshotNames, s.Snapshot)
	}
	if len(snapshotNames) > 0 {
		logger.Info(fmt.Sprintf("Found snapshots %s", strings.Join(snapshotNames, ", ")))
	} else {
		logger.Info("Found snapshots none")
	}

	unknownName := "unknown-" + yesterday
	var unknownIndices []string
	for _, s := range allSnapshots {
		if s.Snapshot == unknownName {
			unknownIndices = append(unknownIndices, s.Indices...)
			break
		}
	}
	if len(unknownIndices) > 0 {
		logger.Info(fmt.Sprintf("Indices in %s", unknownName))
		logger.Info(fmt.Sprintf("Unknown snapshot indices %s", strings.Join(unknownIndices, ", ")))
	}

	var missingSnapshots []string
	for _, indexName := range expectedIndicesList {
		if !utils.HasValidSnapshot(indexName, allSnapshots) {
			missingSnapshots = append(missingSnapshots, indexName)
		}
	}

	if len(missingSnapshots) > 0 {
		logger.Warn(fmt.Sprintf("Missing snapshots found count=%d", len(missingSnapshots)))
		logger.Warn(fmt.Sprintf("Missing snapshots list %s", strings.Join(missingSnapshots, ", ")))
		err := sendMissingSnapshotsAlert(cfg, missingSnapshots)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
		}
	} else {
		logger.Info("All snapshots are present")
	}

	logger.Info("Snapshot checking completed")
	return nil
}

func sendMissingSnapshotsAlert(cfg *config.Config, missingSnapshots []string) error {
	logger := logging.NewLogger()
	madisonClient := alerts.NewMadisonClient(cfg.MadisonKey, cfg.MadisonProject, cfg.OSDURL, cfg.MadisonURL)
	response, err := madisonClient.SendMadisonSnapshotMissingAlert(missingSnapshots)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotMissing response=%s", response))
	return nil
}
