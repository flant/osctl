package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshots",
	Short: "Create snapshots",
	Long:  `Create snapshots of indices`,
	RunE:  runSnapshot,
}

func init() {
	addFlags(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	cmdCfg := config.GetCommandConfig(cmd)
	logger := logging.NewLogger()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	logger.Info(fmt.Sprintf("Starting snapshot creation indicesCount=%d unknownSnapshot=%t", len(indicesConfig), unknownConfig.Snapshot))

	client, err := utils.NewOSClientFromCommandConfig(cmdCfg)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	var madisonClient *alerts.Client
	if cfg.MadisonKey != "" && cfg.OSDURL != "" && cfg.MadisonURL != "" {
		madisonClient = alerts.NewMadisonClient(cfg.MadisonKey, cfg.OSDURL, cfg.MadisonURL)
	}

	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), cfg.DateFormat)
	today := utils.FormatDate(time.Now(), cfg.DateFormat)

	var indicesToSnapshot []string
	var unknownIndices []string

	systemConfigs := make([]config.IndexConfig, 0)
	regularConfigs := make([]config.IndexConfig, 0)

	for _, indexConfig := range indicesConfig {
		if indexConfig.System {
			systemConfigs = append(systemConfigs, indexConfig)
		} else {
			regularConfigs = append(regularConfigs, indexConfig)
		}
	}

	if len(systemConfigs) > 0 {
		allSystemIndices, err := client.GetIndicesWithFields(".*", "index,ss", "ss:desc")
		if err != nil {
			return fmt.Errorf("failed to get system indices: %v", err)
		}
		var sysNames []string
		for _, idx := range allSystemIndices {
			sysNames = append(sysNames, idx.Index)
		}
		if len(sysNames) > 0 {
			logger.Info(fmt.Sprintf("Found system indices %s", strings.Join(sysNames, ", ")))
		} else {
			logger.Info("Found system indices none")
		}

		for _, idx := range allSystemIndices {
			indexName := idx.Index
			indexConfig := utils.FindMatchingIndexConfig(indexName, systemConfigs)
			if indexConfig != nil && indexConfig.Snapshot && !indexConfig.ManualSnapshot {
				indicesToSnapshot = append(indicesToSnapshot, indexName)
			}
		}
	}

	if len(regularConfigs) > 0 {
		allRegularIndices, err := client.GetIndicesWithFields("*"+yesterday+"*", "index,ss", "ss:desc")
		if err != nil {
			return fmt.Errorf("failed to get regular indices: %v", err)
		}
		var regNames []string
		for _, idx := range allRegularIndices {
			regNames = append(regNames, idx.Index)
		}
		if len(regNames) > 0 {
			logger.Info(fmt.Sprintf("Found regular indices %s", strings.Join(regNames, ", ")))
		} else {
			logger.Info("Found regular indices none")
		}

		for _, idx := range allRegularIndices {
			indexName := idx.Index
			indexConfig := utils.FindMatchingIndexConfig(indexName, regularConfigs)
			if indexConfig != nil && indexConfig.Snapshot && !indexConfig.ManualSnapshot {
				indicesToSnapshot = append(indicesToSnapshot, indexName)
			} else {
				unknownIndices = append(unknownIndices, indexName)
			}
		}

		unknownIndices = utils.FilterUnknownIndices(unknownIndices)
	}

	snapshotGroups := utils.GroupIndicesForSnapshots(indicesToSnapshot, indicesConfig, today)
	if len(indicesToSnapshot) > 0 {
		logger.Info(fmt.Sprintf("Indices to snapshot %s", strings.Join(indicesToSnapshot, ", ")))
	} else {
		logger.Info("Indices to snapshot none")
	}

	if unknownConfig.Snapshot && !unknownConfig.ManualSnapshot && len(unknownIndices) > 0 {
		snapshotGroups = append(snapshotGroups, utils.SnapshotGroup{
			SnapshotName: "unknown-" + today,
			Indices:      unknownIndices,
			Pattern:      "unknown",
			Kind:         "unknown",
		})
	}

	if cfg.GetDryRun() {
		fmt.Println("\nDRY RUN: Snapshot creation plan")
		fmt.Println("=" + strings.Repeat("=", 50))

		for i, group := range snapshotGroups {
			fmt.Printf("\nSnapshot %d: %s\n", i+1, group.SnapshotName)
			fmt.Printf("Pattern: %s (%s)\n", group.Pattern, group.Kind)
			fmt.Printf("Indices (%d):\n", len(group.Indices))

			for _, index := range group.Indices {
				fmt.Printf("  %s\n", index)
			}
			fmt.Println("=" + strings.Repeat("=", 30))
		}

		fmt.Printf("\nDRY RUN: Would create %d snapshots\n", len(snapshotGroups))
		return nil
	}

	if !cfg.GetDryRun() {
		allSnapshots, err := client.GetSnapshots(cfg.SnapshotRepo, "*"+today+"*")
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %v", err)
		}
		var existingNames []string
		for _, s := range allSnapshots {
			existingNames = append(existingNames, s.Snapshot)
		}
		if len(existingNames) > 0 {
			logger.Info(fmt.Sprintf("Existing snapshots today %s", strings.Join(existingNames, ", ")))
		} else {
			logger.Info("Existing snapshots today none")
		}

		for _, group := range snapshotGroups {
			exists, err := utils.CheckAndCleanSnapshot(group.SnapshotName, strings.Join(group.Indices, ","), allSnapshots, client, cfg.SnapshotRepo, logger)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to check/clean snapshot snapshot=%s error=%v", group.SnapshotName, err))
				continue
			}

			if exists {
				logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", group.SnapshotName))
				continue
			}

			indicesStr := strings.Join(group.Indices, ",")
			logger.Info(fmt.Sprintf("Creating snapshot %s", group.SnapshotName))
			logger.Info(fmt.Sprintf("Snapshot indices %s", indicesStr))
			err = utils.CreateSnapshotWithRetry(client, group.SnapshotName, indicesStr, cfg.SnapshotRepo, madisonClient, logger)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to create snapshot after retries snapshot=%s error=%v", group.SnapshotName, err))
				continue
			}
		}
	}

	logger.Info("Snapshot creation completed")
	return nil
}
