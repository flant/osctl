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
	repoGroups := map[string]utils.SnapshotGroup{}
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
				if indexConfig.Repository != "" {
					var snapshotName string
					if indexConfig.Kind == "regex" {
						snapshotName = indexConfig.Name + "-" + today
					} else {
						snapshotName = indexConfig.Value + "-" + today
					}
					key := indexConfig.Repository + "|" + snapshotName
					if g, ok := repoGroups[key]; ok {
						g.Indices = append(g.Indices, indexName)
						repoGroups[key] = g
					} else {
						repoGroups[key] = utils.SnapshotGroup{
							SnapshotName: snapshotName,
							Indices:      []string{indexName},
							Pattern:      indexConfig.Value,
							Kind:         indexConfig.Kind,
						}
					}
				} else {
					indicesToSnapshot = append(indicesToSnapshot, indexName)
				}
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
	if len(repoGroups) > 0 {
		repoKeys := make([]string, 0, len(repoGroups))
		for k := range repoGroups {
			repoKeys = append(repoKeys, k)
		}
		logger.Info(fmt.Sprintf("Repo-specific snapshot groups count=%d keys=%s", len(repoGroups), strings.Join(repoKeys, ", ")))
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
		existingMain, err := client.GetSnapshots(cfg.SnapshotRepo, "*"+today+"*")
		if err != nil {
			existingMain = nil
		}
		filteredMain := make([]utils.SnapshotGroup, 0, len(snapshotGroups))
		inProgressMain := make([]string, 0)
		for _, g := range snapshotGroups {
			if state, ok := utils.GetSnapshotStateByName(g.SnapshotName, existingMain); ok && state == "SUCCESS" {
				continue
			}
			if state, ok := utils.GetSnapshotStateByName(g.SnapshotName, existingMain); ok && state == "IN_PROGRESS" {
				inProgressMain = append(inProgressMain, fmt.Sprintf("repo=%s snapshot=%s", cfg.SnapshotRepo, g.SnapshotName))
				continue
			}
			filteredMain = append(filteredMain, g)
		}

		perRepo := map[string][]utils.SnapshotGroup{}
		for k, g := range repoGroups {
			parts := strings.SplitN(k, "|", 2)
			repo := parts[0]
			perRepo[repo] = append(perRepo[repo], g)
		}
		filteredPerRepo := map[string][]utils.SnapshotGroup{}
		inProgressPerRepo := make([]string, 0)
		for repo, groups := range perRepo {
			existing, err := client.GetSnapshots(repo, "*"+today+"*")
			if err != nil {
				existing = nil
			}
			for _, g := range groups {
				if state, ok := utils.GetSnapshotStateByName(g.SnapshotName, existing); ok && state == "SUCCESS" {
					continue
				}
				if state, ok := utils.GetSnapshotStateByName(g.SnapshotName, existing); ok && state == "IN_PROGRESS" {
					inProgressPerRepo = append(inProgressPerRepo, fmt.Sprintf("repo=%s snapshot=%s", repo, g.SnapshotName))
					continue
				}
				filteredPerRepo[repo] = append(filteredPerRepo[repo], g)
			}
		}

		fmt.Println("\nDRY RUN: Snapshot creation plan")
		fmt.Println("=" + strings.Repeat("=", 50))

		if len(inProgressMain)+len(inProgressPerRepo) > 0 {
			fmt.Println("\nCurrently IN_PROGRESS snapshots:")
			for _, msg := range inProgressMain {
				fmt.Printf("  %s\n", msg)
			}
			for _, msg := range inProgressPerRepo {
				fmt.Printf("  %s\n", msg)
			}
			fmt.Println("=" + strings.Repeat("=", 30))
		}

		for i, group := range filteredMain {
			fmt.Printf("\nSnapshot %d (repo %s): %s\n", i+1, cfg.SnapshotRepo, group.SnapshotName)
			fmt.Printf("Pattern: %s (%s)\n", group.Pattern, group.Kind)
			fmt.Printf("Indices (%d):\n", len(group.Indices))
			for _, index := range group.Indices {
				fmt.Printf("  %s\n", index)
			}
			fmt.Println("=" + strings.Repeat("=", 30))
		}

		if len(filteredPerRepo) > 0 {
			for repo, groups := range filteredPerRepo {
				for _, g := range groups {
					fmt.Printf("\nSnapshot (repo %s): %s\n", repo, g.SnapshotName)
					fmt.Printf("Pattern: %s (%s)\n", g.Pattern, g.Kind)
					fmt.Printf("Indices (%d):\n", len(g.Indices))
					for _, index := range g.Indices {
						fmt.Printf("  %s\n", index)
					}
					fmt.Println("=" + strings.Repeat("=", 30))
				}
			}
		}

		total := len(filteredMain)
		for _, groups := range filteredPerRepo {
			total += len(groups)
		}
		fmt.Printf("\nDRY RUN: Would create %d snapshots\n", total)
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

		if len(repoGroups) > 0 {
			perRepo := map[string][]utils.SnapshotGroup{}
			for k, g := range repoGroups {
				parts := strings.SplitN(k, "|", 2)
				repo := parts[0]
				perRepo[repo] = append(perRepo[repo], g)
			}
			for repo, groups := range perRepo {
				existing, err := client.GetSnapshots(repo, "*"+today+"*")
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to get snapshots from repo repo=%s error=%v", repo, err))
					continue
				}
				for _, g := range groups {
					exists, err := utils.CheckAndCleanSnapshot(g.SnapshotName, strings.Join(g.Indices, ","), existing, client, repo, logger)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to check/clean snapshot repo=%s snapshot=%s error=%v", repo, g.SnapshotName, err))
						continue
					}
					if exists {
						logger.Info(fmt.Sprintf("Valid snapshot already exists repo=%s snapshot=%s", repo, g.SnapshotName))
						continue
					}
					indicesStr := strings.Join(g.Indices, ",")
					logger.Info(fmt.Sprintf("Creating snapshot repo=%s snapshot=%s", repo, g.SnapshotName))
					logger.Info(fmt.Sprintf("Snapshot indices %s", indicesStr))
					err = utils.CreateSnapshotWithRetry(client, g.SnapshotName, indicesStr, repo, madisonClient, logger)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to create snapshot after retries repo=%s snapshot=%s error=%v", repo, g.SnapshotName, err))
						continue
					}
				}
			}
		}
	}

	logger.Info("Snapshot creation completed")
	return nil
}
