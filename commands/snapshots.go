package commands

import (
	"fmt"
	"math/rand"
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
	logger := logging.NewLogger()
	defaultRepo := cfg.GetSnapshotRepo()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()

	logger.Info(fmt.Sprintf("Starting snapshot creation indicesCount=%d unknownSnapshot=%t", len(indicesConfig), unknownConfig.Snapshot))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	var madisonClient *alerts.Client
	if cfg.GetMadisonKey() != "" && cfg.GetOSDURL() != "" && cfg.GetMadisonURL() != "" {
		madisonClient = alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	}

	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), cfg.GetDateFormat())
	today := utils.FormatDate(time.Now(), cfg.GetDateFormat())

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
		sysNames := utils.IndexInfosToNames(allSystemIndices)
		if len(sysNames) > 0 {
			logger.Info(fmt.Sprintf("Found system indices %s", strings.Join(sysNames, ", ")))
		} else {
			logger.Info("Found system indices none")
		}

		for _, idx := range allSystemIndices {
			indexName := idx.Index
			indexConfig := utils.FindMatchingIndexConfig(indexName, systemConfigs)
			if indexConfig != nil && indexConfig.Snapshot && !indexConfig.ManualSnapshot {
				utils.AddIndexToSnapshotGroups(indexName, *indexConfig, today, repoGroups, &indicesToSnapshot)
			}
		}
	}

	if len(regularConfigs) > 0 {
		allRegularIndices, err := client.GetIndicesWithFields("*"+yesterday+"*", "index,ss", "ss:desc")
		if err != nil {
			return fmt.Errorf("failed to get regular indices: %v", err)
		}
		regNames := utils.IndexInfosToNames(allRegularIndices)
		if len(regNames) > 0 {
			logger.Info(fmt.Sprintf("Found regular indices %s", strings.Join(regNames, ", ")))
		} else {
			logger.Info("Found regular indices none")
		}

		for _, idx := range allRegularIndices {
			indexName := idx.Index
			indexConfig := utils.FindMatchingIndexConfig(indexName, regularConfigs)
			if indexConfig != nil && indexConfig.Snapshot && !indexConfig.ManualSnapshot {
				utils.AddIndexToSnapshotGroups(indexName, *indexConfig, today, repoGroups, &indicesToSnapshot)
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
		existingMain, err := client.GetSnapshots(defaultRepo, "*"+today+"*")
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
				inProgressMain = append(inProgressMain, fmt.Sprintf("repo=%s snapshot=%s", defaultRepo, g.SnapshotName))
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
			fmt.Printf("\nSnapshot %d (repo %s): %s\n", i+1, defaultRepo, group.SnapshotName)
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

	var successfulSnapshots []string
	var failedSnapshots []string

	if !cfg.GetDryRun() {
		randomWaitSeconds := rand.Intn(291) + 10
		randomWaitDuration := time.Duration(randomWaitSeconds) * time.Second
		logger.Info(fmt.Sprintf("Waiting %d seconds before starting snapshot creation to distribute load", randomWaitSeconds))
		time.Sleep(randomWaitDuration)

		allSnapshots, err := utils.GetSnapshotsIgnore404(client, defaultRepo, "*"+today+"*")
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %v", err)
		}
		existingNames := utils.SnapshotsToNames(allSnapshots)
		if len(existingNames) > 0 {
			logger.Info(fmt.Sprintf("Existing snapshots today %s", strings.Join(existingNames, ", ")))
		} else {
			logger.Info("Existing snapshots today none")
		}

		for _, group := range snapshotGroups {
			if state, ok, err := utils.CheckSnapshotStateInRepo(client, defaultRepo, group.SnapshotName); err == nil && ok {
				if state == "SUCCESS" {
					logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", group.SnapshotName))
					continue
				}
				if state == "IN_PROGRESS" {
					logger.Info(fmt.Sprintf("Snapshot is currently IN_PROGRESS snapshot=%s repo=%s", group.SnapshotName, defaultRepo))
					continue
				}
			}

			exists, err := utils.CheckAndCleanSnapshot(group.SnapshotName, strings.Join(group.Indices, ","), allSnapshots, client, defaultRepo, logger)
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
			err = utils.CreateSnapshotWithRetry(client, group.SnapshotName, indicesStr, defaultRepo, madisonClient, logger, 60*time.Second)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to create snapshot after retries snapshot=%s error=%v", group.SnapshotName, err))
				failedSnapshots = append(failedSnapshots, group.SnapshotName)
				continue
			}
			successfulSnapshots = append(successfulSnapshots, group.SnapshotName)
		}

		if len(repoGroups) > 0 {
			perRepo := map[string][]utils.SnapshotGroup{}
			for k, g := range repoGroups {
				parts := strings.SplitN(k, "|", 2)
				repo := parts[0]
				perRepo[repo] = append(perRepo[repo], g)
			}
			for repo, groups := range perRepo {
				existing, err := utils.GetSnapshotsIgnore404(client, repo, "*"+today+"*")
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to get snapshots from repo repo=%s error=%v", repo, err))
					continue
				}
				for _, g := range groups {
					if state, ok, err := utils.CheckSnapshotStateInRepo(client, repo, g.SnapshotName); err == nil && ok {
						if state == "SUCCESS" {
							logger.Info(fmt.Sprintf("Valid snapshot already exists repo=%s snapshot=%s", repo, g.SnapshotName))
							continue
						}
						if state == "IN_PROGRESS" {
							logger.Info(fmt.Sprintf("Snapshot is currently IN_PROGRESS repo=%s snapshot=%s", repo, g.SnapshotName))
							continue
						}
					}
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
					err = utils.CreateSnapshotWithRetry(client, g.SnapshotName, indicesStr, repo, madisonClient, logger, 60*time.Second)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to create snapshot after retries repo=%s snapshot=%s error=%v", repo, g.SnapshotName, err))
						failedSnapshots = append(failedSnapshots, fmt.Sprintf("%s (repo=%s)", g.SnapshotName, repo))
						continue
					}
					successfulSnapshots = append(successfulSnapshots, fmt.Sprintf("%s (repo=%s)", g.SnapshotName, repo))
				}
			}
		}
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("SNAPSHOT CREATION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulSnapshots) > 0 {
			logger.Info(fmt.Sprintf("Successfully created: %d snapshots", len(successfulSnapshots)))
			for _, name := range successfulSnapshots {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		}
		if len(failedSnapshots) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Failed to create: %d snapshots", len(failedSnapshots)))
			for _, name := range failedSnapshots {
				logger.Info(fmt.Sprintf("  ✗ %s", name))
			}
		}
		if len(successfulSnapshots) == 0 && len(failedSnapshots) == 0 {
			logger.Info("No snapshots were created")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info("Snapshot creation completed")
	return nil
}
