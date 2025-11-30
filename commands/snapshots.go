package commands

import (
	"fmt"
	"math/rand"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"sort"
	"strconv"
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
	indexSizes := make(map[string]int64)

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
			if size, err := strconv.ParseInt(idx.Size, 10, 64); err == nil {
				indexSizes[indexName] = size
			}
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
			if size, err := strconv.ParseInt(idx.Size, 10, 64); err == nil {
				indexSizes[indexName] = size
			}
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

	sort.Slice(snapshotGroups, func(i, j int) bool {
		var sizeI, sizeJ int64
		for _, idx := range snapshotGroups[i].Indices {
			sizeI += indexSizes[idx]
		}
		for _, idx := range snapshotGroups[j].Indices {
			sizeJ += indexSizes[idx]
		}
		return sizeI > sizeJ
	})

	if cfg.GetDryRun() {
		existingMain, err := utils.GetSnapshotsIgnore404(client, defaultRepo, "*"+today+"*")
		if err != nil {
			existingMain = nil
		}
		if existingMain == nil {
			existingMain = []opensearch.Snapshot{}
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
			existing, err := utils.GetSnapshotsIgnore404(client, repo, "*"+today+"*")
			if err != nil {
				existing = nil
			}
			if existing == nil {
				existing = []opensearch.Snapshot{}
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

		logger.Info("DRY RUN: Snapshot creation plan")
		logger.Info("=" + strings.Repeat("=", 50))

		if len(inProgressMain)+len(inProgressPerRepo) > 0 {
			logger.Info("")
			logger.Info("Currently IN_PROGRESS snapshots:")
			for _, msg := range inProgressMain {
				logger.Info(fmt.Sprintf("  %s", msg))
			}
			for _, msg := range inProgressPerRepo {
				logger.Info(fmt.Sprintf("  %s", msg))
			}
			logger.Info("=" + strings.Repeat("=", 30))
		}

		for i, group := range filteredMain {
			logger.Info("")
			logger.Info(fmt.Sprintf("Snapshot %d (repo %s): %s", i+1, defaultRepo, group.SnapshotName))
			logger.Info(fmt.Sprintf("Pattern: %s (%s)", group.Pattern, group.Kind))
			logger.Info(fmt.Sprintf("Indices (%d):", len(group.Indices)))
			for _, index := range group.Indices {
				logger.Info(fmt.Sprintf("  %s", index))
			}
			logger.Info("=" + strings.Repeat("=", 30))
		}

		if len(filteredPerRepo) > 0 {
			for repo, groups := range filteredPerRepo {
				for _, g := range groups {
					logger.Info("")
					logger.Info(fmt.Sprintf("Snapshot (repo %s): %s", repo, g.SnapshotName))
					logger.Info(fmt.Sprintf("Pattern: %s (%s)", g.Pattern, g.Kind))
					logger.Info(fmt.Sprintf("Indices (%d):", len(g.Indices)))
					for _, index := range g.Indices {
						logger.Info(fmt.Sprintf("  %s", index))
					}
					logger.Info("=" + strings.Repeat("=", 30))
				}
			}
		}

		total := len(filteredMain)
		for _, groups := range filteredPerRepo {
			total += len(groups)
		}
		logger.Info("")
		logger.Info(fmt.Sprintf("DRY RUN: Would create %d snapshots", total))
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
		if allSnapshots == nil {
			allSnapshots = []opensearch.Snapshot{}
		}
		existingNames := utils.SnapshotsToNames(allSnapshots)
		if len(existingNames) > 0 {
			logger.Info(fmt.Sprintf("Existing snapshots today %s", strings.Join(existingNames, ", ")))
		} else {
			logger.Info("Existing snapshots today none")
		}

		var snapshotTasks []utils.SnapshotTask
		for _, group := range snapshotGroups {
			if state, ok, err := utils.CheckSnapshotStateInRepo(client, defaultRepo, group.SnapshotName); err == nil && ok {
				if state == "SUCCESS" {
					missingIndices := make([]string, 0)
					for _, snapshot := range allSnapshots {
						if snapshot.Snapshot == group.SnapshotName {
							for _, idx := range group.Indices {
								found := false
								for _, snapshotIndex := range snapshot.Indices {
									if snapshotIndex == idx {
										found = true
										break
									}
								}
								if !found {
									missingIndices = append(missingIndices, idx)
								}
							}
							break
						}
					}
					if len(missingIndices) == 0 {
						logger.Info(fmt.Sprintf("Valid snapshot already exists with all indices snapshot=%s", group.SnapshotName))
						continue
					}
					randomSuffix := utils.GenerateRandomAlphanumericString(6)
					parts := strings.Split(group.SnapshotName, "-")
					if len(parts) > 0 {
						datePart := parts[len(parts)-1]
						baseName := strings.Join(parts[:len(parts)-1], "-")
						newSnapshotName := baseName + "-" + randomSuffix + "-" + datePart
						logger.Info(fmt.Sprintf("Some indices missing in existing snapshot, creating additional snapshot original=%s new=%s missingIndicesCount=%d", group.SnapshotName, newSnapshotName, len(missingIndices)))
						indicesStr := strings.Join(missingIndices, ",")
						var totalSize int64
						for _, idx := range missingIndices {
							totalSize += indexSizes[idx]
						}
						snapshotTasks = append(snapshotTasks, utils.SnapshotTask{
							SnapshotName: newSnapshotName,
							IndicesStr:   indicesStr,
							Repo:         defaultRepo,
							Namespace:    cfg.GetKubeNamespace(),
							DateStr:      today,
							PollInterval: 60 * time.Second,
							Size:         totalSize,
						})
					}
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
			var totalSize int64
			for _, idx := range group.Indices {
				totalSize += indexSizes[idx]
			}
			snapshotTasks = append(snapshotTasks, utils.SnapshotTask{
				SnapshotName: group.SnapshotName,
				IndicesStr:   indicesStr,
				Repo:         defaultRepo,
				Namespace:    cfg.GetKubeNamespace(),
				DateStr:      today,
				PollInterval: 60 * time.Second,
				Size:         totalSize,
			})
		}

		if len(snapshotTasks) > 0 {
			successful, failed := utils.CreateSnapshotsInParallel(client, snapshotTasks, cfg.GetMaxConcurrentSnapshots(), madisonClient, logger, true)
			successfulSnapshots = append(successfulSnapshots, successful...)
			failedSnapshots = append(failedSnapshots, failed...)
		}

		if len(repoGroups) > 0 {
			perRepo := map[string][]utils.SnapshotGroup{}
			for k, g := range repoGroups {
				parts := strings.SplitN(k, "|", 2)
				repo := parts[0]
				perRepo[repo] = append(perRepo[repo], g)
			}
			var repoSnapshotTasks []utils.SnapshotTask
			for repo, groups := range perRepo {
				sort.Slice(groups, func(i, j int) bool {
					var sizeI, sizeJ int64
					for _, idx := range groups[i].Indices {
						sizeI += indexSizes[idx]
					}
					for _, idx := range groups[j].Indices {
						sizeJ += indexSizes[idx]
					}
					return sizeI > sizeJ
				})
				existing, err := utils.GetSnapshotsIgnore404(client, repo, "*"+today+"*")
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to get snapshots from repo repo=%s error=%v", repo, err))
					continue
				}
				if existing == nil {
					existing = []opensearch.Snapshot{}
				}
				for _, g := range groups {
					if state, ok, err := utils.CheckSnapshotStateInRepo(client, repo, g.SnapshotName); err == nil && ok {
						if state == "SUCCESS" {
							missingIndices := make([]string, 0)
							for _, snapshot := range existing {
								if snapshot.Snapshot == g.SnapshotName {
									for _, idx := range g.Indices {
										found := false
										for _, snapshotIndex := range snapshot.Indices {
											if snapshotIndex == idx {
												found = true
												break
											}
										}
										if !found {
											missingIndices = append(missingIndices, idx)
										}
									}
									break
								}
							}
							if len(missingIndices) == 0 {
								logger.Info(fmt.Sprintf("Valid snapshot already exists with all indices repo=%s snapshot=%s", repo, g.SnapshotName))
								continue
							}
							randomSuffix := utils.GenerateRandomAlphanumericString(6)
							parts := strings.Split(g.SnapshotName, "-")
							if len(parts) > 0 {
								datePart := parts[len(parts)-1]
								baseName := strings.Join(parts[:len(parts)-1], "-")
								newSnapshotName := baseName + "-" + randomSuffix + "-" + datePart
								logger.Info(fmt.Sprintf("Some indices missing in existing snapshot, creating additional snapshot repo=%s original=%s new=%s missingIndicesCount=%d", repo, g.SnapshotName, newSnapshotName, len(missingIndices)))
								indicesStr := strings.Join(missingIndices, ",")
								var totalSize int64
								for _, idx := range missingIndices {
									totalSize += indexSizes[idx]
								}
								repoSnapshotTasks = append(repoSnapshotTasks, utils.SnapshotTask{
									SnapshotName: newSnapshotName,
									IndicesStr:   indicesStr,
									Repo:         repo,
									Namespace:    cfg.GetKubeNamespace(),
									DateStr:      today,
									PollInterval: 60 * time.Second,
									Size:         totalSize,
								})
							}
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
					var totalSize int64
					for _, idx := range g.Indices {
						totalSize += indexSizes[idx]
					}
					repoSnapshotTasks = append(repoSnapshotTasks, utils.SnapshotTask{
						SnapshotName: g.SnapshotName,
						IndicesStr:   indicesStr,
						Repo:         repo,
						Namespace:    cfg.GetKubeNamespace(),
						DateStr:      today,
						PollInterval: 60 * time.Second,
						Size:         totalSize,
					})
				}
			}
			if len(repoSnapshotTasks) > 0 {
				successful, failed := utils.CreateSnapshotsInParallel(client, repoSnapshotTasks, cfg.GetMaxConcurrentSnapshots(), madisonClient, logger, true)
				successfulSnapshots = append(successfulSnapshots, successful...)
				failedSnapshots = append(failedSnapshots, failed...)
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
