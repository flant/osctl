package commands

import (
	"fmt"
	"math/rand"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotsBackfillCmd = &cobra.Command{
	Use:   "snapshotsbackfill",
	Short: "Backfill missing snapshots for indices",
	Long:  `Backfill missing snapshots for indices. Supports two modes: with --indices-list for specific indices, or without for all historical indices.`,
	RunE:  runSnapshotsBackfill,
}

func init() {
	addFlags(snapshotsBackfillCmd)
}

func runSnapshotsBackfill(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()
	defaultRepo := cfg.GetSnapshotRepo()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	unknownConfig := cfg.GetOsctlIndicesUnknownConfig()
	s3Config := cfg.GetOsctlIndicesS3SnapshotsConfig()

	logger.Info(fmt.Sprintf("Starting snapshots backfill indicesCountConfig=%d unknownSnapshot=%t", len(indicesConfig), unknownConfig.Snapshot))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	var madisonClient *alerts.Client
	if cfg.GetMadisonKey() != "" && cfg.GetOSDURL() != "" && cfg.GetMadisonURL() != "" {
		madisonClient = alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	}

	indicesListFlag := cfg.GetSnapshotsBackfillIndicesList()

	var indicesToProcess []string

	if indicesListFlag != "" {
		indicesToProcess = strings.Split(indicesListFlag, ",")
		for i := range indicesToProcess {
			indicesToProcess[i] = strings.TrimSpace(indicesToProcess[i])
		}
		logger.Info(fmt.Sprintf("Processing indices from --indices-list count=%d", len(indicesToProcess)))
	} else {
		today := utils.FormatDate(time.Now(), cfg.GetDateFormat())
		yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), cfg.GetDateFormat())
		dayBeforeYesterday := utils.FormatDate(time.Now().AddDate(0, 0, -2), cfg.GetDateFormat())

		logger.Info(fmt.Sprintf("Getting all indices excluding today and yesterday today=%s yesterday=%s", today, yesterday))

		allIndices, err := client.GetIndicesWithFields("*", "index")
		if err != nil {
			return fmt.Errorf("failed to get all indices: %v", err)
		}

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

			if extractedDate == dayBeforeYesterday || utils.IsOlderThanCutoff(indexName, dayBeforeYesterday, cfg.GetDateFormat()) {
				indicesToProcess = append(indicesToProcess, indexName)
			}
		}

		logger.Info(fmt.Sprintf("Found indices to process count=%d", len(indicesToProcess)))
		if len(indicesToProcess) > 0 {
			logger.Info(fmt.Sprintf("Indices to process %s", strings.Join(indicesToProcess, ", ")))
		}
	}

	if len(indicesToProcess) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	logger.Info(fmt.Sprintf("Getting all snapshots from repository repo=%s", defaultRepo))
	allSnapshots, err := client.GetSnapshots(defaultRepo, "*")
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
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

	var indicesWithoutSnapshots []string
	var indicesWithoutDate []string

	for _, indexName := range indicesToProcess {
		hasDate := utils.HasDateInName(indexName, cfg.GetDateFormat())
		if !hasDate {
			indicesWithoutDate = append(indicesWithoutDate, indexName)
			logger.Warn(fmt.Sprintf("Skipping index without date in name index=%s", indexName))
			continue
		}

		if !utils.HasValidSnapshot(indexName, allSnapshots) {
			indicesWithoutSnapshots = append(indicesWithoutSnapshots, indexName)
		}
	}

	if len(indicesWithoutDate) > 0 {
		logger.Info(fmt.Sprintf("Skipped indices without date count=%d list=%s", len(indicesWithoutDate), strings.Join(indicesWithoutDate, ", ")))
	}

	if len(indicesWithoutSnapshots) == 0 {
		logger.Info("All indices already have snapshots")
		return nil
	}

	logger.Info(fmt.Sprintf("Indices without snapshots count=%d", len(indicesWithoutSnapshots)))
	if len(indicesWithoutSnapshots) > 0 {
		logger.Info(fmt.Sprintf("Indices without snapshots list=%s", strings.Join(indicesWithoutSnapshots, ", ")))
	}

	dateGroups := utils.GroupIndicesByDate(indicesWithoutSnapshots, cfg.GetDateFormat())
	dateKeys := make([]string, 0, len(dateGroups))
	for k := range dateGroups {
		dateKeys = append(dateKeys, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dateKeys)))

	logger.Info(fmt.Sprintf("Grouped indices by date groups=%d", len(dateGroups)))
	for _, dateKey := range dateKeys {
		indicesForDate := dateGroups[dateKey]
		logger.Info(fmt.Sprintf("Date group date=%s indicesCount=%d indices=%s", dateKey, len(indicesForDate), strings.Join(indicesForDate, ", ")))
	}

	var totalSnapshotsToCreate int
	var allSnapshotsToCreate []utils.SnapshotGroup
	var successfulSnapshots []string
	var failedSnapshots []string

	for _, dateKey := range dateKeys {
		indicesForDate := dateGroups[dateKey]
		logger.Info(fmt.Sprintf("Processing date group date=%s indicesCount=%d", dateKey, len(indicesForDate)))

		goFormat := utils.ConvertDateFormat(cfg.GetDateFormat())
		parsedDate, err := time.Parse(goFormat, dateKey)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to parse date date=%s error=%v", dateKey, err))
			continue
		}
		snapshotDate := utils.FormatDate(parsedDate.AddDate(0, 0, 1), cfg.GetDateFormat())

		var indicesToSnapshot []string
		repoGroups := map[string]utils.SnapshotGroup{}
		var unknownIndices []string

		for _, indexName := range indicesForDate {
			indexConfig := utils.FindMatchingIndexConfig(indexName, indicesConfig)
			if indexConfig != nil {
				if !indexConfig.Snapshot || indexConfig.ManualSnapshot {
					continue
				}

				cutoffDateDaysCount := utils.FormatDate(time.Now().AddDate(0, 0, -indexConfig.DaysCount), cfg.GetDateFormat())
				cutoffDateS3 := ""
				if indexConfig.SnapshotCountS3 > 0 {
					cutoffDateS3 = utils.FormatDate(time.Now().AddDate(0, 0, -indexConfig.SnapshotCountS3), cfg.GetDateFormat())
				} else {
					s3All := s3Config.UnitCount.All
					if s3All > 0 {
						cutoffDateS3 = utils.FormatDate(time.Now().AddDate(0, 0, -s3All), cfg.GetDateFormat())
					}
				}

				cutoffDate := utils.GetLaterCutoffDate(cutoffDateDaysCount, cutoffDateS3, cfg.GetDateFormat())

				if utils.IsOlderThanCutoff(indexName, cutoffDate, cfg.GetDateFormat()) {
					logger.Info(fmt.Sprintf("Skipping index older than cutoff index=%s cutoff=%s", indexName, cutoffDate))
					continue
				}

				utils.AddIndexToSnapshotGroups(indexName, *indexConfig, snapshotDate, repoGroups, &indicesToSnapshot)
			} else {
				unknownIndices = append(unknownIndices, indexName)
			}
		}

		unknownIndices = utils.FilterUnknownIndices(unknownIndices)

		if unknownConfig.Snapshot && !unknownConfig.ManualSnapshot && len(unknownIndices) > 0 {
			cutoffDateDaysCount := utils.FormatDate(time.Now().AddDate(0, 0, -unknownConfig.DaysCount), cfg.GetDateFormat())
			cutoffDateS3 := ""
			s3Unknown := s3Config.UnitCount.Unknown
			if s3Unknown > 0 {
				cutoffDateS3 = utils.FormatDate(time.Now().AddDate(0, 0, -s3Unknown), cfg.GetDateFormat())
			}

			cutoffDate := utils.GetLaterCutoffDate(cutoffDateDaysCount, cutoffDateS3, cfg.GetDateFormat())

			filteredUnknown := make([]string, 0)
			for _, idx := range unknownIndices {
				if !utils.IsOlderThanCutoff(idx, cutoffDate, cfg.GetDateFormat()) {
					filteredUnknown = append(filteredUnknown, idx)
				} else {
					logger.Info(fmt.Sprintf("Skipping unknown index older than cutoff index=%s cutoff=%s", idx, cutoffDate))
				}
			}
			unknownIndices = filteredUnknown
		}

		snapshotGroups := utils.GroupIndicesForSnapshots(indicesToSnapshot, indicesConfig, snapshotDate)

		if unknownConfig.Snapshot && !unknownConfig.ManualSnapshot && len(unknownIndices) > 0 {
			randomSuffix := utils.GenerateRandomAlphanumericString(6)
			snapshotGroups = append(snapshotGroups, utils.SnapshotGroup{
				SnapshotName: "unknown-" + randomSuffix + "-" + snapshotDate,
				Indices:      unknownIndices,
				Pattern:      "unknown",
				Kind:         "unknown",
			})
		}

		if len(snapshotGroups) == 0 && len(repoGroups) == 0 {
			logger.Info(fmt.Sprintf("No snapshots to create for date date=%s", dateKey))
			continue
		}

		if cfg.GetDryRun() {
			existingMain, err := client.GetSnapshots(defaultRepo, "*"+snapshotDate+"*")
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
				existing, err := client.GetSnapshots(repo, "*"+snapshotDate+"*")
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
			fmt.Printf("Index date: %s, Snapshot date: %s\n", dateKey, snapshotDate)

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
			totalSnapshotsToCreate += total
			allSnapshotsToCreate = append(allSnapshotsToCreate, filteredMain...)
			for _, groups := range filteredPerRepo {
				allSnapshotsToCreate = append(allSnapshotsToCreate, groups...)
			}
			fmt.Printf("\nDRY RUN: Would create %d snapshots for index date %s (snapshot date %s)\n", total, dateKey, snapshotDate)
			continue
		}

		if !cfg.GetDryRun() {
			if dateKey == dateKeys[0] {
				randomWaitSeconds := rand.Intn(291) + 10
				randomWaitDuration := time.Duration(randomWaitSeconds) * time.Second
				logger.Info(fmt.Sprintf("Waiting %d seconds before starting snapshot creation to distribute load", randomWaitSeconds))
				time.Sleep(randomWaitDuration)
			}

			allSnapshotsForDate, err := utils.GetSnapshotsIgnore404(client, defaultRepo, "*"+snapshotDate+"*")
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to get snapshots for date date=%s error=%v", dateKey, err))
				continue
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

				exists, err := utils.CheckAndCleanSnapshot(group.SnapshotName, strings.Join(group.Indices, ","), allSnapshotsForDate, client, defaultRepo, logger)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to check/clean snapshot snapshot=%s error=%v", group.SnapshotName, err))
					continue
				}

				if exists {
					logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", group.SnapshotName))
					continue
				}

				indicesStr := strings.Join(group.Indices, ",")
				logger.Info(fmt.Sprintf("Creating snapshot snapshot=%s", group.SnapshotName))
				logger.Info(fmt.Sprintf("Snapshot indices %s", indicesStr))
				err = utils.CreateSnapshotWithRetry(client, group.SnapshotName, indicesStr, defaultRepo, madisonClient, logger, 10*time.Minute)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to create snapshot after retries snapshot=%s error=%v", group.SnapshotName, err))
					failedSnapshots = append(failedSnapshots, group.SnapshotName)
					continue
				}
				successfulSnapshots = append(successfulSnapshots, group.SnapshotName)

				logger.Info("Waiting 10 minutes before next snapshot creation")
				time.Sleep(10 * time.Minute)
			}

			if len(repoGroups) > 0 {
				perRepo := map[string][]utils.SnapshotGroup{}
				for k, g := range repoGroups {
					parts := strings.SplitN(k, "|", 2)
					repo := parts[0]
					perRepo[repo] = append(perRepo[repo], g)
				}
				for repo, groups := range perRepo {
					existing, err := utils.GetSnapshotsIgnore404(client, repo, "*"+snapshotDate+"*")
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
						err = utils.CreateSnapshotWithRetry(client, g.SnapshotName, indicesStr, repo, madisonClient, logger, 10*time.Minute)
						if err != nil {
							logger.Error(fmt.Sprintf("Failed to create snapshot after retries repo=%s snapshot=%s error=%v", repo, g.SnapshotName, err))
							failedSnapshots = append(failedSnapshots, fmt.Sprintf("%s (repo=%s)", g.SnapshotName, repo))
							continue
						}
						successfulSnapshots = append(successfulSnapshots, fmt.Sprintf("%s (repo=%s)", g.SnapshotName, repo))

						logger.Info("Waiting 10 minutes before next snapshot creation")
						time.Sleep(10 * time.Minute)
					}
				}
			}
		}
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("SNAPSHOT BACKFILL SUMMARY")
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

	if cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("DRY RUN SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if totalSnapshotsToCreate == 0 {
			logger.Info("No snapshots would be created")
		} else {
			logger.Info(fmt.Sprintf("Would create %d snapshots total:", totalSnapshotsToCreate))
			logger.Info("")
			for i, group := range allSnapshotsToCreate {
				logger.Info(fmt.Sprintf("%d. Snapshot: %s", i+1, group.SnapshotName))
				logger.Info(fmt.Sprintf("   Pattern: %s (%s)", group.Pattern, group.Kind))
				logger.Info(fmt.Sprintf("   Indices (%d): %s", len(group.Indices), strings.Join(group.Indices, ", ")))
				logger.Info("")
			}
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info("Snapshots backfill completed")
	return nil
}
