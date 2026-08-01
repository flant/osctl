package commands

import (
	"fmt"
	"math/rand"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type snapshotFullPrefixPlan struct {
	cfg     config.IndexConfig
	repo    string
	snap    string
	indices []string
	sizes   map[string]int64
}

func fullPrefixRepo(defaultRepo string, ic config.IndexConfig) string {
	if ic.Repository != "" {
		return ic.Repository
	}
	return defaultRepo
}

func fullPrefixListPattern(ic config.IndexConfig) string {
	base := ic.Value
	if ic.Kind == "regex" {
		base = ic.Name
	}
	return base + "-*"
}

func missingSnapshotIndices(snapshotName string, wantIndices []string, snapshots []opensearch.Snapshot) []string {
	present := map[string]bool{}
	for _, s := range snapshots {
		if s.Snapshot != snapshotName {
			continue
		}
		for _, idx := range s.Indices {
			present[idx] = true
		}
	}
	var missing []string
	for _, idx := range wantIndices {
		if !present[idx] {
			missing = append(missing, idx)
		}
	}
	return missing
}

func suffixedSnapshotName(snapshotName string) string {
	suffix := utils.GenerateRandomAlphanumericString(6)
	parts := strings.Split(snapshotName, "-")
	if len(parts) < 2 {
		return snapshotName + "-" + suffix
	}
	datePart := parts[len(parts)-1]
	baseName := strings.Join(parts[:len(parts)-1], "-")
	return baseName + "-" + suffix + "-" + datePart
}

func runSnapshotFullPrefixCreate(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()
	defaultRepo := cfg.GetSnapshotRepo()
	today := utils.FormatDate(time.Now(), cfg.GetDateFormat())

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	var madisonClient *alerts.Client
	if cfg.GetMadisonKey() != "" && cfg.GetOSDURL() != "" && cfg.GetMadisonURL() != "" {
		madisonClient = alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	}

	logger.Info(fmt.Sprintf("Starting full-prefix snapshot creation date=%s prefixesConfigured=%d es5Compatibility=%t", today, len(indicesConfig), cfg.GetES5Compatibility()))

	var plan []snapshotFullPrefixPlan
	for _, ic := range indicesConfig {
		if !ic.Snapshot || ic.ManualSnapshot {
			continue
		}

		indices, sizes, err := utils.ResolveOpenIndicesForPrefix(client, ic)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to resolve indices for prefix value=%s error=%v", ic.Value, err))
			continue
		}
		if len(indices) == 0 {
			logger.Info(fmt.Sprintf("No open indices for prefix, skipping value=%s", ic.Value))
			continue
		}

		repo := fullPrefixRepo(defaultRepo, ic)
		snap := utils.BuildSnapshotNameFromConfig(ic, today)
		plan = append(plan, snapshotFullPrefixPlan{cfg: ic, repo: repo, snap: snap, indices: indices, sizes: sizes})
		logger.Info(fmt.Sprintf("Prefix planned value=%s snapshot=%s repo=%s openIndices=%d", ic.Value, snap, repo, len(indices)))
	}

	if cfg.GetDryRun() {
		logger.Info("DRY RUN: full-prefix snapshot plan")
		logger.Info("=" + strings.Repeat("=", 50))
		for _, p := range plan {
			logger.Info("")
			logger.Info(fmt.Sprintf("Snapshot: %s (repo %s)", p.snap, p.repo))
			logger.Info(fmt.Sprintf("Prefix: %s (%s)", p.cfg.Value, p.cfg.Kind))
			logger.Info(fmt.Sprintf("Indices (%d):", len(p.indices)))
			for _, index := range p.indices {
				logger.Info(fmt.Sprintf("  %s", index))
			}
			logger.Info("=" + strings.Repeat("=", 30))
		}
		logger.Info("")
		logger.Info(fmt.Sprintf("DRY RUN: Would create %d snapshots", len(plan)))
		return nil
	}

	randomWaitSeconds := rand.Intn(291) + 10
	logger.Info(fmt.Sprintf("Waiting %d seconds before starting snapshot creation to distribute load", randomWaitSeconds))
	time.Sleep(time.Duration(randomWaitSeconds) * time.Second)

	tasksByRepo := map[string][]utils.SnapshotTask{}
	for _, p := range plan {
		snapName := p.snap
		snapIndices := p.indices

		existing, err := utils.GetSnapshotsIgnore404(client, p.repo, p.snap)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to check snapshot repo=%s snapshot=%s error=%v", p.repo, p.snap, err))
			continue
		}
		if existing == nil {
			existing = []opensearch.Snapshot{}
		}

		if state, ok := utils.GetSnapshotStateByName(p.snap, existing); ok {
			if state == "IN_PROGRESS" {
				logger.Info(fmt.Sprintf("Snapshot is currently IN_PROGRESS, skipping snapshot=%s repo=%s", p.snap, p.repo))
				continue
			}
			if state == "SUCCESS" {
				missing := missingSnapshotIndices(p.snap, p.indices, existing)
				if len(missing) == 0 {
					logger.Info(fmt.Sprintf("Snapshot already exists with all today's indices, skipping snapshot=%s repo=%s indices=%d", p.snap, p.repo, len(p.indices)))
					continue
				}
				newName := suffixedSnapshotName(p.snap)
				logger.Info(fmt.Sprintf("Snapshot exists but is missing indices, creating additional snapshot original=%s new=%s missing=%d", p.snap, newName, len(missing)))
				snapName = newName
				snapIndices = missing
			} else {
				exists, err := utils.CheckAndCleanSnapshot(p.snap, strings.Join(p.indices, ","), existing, client, p.repo, logger)
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to check/clean snapshot snapshot=%s error=%v", p.snap, err))
					continue
				}
				if exists {
					logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", p.snap))
					continue
				}
			}
		}

		var totalSize int64
		for _, idx := range snapIndices {
			totalSize += p.sizes[idx]
		}
		tasksByRepo[p.repo] = append(tasksByRepo[p.repo], utils.SnapshotTask{
			SnapshotName: snapName,
			IndicesStr:   strings.Join(snapIndices, ","),
			Repo:         p.repo,
			Namespace:    cfg.GetKubeNamespace(),
			DateStr:      today,
			PollInterval: 60 * time.Second,
			Size:         totalSize,
		})
	}

	var successfulSnapshots []string
	var failedSnapshots []string
	for repo, tasks := range tasksByRepo {
		if len(tasks) == 0 {
			continue
		}
		logger.Info(fmt.Sprintf("Creating snapshots repo=%s count=%d", repo, len(tasks)))
		successful, failed := utils.CreateSnapshotsInParallel(client, tasks, cfg.GetMaxConcurrentSnapshots(), madisonClient, logger, true)
		successfulSnapshots = append(successfulSnapshots, successful...)
		failedSnapshots = append(failedSnapshots, failed...)
	}

	logger.Info(strings.Repeat("=", 60))
	logger.Info("FULL-PREFIX SNAPSHOT CREATION SUMMARY")
	logger.Info(strings.Repeat("=", 60))
	if len(successfulSnapshots) > 0 {
		logger.Info(fmt.Sprintf("Successfully created: %d snapshots", len(successfulSnapshots)))
		for _, name := range successfulSnapshots {
			logger.Info(fmt.Sprintf("  ✓ %s", name))
		}
	}
	if len(failedSnapshots) > 0 {
		logger.Info(fmt.Sprintf("Failed to create: %d snapshots", len(failedSnapshots)))
		for _, name := range failedSnapshots {
			logger.Info(fmt.Sprintf("  ✗ %s", name))
		}
	}
	if len(successfulSnapshots) == 0 && len(failedSnapshots) == 0 {
		logger.Info("No snapshots were created")
	}
	logger.Info(strings.Repeat("=", 60))
	logger.Info("Full-prefix snapshot creation completed")
	return nil
}

func runSnapshotsDeleteFullPrefix(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()
	defaultRepo := cfg.GetSnapshotRepo()
	s3Config := cfg.GetOsctlIndicesS3SnapshotsConfig()

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Starting full-prefix snapshot deletion (day-based retention) prefixesConfigured=%d defaultDays=%d", len(indicesConfig), s3Config.UnitCount.All))

	var successfulDeletions []string
	var failedDeletions []string

	for _, ic := range indicesConfig {
		if !ic.Snapshot {
			continue
		}

		repo := fullPrefixRepo(defaultRepo, ic)
		days := ic.SnapshotCountS3
		if days < 1 {
			days = s3Config.UnitCount.All
		}
		if days < 1 {
			logger.Warn(fmt.Sprintf("No retention days configured for prefix, skipping deletion value=%s", ic.Value))
			continue
		}
		cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), cfg.GetDateFormat())

		snaps, err := utils.GetSnapshotsIgnore404(client, repo, fullPrefixListPattern(ic))
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to list snapshots for prefix value=%s repo=%s error=%v", ic.Value, repo, err))
			continue
		}

		var toDelete []string
		for _, s := range snaps {
			if !utils.MatchesSnapshot(s.Snapshot, ic) {
				continue
			}
			if s.State == "IN_PROGRESS" {
				continue
			}
			if !utils.HasDateInName(s.Snapshot, cfg.GetDateFormat()) {
				continue
			}
			if utils.IsOlderThanCutoff(s.Snapshot, cutoffDate, cfg.GetDateFormat()) {
				toDelete = append(toDelete, s.Snapshot)
			}
		}

		if len(toDelete) == 0 {
			logger.Info(fmt.Sprintf("Nothing to delete for prefix value=%s repo=%s days=%d cutoff=%s", ic.Value, repo, days, cutoffDate))
			continue
		}

		logger.Info(fmt.Sprintf("Prefix retention value=%s repo=%s days=%d cutoff=%s delete=%d: %s", ic.Value, repo, days, cutoffDate, len(toDelete), strings.Join(toDelete, ", ")))

		if cfg.GetDryRun() {
			logger.Info(fmt.Sprintf("DRY RUN: would delete %d snapshots for prefix value=%s", len(toDelete), ic.Value))
			continue
		}

		successful, failed, _ := utils.BatchDeleteSnapshots(client, toDelete, repo, cfg.GetDryRun(), logger)
		for _, name := range successful {
			successfulDeletions = append(successfulDeletions, fmt.Sprintf("%s (repo=%s)", name, repo))
		}
		for _, name := range failed {
			failedDeletions = append(failedDeletions, fmt.Sprintf("%s (repo=%s)", name, repo))
		}
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("FULL-PREFIX SNAPSHOT DELETION SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulDeletions) > 0 {
			logger.Info(fmt.Sprintf("Successfully deleted: %d snapshots", len(successfulDeletions)))
			for _, name := range successfulDeletions {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		}
		if len(failedDeletions) > 0 {
			logger.Info(fmt.Sprintf("Failed to delete: %d snapshots", len(failedDeletions)))
			for _, name := range failedDeletions {
				logger.Info(fmt.Sprintf("  ✗ %s", name))
			}
		}
		if len(successfulDeletions) == 0 && len(failedDeletions) == 0 {
			logger.Info("No snapshots were deleted")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	logger.Info("Full-prefix snapshot deletion completed")
	return nil
}

const fullPrefixStaleMaxDays = 2

func runSnapshotsCheckerFullPrefix(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()
	defaultRepo := cfg.GetSnapshotRepo()
	today := utils.FormatDate(time.Now(), cfg.GetDateFormat())

	indicesConfig, err := cfg.GetOsctlIndices()
	if err != nil {
		return fmt.Errorf("failed to get osctl indices: %v", err)
	}

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	logger.Info(fmt.Sprintf("Starting full-prefix snapshot checking prefixesConfigured=%d maxAgeDays=%d", len(indicesConfig), fullPrefixStaleMaxDays))

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -fullPrefixStaleMaxDays), cfg.GetDateFormat())

	var missing []string
	for _, ic := range indicesConfig {
		if !ic.Snapshot || ic.ManualSnapshot {
			continue
		}

		repo := fullPrefixRepo(defaultRepo, ic)

		openIndices, _, err := utils.ResolveOpenIndicesForPrefix(client, ic)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to resolve indices for prefix value=%s error=%v", ic.Value, err))
			missing = append(missing, ic.Value)
			continue
		}

		snaps, err := utils.GetSnapshotsIgnore404(client, repo, fullPrefixListPattern(ic))
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to list snapshots for prefix value=%s repo=%s error=%v", ic.Value, repo, err))
			missing = append(missing, ic.Value)
			continue
		}

		covered := map[string]bool{}
		recentSnapshots := 0
		for _, s := range snaps {
			if s.State != "SUCCESS" || !utils.MatchesSnapshot(s.Snapshot, ic) {
				continue
			}
			if !utils.HasDateInName(s.Snapshot, cfg.GetDateFormat()) {
				continue
			}
			if utils.IsOlderThanCutoff(s.Snapshot, cutoffDate, cfg.GetDateFormat()) {
				continue
			}
			recentSnapshots++
			for _, idx := range s.Indices {
				covered[idx] = true
			}
		}

		if recentSnapshots == 0 {
			logger.Warn(fmt.Sprintf("No recent successful snapshot for prefix value=%s repo=%s cutoff=%s", ic.Value, repo, cutoffDate))
			missing = append(missing, ic.Value)
			continue
		}

		var missingIndices []string
		for _, idx := range openIndices {
			if !covered[idx] {
				missingIndices = append(missingIndices, idx)
			}
		}
		if len(missingIndices) > 0 {
			logger.Warn(fmt.Sprintf("Recent snapshot(s) do not cover all open indices prefix=%s repo=%s missing=%d: %s", ic.Value, repo, len(missingIndices), strings.Join(missingIndices, ", ")))
			missing = append(missing, missingIndices...)
			continue
		}

		logger.Info(fmt.Sprintf("Prefix OK value=%s repo=%s recentSnapshots=%d openIndices=%d", ic.Value, repo, recentSnapshots, len(openIndices)))
	}

	if len(missing) == 0 {
		logger.Info("All prefixes have a recent successful snapshot")
		logger.Info("Full-prefix snapshot checking completed")
		return nil
	}

	logger.Warn(fmt.Sprintf("Prefixes without a recent snapshot count=%d list=%s", len(missing), strings.Join(missing, ", ")))

	if cfg.GetDryRun() {
		logger.Info("DRY RUN: Would send Madison alert for missing snapshots")
		logger.Info("Full-prefix snapshot checking completed")
		return nil
	}

	madisonClient := alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	response, err := madisonClient.SendMadisonSnapshotMissingAlert(missing, defaultRepo, cfg.GetKubeNamespace(), today)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
		return fmt.Errorf("failed to send Madison alert: %v", err)
	}
	logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotMissing response=%s", response))

	logger.Info("Full-prefix snapshot checking completed")
	return nil
}
