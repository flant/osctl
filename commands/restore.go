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

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore indices from today's snapshots",
	Long: `Restore indices from all of today's snapshots in the configured repository.
Snapshots are processed largest-first, in parallel (max_concurrent_snapshots workers).
Indices are taken from the snapshots themselves; already-existing indices are skipped.
Each restore is awaited until its indices are healthy.`,
	RunE: runRestore,
}

func init() {
	addFlags(restoreCmd)
}

func runRestore(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()

	repo := cfg.GetSnapshotRepo()
	if repo == "" {
		return fmt.Errorf("snap-repo is required for restore")
	}
	maxConcurrent := cfg.GetMaxConcurrentSnapshots()
	dateFormat := cfg.GetDateFormat()
	today := utils.FormatDate(time.Now(), dateFormat)

	logger.Info(fmt.Sprintf("Starting restore from today's snapshots repo=%s today=%s maxConcurrent=%d dryRun=%t", repo, today, maxConcurrent, cfg.GetDryRun()))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	pattern := "*" + today + "*"
	logger.Info(fmt.Sprintf("Listing today's snapshots via filter pattern=%s", pattern))
	snapshots, err := client.GetSnapshotsDetailed(repo, pattern)
	if err != nil {
		if strings.Contains(err.Error(), "snapshot_missing_exception") || strings.Contains(err.Error(), "404") {
			logger.Info("No snapshots found for today")
			return nil
		}
		return fmt.Errorf("failed to list snapshots: %v", err)
	}
	if len(snapshots) == 0 {
		logger.Info("No snapshots found for today")
		return nil
	}

	var tasks []utils.RestoreTask
	var notReady []string
	for _, s := range snapshots {
		if s.State != "SUCCESS" {
			logger.Warn(fmt.Sprintf("Snapshot not SUCCESS, skipping restore snapshot=%s state=%s", s.Snapshot, s.State))
			notReady = append(notReady, fmt.Sprintf("%s(%s)", s.Snapshot, s.State))
			continue
		}
		size, err := utils.GetSnapshotSize(client, repo, s.Snapshot)
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to get snapshot size, using 0 for ordering snapshot=%s error=%v", s.Snapshot, err))
		}
		tasks = append(tasks, utils.RestoreTask{
			SnapshotName: s.Snapshot,
			Indices:      s.Indices,
			Repo:         repo,
			Size:         size,
			PollInterval: 30 * time.Second,
		})
	}

	if cfg.GetDryRun() {
		sorted := utils.SortRestoreTasksBySizeDesc(tasks)
		logger.Info("DRY RUN: Restore plan (largest first)")
		logger.Info("=" + strings.Repeat("=", 50))
		for i, t := range sorted {
			logger.Info(fmt.Sprintf("Restore %d: snapshot=%s indicesCount=%d indices=%s", i+1, t.SnapshotName, len(t.Indices), strings.Join(t.Indices, ",")))
		}
		if len(notReady) > 0 {
			logger.Warn(fmt.Sprintf("Not-ready today snapshots (would be skipped): %s", strings.Join(notReady, ", ")))
		}
		logger.Info(fmt.Sprintf("DRY RUN: Would restore %d snapshots with %d parallel workers", len(sorted), maxConcurrent))
		return nil
	}

	if len(tasks) == 0 {
		logger.Info("No SUCCESS snapshots to restore")
		if len(notReady) > 0 {
			return fmt.Errorf("no restorable snapshots; not-ready today snapshots: %s", strings.Join(notReady, ", "))
		}
		return nil
	}

	successful, failed := utils.RestoreSnapshotsInParallel(client, tasks, maxConcurrent, logger)

	logger.Info(strings.Repeat("=", 60))
	logger.Info("RESTORE SUMMARY")
	logger.Info(strings.Repeat("=", 60))
	if len(successful) > 0 {
		logger.Info(fmt.Sprintf("Successfully restored: %d snapshots", len(successful)))
		for _, name := range successful {
			logger.Info(fmt.Sprintf("  ✓ %s", name))
		}
	}
	if len(failed) > 0 {
		logger.Info("")
		logger.Info(fmt.Sprintf("Failed to restore: %d snapshots", len(failed)))
		for _, name := range failed {
			logger.Info(fmt.Sprintf("  ✗ %s", name))
		}
	}
	if len(notReady) > 0 {
		logger.Info("")
		logger.Info(fmt.Sprintf("Skipped (not SUCCESS): %d snapshots", len(notReady)))
		for _, name := range notReady {
			logger.Info(fmt.Sprintf("  - %s", name))
		}
	}
	logger.Info(strings.Repeat("=", 60))

	if len(failed) > 0 || len(notReady) > 0 {
		return fmt.Errorf("restore finished with problems: failed=%d notReady=%d", len(failed), len(notReady))
	}

	logger.Info("Restore completed successfully")
	return nil
}
