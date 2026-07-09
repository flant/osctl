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

const restorePendingPollInterval = 60 * time.Second

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore indices from today's snapshots",
	Long: `Restore indices from today's snapshots in the configured repository.
Snapshots are processed largest-first, in parallel (max_concurrent_snapshots workers).
Only indices matching --index-filter are restored (the rest of a snapshot is ignored).
SUCCESS snapshots are restored immediately; IN_PROGRESS ones are waited for and restored
once they become SUCCESS; FAILED ones and failed restores raise Madison alerts but do not
abort the job.`,
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
	filter := cfg.GetRestoreIndexFilter()
	namespace := cfg.GetKubeNamespace()

	logger.Info(fmt.Sprintf("Starting restore from today's snapshots repo=%s today=%s maxConcurrent=%d filters=%d dryRun=%t", repo, today, maxConcurrent, len(filter), cfg.GetDryRun()))
	if len(filter) > 0 {
		logger.Info("Index filter patterns: " + strings.Join(filter, ", "))
	} else {
		logger.Info("Index filter patterns: none (all indices in the snapshots will be restored)")
	}

	var madisonClient *alerts.Client
	if cfg.GetMadisonKey() != "" && cfg.GetOSDURL() != "" && cfg.GetMadisonURL() != "" {
		madisonClient = alerts.NewMadisonClient(cfg.GetMadisonKey(), cfg.GetOSDURL(), cfg.GetMadisonURL())
	} else {
		logger.Warn("Madison is not fully configured (madison-key/osd-url/madison-url) — alerts will be skipped")
	}

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

	problems := false
	var readyTasks []utils.RestoreTask
	var pending []string

	for _, s := range snapshots {
		matched := utils.FilterIndices(s.Indices, filter)
		if len(matched) == 0 {
			logger.Info(fmt.Sprintf("Snapshot has no indices matching filter, skipping snapshot=%s state=%s indicesInSnapshot=%d", s.Snapshot, s.State, len(s.Indices)))
			continue
		}

		switch s.State {
		case "SUCCESS":
			readyTasks = append(readyTasks, buildRestoreTask(client, repo, s.Snapshot, matched, logger))
		case "IN_PROGRESS", "STARTED":
			logger.Info(fmt.Sprintf("Snapshot IN_PROGRESS, deferred to end of queue snapshot=%s matchedIndices=%d", s.Snapshot, len(matched)))
			pending = append(pending, s.Snapshot)
		default:
			problems = true
			logger.Error(fmt.Sprintf("Snapshot in non-restorable state, alerting snapshot=%s state=%s", s.Snapshot, s.State))
			sendSnapshotStateFailed(madisonClient, s.Snapshot, s.State, repo, namespace, today, logger)
		}
	}

	if cfg.GetDryRun() {
		sorted := utils.SortRestoreTasksBySizeDesc(readyTasks)
		logger.Info("DRY RUN: Restore plan (largest first)")
		logger.Info("=" + strings.Repeat("=", 50))
		for i, t := range sorted {
			logger.Info(fmt.Sprintf("Restore %d: snapshot=%s matchedIndices=%d indices=%s", i+1, t.SnapshotName, len(t.Indices), strings.Join(t.Indices, ",")))
		}
		if len(pending) > 0 {
			logger.Info(fmt.Sprintf("Would wait for IN_PROGRESS snapshots: %s", strings.Join(pending, ", ")))
		}
		logger.Info(fmt.Sprintf("DRY RUN: Would restore %d ready snapshots with %d parallel workers (+%d pending)", len(sorted), maxConcurrent, len(pending)))
		return nil
	}

	var successful, failed []string

	if len(readyTasks) > 0 {
		succ, fail := utils.RestoreSnapshotsInParallel(client, readyTasks, maxConcurrent, madisonClient, namespace, today, logger)
		successful = append(successful, succ...)
		failed = append(failed, fail...)
	} else {
		logger.Info("No SUCCESS snapshots ready to restore in the first pass")
	}

	// IN_PROGRESS snapshots: wait, then restore as they turn SUCCESS. Loop until none pending.
	for len(pending) > 0 {
		logger.Info(fmt.Sprintf("Waiting for %d IN_PROGRESS snapshots before rechecking: %s", len(pending), strings.Join(pending, ", ")))
		time.Sleep(restorePendingPollInterval)

		var stillPending []string
		var nowReady []utils.RestoreTask
		for _, name := range pending {
			snaps, err := client.GetSnapshotsDetailed(repo, name)
			if err != nil {
				logger.Warn(fmt.Sprintf("Failed to recheck pending snapshot, keeping in queue snapshot=%s error=%v", name, err))
				stillPending = append(stillPending, name)
				continue
			}
			if len(snaps) == 0 {
				logger.Warn(fmt.Sprintf("Pending snapshot disappeared from repo snapshot=%s", name))
				problems = true
				continue
			}
			s := snaps[0]
			switch s.State {
			case "SUCCESS":
				matched := utils.FilterIndices(s.Indices, filter)
				if len(matched) == 0 {
					logger.Info(fmt.Sprintf("Pending snapshot became SUCCESS but no matching indices snapshot=%s", name))
					continue
				}
				logger.Info(fmt.Sprintf("Pending snapshot became SUCCESS, will restore snapshot=%s matchedIndices=%d", name, len(matched)))
				nowReady = append(nowReady, buildRestoreTask(client, repo, name, matched, logger))
			case "IN_PROGRESS", "STARTED":
				stillPending = append(stillPending, name)
			default:
				problems = true
				logger.Error(fmt.Sprintf("Pending snapshot ended in non-restorable state, alerting snapshot=%s state=%s", name, s.State))
				sendSnapshotStateFailed(madisonClient, name, s.State, repo, namespace, today, logger)
			}
		}
		pending = stillPending

		if len(nowReady) > 0 {
			succ, fail := utils.RestoreSnapshotsInParallel(client, nowReady, maxConcurrent, madisonClient, namespace, today, logger)
			successful = append(successful, succ...)
			failed = append(failed, fail...)
		}
	}

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
		logger.Info(fmt.Sprintf("Restored with errors: %d snapshots", len(failed)))
		for _, name := range failed {
			logger.Info(fmt.Sprintf("  ✗ %s", name))
		}
	}
	logger.Info(strings.Repeat("=", 60))

	if len(failed) > 0 || problems {
		return fmt.Errorf("restore finished with problems: failedSnapshots=%d otherProblems=%t", len(failed), problems)
	}

	logger.Info("Restore completed successfully")
	return nil
}

func buildRestoreTask(client *opensearch.Client, repo, snapshot string, indices []string, logger *logging.Logger) utils.RestoreTask {
	size, err := utils.GetSnapshotSize(client, repo, snapshot)
	if err != nil {
		logger.Warn(fmt.Sprintf("Failed to get snapshot size, using 0 for ordering snapshot=%s error=%v", snapshot, err))
	}
	return utils.RestoreTask{
		SnapshotName: snapshot,
		Indices:      indices,
		Repo:         repo,
		Size:         size,
		PollInterval: 30 * time.Second,
	}
}

func sendSnapshotStateFailed(madisonClient *alerts.Client, snapshot, state, repo, namespace, dateStr string, logger *logging.Logger) {
	if madisonClient == nil {
		return
	}
	if _, err := madisonClient.SendMadisonSnapshotStateFailedAlert(snapshot, state, repo, namespace, dateStr); err != nil {
		logger.Error(fmt.Sprintf("Failed to send Madison snapshot-state alert snapshot=%s error=%v", snapshot, err))
	} else {
		logger.Info(fmt.Sprintf("Madison alert sent: type=SnapshotStateFailed snapshot=%s state=%s", snapshot, state))
	}
}
