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

	logger.Info(fmt.Sprintf("Starting restore repo=%s today=%s daysCount=%d date=%q maxConcurrent=%d filters=%d dryRun=%t", repo, today, cfg.GetRestoreDaysCount(), cfg.GetRestoreDate(), maxConcurrent, len(filter), cfg.GetDryRun()))
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

	problems := false

	activeIdx, aerr := client.ActiveSnapshotRecoveryIndices()
	if aerr != nil {
		logger.Warn(fmt.Sprintf("Preflight: failed to list active restores error=%v", aerr))
	}
	failedIdx, ferr := client.RestoreFailedPrimaryIndices()
	if ferr != nil {
		logger.Warn(fmt.Sprintf("Preflight: failed to list failed restores error=%v", ferr))
	}
	ourActive := utils.FilterIndices(activeIdx, filter)
	ourFailed := utils.FilterIndices(failedIdx, filter)
	foreign := foreignRestores(activeIdx, failedIdx, filter)
	logger.Info(fmt.Sprintf("Preflight restores: ourActive=%d ourFailed=%d foreign=%d", len(ourActive), len(ourFailed), len(foreign)))
	if len(ourActive) > 0 {
		logger.Info("Our restores in progress: " + strings.Join(ourActive, ", "))
	}

	if !cfg.GetDryRun() {
		if len(foreign) >= maxConcurrent {
			logger.Error(fmt.Sprintf("Foreign restores at/over concurrency, aborting count=%d max=%d list=%s", len(foreign), maxConcurrent, strings.Join(foreign, ", ")))
			if madisonClient != nil {
				if _, e := madisonClient.SendMadisonForeignRestoreAlert(foreign, namespace, today); e != nil {
					logger.Error(fmt.Sprintf("Failed to send Madison foreign-restore alert error=%v", e))
				} else {
					logger.Info("Madison alert sent: type=SnapshotRestoreForeign")
				}
			}
			return fmt.Errorf("aborting: %d foreign restores in progress (>= max %d)", len(foreign), maxConcurrent)
		} else if len(foreign) > 0 {
			logger.Warn(fmt.Sprintf("Foreign restores in progress (not in filter, continuing): %s", strings.Join(foreign, ", ")))
		}

		slots := maxConcurrent - len(ourActive)
		for _, idx := range ourFailed {
			if slots <= 0 {
				logger.Info("No restore slot free for more repairs this run; remaining failed restores will be handled on a later run")
				break
			}
			if rerr := utils.RepairFailedRestore(client, idx, filter, maxConcurrent, restorePendingPollInterval, logger); rerr != nil {
				problems = true
				logger.Error(fmt.Sprintf("Failed to repair failed restore index=%s error=%v", idx, rerr))
				if madisonClient != nil {
					if _, e := madisonClient.SendMadisonRestoreFailedAlert("(repair)", idx, repo, namespace, today); e != nil {
						logger.Error(fmt.Sprintf("Failed to send Madison restore-failed alert error=%v", e))
					}
				}
				continue
			}
			slots--
		}
	}

	dates := restoreDates(cfg)
	logger.Info("Restore target dates: " + strings.Join(dates, ", "))

	var successful, failed []string
	for _, date := range dates {
		succ, fail, prob := restoreForDate(client, repo, date, filter, maxConcurrent, madisonClient, namespace, cfg.GetDryRun(), logger)
		successful = append(successful, succ...)
		failed = append(failed, fail...)
		if prob {
			problems = true
		}
	}

	if cfg.GetDryRun() {
		return nil
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

func restoreDates(cfg *config.Config) []string {
	if d := cfg.GetRestoreDate(); d != "" {
		return []string{d}
	}
	n := cfg.GetRestoreDaysCount()
	if n < 1 {
		n = 1
	}
	df := cfg.GetDateFormat()
	now := time.Now()
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, utils.FormatDate(now.AddDate(0, 0, -i), df))
	}
	return out
}

func restoreForDate(client *opensearch.Client, repo, date string, filter []string, maxConcurrent int, madisonClient *alerts.Client, namespace string, dryRun bool, logger *logging.Logger) ([]string, []string, bool) {
	problems := false
	pattern := "*" + date + "*"
	logger.Info(fmt.Sprintf("Listing snapshots for date=%s via filter pattern=%s", date, pattern))
	snapshots, err := client.GetSnapshotsDetailed(repo, pattern)
	if err != nil {
		if strings.Contains(err.Error(), "snapshot_missing_exception") || strings.Contains(err.Error(), "404") {
			logger.Info(fmt.Sprintf("No snapshots found for date=%s", date))
			return nil, nil, false
		}
		logger.Error(fmt.Sprintf("Failed to list snapshots for date=%s error=%v", date, err))
		return nil, nil, true
	}
	if len(snapshots) == 0 {
		logger.Info(fmt.Sprintf("No snapshots found for date=%s", date))
		return nil, nil, false
	}

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
			sendSnapshotStateFailed(madisonClient, s.Snapshot, s.State, repo, namespace, date, logger)
		}
	}

	if dryRun {
		sorted := utils.SortRestoreTasksBySizeDesc(readyTasks)
		logger.Info(fmt.Sprintf("DRY RUN date=%s: restore plan (largest first)", date))
		for i, t := range sorted {
			logger.Info(fmt.Sprintf("Restore %d: snapshot=%s matchedIndices=%d indices=%s", i+1, t.SnapshotName, len(t.Indices), strings.Join(t.Indices, ",")))
		}
		if len(pending) > 0 {
			logger.Info(fmt.Sprintf("Would wait for IN_PROGRESS snapshots: %s", strings.Join(pending, ", ")))
		}
		logger.Info(fmt.Sprintf("DRY RUN date=%s: would restore %d snapshots (+%d pending)", date, len(sorted), len(pending)))
		return nil, nil, problems
	}

	var successful, failed []string
	if len(readyTasks) > 0 {
		succ, fail := utils.RestoreSnapshotsInParallel(client, readyTasks, maxConcurrent, madisonClient, namespace, date, filter, logger)
		successful = append(successful, succ...)
		failed = append(failed, fail...)
	} else {
		logger.Info(fmt.Sprintf("No SUCCESS snapshots ready to restore for date=%s", date))
	}

	for len(pending) > 0 {
		logger.Info(fmt.Sprintf("Waiting for %d IN_PROGRESS snapshots (date=%s) before rechecking: %s", len(pending), date, strings.Join(pending, ", ")))
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
				sendSnapshotStateFailed(madisonClient, name, s.State, repo, namespace, date, logger)
			}
		}
		pending = stillPending

		if len(nowReady) > 0 {
			succ, fail := utils.RestoreSnapshotsInParallel(client, nowReady, maxConcurrent, madisonClient, namespace, date, filter, logger)
			successful = append(successful, succ...)
			failed = append(failed, fail...)
		}
	}

	return successful, failed, problems
}

func foreignRestores(active, failed, filter []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, idx := range append(append([]string{}, active...), failed...) {
		if seen[idx] || utils.MatchesAnyPattern(idx, filter) {
			continue
		}
		seen[idx] = true
		out = append(out, idx)
	}
	return out
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
