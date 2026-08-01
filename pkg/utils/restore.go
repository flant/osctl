package utils

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

type RestoreTask struct {
	SnapshotName string
	Indices      []string
	Repo         string
	Size         int64
	PollInterval time.Duration
}

func MatchesAnyPattern(name string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if ok, err := path.Match(p, name); err == nil && ok {
			return true
		}
	}
	return false
}

func FilterIndices(indices, patterns []string) []string {
	if len(patterns) == 0 {
		return indices
	}
	out := make([]string, 0, len(indices))
	for _, idx := range indices {
		if MatchesAnyPattern(idx, patterns) {
			out = append(out, idx)
		}
	}
	return out
}

func GetSnapshotSize(client *opensearch.Client, repo, snapshot string) (int64, error) {
	detail, err := client.GetSnapshotStatusDetail(repo, snapshot)
	if err != nil {
		return 0, err
	}
	if len(detail.Snapshots) == 0 {
		return 0, nil
	}
	return detail.Snapshots[0].Stats.Total.SizeInBytes, nil
}

func SortRestoreTasksBySizeDesc(tasks []RestoreTask) []RestoreTask {
	sorted := make([]RestoreTask, len(tasks))
	copy(sorted, tasks)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Size > sorted[j].Size })
	return sorted
}

func RestoreSnapshotsInParallel(client *opensearch.Client, tasks []RestoreTask, maxConcurrent int, madisonClient *alerts.Client, namespace, dateStr string, filter []string, logger *logging.Logger) ([]string, []string) {
	var successful, failed []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	if maxConcurrent < 1 {
		maxConcurrent = 1
	}

	sorted := SortRestoreTasksBySizeDesc(tasks)

	logger.Info(fmt.Sprintf("Starting parallel restore tasksCount=%d maxConcurrent=%d sortOrder=descending (largest first)", len(sorted), maxConcurrent))
	if len(sorted) > 0 {
		order := make([]string, 0, len(sorted))
		for _, t := range sorted {
			order = append(order, fmt.Sprintf("%s(%s)", t.SnapshotName, formatSize(t.Size)))
		}
		logger.Info("Restore order: " + strings.Join(order, ", "))
	}

	taskChan := make(chan RestoreTask, len(sorted))
	for _, t := range sorted {
		taskChan <- t
	}
	close(taskChan)

	logger.Info(fmt.Sprintf("Creating %d worker goroutines for restore", maxConcurrent))
	for i := 0; i < maxConcurrent; i++ {
		workerID := i + 1
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for task := range taskChan {
				err := RestoreOneSnapshot(client, task, madisonClient, namespace, dateStr, filter, maxConcurrent, logger, id)
				mu.Lock()
				if err != nil {
					logger.Error(fmt.Sprintf("Worker %d: Snapshot restored with errors snapshot=%s error=%v", id, task.SnapshotName, err))
					failed = append(failed, task.SnapshotName)
				} else {
					logger.Info(fmt.Sprintf("Worker %d: Successfully restored snapshot snapshot=%s", id, task.SnapshotName))
					successful = append(successful, task.SnapshotName)
				}
				mu.Unlock()
			}
			logger.Info(fmt.Sprintf("Worker %d: Finished processing all assigned restore tasks", id))
		}(workerID)
	}

	wg.Wait()
	return successful, failed
}

func RestoreOneSnapshot(client *opensearch.Client, task RestoreTask, madisonClient *alerts.Client, namespace, dateStr string, filter []string, maxConcurrent int, logger *logging.Logger, workerID int) error {
	start := time.Now()
	logger.Info(fmt.Sprintf("Worker %d: Starting restore snapshot=%s repo=%s size=%s indicesCount=%d", workerID, task.SnapshotName, task.Repo, formatSize(task.Size), len(task.Indices)))

	var failedIndices []string
	for _, idx := range task.Indices {
		idx = strings.TrimSpace(idx)
		if idx == "" {
			continue
		}

		if err := restoreSingleIndex(client, task, idx, filter, maxConcurrent, logger, workerID); err != nil {
			logger.Error(fmt.Sprintf("Worker %d: Failed to restore index index=%s snapshot=%s error=%v", workerID, idx, task.SnapshotName, err))
			failedIndices = append(failedIndices, idx)
			if madisonClient != nil {
				if _, aerr := madisonClient.SendMadisonRestoreFailedAlert(task.SnapshotName, idx, task.Repo, namespace, dateStr); aerr != nil {
					logger.Error(fmt.Sprintf("Worker %d: Failed to send Madison restore-failed alert index=%s error=%v", workerID, idx, aerr))
				} else {
					logger.Info(fmt.Sprintf("Worker %d: Madison alert sent: type=SnapshotRestoreFailed index=%s", workerID, idx))
				}
			}
			continue
		}
	}

	logger.Info(fmt.Sprintf("Worker %d: Snapshot restore done snapshot=%s duration=%s failedIndices=%d", workerID, task.SnapshotName, formatDuration(time.Since(start)), len(failedIndices)))
	if len(failedIndices) > 0 {
		return fmt.Errorf("failed indices: %s", strings.Join(failedIndices, ", "))
	}
	return nil
}

func restoreSingleIndex(client *opensearch.Client, task RestoreTask, index string, filter []string, maxConcurrent int, logger *logging.Logger, workerID int) error {
	class, err := ClassifyRestore(client, index)
	if err != nil {
		logger.Warn(fmt.Sprintf("Worker %d: Failed to classify index, will attempt restore index=%s error=%v", workerID, index, err))
		class = RestoreMissing
	}
	switch class {
	case RestoreDone:
		logger.Info(fmt.Sprintf("Worker %d: Index already restored and healthy, skipping index=%s", workerID, index))
		return nil
	case RestoreRestoring:
		logger.Info(fmt.Sprintf("Worker %d: Index already restoring, waiting for it index=%s snapshot=%s", workerID, index, task.SnapshotName))
		return WaitForRestore(client, []string{index}, task.PollInterval, logger, workerID, task.SnapshotName)
	case RestoreFailed:
		logger.Warn(fmt.Sprintf("Worker %d: Index has a failed restore, deleting to retry index=%s", workerID, index))
		if derr := client.DeleteIndex(index); derr != nil {
			return fmt.Errorf("failed to delete failed-restore index %s: %v", index, derr)
		}
	}

	WaitForOurRestoreSlot(client, filter, maxConcurrent, task.PollInterval, logger, workerID)

	start := time.Now()
	logger.Info(fmt.Sprintf("Worker %d: Restoring index=%s snapshot=%s", workerID, index, task.SnapshotName))
	if err := client.RestoreSnapshot(task.Repo, task.SnapshotName, restoreBodyFor(index)); err != nil {
		return fmt.Errorf("failed to start restore: %v", err)
	}
	if err := WaitForRestore(client, []string{index}, task.PollInterval, logger, workerID, task.SnapshotName); err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Worker %d: Index restored and verified index=%s snapshot=%s duration=%s", workerID, index, task.SnapshotName, formatDuration(time.Since(start))))
	return nil
}

func restoreBodyFor(index string) map[string]any {
	return map[string]any{
		"indices":              index,
		"ignore_unavailable":   true,
		"include_global_state": false,
		"include_aliases":      false,
		"ignore_index_settings": []string{
			"index.routing.allocation.require.temp",
		},
		"index_settings": map[string]any{
			"index.number_of_replicas": 0,
		},
	}
}

type RestoreClass int

const (
	RestoreMissing RestoreClass = iota
	RestoreDone
	RestoreRestoring
	RestoreFailed
)

func ClassifyRestore(client *opensearch.Client, index string) (RestoreClass, error) {
	exists, err := client.IndexExists(index)
	if err != nil {
		return RestoreMissing, err
	}
	if !exists {
		return RestoreMissing, nil
	}
	rows, err := client.GetShardRows(index)
	if err != nil {
		return RestoreMissing, err
	}
	var priTotal, priStarted, priFailed, initializing int
	for _, r := range rows {
		if strings.EqualFold(r.Prirep, "p") {
			priTotal++
			if strings.EqualFold(r.State, "STARTED") {
				priStarted++
			} else if strings.EqualFold(r.State, "UNASSIGNED") && r.UnassignedReason == "NEW_INDEX_RESTORED" {
				priFailed++
			}
		}
		if strings.EqualFold(r.State, "INITIALIZING") {
			initializing++
		}
	}
	switch {
	case priFailed > 0:
		return RestoreFailed, nil
	case initializing > 0:
		return RestoreRestoring, nil
	case priTotal > 0 && priStarted == priTotal:
		return RestoreDone, nil
	default:
		return RestoreRestoring, nil
	}
}

func WaitForOurRestoreSlot(client *opensearch.Client, filter []string, maxConcurrent int, pollInterval time.Duration, logger *logging.Logger, workerID int) {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	for {
		active, err := client.ActiveSnapshotRecoveryIndices()
		if err != nil {
			logger.Warn(fmt.Sprintf("Worker %d: Failed to poll active restores, proceeding without slot wait error=%v", workerID, err))
			return
		}
		ours := FilterIndices(active, filter)
		if len(ours) < maxConcurrent {
			return
		}
		logger.Info(fmt.Sprintf("Worker %d: Restore slot busy ourActiveRestores=%d max=%d, waiting", workerID, len(ours), maxConcurrent))
		time.Sleep(pollInterval)
	}
}

func RepairFailedRestore(client *opensearch.Client, index string, filter []string, maxConcurrent int, pollInterval time.Duration, logger *logging.Logger) error {
	repo, snap, ok, err := client.RestoreSourceOfIndex(index)
	if err != nil {
		return fmt.Errorf("could not read restore source for %s: %v", index, err)
	}
	if !ok {
		return fmt.Errorf("could not determine restore source snapshot for %s", index)
	}
	if derr := client.DeleteIndex(index); derr != nil {
		return fmt.Errorf("failed to delete failed-restore index %s: %v", index, derr)
	}
	WaitForOurRestoreSlot(client, filter, maxConcurrent, pollInterval, logger, 0)
	if err := client.RestoreSnapshot(repo, snap, restoreBodyFor(index)); err != nil {
		return fmt.Errorf("failed to restart restore for %s from %s/%s: %v", index, repo, snap, err)
	}
	logger.Info(fmt.Sprintf("Repaired failed restore: deleted and re-restoring index=%s from repo=%s snapshot=%s", index, repo, snap))
	return nil
}

func WaitForRestore(client *opensearch.Client, indices []string, pollInterval time.Duration, logger *logging.Logger, workerID int, snapshotName string) error {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}
	const maxPollErrors = 5
	pollErrors := 0
	start := time.Now()

	for {
		health, err := client.GetIndicesHealth(indices)
		if err != nil {
			pollErrors++
			logger.Warn(fmt.Sprintf("Worker %d: Failed to poll restore health snapshot=%s error=%v (%d/%d)", workerID, snapshotName, err, pollErrors, maxPollErrors))
			if pollErrors >= maxPollErrors {
				return fmt.Errorf("exceeded consecutive health poll errors for snapshot %s: %v", snapshotName, err)
			}
			time.Sleep(pollInterval)
			continue
		}
		pollErrors = 0

		allReady := true
		anyRed := false
		var ready, total int
		for _, idx := range indices {
			total++
			h, ok := health[idx]
			if !ok {
				allReady = false
				continue
			}
			if h.Status == "red" {
				anyRed = true
			}
			if h.NumberOfShards > 0 && h.ActivePrimaryShards >= h.NumberOfShards && h.Status != "red" {
				ready++
			} else {
				allReady = false
			}
		}

		if allReady {
			logger.Info(fmt.Sprintf("Worker %d: Restore healthy snapshot=%s indices=%d/%d elapsed=%s", workerID, snapshotName, ready, total, formatDuration(time.Since(start))))
			return nil
		}

		state := "recovering"
		if anyRed {
			state = "recovering (some red)"
		}
		logger.Info(fmt.Sprintf("Worker %d: Restore in progress snapshot=%s state=%s readyIndices=%d/%d elapsed=%s", workerID, snapshotName, state, ready, total, formatDuration(time.Since(start))))
		time.Sleep(pollInterval)
	}
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
