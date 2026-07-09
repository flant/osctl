package utils

import (
	"fmt"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
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

func RestoreSnapshotsInParallel(client *opensearch.Client, tasks []RestoreTask, maxConcurrent int, logger *logging.Logger) ([]string, []string) {
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
				err := RestoreOneSnapshot(client, task, logger, id)
				mu.Lock()
				if err != nil {
					logger.Error(fmt.Sprintf("Worker %d: Failed to restore snapshot snapshot=%s error=%v", id, task.SnapshotName, err))
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

func RestoreOneSnapshot(client *opensearch.Client, task RestoreTask, logger *logging.Logger, workerID int) error {
	toRestore := make([]string, 0, len(task.Indices))
	for _, idx := range task.Indices {
		idx = strings.TrimSpace(idx)
		if idx == "" {
			continue
		}
		exists, err := client.IndexExists(idx)
		if err != nil {
			logger.Warn(fmt.Sprintf("Worker %d: Failed to check index existence, will attempt restore index=%s error=%v", workerID, idx, err))
		} else if exists {
			logger.Info(fmt.Sprintf("Worker %d: Index already exists, skipping index=%s snapshot=%s", workerID, idx, task.SnapshotName))
			continue
		}
		toRestore = append(toRestore, idx)
	}

	if len(toRestore) == 0 {
		logger.Info(fmt.Sprintf("Worker %d: All indices already present, nothing to restore snapshot=%s", workerID, task.SnapshotName))
		return nil
	}

	start := time.Now()
	logger.Info(fmt.Sprintf("Worker %d: Starting restore snapshot=%s repo=%s size=%s indicesCount=%d", workerID, task.SnapshotName, task.Repo, formatSize(task.Size), len(toRestore)))
	logger.Info(fmt.Sprintf("Worker %d: Restore indices %s", workerID, strings.Join(toRestore, ", ")))

	body := map[string]any{
		"indices":              strings.Join(toRestore, ","),
		"ignore_unavailable":   true,
		"include_global_state": false,
		"include_aliases":      false,
	}
	if err := client.RestoreSnapshot(task.Repo, task.SnapshotName, body); err != nil {
		return fmt.Errorf("failed to start restore: %v", err)
	}

	if err := WaitForRestore(client, toRestore, task.PollInterval, logger, workerID, task.SnapshotName); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Worker %d: Restore completed and verified snapshot=%s duration=%s indicesCount=%d", workerID, task.SnapshotName, formatDuration(time.Since(start)), len(toRestore)))
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
