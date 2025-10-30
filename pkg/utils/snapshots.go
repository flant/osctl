package utils

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"strings"
	"time"
)

func HasValidSnapshot(index string, snapshots []opensearch.Snapshot) bool {
	for _, snapshot := range snapshots {
		if snapshot.State != "SUCCESS" {
			continue
		}
		for _, snapshotIndex := range snapshot.Indices {
			if snapshotIndex == index {
				return true
			}
		}
	}
	return false
}

func CheckAndCleanSnapshot(snapshotName string, indexName string, snapshots []opensearch.Snapshot, client *opensearch.Client, snapRepo string, logger *logging.Logger) (bool, error) {
	for _, snapshot := range snapshots {
		if snapshot.Snapshot == snapshotName {
			if snapshot.State == "SUCCESS" {
				return true, nil
			}
			if snapshot.State == "PARTIAL" || snapshot.State == "FAILED" {
				logger.Info(fmt.Sprintf("Deleting PARTIAL/FAILED snapshot snapshot=%s state=%s", snapshotName, snapshot.State))
				err := client.DeleteSnapshotsBatch(snapRepo, []string{snapshotName})
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to delete PARTIAL/FAILED snapshot snapshot=%s error=%v", snapshotName, err))
					return false, err
				}
				return false, nil
			}
		}
	}

	return false, nil
}

func WaitForSnapshotCompletion(client *opensearch.Client, logger *logging.Logger, targetSnapshot string) error {
	for {
		status, err := client.GetSnapshotStatus()
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get snapshot status error=%v", err))
			time.Sleep(60 * time.Second)
			continue
		}

		if len(status.Snapshots) == 0 {
			break
		}

		identifiers := make([]string, 0, len(status.Snapshots))
		for _, s := range status.Snapshots {
			if s.Repository != "" || s.Snapshot != "" {
				if s.Repository != "" && s.Snapshot != "" {
					identifiers = append(identifiers, fmt.Sprintf("%s/%s", s.Repository, s.Snapshot))
				} else if s.Snapshot != "" {
					identifiers = append(identifiers, s.Snapshot)
				}
			}
		}

		if len(identifiers) > 0 {
			if targetSnapshot != "" {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d jobs=%v target=%s", len(status.Snapshots), identifiers, targetSnapshot))
			} else {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d jobs=%v", len(status.Snapshots), identifiers))
			}
		} else {
			if targetSnapshot != "" {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d target=%s", len(status.Snapshots), targetSnapshot))
			} else {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d", len(status.Snapshots)))
			}
		}
		time.Sleep(60 * time.Second)
	}
	return nil
}

func WaitForSnapshotTasks(client *opensearch.Client, logger *logging.Logger, targetSnapshot string) error {
	for {
		tasks, err := client.GetTasks()
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get tasks error=%v", err))
			time.Sleep(60 * time.Second)
			continue
		}

		hasSnapshotTasks := false
		jobDescriptions := []string{}
		for _, node := range tasks.Nodes {
			for _, task := range node.Tasks {
				if strings.Contains(task.Action, "snapshot") {
					hasSnapshotTasks = true
					if task.Description != "" {
						jobDescriptions = append(jobDescriptions, task.Description)
					} else {
						jobDescriptions = append(jobDescriptions, task.Action)
					}
				}
			}
		}

		if !hasSnapshotTasks {
			break
		}

		if len(jobDescriptions) > 0 {
			if targetSnapshot != "" {
				logger.Info(fmt.Sprintf("Waiting for snapshot tasks to complete jobs=%v target=%s", jobDescriptions, targetSnapshot))
			} else {
				logger.Info(fmt.Sprintf("Waiting for snapshot tasks to complete jobs=%v", jobDescriptions))
			}
		} else {
			if targetSnapshot != "" {
				logger.Info(fmt.Sprintf("Waiting for snapshot tasks to complete target=%s", targetSnapshot))
			} else {
				logger.Info("Waiting for snapshot tasks to complete")
			}
		}
		time.Sleep(60 * time.Second)
	}
	return nil
}

func FilterUnknownSnapshots(snapshots []string) []string {
	var filtered []string
	for _, snapshotName := range snapshots {
		parts := strings.Split(snapshotName, "-")
		if len(parts) < 2 {
			continue
		}
		indexName := strings.Join(parts[:len(parts)-1], "-")
		if !ShouldSkipIndex(indexName) {
			filtered = append(filtered, snapshotName)
		}
	}
	return filtered
}

func CreateSnapshotWithRetry(client *opensearch.Client, snapshotName, indexName, snapRepo string, madisonClient interface{}, logger *logging.Logger) error {
	const maxRetries = 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Info(fmt.Sprintf("Creating snapshot attempt snapshot=%s attempt=%d maxRetries=%d", snapshotName, attempt, maxRetries))

		err := WaitForSnapshotCompletion(client, logger, "")
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to wait for snapshot completion error=%v", err))
		}

		err = WaitForSnapshotTasks(client, logger, "")
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to wait for snapshot tasks error=%v", err))
		}

		snapshotRequest := map[string]interface{}{
			"indices":              indexName,
			"ignore_unavailable":   true,
			"include_global_state": false,
		}

		startTime := time.Now()

		err = client.CreateSnapshot(snapRepo, snapshotName, snapshotRequest)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to create snapshot snapshot=%s attempt=%d error=%v", snapshotName, attempt, err))
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return err
		}

		logger.Info(fmt.Sprintf("Waiting for snapshot completion snapshot=%s", snapshotName))

	pollLoop:
		for {
			snapshots, err := client.GetSnapshots(snapRepo, snapshotName)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to get snapshots snapshot=%s error=%v", snapshotName, err))
				if attempt < maxRetries {
					time.Sleep(60 * time.Second)
					continue
				}
				return err
			}
			if len(snapshots) == 0 {
				logger.Info(fmt.Sprintf("Waiting for snapshot visibility snapshot=%s", snapshotName))
				time.Sleep(60 * time.Second)
				continue
			}

			snapshot := snapshots[0]
			if snapshot.State == "IN_PROGRESS" {
				logger.Info(fmt.Sprintf("Snapshot still in progress snapshot=%s", snapshotName))
				time.Sleep(60 * time.Second)
				continue
			}

			switch snapshot.State {
			case "SUCCESS":
				duration := time.Since(startTime)
				durationStr := formatDuration(duration)
				logger.Info(fmt.Sprintf("Snapshot created successfully snapshot=%s duration=%s", snapshotName, durationStr))
				return nil
			case "PARTIAL", "FAILED":
				logger.Warn(fmt.Sprintf("Snapshot is PARTIAL/FAILED, deleting and retrying snapshot=%s state=%s", snapshotName, snapshot.State))
				err := client.DeleteSnapshotsBatch(snapRepo, []string{snapshotName})
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to delete PARTIAL/FAILED snapshot snapshot=%s error=%v", snapshotName, err))
				}
				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * time.Second)
					break pollLoop
				}
			default:
				logger.Warn(fmt.Sprintf("Unknown snapshot state snapshot=%s state=%s", snapshotName, snapshot.State))
				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * time.Second)
					break pollLoop
				}
			}

			break
		}
	}

	logger.Error(fmt.Sprintf("Snapshot creation failed after all retries snapshot=%s maxRetries=%d", snapshotName, maxRetries))
	SendSnapshotFailureAlert(snapshotName, indexName, madisonClient, logger)
	return fmt.Errorf("snapshot creation failed after %d retries", maxRetries)
}

func SendSnapshotFailureAlert(snapshotName, indexName string, madisonClient interface{}, logger *logging.Logger) {
	logger.Error(fmt.Sprintf("SENDING ALERT: Snapshot creation failed snapshot=%s index=%s message=%s", snapshotName, indexName,
		fmt.Sprintf("Snapshot %s for index %s failed to create after 5 retries", snapshotName, indexName)))

	if madisonClient != nil {
		if client, ok := madisonClient.(interface {
			SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName string) (string, error)
		}); ok {
			response, err := client.SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
			} else {
				logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotCreationFailed response=%s", response))
			}
		}
	}
}

func FindMatchingSnapshotConfig(snapshotName string, indicesConfig []config.IndexConfig) *config.IndexConfig {
	for _, indexConfig := range indicesConfig {
		if !indexConfig.Snapshot {
			continue
		}

		if MatchesSnapshot(snapshotName, indexConfig) {
			return &indexConfig
		}
	}
	return nil
}

func DeleteSnapshotsBatch(client *opensearch.Client, snapshots []string, snapRepo string, dryRun bool, logger *logging.Logger) error {
	const batchSize = 10

	if dryRun {
		logger.Info(fmt.Sprintf("Dry run: would delete snapshots count=%d", len(snapshots)))
		return nil
	}

	for i := 0; i < len(snapshots); i += batchSize {
		end := i + batchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]
		logger.Info(fmt.Sprintf("Deleting snapshots batch batch=%d snapshots=%v", i/batchSize+1, batch))

		err := client.DeleteSnapshotsBatch(snapRepo, batch)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete snapshots batch snapshots=%v error=%v", batch, err))
			continue
		}
		logger.Info(fmt.Sprintf("Snapshots batch deleted successfully snapshots=%v", batch))
	}

	return nil
}

type SnapshotGroup struct {
	SnapshotName string
	Indices      []string
	Pattern      string
	Kind         string
}

func GroupIndicesForSnapshots(indices []string, indicesConfig []config.IndexConfig, dateStr string) []SnapshotGroup {
	var groups []SnapshotGroup
	usedIndices := make(map[string]bool)

	for _, indexConfig := range indicesConfig {
		if !indexConfig.Snapshot {
			continue
		}

		var matchingIndices []string
		for _, indexName := range indices {
			if usedIndices[indexName] {
				continue
			}
			if MatchesIndex(indexName, indexConfig) {
				matchingIndices = append(matchingIndices, indexName)
				usedIndices[indexName] = true
			}
		}

		if len(matchingIndices) > 0 {
			var snapshotName string
			if indexConfig.Kind == "regex" {
				snapshotName = indexConfig.Name + "-" + dateStr
			} else {
				snapshotName = indexConfig.Value + "-" + dateStr
			}

			groups = append(groups, SnapshotGroup{
				SnapshotName: snapshotName,
				Indices:      matchingIndices,
				Pattern:      indexConfig.Value,
				Kind:         indexConfig.Kind,
			})
		}
	}

	var unknownIndices []string
	for _, indexName := range indices {
		if !usedIndices[indexName] {
			unknownIndices = append(unknownIndices, indexName)
		}
	}

	if len(unknownIndices) > 0 {
		groups = append(groups, SnapshotGroup{
			SnapshotName: "unknown-" + dateStr,
			Indices:      unknownIndices,
			Pattern:      "unknown",
			Kind:         "unknown",
		})
	}

	return groups
}

func formatSnapshotDuration(durationMillis int64) string {
	if durationMillis == 0 {
		return "unknown"
	}

	durationSeconds := durationMillis / 1000

	if durationSeconds < 60 {
		return fmt.Sprintf("%ds", durationSeconds)
	} else if durationSeconds < 3600 {
		minutes := durationSeconds / 60
		seconds := durationSeconds % 60
		if seconds == 0 {
			return fmt.Sprintf("%dm", minutes)
		}
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	} else {
		hours := durationSeconds / 3600
		minutes := (durationSeconds % 3600) / 60
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh%dm", hours, minutes)
	}
}

func formatDuration(d time.Duration) string {
	totalSeconds := int64(d.Seconds())
	if totalSeconds < 60 {
		return fmt.Sprintf("%ds", totalSeconds)
	}

	minutes := totalSeconds / 60
	seconds := totalSeconds % 60
	if seconds == 0 {
		return fmt.Sprintf("%dm", minutes)
	}
	return fmt.Sprintf("%dm%ds", minutes, seconds)
}
