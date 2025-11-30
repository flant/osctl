package utils

import (
	"fmt"
	"math/rand"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"strings"
	"time"
)

func GetSnapshotsIgnore404(client *opensearch.Client, repo, pattern string) ([]opensearch.Snapshot, error) {
	snapshots, err := client.GetSnapshots(repo, pattern)
	if err != nil {
		if strings.Contains(err.Error(), "snapshot_missing_exception") || strings.Contains(err.Error(), "404") {
			return nil, nil
		}
		return nil, err
	}
	return snapshots, nil
}

func SnapshotsToNames(snapshots []opensearch.Snapshot) []string {
	names := make([]string, len(snapshots))
	for i, s := range snapshots {
		names[i] = s.Snapshot
	}
	return names
}

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
				err := client.DeleteSnapshots(snapRepo, []string{snapshotName})
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to delete PARTIAL/FAILED snapshot snapshot=%s error=%v", snapshotName, err))
					return false, err
				}
				logger.Info(fmt.Sprintf("Deleted PARTIAL/FAILED snapshot snapshot=%s state=%s", snapshotName, snapshot.State))
				return false, nil
			}
		}
	}

	return false, nil
}

func GetSnapshotStateByName(snapshotName string, snapshots []opensearch.Snapshot) (string, bool) {
	for _, snapshot := range snapshots {
		if snapshot.Snapshot == snapshotName {
			return snapshot.State, true
		}
	}
	return "", false
}

func CheckSnapshotStateInRepo(client *opensearch.Client, repo string, snapshotName string) (string, bool, error) {
	snaps, err := GetSnapshotsIgnore404(client, repo, snapshotName)
	if err != nil {
		return "", false, err
	}
	if len(snaps) == 0 {
		return "", false, nil
	}
	if state, ok := GetSnapshotStateByName(snapshotName, snaps); ok {
		return state, true, nil
	}
	return "", false, nil
}

func WaitForSnapshotCompletion(client *opensearch.Client, logger *logging.Logger, targetSnapshot string, targetRepo string) error {
	for {
		status, err := client.GetSnapshotStatus()
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to get snapshot status error=%v", err))
			time.Sleep(60 * time.Second)
			continue
		}

		filtered := status.Snapshots
		if targetRepo != "" || targetSnapshot != "" {
			tmp := []opensearch.SnapshotInfo{}
			for _, s := range status.Snapshots {
				if targetRepo != "" && s.Repository != targetRepo {
					continue
				}
				if targetSnapshot != "" && s.Snapshot != targetSnapshot {
					continue
				}
				tmp = append(tmp, s)
			}
			filtered = tmp
		}

		if len(filtered) == 0 {
			break
		}

		identifiers := make([]string, 0, len(filtered))
		for _, s := range filtered {
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
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d jobs=%v target=%s", len(identifiers), identifiers, targetSnapshot))
			} else {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete count=%d jobs=%v", len(identifiers), identifiers))
			}
		} else {
			if targetSnapshot != "" {
				logger.Info(fmt.Sprintf("Waiting for snapshots to complete target=%s", targetSnapshot))
			} else {
				logger.Info("Waiting for snapshots to complete")
			}
		}
		time.Sleep(60 * time.Second)
	}
	return nil
}

func WaitForSnapshotTasks(client *opensearch.Client, logger *logging.Logger, targetSnapshot string, targetRepo string) error {
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
					desc := task.Description
					if desc == "" {
						desc = task.Action
					}
					if targetRepo != "" || targetSnapshot != "" {
						match := false
						if targetRepo != "" && strings.Contains(desc, targetRepo+"/") {
							match = true
						}
						if targetSnapshot != "" && strings.Contains(desc, targetSnapshot) {
							match = true
						}
						if !match {
							continue
						}
					}
					hasSnapshotTasks = true
					jobDescriptions = append(jobDescriptions, desc)
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

func CheckIndicesExist(client *opensearch.Client, indicesStr string, logger *logging.Logger) ([]string, error) {
	indices := strings.Split(indicesStr, ",")
	existingIndices := make([]string, 0)

	for _, indexName := range indices {
		indexName = strings.TrimSpace(indexName)
		if indexName == "" {
			continue
		}

		indicesInfo, err := client.GetIndicesWithFields(indexName, "index")
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to check index existence index=%s error=%v", indexName, err))
			continue
		}

		found := false
		for _, idx := range indicesInfo {
			if idx.Index == indexName {
				found = true
				break
			}
		}

		if found {
			existingIndices = append(existingIndices, indexName)
		} else {
			logger.Warn(fmt.Sprintf("Index does not exist, skipping snapshot creation index=%s", indexName))
		}
	}

	return existingIndices, nil
}

func CreateSnapshotWithRetry(client *opensearch.Client, snapshotName, indexName, snapRepo, namespace, dateStr string, madisonClient interface{}, logger *logging.Logger, pollInterval time.Duration) error {
	const maxRetries = 7

	existingIndices, err := CheckIndicesExist(client, indexName, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to check indices existence snapshot=%s error=%v", snapshotName, err))
		return err
	}

	if len(existingIndices) == 0 {
		logger.Warn(fmt.Sprintf("No existing indices found, skipping snapshot creation snapshot=%s indices=%s", snapshotName, indexName))
		return fmt.Errorf("no existing indices to snapshot")
	}

	existingIndicesStr := strings.Join(existingIndices, ",")
	if existingIndicesStr != indexName {
		logger.Info(fmt.Sprintf("Some indices were removed, using only existing indices snapshot=%s existing=%s original=%s", snapshotName, existingIndicesStr, indexName))
		indexName = existingIndicesStr
	}

retryLoop:
	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.Info(fmt.Sprintf("Creating snapshot attempt snapshot=%s attempt=%d maxRetries=%d", snapshotName, attempt, maxRetries))

		err := WaitForSnapshotCompletion(client, logger, "", snapRepo)
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to wait for snapshot completion error=%v", err))
		}

		err = WaitForSnapshotTasks(client, logger, "", snapRepo)
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to wait for snapshot tasks error=%v", err))
		}

		snapshotRequest := map[string]any{
			"indices":              indexName,
			"ignore_unavailable":   true,
			"include_global_state": false,
		}

		startTime := time.Now()

		err = client.CreateSnapshot(snapRepo, snapshotName, snapshotRequest)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to create snapshot snapshot=%s attempt=%d error=%v", snapshotName, attempt, err))
			if attempt < maxRetries {
				time.Sleep(pollInterval)
				continue
			}
			logger.Error(fmt.Sprintf("Snapshot creation failed after all retries snapshot=%s maxRetries=%d", snapshotName, maxRetries))
			logger.Error(fmt.Sprintf("SENDING ALERT: Snapshot creation failed snapshot=%s index=%s message=%s", snapshotName, indexName,
				fmt.Sprintf("Snapshot %s for index %s failed to create after %d retries", snapshotName, indexName, maxRetries)))
			if madisonClient != nil {
				if client, ok := madisonClient.(*alerts.Client); ok {
					response, err := client.SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName, snapRepo, namespace, dateStr)
					if err != nil {
						logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
					} else {
						logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotCreationFailed response=%s", response))
					}
				}
			}
			return err
		}

		logger.Info(fmt.Sprintf("Waiting for snapshot completion snapshot=%s", snapshotName))

		const maxWaitForVisibility = 15 * time.Minute
		visibilityDeadline := startTime.Add(maxWaitForVisibility)

		for {
			snapshots, err := client.GetSnapshots(snapRepo, snapshotName)
			if err != nil {
				if time.Now().After(visibilityDeadline) {
					logger.Error(fmt.Sprintf("Error getting snapshot after creation timeout snapshot=%s timeout=%v attempt=%d error=%v", snapshotName, maxWaitForVisibility, attempt, err))
					if attempt < maxRetries {
						continue retryLoop
					}
					return fmt.Errorf("snapshot %s error after creation timeout: %v, attempt=%d", snapshotName, err, attempt)
				}
				logger.Error(fmt.Sprintf("Failed to get snapshots snapshot=%s error=%v attempt=%d, error might be transient, wait a bit and retry", snapshotName, err, attempt))
				time.Sleep(pollInterval)
				continue
			}
			if len(snapshots) == 0 {
				if time.Now().After(visibilityDeadline) {
					logger.Error(fmt.Sprintf("Snapshot not found in list after creation timeout snapshot=%s timeout=%v attempt=%d", snapshotName, maxWaitForVisibility, attempt))
					if attempt < maxRetries {
						continue retryLoop
					}
					return fmt.Errorf("snapshot %s not found in list after creation", snapshotName)
				}
				logger.Info(fmt.Sprintf("Waiting for snapshot visibility snapshot=%s attempt=%d", snapshotName, attempt))
				time.Sleep(pollInterval)
				continue
			}

			snapshot := snapshots[0]
			if snapshot.State == "IN_PROGRESS" {
				logger.Info(fmt.Sprintf("Snapshot still in progress snapshot=%s", snapshotName))
				time.Sleep(pollInterval)
				continue
			}

			switch snapshot.State {
			case "SUCCESS":
				duration := time.Since(startTime)
				durationStr := formatDuration(duration)
				logger.Info(fmt.Sprintf("Snapshot created successfully snapshot=%s duration=%s attempt=%d", snapshotName, durationStr, attempt))
				return nil
			case "PARTIAL", "FAILED":
				duration := time.Since(startTime)
				durationStr := formatDuration(duration)
				logger.Warn(fmt.Sprintf("Snapshot is PARTIAL/FAILED, deleting and retrying snapshot=%s state=%s duration=%s attempt=%d", snapshotName, snapshot.State, durationStr, attempt))
				err := client.DeleteSnapshots(snapRepo, []string{snapshotName})
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to delete PARTIAL/FAILED snapshot snapshot=%s error=%v", snapshotName, err))
				} else {
					logger.Info(fmt.Sprintf("Deleted PARTIAL/FAILED snapshot snapshot=%s state=%s duration=%s attempt=%d", snapshotName, snapshot.State, durationStr, attempt))
				}
				if attempt < maxRetries {
					logger.Info(fmt.Sprintf("Waiting 15 minutes before retry attempt=%d maxRetries=%d", attempt+1, maxRetries))
					time.Sleep(15 * time.Minute)
					continue retryLoop
				}
			default:
				logger.Warn(fmt.Sprintf("Unknown snapshot state snapshot=%s state=%s attempt=%d", snapshotName, snapshot.State, attempt))
				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * time.Second)
					logger.Warn(fmt.Sprintf("Unknown snapshot state snapshot=%s state=%s attempt=%d, try again", snapshotName, snapshot.State, attempt))
					continue retryLoop
				}
			}

			break
		}
	}

	logger.Error(fmt.Sprintf("Snapshot creation failed after all retries snapshot=%s maxRetries=%d", snapshotName, maxRetries))
	logger.Error(fmt.Sprintf("SENDING ALERT: Snapshot creation failed snapshot=%s index=%s message=%s", snapshotName, indexName,
		fmt.Sprintf("Snapshot %s for index %s failed to create after %d retries", snapshotName, indexName, maxRetries)))
	if madisonClient != nil {
		if client, ok := madisonClient.(*alerts.Client); ok {
			response, err := client.SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName, snapRepo, namespace, dateStr)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to send Madison alert error=%v", err))
			} else {
				logger.Info(fmt.Sprintf("Madison alert sent successfully: type=SnapshotCreationFailed response=%s", response))
			}
		}
	}
	return fmt.Errorf("snapshot creation failed after %d retries", maxRetries)
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

func BatchDeleteSnapshots(client *opensearch.Client, snapshots []string, snapRepo string, dryRun bool, logger *logging.Logger) ([]string, []string, error) {
	const batchSize = 10
	const maxRetries = 7

	var successful []string
	var failed []string

	if dryRun {
		logger.Info(fmt.Sprintf("Dry run: would delete snapshots count=%d", len(snapshots)))
		return nil, nil, nil
	}

	for i := 0; i < len(snapshots); i += batchSize {
		end := i + batchSize
		if end > len(snapshots) {
			end = len(snapshots)
		}

		batch := snapshots[i:end]

		randomWaitMinutes := rand.Intn(5) + 1
		randomWaitDuration := time.Duration(randomWaitMinutes) * time.Minute
		logger.Info(fmt.Sprintf("Waiting %d minutes before deleting batch batch=%d snapshots=%v", randomWaitMinutes, i/batchSize+1, batch))
		time.Sleep(randomWaitDuration)

		var lastErr error
		for attempt := 1; attempt <= maxRetries; attempt++ {
			existingSnapshots := make([]string, 0)
			for _, snapshotName := range batch {
				snapshots, err := GetSnapshotsIgnore404(client, snapRepo, snapshotName)
				if err != nil {
					logger.Warn(fmt.Sprintf("Failed to check snapshot existence snapshot=%s error=%v, will try to delete", snapshotName, err))
					existingSnapshots = append(existingSnapshots, snapshotName)
					continue
				}
				if len(snapshots) > 0 {
					existingSnapshots = append(existingSnapshots, snapshotName)
				} else {
					logger.Info(fmt.Sprintf("Snapshot already deleted, skipping snapshot=%s", snapshotName))
				}
			}

			if len(existingSnapshots) == 0 {
				logger.Info(fmt.Sprintf("All snapshots from batch already deleted batch=%d attempt=%d snapshots=%v", i/batchSize+1, attempt, batch))
				break
			}

			logger.Info(fmt.Sprintf("Deleting snapshots batch batch=%d attempt=%d maxRetries=%d snapshots=%v", i/batchSize+1, attempt, maxRetries, existingSnapshots))

			err := client.DeleteSnapshots(snapRepo, existingSnapshots)
			if err != nil {
				lastErr = err
				logger.Error(fmt.Sprintf("Failed to delete snapshots batch batch=%d attempt=%d snapshots=%v error=%v", i/batchSize+1, attempt, existingSnapshots, err))
				if attempt < maxRetries {
					logger.Info(fmt.Sprintf("Waiting 5 minutes before retry batch=%d attempt=%d", i/batchSize+1, attempt+1))
					time.Sleep(5 * time.Minute)
					continue
				}
			} else {
				logger.Info(fmt.Sprintf("Snapshots batch deleted successfully batch=%d attempt=%d snapshots=%v", i/batchSize+1, attempt, existingSnapshots))
				successful = append(successful, existingSnapshots...)
				break
			}
		}

		if lastErr != nil {
			logger.Error(fmt.Sprintf("Failed to delete snapshots batch after all retries batch=%d maxRetries=%d snapshots=%v error=%v", i/batchSize+1, maxRetries, batch, lastErr))
			failed = append(failed, batch...)
		}
	}

	return successful, failed, nil
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
			snapshotName := BuildSnapshotNameFromConfig(indexConfig, dateStr)
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

func BuildSnapshotName(kind, name, value, dateStr string) string {
	if kind == "regex" {
		return name + "-" + dateStr
	}
	return value + "-" + dateStr
}

func BuildSnapshotNameFromConfig(indexConfig config.IndexConfig, dateStr string) string {
	return BuildSnapshotName(indexConfig.Kind, indexConfig.Name, indexConfig.Value, dateStr)
}

func AddIndexToSnapshotGroups(indexName string, indexConfig config.IndexConfig, dateStr string, repoGroups map[string]SnapshotGroup, indicesToSnapshot *[]string) {
	if indexConfig.Repository != "" {
		snapshotName := BuildSnapshotNameFromConfig(indexConfig, dateStr)
		key := indexConfig.Repository + "|" + snapshotName
		if g, ok := repoGroups[key]; ok {
			g.Indices = append(g.Indices, indexName)
			repoGroups[key] = g
		} else {
			repoGroups[key] = SnapshotGroup{
				SnapshotName: snapshotName,
				Indices:      []string{indexName},
				Pattern:      indexConfig.Value,
				Kind:         indexConfig.Kind,
			}
		}
	} else {
		*indicesToSnapshot = append(*indicesToSnapshot, indexName)
	}
}
