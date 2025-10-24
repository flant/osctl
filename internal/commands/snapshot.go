package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Create snapshots",
	Long:  `Create snapshots of indices`,
	RunE:  runSnapshot,
}

func init() {
	snapshotCmd.Flags().String("index-name", "", "Index name to snapshot")
	snapshotCmd.Flags().Bool("system-index", false, "Is system index")
	snapshotCmd.Flags().String("snap-repo", "", "Snapshot repository name")
	snapshotCmd.Flags().Bool("check-indices-exists", false, "Check if indices exist before snapshot")
	snapshotCmd.Flags().String("kind", "prefix", "Matching kind: prefix or regex")
	snapshotCmd.Flags().StringSlice("exclude-list", []string{}, "List of indices to exclude from unknown snapshot")
	snapshotCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")
	snapshotCmd.Flags().Bool("dry-run", false, "Show what would be created without actually creating")

	addCommonFlags(snapshotCmd)
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	indexName, _ := cmd.Flags().GetString("index-name")
	systemIndex, _ := cmd.Flags().GetBool("system-index")
	snapRepo, _ := cmd.Flags().GetString("snap-repo")
	checkIndicesExists, _ := cmd.Flags().GetBool("check-indices-exists")
	kind, _ := cmd.Flags().GetString("kind")
	excludeList, _ := cmd.Flags().GetStringSlice("exclude-list")
	dateFormat, _ := cmd.Flags().GetString("date-format")
	dryRun, _ := cmd.Flags().GetBool("dry-run")

	if indexName == "" {
		return fmt.Errorf("index-name parameter is required")
	}

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	today := utils.FormatDate(time.Now(), dateFormat)
	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), dateFormat)
	logger.Info("Starting snapshot creation", "indexName", indexName, "systemIndex", systemIndex, "snapRepo", snapRepo, "today", today, "yesterday", yesterday, "kind", kind)

	var filteredIndices []string

	if systemIndex {
		logger.Info("Checking system index existence", "index", indexName)
		indices, err := client.GetIndices(indexName)
		if err != nil || len(indices) == 0 {
			logger.Info("System index does not exist, skipping snapshot", "index", indexName)
			return nil
		}
		logger.Info("System index exists, proceeding with snapshot", "index", indexName)
	} else if indexName == "unknown" {
		datePattern := "*" + yesterday + "*"
		logger.Info("Getting unknown indices with date pattern", "pattern", datePattern)
		allIndices, err := client.GetIndices(datePattern)
		if err != nil {
			return fmt.Errorf("failed to get indices with date pattern: %v", err)
		}
		logger.Info("Retrieved unknown indices", "count", len(allIndices))

		var filteredIndices []string
		for _, idx := range allIndices {

			shouldExclude := false
			for _, exclude := range excludeList {
				if idx == exclude {
					shouldExclude = true
					break
				}
			}
			if shouldExclude {
				continue
			}

			filteredIndices = append(filteredIndices, idx)
		}

		if len(filteredIndices) == 0 {
			logger.Info("No unknown indices found for snapshot", "date", yesterday)
			return nil
		}

		logger.Info("Found unknown indices for snapshot", "count", len(filteredIndices), "indices", filteredIndices)
	} else {
		var pattern string
		if kind == "regex" {
			pattern = "*" + yesterday + "*"
		} else {
			pattern = indexName + "-" + yesterday + "*"
		}

		logger.Info("Getting indices with pattern", "pattern", pattern)
		indices, err := client.GetIndices(pattern)
		if err != nil || len(indices) == 0 {
			if checkIndicesExists {
				return fmt.Errorf("index %s-%s does not exist", indexName, yesterday)
			}
			logger.Info("Index does not exist, skipping snapshot", "index", indexName, "date", yesterday)
			return nil
		}
	}

	snapshotName := indexName + "-" + today
	snapshots, err := client.GetSnapshots(snapRepo, snapshotName)
	if err == nil && len(snapshots) > 0 && snapshots[0].State == "SUCCESS" {
		logger.Info("Snapshot already exists, skipping creation", "snapshot", snapshotName)
		return nil
	}

	err = waitForRunningSnapshots(client)
	if err != nil {
		return fmt.Errorf("failed to wait for running snapshots: %v", err)
	}

	err = waitForSnapshotTasks(client)
	if err != nil {
		return fmt.Errorf("failed to wait for snapshot tasks: %v", err)
	}

	var indicesList []string
	if indexName == "unknown" {
		indicesList = filteredIndices
	}

	if dryRun {
		logger.Info("DRY RUN: Would create snapshot", "snapshot", snapshotName)
		if indexName == "unknown" {
			logger.Info("DRY RUN: Would snapshot indices", "indices", indicesList)
		} else if systemIndex {
			logger.Info("DRY RUN: Would snapshot system index", "index", indexName)
		} else {
			pattern := indexName + "-" + yesterday + "*"
			logger.Info("DRY RUN: Would snapshot indices with pattern", "pattern", pattern)
		}
		return nil
	}

	err = createSnapshot(client, snapRepo, snapshotName, indexName, yesterday, systemIndex, indicesList)
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %v", err)
	}

	logger.Info("Snapshot created successfully", "snapshot", snapshotName)
	return nil
}

func waitForRunningSnapshots(client *opensearch.Client) error {
	for {
		time.Sleep(time.Duration(10+time.Now().UnixNano()%60) * time.Second)

		status, err := client.GetSnapshotStatus()
		if err != nil {
			return fmt.Errorf("failed to get snapshot status: %v", err)
		}

		if len(status.Snapshots) == 0 {
			break
		}
	}

	return nil
}

func waitForSnapshotTasks(client *opensearch.Client) error {
	for {
		time.Sleep(time.Duration(10+time.Now().UnixNano()%60) * time.Second)

		tasks, err := client.GetTasks()
		if err != nil {
			return fmt.Errorf("failed to get tasks: %v", err)
		}

		hasSnapshotTasks := false
		for _, node := range tasks.Nodes {
			for _, task := range node.Tasks {
				if task.Action == "indices:admin/snapshot/create" {
					hasSnapshotTasks = true
					break
				}
			}
			if hasSnapshotTasks {
				break
			}
		}

		if !hasSnapshotTasks {
			break
		}
	}

	return nil
}

func createSnapshot(client *opensearch.Client, snapRepo, snapshotName, indexName, yesterday string, systemIndex bool, indicesList []string) error {
	var indices interface{}

	if systemIndex {
		indices = indexName
	} else if indexName == "unknown" {
		indices = indicesList
	} else {
		indices = indexName + "-" + yesterday + "*"
	}

	snapshotRequest := map[string]interface{}{
		"indices":              indices,
		"ignore_unavailable":   true,
		"include_global_state": false,
	}

	return client.CreateSnapshot(snapRepo, snapshotName, snapshotRequest)
}
