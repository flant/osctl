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
	snapshotCmd.Flags().Bool("wildcard", false, "Use wildcard matching for index names")
	snapshotCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")

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
	dateFormat, _ := cmd.Flags().GetString("date-format")

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

	if systemIndex {
		indices, err := client.GetIndices(indexName)
		if err != nil || len(indices) == 0 {
			logger.Info("System index does not exist, skipping snapshot", "index", indexName)
			return nil
		}
	} else if indexName == "unknown" {
		// For unknown indices, proceed with snapshot
	} else {
		pattern := indexName + "-" + yesterday + "*"
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

	err = createSnapshot(client, snapRepo, snapshotName, indexName, yesterday, systemIndex)
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

func createSnapshot(client *opensearch.Client, snapRepo, snapshotName, indexName, yesterday string, systemIndex bool) error {
	var indicesPattern string

	if systemIndex {
		indicesPattern = indexName
	} else {
		indicesPattern = indexName + "-" + yesterday + "*"
	}

	snapshotRequest := map[string]interface{}{
		"indices":              indicesPattern,
		"ignore_unavailable":   true,
		"include_global_state": false,
	}

	return client.CreateSnapshot(snapRepo, snapshotName, snapshotRequest)
}
