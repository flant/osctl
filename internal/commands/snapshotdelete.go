package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var snapshotDeleteCmd = &cobra.Command{
	Use:   "snapshotdelete",
	Short: "Delete snapshots",
	Long:  `Delete snapshots`,
	RunE:  runSnapshotDelete,
}

func init() {
	snapshotDeleteCmd.Flags().String("repo", "", "Snapshot repository name")
	snapshotDeleteCmd.Flags().String("index", "", "Index name pattern for snapshots")
	snapshotDeleteCmd.Flags().Int("days", 180, "Number of days to keep snapshots")
	snapshotDeleteCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for snapshot names")
	snapshotDeleteCmd.Flags().Bool("dangle-snapshots", false, "Delete dangling snapshots")
	snapshotDeleteCmd.Flags().StringSlice("exclude-list", []string{}, "List of snapshots to exclude from dangling deletion")
	snapshotDeleteCmd.Flags().Bool("wildcard", false, "Use wildcard matching for snapshot names")

	addCommonFlags(snapshotDeleteCmd)
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	repo, _ := cmd.Flags().GetString("repo")
	index, _ := cmd.Flags().GetString("index")
	days, _ := cmd.Flags().GetInt("days")
	dateFormat, _ := cmd.Flags().GetString("date-format")
	dangleSnapshots, _ := cmd.Flags().GetBool("dangle-snapshots")
	excludeList, _ := cmd.Flags().GetStringSlice("exclude-list")
	wildcard, _ := cmd.Flags().GetBool("wildcard")

	if repo == "" {
		return fmt.Errorf("repo parameter is required")
	}

	if index == "" && !dangleSnapshots {
		return fmt.Errorf("index parameter is required or use --dangle-snapshots flag")
	}

	if dangleSnapshots && len(excludeList) == 0 {
		return fmt.Errorf("exclude-list parameter is required for dangling snapshots")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), dateFormat)

	var allSnapshots []opensearch.Snapshot

	if dangleSnapshots || index == "unknown" {
		allSnapshots, err = client.GetSnapshots(repo, "unknown*")
	} else {
		allSnapshots, err = client.GetSnapshots(repo, index+"*")
	}

	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	var snapshotsToDelete []string
	for _, snapshot := range allSnapshots {
		if shouldDeleteSnapshot(snapshot.Snapshot, index, dangleSnapshots, excludeList, wildcard, cutoffDate, dateFormat) {
			snapshotsToDelete = append(snapshotsToDelete, snapshot.Snapshot)
		}
	}

	if len(snapshotsToDelete) == 0 {
		logger.Info("No snapshots found for deletion")
		return nil
	}

	logger.Info("Found snapshots for deletion", "count", len(snapshotsToDelete))

	for _, snapshot := range snapshotsToDelete {
		if err := client.DeleteSnapshot(repo, snapshot); err != nil {
			logger.Error("Failed to delete snapshot", "snapshot", snapshot, "error", err)
			continue
		}

		logger.Info("Deleted snapshot", "snapshot", snapshot)
	}

	logger.Info("Snapshots deletion completed", "processed", len(snapshotsToDelete))
	return nil
}

func shouldDeleteSnapshot(snapshot, targetIndex string, dangleSnapshots bool, excludeList []string, wildcard bool, cutoffDate, dateFormat string) bool {
	if dangleSnapshots {
		return isDanglingSnapshot(snapshot, excludeList) && utils.IsOlderThanCutoff(snapshot, cutoffDate, dateFormat)
	}

	if !isSnapshotMatching(snapshot, targetIndex, wildcard) {
		return false
	}

	return utils.IsOlderThanCutoff(snapshot, cutoffDate, dateFormat)
}

func isSnapshotMatching(snapshot, targetIndex string, wildcard bool) bool {
	if len(snapshot) < len(targetIndex) {
		return false
	}

	if wildcard {
		return snapshot[:len(targetIndex)] == targetIndex
	}

	extractedDate := utils.ExtractDateFromIndex(snapshot, "%Y.%m.%d")
	if extractedDate == "" {
		return false
	}

	expectedPattern := targetIndex + "-" + extractedDate
	return len(snapshot) >= len(expectedPattern) && snapshot[:len(expectedPattern)] == expectedPattern
}

func isDanglingSnapshot(snapshot string, excludeList []string) bool {
	for _, exclude := range excludeList {
		if isSnapshotMatching(snapshot, exclude, true) {
			return false
		}
	}

	return true
}
