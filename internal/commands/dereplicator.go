package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var dereplicatorCmd = &cobra.Command{
	Use:   "dereplicator",
	Short: "Reduce replicas for old indices",
	Long: `Reduce replicas to 0 for indices older than specified days.
Optionally checks for snapshots before reducing replicas.`,
	RunE: runDereplicator,
}

func init() {
	dereplicatorCmd.Flags().Int("days-count", 2, "Number of days to keep with replicas")
	dereplicatorCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")
	dereplicatorCmd.Flags().Bool("use-snapshot", false, "Check for snapshots before reducing replicas")
	dereplicatorCmd.Flags().String("snap-repo", "", "Snapshot repository name")

	addCommonFlags(dereplicatorCmd)
}

func runDereplicator(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	daysCount, _ := cmd.Flags().GetInt("days-count")
	dateFormat, _ := cmd.Flags().GetString("date-format")
	useSnapshot, _ := cmd.Flags().GetBool("use-snapshot")
	snapRepo, _ := cmd.Flags().GetString("snap-repo")

	if useSnapshot && snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required when use-snapshot is enabled")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	excludeDays := getExcludeDays(daysCount, dateFormat)
	logger.Info("Excluding days", "days", excludeDays)

	indices, err := client.GetIndicesWithReplicas("*")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	var targetIndices []string
	for _, idx := range indices {
		if shouldProcessIndex(idx.Index, idx.Rep, excludeDays) {
			targetIndices = append(targetIndices, idx.Index)
		}
	}

	if len(targetIndices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	var snapshots []opensearch.Snapshot
	if useSnapshot {
		snapshots, err = client.GetSnapshots(snapRepo, "*")
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %v", err)
		}
	}

	var problemIndices []string
	for _, index := range targetIndices {
		if useSnapshot && !hasValidSnapshot(index, snapshots) {
			logger.Warn("No valid snapshot found", "index", index)
			problemIndices = append(problemIndices, index)
			continue
		}

		if err := client.SetReplicas(index, 0); err != nil {
			logger.Error("Failed to set replicas", "index", index, "error", err)
			problemIndices = append(problemIndices, index)
		} else {
			logger.Info("Successfully set replicas to 0", "index", index)
		}
	}

	if len(problemIndices) > 0 {
		logger.Warn("Problem indices", "indices", problemIndices)
		return fmt.Errorf("failed to process %d indices", len(problemIndices))
	}

	logger.Info("Dereplicator completed successfully")
	return nil
}

func getExcludeDays(daysCount int, dateFormat string) []string {
	var days []string
	for i := 0; i < daysCount; i++ {
		day := time.Now().AddDate(0, 0, -i)
		days = append(days, day.Format(dateFormat))
	}
	return days
}

func shouldProcessIndex(index, replicas string, excludeDays []string) bool {
	if strings.HasPrefix(index, ".") {
		return false
	}
	if replicas == "0" {
		return false
	}
	for _, day := range excludeDays {
		if strings.Contains(index, day) {
			return false
		}
	}
	return true
}

func hasValidSnapshot(index string, snapshots []opensearch.Snapshot) bool {
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
