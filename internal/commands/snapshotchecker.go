package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/madison"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotCheckerCmd = &cobra.Command{
	Use:   "snapshotchecker",
	Short: "Check for missing snapshots and send alerts",
	Long: `Check for missing snapshots of indices and send alerts to Madison.
Supports both whitelist and exclude list modes.`,
	RunE: runSnapshotChecker,
}

func init() {
	snapshotCheckerCmd.Flags().String("snap-repo", "", "Snapshot repository name")
	snapshotCheckerCmd.Flags().String("whitelist", "", "Comma-separated list of index prefixes to include")
	snapshotCheckerCmd.Flags().String("exclude-list", "", "Comma-separated list of index prefixes to exclude")

	addCommonFlags(snapshotCheckerCmd)
}

func runSnapshotChecker(cmd *cobra.Command, args []string) error {
	snapRepo, _ := cmd.Flags().GetString("snap-repo")
	whitelist, _ := cmd.Flags().GetString("whitelist")
	excludeList, _ := cmd.Flags().GetString("exclude-list")
	madisonKey, _ := cmd.Flags().GetString("madison-key")
	madisonProject, _ := cmd.Flags().GetString("madison-project")
	osdURL, _ := cmd.Flags().GetString("osd-url")
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	madisonURL, _ := cmd.Flags().GetString("madison-url")
	dateFormat, _ := cmd.Flags().GetString("date-format")

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}
	if madisonKey == "" || madisonProject == "" || osdURL == "" || madisonURL == "" {
		return fmt.Errorf("madison-key, madison-project, osd-url and madison-url parameters are required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	targetDate := utils.FormatDate(time.Now().AddDate(0, 0, -2), dateFormat)
	indices, err := client.GetIndices(fmt.Sprintf("*%s*", targetDate))
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	targetIndices := filterIndices(indices, excludeList, whitelist)
	if len(targetIndices) == 0 {
		logger.Info("No target indices found")
		return nil
	}

	snapshotDate := utils.FormatDate(time.Now().AddDate(0, 0, -1), dateFormat)
	snapshots, err := client.GetSnapshots(snapRepo, fmt.Sprintf("*%s*", snapshotDate))
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	snapshotIndices := make(map[string]bool)
	for _, snapshot := range snapshots {
		if snapshot.State == "SUCCESS" {
			for _, index := range snapshot.Indices {
				snapshotIndices[index] = true
			}
		}
	}

	var missingSnapshots []string
	for _, index := range targetIndices {
		if !snapshotIndices[index] {
			missingSnapshots = append(missingSnapshots, index)
		}
	}

	if len(missingSnapshots) == 0 {
		logger.Info("All target indices have snapshots")
		return nil
	}

	madisonClient := madison.NewClient(madisonKey, madisonProject, osdURL, madisonURL)
	if err := madisonClient.SendSnapshotMissingAlert(missingSnapshots); err != nil {
		return fmt.Errorf("failed to send Madison alert: %v", err)
	}

	logger.Info("Sent Madison alert", "missingCount", len(missingSnapshots))
	return nil
}

func filterIndices(indices []string, excludeList, whitelist string) []string {
	var result []string

	for _, index := range indices {
		if strings.HasPrefix(index, ".") {
			continue
		}

		if whitelist != "" {
			if hasPrefix(index, whitelist) {
				result = append(result, index)
			}
		} else if excludeList != "" {
			if !hasPrefix(index, excludeList) {
				result = append(result, index)
			}
		} else {
			result = append(result, index)
		}
	}

	return result
}

func hasPrefix(index, prefixList string) bool {
	prefixes := strings.Split(prefixList, ",")
	for _, prefix := range prefixes {
		if strings.HasPrefix(index, strings.TrimSpace(prefix)) {
			return true
		}
	}
	return false
}
