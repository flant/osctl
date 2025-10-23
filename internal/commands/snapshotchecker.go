package commands

import (
	"osctl/internal/logging"
	"osctl/internal/madison"
	"osctl/internal/opensearch"
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
	snapshotCheckerCmd.Flags().String("madison-key", "", "Madison API key")
	snapshotCheckerCmd.Flags().String("madison-project", "", "Madison project name")
	snapshotCheckerCmd.Flags().String("kibana-host", "", "Kibana host for alert labels")
	snapshotCheckerCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")

	addCommonFlags(snapshotCheckerCmd)
}

func runSnapshotChecker(cmd *cobra.Command, args []string) error {

	snapRepo, _ := cmd.Flags().GetString("snap-repo")
	whitelist, _ := cmd.Flags().GetString("whitelist")
	excludeList, _ := cmd.Flags().GetString("exclude-list")
	madisonKey, _ := cmd.Flags().GetString("madison-key")
	madisonProject, _ := cmd.Flags().GetString("madison-project")
	kibanaHost, _ := cmd.Flags().GetString("kibana-host")
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")

	if snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required")
	}

	logger := logging.NewLogger()

	client := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)

	targetDate := time.Now().AddDate(0, 0, -2).Format("2006.01.02")
	pattern := fmt.Sprintf("*%s*", targetDate)

	indices, err := client.GetIndices(pattern)
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	logger.Info("Found target indices", "count", len(indices), "date", targetDate)

	targetIndices := filterIndices(indices, excludeList, whitelist, logger)
	logger.Info("Filtered target indices", "count", len(targetIndices))

	snapshotDate := time.Now().AddDate(0, 0, -1).Format("2006.01.02")
	snapshotPattern := fmt.Sprintf("*%s*", snapshotDate)

	snapshots, err := client.GetSnapshots(snapRepo, snapshotPattern)
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	logger.Info("Found snapshots", "count", len(snapshots), "date", snapshotDate)

	snapshotIndices := make(map[string]bool)
	for _, snapshot := range snapshots {
		if snapshot.State == "SUCCESS" {
			for _, index := range snapshot.Indices {
				snapshotIndices[index] = true
			}
		}
	}

	missingSnapshots := []string{}
	for _, index := range targetIndices {
		if !snapshotIndices[index] {
			missingSnapshots = append(missingSnapshots, index)
			logger.Warn("Missing snapshot for index", "index", index)
		}
	}

	if len(missingSnapshots) == 0 {
		logger.Info("All target indices have snapshots")
		return nil
	}

	logger.Warn("Found indices without snapshots", "count", len(missingSnapshots), "indices", missingSnapshots)

	if madisonKey != "" && madisonProject != "" {
		madisonClient := madison.NewClient(madisonKey, madisonProject, kibanaHost)
		if err := madisonClient.SendSnapshotMissingAlert(missingSnapshots); err != nil {
			logger.Error("Failed to send Madison alert", "error", err)
			return err
		}
		logger.Info("Successfully sent Madison alert", "count", len(missingSnapshots))
	} else {
		logger.Info("Madison key or project not configured, skipping alert")
	}

	return nil
}

func filterIndices(indices []string, excludeList, whitelist string, logger *logging.Logger) []string {
	var result []string
	var excludePrefixes, whitelistPrefixes []string

	if excludeList != "" {
		excludePrefixes = strings.Split(excludeList, ",")
		for i, prefix := range excludePrefixes {
			excludePrefixes[i] = strings.TrimSpace(prefix)
		}
	}

	if whitelist != "" {
		whitelistPrefixes = strings.Split(whitelist, ",")
		for i, prefix := range whitelistPrefixes {
			whitelistPrefixes[i] = strings.TrimSpace(prefix)
		}
	}

	useWhitelist := len(whitelistPrefixes) > 0 && whitelistPrefixes[0] != ""
	logger.Info("Using filter mode", "mode", map[bool]string{true: "whitelist", false: "exclude"}[useWhitelist])

	for _, index := range indices {

		if strings.HasPrefix(index, ".") {
			continue
		}

		if useWhitelist {

			whitelisted := false
			for _, prefix := range whitelistPrefixes {
				if prefix != "" && strings.HasPrefix(index, prefix) {
					whitelisted = true
					break
				}
			}
			if whitelisted {
				result = append(result, index)
			}
		} else {

			excluded := false
			for _, prefix := range excludePrefixes {
				if prefix != "" && strings.HasPrefix(index, prefix) {
					excluded = true
					break
				}
			}
			if !excluded {
				result = append(result, index)
			}
		}
	}

	return result
}
