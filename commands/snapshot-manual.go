package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var snapshotManualCmd = &cobra.Command{
	Use:   "snapshot-manual",
	Short: "Create manual snapshots for specific indices",
	Long:  `Create snapshots for indices with manual_snapshot flag enabled`,
	RunE:  runSnapshotManual,
}

func init() {
	addFlags(snapshotManualCmd)
}

func runSnapshotManual(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()

	kind := cfg.SnapshotManualKind
	value := cfg.SnapshotManualValue
	name := cfg.SnapshotManualName
	system := cfg.GetSnapshotManualSystem()

	if value == "" {
		return fmt.Errorf("value is required")
	}

	if kind == "regex" && name == "" {
		return fmt.Errorf("name is required for regex patterns")
	}

	logger.Info(fmt.Sprintf("Starting manual snapshot creation kind=%s value=%s name=%s system=%t", kind, value, name, system))

	client, err := utils.NewOSClientFromConfig(cfg)
	if err != nil {
		return err
	}

	var madisonClient *alerts.Client
	if cfg.MadisonKey != "" && cfg.OSDURL != "" && cfg.MadisonURL != "" {
		madisonClient = alerts.NewMadisonClient(cfg.MadisonKey, cfg.OSDURL, cfg.MadisonURL)
	}

	yesterday := utils.FormatDate(time.Now().AddDate(0, 0, -1), cfg.DateFormat)
	today := utils.FormatDate(time.Now(), cfg.DateFormat)

	var allIndices []opensearch.IndexInfo

	if system {
		allIndices, err = client.GetIndicesWithFields(".*", "index,ss", "ss:desc")
	} else {
		allIndices, err = client.GetIndicesWithFields("*"+yesterday+"*", "index,ss", "ss:desc")
	}
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	allNames := utils.IndexInfosToNames(allIndices)
	if len(allNames) > 0 {
		logger.Info(fmt.Sprintf("Found indices %s", strings.Join(allNames, ", ")))
	} else {
		logger.Info("Found indices none")
	}

	var matchingIndices []string
	for _, idx := range allIndices {
		indexConfig := config.IndexConfig{
			Kind:   kind,
			Value:  value,
			Name:   name,
			System: system,
		}
		if utils.MatchesIndex(idx.Index, indexConfig) {
			matchingIndices = append(matchingIndices, idx.Index)
		}
	}

	if len(matchingIndices) == 0 {
		logger.Info("No matching indices found")
		return nil
	}

	logger.Info(fmt.Sprintf("Matched indices %s", strings.Join(matchingIndices, ", ")))

	snapshotName := utils.BuildSnapshotName(kind, name, value, today)

	repoToUse := cfg.SnapshotRepo
	if cfg.SnapshotManualRepo != "" {
		repoToUse = cfg.SnapshotManualRepo
	}

	if cfg.GetDryRun() {
		if state, ok, _ := utils.CheckSnapshotStateInRepo(client, repoToUse, snapshotName); ok && state == "SUCCESS" {
			logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", snapshotName))
			return nil
		}
		if state, ok, _ := utils.CheckSnapshotStateInRepo(client, repoToUse, snapshotName); ok && state == "IN_PROGRESS" {
			logger.Info(fmt.Sprintf("Snapshot is currently IN_PROGRESS snapshot=%s repo=%s", snapshotName, repoToUse))
			return nil
		}
		fmt.Println("\nDRY RUN: Manual snapshot creation plan")
		fmt.Println("=" + strings.Repeat("=", 50))

		fmt.Printf("\nSnapshot (repo %s): %s\n", repoToUse, snapshotName)
		fmt.Printf("Pattern: %s (%s)\n", value, kind)
		fmt.Printf("Indices (%d):\n", len(matchingIndices))

		for _, index := range matchingIndices {
			fmt.Printf("  %s\n", index)
		}

		fmt.Printf("\nDRY RUN: Would create 1 manual snapshot\n")
		return nil
	}

	err = utils.WaitForSnapshotCompletion(client, logger, "", repoToUse)
	if err != nil {
		return fmt.Errorf("failed to wait for snapshot completion: %v", err)
	}

	err = utils.WaitForSnapshotTasks(client, logger, "", repoToUse)
	if err != nil {
		return fmt.Errorf("failed to wait for snapshot tasks: %v", err)
	}

	allSnapshots, err := utils.GetSnapshotsIgnore404(client, repoToUse, "*"+today+"*")
	if err != nil {
		return fmt.Errorf("failed to get snapshots: %v", err)
	}

	existNames := utils.SnapshotsToNames(allSnapshots)
	if len(existNames) > 0 {
		logger.Info(fmt.Sprintf("Existing snapshots today %s", strings.Join(existNames, ", ")))
	} else {
		logger.Info("Existing snapshots today none")
	}

	exists, err := utils.CheckAndCleanSnapshot(snapshotName, strings.Join(matchingIndices, ","), allSnapshots, client, repoToUse, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to check/clean snapshot snapshot=%s error=%v", snapshotName, err))
		return err
	}

	if exists {
		logger.Info(fmt.Sprintf("Valid snapshot already exists snapshot=%s", snapshotName))
		return nil
	}

	indicesStr := strings.Join(matchingIndices, ",")
	logger.Info(fmt.Sprintf("Creating snapshot %s", snapshotName))
	logger.Info(fmt.Sprintf("Snapshot indices %s", indicesStr))
	err = utils.CreateSnapshotWithRetry(client, snapshotName, indicesStr, repoToUse, madisonClient, logger)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create snapshot after retries snapshot=%s error=%v", snapshotName, err))
		return err
	}

	logger.Info("Manual snapshot creation completed")
	return nil
}
