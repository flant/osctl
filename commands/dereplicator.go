package commands

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
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
	addFlags(dereplicatorCmd)
}

func runDereplicator(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()

	daysCount := cfg.GetDereplicatorDaysCount()
	dateFormat := cfg.GetDateFormat()
	useSnapshot := cfg.GetDereplicatorUseSnapshot()
	snapRepo := cfg.GetSnapshotRepo()
	dryRun := cfg.GetDryRun()

	if useSnapshot && snapRepo == "" {
		return fmt.Errorf("snap-repo parameter is required when use-snapshot is enabled")
	}

	logger := logging.NewLogger()
	logger.Info(fmt.Sprintf("Starting dereplication process daysCount=%d useSnapshot=%t snapRepo=%s dryRun=%t", daysCount, useSnapshot, snapRepo, dryRun))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	indices, err := client.GetIndicesWithFields("*", "index,rep")
	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	allNames := utils.IndexInfosToNames(indices)
	if len(allNames) > 0 {
		logger.Info(fmt.Sprintf("Found indices %s", strings.Join(allNames, ", ")))
	} else {
		logger.Info("Found indices none")
	}

	var targetIndices []string
	for _, idx := range indices {
		if shouldProcessIndex(idx.Index, idx.Rep, daysCount, dateFormat) {
			targetIndices = append(targetIndices, idx.Index)
		}
	}

	if len(targetIndices) == 0 {
		logger.Info("No indices to process")
		return nil
	}

	logger.Info(fmt.Sprintf("Indices to dereplicate %s", strings.Join(targetIndices, ", ")))

	var snapshots []opensearch.Snapshot
	if useSnapshot {
		snapshots, err = client.GetSnapshots(snapRepo, "*")
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %v", err)
		}
	}

	var successfulDereplications []string
	var problemIndices []string
	var skippedNoSnapshot []string
	for _, index := range targetIndices {
		if useSnapshot && !utils.HasValidSnapshot(index, snapshots) {
			logger.Warn(fmt.Sprintf("No valid snapshot found index=%s", index))
			skippedNoSnapshot = append(skippedNoSnapshot, index)
			continue
		}

		if dryRun {
			logger.Info(fmt.Sprintf("DRY RUN: Would set replicas to 0 index=%s", index))
			successfulDereplications = append(successfulDereplications, index)
			continue
		}

		if err := client.SetReplicas(index, 0); err != nil {
			logger.Error(fmt.Sprintf("Failed to set replicas index=%s error=%v", index, err))
			problemIndices = append(problemIndices, index)
		} else {
			logger.Info(fmt.Sprintf("Successfully set replicas to 0 index=%s", index))
			successfulDereplications = append(successfulDereplications, index)
		}
	}

	if !dryRun {
		fmt.Println("\n" + strings.Repeat("=", 60))
		fmt.Println("DEREPLICATOR SUMMARY")
		fmt.Println(strings.Repeat("=", 60))
		if len(successfulDereplications) > 0 {
			fmt.Printf("Successfully dereplicated: %d indices\n", len(successfulDereplications))
			for _, name := range successfulDereplications {
				fmt.Printf("  ✓ %s\n", name)
			}
		}
		if len(problemIndices) > 0 {
			fmt.Printf("\nFailed to dereplicate: %d indices\n", len(problemIndices))
			for _, name := range problemIndices {
				fmt.Printf("  ✗ %s\n", name)
			}
		}
		if len(skippedNoSnapshot) > 0 {
			fmt.Printf("\nSkipped (no valid snapshot): %d indices\n", len(skippedNoSnapshot))
			for _, name := range skippedNoSnapshot {
				fmt.Printf("  - %s\n", name)
			}
		}
		if len(successfulDereplications) == 0 && len(problemIndices) == 0 && len(skippedNoSnapshot) == 0 {
			fmt.Println("No indices were dereplicated")
		}
		fmt.Println(strings.Repeat("=", 60))
	}

	if len(skippedNoSnapshot) > 0 {
		logger.Warn(fmt.Sprintf("Skipped (no valid snapshot) %s", strings.Join(skippedNoSnapshot, ", ")))
	}

	if len(problemIndices) > 0 {
		logger.Warn(fmt.Sprintf("Problem indices %s", strings.Join(problemIndices, ", ")))
		return fmt.Errorf("failed to process %d indices", len(problemIndices))
	}

	logger.Info("Dereplicator completed successfully")
	return nil
}

func shouldProcessIndex(index, replicas string, daysCount int, dateFormat string) bool {
	if strings.HasPrefix(index, ".") {
		return false
	}

	if replicas == "0" {
		return false
	}

	cutoffDate := time.Now().AddDate(0, 0, -daysCount)
	cutoffDateStr := utils.FormatDate(cutoffDate, dateFormat)

	return utils.IsOlderThanCutoff(index, cutoffDateStr, dateFormat)
}
