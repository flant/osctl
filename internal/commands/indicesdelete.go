package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"curator-go/internal/utils"
	"fmt"
	"regexp"
	"time"

	"github.com/spf13/cobra"
)

var indicesDeleteCmd = &cobra.Command{
	Use:   "indicesdelete",
	Short: "Delete indices",
	Long:  `Delete indices`,
	RunE:  runIndicesDelete,
}

func init() {
	indicesDeleteCmd.Flags().String("index", "", "Index name to delete")
	indicesDeleteCmd.Flags().Int("days", 30, "Number of days to keep indices")
	indicesDeleteCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")
	indicesDeleteCmd.Flags().Bool("unknown", false, "Delete unknown indices")
	indicesDeleteCmd.Flags().StringSlice("exclude-list", []string{}, "List of indices to exclude from unknown deletion")
	indicesDeleteCmd.Flags().String("kind", "prefix", "Matching kind: prefix or regex")

	addCommonFlags(indicesDeleteCmd)
}

func runIndicesDelete(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	index, _ := cmd.Flags().GetString("index")
	days, _ := cmd.Flags().GetInt("days")
	dateFormat, _ := cmd.Flags().GetString("date-format")
	unknown, _ := cmd.Flags().GetBool("unknown")
	excludeList, _ := cmd.Flags().GetStringSlice("exclude-list")
	kind, _ := cmd.Flags().GetString("kind")

	if index == "" && !unknown {
		return fmt.Errorf("index parameter is required or use --unknown flag")
	}

	if unknown && len(excludeList) == 0 {
		return fmt.Errorf("exclude-list parameter is required for unknown indices")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	cutoffDate := utils.FormatDate(time.Now().AddDate(0, 0, -days), dateFormat)

	var allIndices []string

	if unknown {
		allIndices, err = client.GetIndices("*")
	} else if kind == "regex" {
		allIndices, err = client.GetIndices("*")
	} else {
		allIndices, err = client.GetIndices(index + "*")
	}

	if err != nil {
		return fmt.Errorf("failed to get indices: %v", err)
	}

	var indicesToDelete []string
	for _, idx := range allIndices {
		if shouldDeleteIndex(idx, index, unknown, excludeList, kind, cutoffDate, dateFormat) {
			indicesToDelete = append(indicesToDelete, idx)
		}
	}

	if len(indicesToDelete) == 0 {
		logger.Info("No indices found for deletion")
		return nil
	}

	logger.Info("Found indices for deletion", "count", len(indicesToDelete))

	for _, idx := range indicesToDelete {
		if err := client.DeleteIndex(idx); err != nil {
			logger.Error("Failed to delete index", "index", idx, "error", err)
			continue
		}

		logger.Info("Deleted index", "index", idx)
	}

	logger.Info("Indices deletion completed", "processed", len(indicesToDelete))
	return nil
}

func shouldDeleteIndex(index, targetIndex string, unknown bool, excludeList []string, kind, cutoffDate, dateFormat string) bool {
	if unknown {
		return isUnknownIndex(index, excludeList) && utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
	}

	if !isIndexMatching(index, targetIndex, kind) {
		return false
	}

	return utils.IsOlderThanCutoff(index, cutoffDate, dateFormat)
}

func isIndexMatching(index, targetIndex string, kind string) bool {
	if kind == "regex" {
		matched, err := regexp.MatchString(targetIndex, index)
		return err == nil && matched
	}

	// prefix matching (default)
	if len(index) < len(targetIndex) {
		return false
	}
	return index[:len(targetIndex)] == targetIndex
}

func isUnknownIndex(index string, excludeList []string) bool {
	if len(index) < 1 {
		return false
	}

	if index[0] == '.' {
		return false
	}

	if len(index) >= 9 && (index[:9] == "restored-" || index[:9] == "extracted") {
		return false
	}

	for _, exclude := range excludeList {
		if isIndexMatching(index, exclude, "prefix") {
			return false
		}
	}

	return true
}
