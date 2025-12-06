package utils

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"regexp"
	"strings"
)

func IndexInfosToNames(list []opensearch.IndexInfo) []string {
	names := make([]string, 0, len(list))
	for _, idx := range list {
		names = append(names, idx.Index)
	}
	return names
}

func IsSystem(indexConfig config.IndexConfig, nameOrPattern string) bool {
	if indexConfig.Kind == "regex" {
		return false
	}

	if indexConfig.System {
		return true
	}

	if indexConfig.Kind == "prefix" && strings.HasPrefix(nameOrPattern, ".") {
		return true
	}

	return false
}

func MatchesIndex(indexName string, indexConfig config.IndexConfig) bool {
	if !indexConfig.System && ShouldSkipIndex(indexName) {
		return false
	}

	isSystemIndex := IsSystem(indexConfig, indexName)
	isSystemConfig := IsSystem(indexConfig, indexConfig.Value)

	if isSystemConfig && !isSystemIndex {
		return false
	}

	if !isSystemConfig && isSystemIndex {
		return false
	}

	switch indexConfig.Kind {
	case "prefix":
		return strings.HasPrefix(indexName, indexConfig.Value)
	case "regex":
		matched, _ := regexp.MatchString(indexConfig.Value, indexName)
		return matched
	default:
		return false
	}
}

func ShouldSkipIndex(indexName string) bool {
	if strings.HasPrefix(indexName, ".") {
		return true
	}
	if strings.HasPrefix(indexName, "extracted_") {
		return true
	}
	return false
}

func ShouldSkipIndexRetention(indexName string) bool {
	return strings.HasPrefix(indexName, ".")
}

func FindMatchingIndexConfig(indexName string, indicesConfig []config.IndexConfig) *config.IndexConfig {
	for _, indexConfig := range indicesConfig {
		if MatchesIndex(indexName, indexConfig) {
			return &indexConfig
		}
	}
	return nil
}

func FilterUnknownIndices(indices []string) []string {
	var filtered []string
	for _, indexName := range indices {
		if !ShouldSkipIndex(indexName) {
			filtered = append(filtered, indexName)
		}
	}
	return filtered
}

func MatchesSnapshot(snapshotName string, indexConfig config.IndexConfig) bool {
	parts := strings.Split(snapshotName, "-")
	if len(parts) < 2 {
		return false
	}

	indexName := strings.Join(parts[:len(parts)-1], "-")

	return MatchesIndex(indexName, indexConfig)
}

func BatchDeleteIndices(client *opensearch.Client, indices []string, dryRun bool, logger *logging.Logger) ([]string, []string, error) {
	const batchSize = 10

	var successful []string
	var failed []string

	if dryRun {
		logger.Info(fmt.Sprintf("Dry run: would delete indices count=%d", len(indices)))
		return nil, nil, nil
	}

	for i := 0; i < len(indices); i += batchSize {
		end := i + batchSize
		if end > len(indices) {
			end = len(indices)
		}

		batch := indices[i:end]
		logger.Info(fmt.Sprintf("Deleting indices batch batch=%d indices=%v", i/batchSize+1, batch))

		err := client.DeleteIndices(batch)
		if err != nil {
			logger.Error(fmt.Sprintf("Failed to delete indices batch indices=%v error=%v", batch, err))
			failed = append(failed, batch...)
			continue
		}
		logger.Info(fmt.Sprintf("Indices batch deleted successfully indices=%v", batch))
		successful = append(successful, batch...)
	}

	return successful, failed, nil
}

func NormalizeTenantName(name string) string {
	return strings.ReplaceAll(name, "-", "")
}
