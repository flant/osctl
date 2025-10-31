package commands

import (
	"fmt"
	"math"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var shardingCmd = &cobra.Command{
	Use:   "sharding",
	Short: "Automatically create index templates with optimal shard counts",
	Long: `Analyze current indices and create index templates with optimal shard counts
based on index size and cluster node count.`,
	RunE: runSharding,
}

func init() {
	addFlags(shardingCmd)
}

func runSharding(cmd *cobra.Command, args []string) error {
	cfg := config.GetCommandConfig(cmd)

	logger := logging.NewLogger()
	dryRun := cfg.GetDryRun()
	client, err := opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	exclude := cfg.ShardingExcludeRegex
	var excludeRe *regexp.Regexp
	if exclude != "" {
		excludeRe, _ = regexp.Compile(exclude)
	}
	targetGiB := cfg.GetShardingTargetSizeGiB()
	logger.Info(fmt.Sprintf("Sharding target size: %d GiB", targetGiB))
	targetBytes := int64(targetGiB) * 1024 * 1024 * 1024

	today := utils.FormatDate(time.Now(), cfg.DateFormat)
	indicesAll, err := client.GetIndicesWithFields("*", "index,ss")
	if err != nil {
		return err
	}
	indicesToday, err := client.GetIndicesWithFields(fmt.Sprintf("*-%s*,-.*", today), "index,ss", "ss")
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Indices discovered: total=%d, today=%d", len(indicesAll), len(indicesToday)))

	sizes := map[string]int64{}
	for _, ii := range indicesAll {
		if strings.HasPrefix(ii.Index, ".") {
			continue
		}
		if b, err := strconv.ParseInt(ii.Size, 10, 64); err == nil {
			sizes[ii.Index] = b
		}
	}

	dataNodes, err := client.GetDataNodeCount("")
	if err != nil {
		return err
	}

	for _, it := range indicesToday {
		name := it.Index
		if strings.HasPrefix(name, ".") {
			logger.Info(fmt.Sprintf("Skip system index %s", name))
			continue
		}
		base := name
		base = strings.TrimSuffix(base, today)
		pattern := base + "*"
		if excludeRe != nil && excludeRe.MatchString(pattern) {
			logger.Info(fmt.Sprintf("Skip excluded pattern %s", pattern))
			continue
		}
		dashCount := strings.Count(pattern, "-")
		priority := dashCount * 1000
		templateName := strings.Replace(name, today, "sharding", 1)

		maxSize := computeMaxSizeForPattern(sizes, base, it.Size)
		shards := computeShardCount(maxSize, targetBytes, dataNodes, name, logger)
		logger.Info(fmt.Sprintf("Evaluate pattern=%s template=%s maxSize=%dB targetBytes=%dB shards=%d dataNodes=%d priority=%d", pattern, templateName, maxSize, targetBytes, shards, dataNodes, priority))

		existing, err := client.FindIndexTemplateByPattern(pattern)
		if err != nil {
			return err
		}
		settings := map[string]any{
			"index": map[string]any{
				"number_of_shards":           shards,
				"number_of_replicas":         1,
				"mapping.total_fields.limit": 2000,
				"query.default_field":        []string{"message", "text", "log", "original_message"},
			},
		}
		template := map[string]any{
			"index_patterns": []string{pattern},
			"priority":       priority,
			"template": map[string]any{
				"settings": settings["index"],
			},
		}
		if existing == "" {
			if dryRun {
				logger.Info(fmt.Sprintf("DRY RUN: Would create index template %s for pattern %s with %d shards", templateName, pattern, shards))
			} else {
				logger.Info(fmt.Sprintf("Create index template %s for pattern %s with %d shards", templateName, pattern, shards))
				if err := client.PutIndexTemplate(templateName, template); err != nil {
					return err
				}
			}
		} else {
			if dryRun {
				logger.Info(fmt.Sprintf("DRY RUN: Would update existing template %s: set number_of_shards=%d", existing, shards))
			} else {
				logger.Info(fmt.Sprintf("Update existing template %s: set number_of_shards=%d", existing, shards))
				current := map[string]any{
					"template": map[string]any{
						"settings": map[string]any{
							"index": map[string]any{
								"number_of_shards": shards,
							},
						},
					},
				}
				if err := client.PutIndexTemplate(existing, current); err != nil {
					return err
				}
			}
		}
		if dryRun {
			logger.Info(fmt.Sprintf("DRY RUN: Would apply sharding template %s for pattern %s with %d shards", templateName, pattern, shards))
		} else {
			logger.Info(fmt.Sprintf("Applied sharding template %s for pattern %s with %d shards", templateName, pattern, shards))
		}
	}
	return nil
}

func computeMaxSizeForPattern(sizes map[string]int64, base string, todaySizeStr string) int64 {
	maxSize := int64(0)
	for idx, sz := range sizes {
		if strings.HasPrefix(idx, strings.TrimSuffix(base, "-")) {
			if sz > maxSize {
				maxSize = sz
			}
		}
	}
	if szToday, err := strconv.ParseInt(todaySizeStr, 10, 64); err == nil && szToday > maxSize {
		maxSize = szToday
	}
	return maxSize
}

func computeShardCount(maxSize int64, targetBytes int64, dataNodes int, indexName string, logger *logging.Logger) int {
	shards := 1
	if maxSize > targetBytes {
		shards = int(math.Floor(float64(maxSize)/float64(targetBytes))) + 1
		if shards > dataNodes {
			logger.Warn(fmt.Sprintf("Index %s needs %d primary shards, but cluster has %d data nodes. Reducing to %d", indexName, shards, dataNodes, dataNodes))
			shards = dataNodes
		}
	}
	return shards
}
