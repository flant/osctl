package commands

import (
	"encoding/json"
	"fmt"
	"math"
	"osctl/pkg/config"
	"osctl/pkg/logging"
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
	cfg := config.GetConfig()

	logger := logging.NewLogger()
	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	exclude := cfg.GetShardingExcludeRegex()
	var excludeRe *regexp.Regexp
	if exclude != "" {
		excludeRe, _ = regexp.Compile(exclude)
	}
	targetGiB := cfg.GetShardingTargetSizeGiB()
	logger.Info(fmt.Sprintf("Sharding target size: %d GiB", targetGiB))
	targetBytes := int64(targetGiB) * 1024 * 1024 * 1024

	today := utils.FormatDate(time.Now(), cfg.GetDateFormat())
	indicesAll, err := client.GetIndicesWithFields("*", "index,pri.store.size")
	if err != nil {
		return err
	}
	indicesToday, err := client.GetIndicesWithFields(fmt.Sprintf("*-%s*,-.*", today), "index,pri.store.size", "pri.store.size")
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Indices discovered: total=%d, today=%d", len(indicesAll), len(indicesToday)))

	sizes := map[string]int64{}
	for _, ii := range indicesAll {
		if strings.HasPrefix(ii.Index, ".") {
			continue
		}
		if b, err := strconv.ParseInt(ii.PriStoreSize, 10, 64); err == nil {
			sizes[ii.Index] = b
		} else {
			logger.Info(fmt.Sprintf("DEBUG: Failed to parse size as int for index %s: %s", ii.Index, ii.PriStoreSize))
			sizes[ii.Index] = 0
		}
	}

	dataNodes, err := client.GetDataNodeCount("")
	if err != nil {
		return err
	}

	type templateChange struct {
		action      string
		template    string
		pattern     string
		shards      int
		replicas    int
		priority    int
		oldShards   int
		oldReplicas int
	}
	var changes []templateChange
	successfulChanges := make([]templateChange, 0)
	failedChanges := make([]templateChange, 0)

	type patternInfo struct {
		base       string
		pattern    string
		maxSize    int64
		maxSizeStr string
		indices    []string
		isHourly   bool
	}
	patterns := make(map[string]*patternInfo)

	for _, it := range indicesToday {
		name := it.Index
		if strings.HasPrefix(name, ".") {
			logger.Info(fmt.Sprintf("Skip system index %s", name))
			continue
		}
		base := name
		isHourly := false
		if pos := strings.LastIndex(name, today); pos >= 0 {
			restAfterDate := name[pos+len(today):]
			if len(restAfterDate) > 0 && strings.HasPrefix(restAfterDate, "-") {
				isHourly = true
			}
			base = strings.TrimSuffix(name[:pos], "-")
		} else {
			base = strings.TrimSuffix(base, today)
		}
		pattern := base + "-*"
		if excludeRe != nil && excludeRe.MatchString(pattern) {
			logger.Info(fmt.Sprintf("Skip excluded pattern %s", pattern))
			continue
		}
		if pi, ok := patterns[pattern]; ok {
			pi.indices = append(pi.indices, name)
			pi.isHourly = pi.isHourly || isHourly
			if sz, err := strconv.ParseInt(it.PriStoreSize, 10, 64); err == nil && sz > pi.maxSize {
				pi.maxSize = sz
				pi.maxSizeStr = it.PriStoreSize
			}
		} else {
			patterns[pattern] = &patternInfo{
				base:       base,
				pattern:    pattern,
				maxSize:    0,
				maxSizeStr: it.PriStoreSize,
				indices:    []string{name},
				isHourly:   isHourly,
			}
			if sz, err := strconv.ParseInt(it.PriStoreSize, 10, 64); err == nil {
				patterns[pattern].maxSize = sz
			}
		}
	}

	allTemplates, err := client.GetAllIndexTemplates()
	if err == nil {
		logger.Info(fmt.Sprintf("DEBUG: Found %d existing index templates", len(allTemplates.IndexTemplates)))
		for _, t := range allTemplates.IndexTemplates {
			patternsStr := strings.Join(t.IndexTemplate.IndexPatterns, ", ")
			logger.Info(fmt.Sprintf("DEBUG: Template=%s patterns=[%s] priority=%d", t.Name, patternsStr, t.IndexTemplate.Priority))
		}
	} else {
		logger.Info(fmt.Sprintf("DEBUG: Failed to get all templates: %v", err))
	}

	for pattern, pi := range patterns {
		dashCount := strings.Count(pattern, "-")
		priority := dashCount * 1000
		templateName := pi.base + "-sharding"

		maxSize := computeMaxSizeForPattern(sizes, pi.base, pi.maxSizeStr, pattern, logger)
		if maxSize < pi.maxSize {
			maxSize = pi.maxSize
		}
		shards := computeShardCount(maxSize, targetBytes, dataNodes, pi.indices[0], logger)
		logger.Info(fmt.Sprintf("Evaluate pattern=%s template=%s indices=%d maxSize=%dB targetBytes=%dB shards=%d dataNodes=%d priority=%d", pattern, templateName, len(pi.indices), maxSize, targetBytes, shards, dataNodes, priority))

		logger.Info(fmt.Sprintf("DEBUG: Checking for existing template with pattern=%s", pattern))
		normalizedPattern := strings.TrimSuffix(pattern, "*")
		logger.Info(fmt.Sprintf("DEBUG: normalizedPattern=%s", normalizedPattern))
		existing, err := client.FindIndexTemplateByPattern(pattern)
		if err != nil {
			return err
		}
		if existing != "" {
			logger.Info(fmt.Sprintf("DEBUG: Found existing template=%s for pattern=%s", existing, pattern))
		} else {
			logger.Info(fmt.Sprintf("DEBUG: No existing template found for pattern=%s (normalized: %s)", pattern, normalizedPattern))
		}
		replicas := 1
		if dataNodes <= 1 {
			replicas = 0
		}
		indexSettings := map[string]any{
			"number_of_shards":           shards,
			"number_of_replicas":         replicas,
			"mapping.total_fields.limit": 2000,
			"query.default_field":        []string{"message", "text", "log", "original_message"},
		}
		if cfg.GetShardingRoutingAllocationTemp() != "" {
			indexSettings["routing"] = map[string]any{
				"allocation": map[string]any{
					"require": map[string]any{
						"temp": cfg.GetShardingRoutingAllocationTemp(),
					},
				},
			}
		}
		settings := map[string]any{
			"index": indexSettings,
		}
		template := map[string]any{
			"index_patterns": []string{pattern},
			"priority":       priority,
			"template": map[string]any{
				"settings": settings["index"],
			},
		}
		if pi.isHourly {
			template["composed_of"] = []string{"default_template"}
		}
		if existing == "" {
			ch := templateChange{
				action:   "create",
				template: templateName,
				pattern:  pattern,
				shards:   shards,
				replicas: replicas,
				priority: priority,
			}
			if cfg.GetDryRun() {
				logger.Info(fmt.Sprintf("DRY RUN: Would create index template %s for pattern %s with shards=%d replicas=%d priority=%d", templateName, pattern, shards, replicas, priority))
				changes = append(changes, ch)
			} else {
				logger.Info(fmt.Sprintf("Create index template %s for pattern %s with %d shards", templateName, pattern, shards))
				if err := client.PutIndexTemplate(templateName, template); err != nil {
					logger.Error(fmt.Sprintf("Failed to create index template template=%s pattern=%s error=%v", templateName, pattern, err))
					failedChanges = append(failedChanges, ch)
					continue
				}
				successfulChanges = append(successfulChanges, ch)
			}
		} else {
			curShards := 1
			if tpl, err := client.GetIndexTemplate(existing); err == nil {
				if len(tpl.IndexTemplates) > 0 {
					if tset, ok := tpl.IndexTemplates[0].IndexTemplate.Template["settings"].(map[string]any); ok {
						if idx, ok := tset["index"].(map[string]any); ok {
							if v, ok := idx["number_of_shards"]; ok {
								logger.Info(fmt.Sprintf("DEBUG: Template %s current shards value: %v (type: %T)", existing, v, v))
								var s int
								switch val := v.(type) {
								case string:
									if parsed, err := strconv.Atoi(val); err == nil {
										s = parsed
									}
								case int:
									s = val
								case int64:
									s = int(val)
								case float64:
									s = int(val)
								default:
									if parsed, err := strconv.Atoi(fmt.Sprintf("%v", v)); err == nil {
										s = parsed
									}
								}
								if s > 0 {
									curShards = s
								}
							}
						}
					}
				}
			}
			logger.Info(fmt.Sprintf("DEBUG: Template %s: current shards=%d, target shards=%d", existing, curShards, shards))
			if curShards == shards {
				logger.Info(fmt.Sprintf("Template %s already has correct shards: %d", existing, shards))
				continue
			}
			ch := templateChange{
				action:      "update",
				template:    existing,
				pattern:     pattern,
				shards:      shards,
				replicas:    replicas,
				oldShards:   curShards,
				oldReplicas: replicas,
			}
			if cfg.GetDryRun() {
				logger.Info(fmt.Sprintf("DRY RUN: Would update template %s: shards %d to %d", existing, curShards, shards))
				changes = append(changes, ch)
			} else {
				logger.Info(fmt.Sprintf("Update existing template %s: set number_of_shards=%d", existing, shards))
				var current map[string]any
				if tpl, err := client.GetIndexTemplate(existing); err == nil && len(tpl.IndexTemplates) > 0 {
					it := tpl.IndexTemplates[0].IndexTemplate
					templateJSON, _ := json.Marshal(it)
					json.Unmarshal(templateJSON, &current)
					if settings, ok := current["template"].(map[string]any); ok {
						if settingsMap, ok := settings["settings"].(map[string]any); ok {
							if indexSettings, ok := settingsMap["index"].(map[string]any); ok {
								indexSettings["number_of_shards"] = shards
								if queryField, exists := indexSettings["query"]; exists {
									if queryMap, ok := queryField.(map[string]any); ok {
										queryMap["default_field"] = []string{"message", "text", "log", "original_message"}
									} else {
										indexSettings["query"] = map[string]any{
											"default_field": []string{"message", "text", "log", "original_message"},
										}
									}
								} else {
									indexSettings["query"] = map[string]any{
										"default_field": []string{"message", "text", "log", "original_message"},
									}
								}
							}
						}
					}
				} else {
					current = map[string]any{
						"index_patterns": []string{pattern},
						"priority":       priority,
						"template": map[string]any{
							"settings": map[string]any{
								"index": map[string]any{
									"number_of_shards": shards,
									"query": map[string]any{
										"default_field": []string{"message", "text", "log", "original_message"},
									},
								},
							},
						},
					}
				}
				if err := client.PutIndexTemplate(existing, current); err != nil {
					logger.Error(fmt.Sprintf("Failed to update index template template=%s pattern=%s error=%v", existing, pattern, err))
					failedChanges = append(failedChanges, ch)
					continue
				}
				successfulChanges = append(successfulChanges, ch)
			}
		}
	}

	if cfg.GetDryRun() && len(changes) > 0 {
		logger.Info("")
		logger.Info("DRY RUN SUMMARY")
		logger.Info("===============")
		logger.Info(fmt.Sprintf("Total templates to process: %d", len(changes)))
		logger.Info("")
		for _, ch := range changes {
			if ch.action == "create" {
				logger.Info(fmt.Sprintf("CREATE: template=%s pattern=%s shards=%d replicas=%d priority=%d", ch.template, ch.pattern, ch.shards, ch.replicas, ch.priority))
			} else {
				logger.Info(fmt.Sprintf("UPDATE: template=%s pattern=%s shards %d to %d", ch.template, ch.pattern, ch.oldShards, ch.shards))
			}
		}
		logger.Info("")
		return nil
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("SHARDING SUMMARY")
		logger.Info(strings.Repeat("=", 60))
		if len(successfulChanges) > 0 {
			logger.Info(fmt.Sprintf("Successfully changed: %d templates", len(successfulChanges)))
			for _, ch := range successfulChanges {
				if ch.action == "create" {
					logger.Info(fmt.Sprintf("  ✓ Created: %s (pattern=%s, shards=%d, replicas=%d)", ch.template, ch.pattern, ch.shards, ch.replicas))
				} else {
					logger.Info(fmt.Sprintf("  ✓ Updated: %s (pattern=%s, shards %d→%d)", ch.template, ch.pattern, ch.oldShards, ch.shards))
				}
			}
		}
		if len(failedChanges) > 0 {
			logger.Info("")
			logger.Info(fmt.Sprintf("Failed to change: %d templates", len(failedChanges)))
			for _, ch := range failedChanges {
				if ch.action == "create" {
					logger.Info(fmt.Sprintf("  ✗ Failed to create: %s (pattern=%s)", ch.template, ch.pattern))
				} else {
					logger.Info(fmt.Sprintf("  ✗ Failed to update: %s (pattern=%s)", ch.template, ch.pattern))
				}
			}
		}
		if len(successfulChanges) == 0 && len(failedChanges) == 0 {
			logger.Info("No templates were changed")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	return nil
}

func computeMaxSizeForPattern(sizes map[string]int64, base string, todaySizeStr string, pattern string, logger *logging.Logger) int64 {
	normalizedBase := strings.TrimSuffix(base, "-")
	searchPrefix := normalizedBase + "-"

	logger.Info(fmt.Sprintf("DEBUG computeMaxSize: base=%s normalizedBase=%s searchPrefix=%s pattern=%s", base, normalizedBase, searchPrefix, pattern))

	maxSize := int64(0)
	foundCount := 0
	var foundIndices []string
	for idx, sz := range sizes {
		if strings.HasPrefix(idx, searchPrefix) {
			foundCount++
			if len(foundIndices) < 5 {
				foundIndices = append(foundIndices, fmt.Sprintf("%s(%dB)", idx, sz))
			}
			if sz > maxSize {
				maxSize = sz
			}
		}
	}
	if len(foundIndices) >= 5 {
		foundIndices = append(foundIndices, fmt.Sprintf("... and %d more", foundCount-5))
	}
	logger.Info(fmt.Sprintf("DEBUG computeMaxSize: pattern=%s foundCount=%d maxSize=%dB foundIndices=%v", pattern, foundCount, maxSize, foundIndices))

	var szToday int64
	if b, err := strconv.ParseInt(todaySizeStr, 10, 64); err == nil {
		szToday = b
	} else {
		logger.Info(fmt.Sprintf("DEBUG computeMaxSize: pattern=%s todaySizeStr=%s parse error=%v", pattern, todaySizeStr, err))
		szToday = 0
	}
	if szToday > maxSize {
		logger.Info(fmt.Sprintf("DEBUG computeMaxSize: pattern=%s todaySizeStr=%s todaySize=%dB (using today size)", pattern, todaySizeStr, szToday))
		maxSize = szToday
	} else if szToday == 0 && todaySizeStr != "" && todaySizeStr != "0" {
		logger.Info(fmt.Sprintf("DEBUG computeMaxSize: pattern=%s todaySizeStr=%s could not parse size", pattern, todaySizeStr))
	}

	logger.Info(fmt.Sprintf("DEBUG computeMaxSize: pattern=%s final maxSize=%dB", pattern, maxSize))
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
