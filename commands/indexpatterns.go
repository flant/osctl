package commands

import (
	"fmt"
	"os"
	"osctl/pkg/config"
	"osctl/pkg/kibana"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/spf13/cobra"
)

var indexPatternsCmd = &cobra.Command{
	Use:   "indexpatterns",
	Short: "Manage Kibana index patterns",
	Long: `Create and manage Kibana index patterns for discovered indices.
Supports both multitenancy and single-tenant modes.`,
	RunE: runIndexPatterns,
}

func init() {
	addFlags(indexPatternsCmd)
}

func runIndexPatterns(cmd *cobra.Command, args []string) error {
	var refreshedPatterns []string
	var failedRefreshedPatterns []string

	cfg := config.GetConfig()

	if cfg.GetOSDURL() == "" {
		return fmt.Errorf("osd-url parameter is required")
	}
	logger := logging.NewLogger()
	osClient, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	if cfg.GetIndexPatternsRefreshEnabled() {

		user := cfg.GetKibanaUser()
		pass := cfg.GetKibanaPass()
		_, _, existingIdsTitiles, err := getExistingIndexPatternTitles(osClient, ".kibana*")
		if err != nil {
			return err
		}
		kb := kibana.NewClient(utils.NormalizeURL(cfg.GetOSDURL()), user, pass, cfg.GetTimeout())
		logger.Info(fmt.Sprintf("Will refresh %d existing index-patterns", len(existingIdsTitiles)))
		for ip_id, ip_title := range existingIdsTitiles {
			if ip_title == "*" {
				logger.Warn("index-pattern '*' is too general and will be ignored")
				continue
			}
			indices, err := osClient.GetIndicesWithFields(ip_title, "index")
			if err != nil {
				logger.Warn(fmt.Sprintf("Failed to check indices for pattern %s: %v, will skip refresh", ip_title, err))
				continue
			}
			if len(indices) == 0 {
				logger.Info(fmt.Sprintf("Skipping index-pattern %s:%s - no matching indices found in cluster", ip_id, ip_title))
				continue
			}
			exists, err := kb.CheckIndexPatternExists(ip_id)
			if err != nil {
				logger.Warn(fmt.Sprintf("Failed to check if index-pattern exists %s:%s: %v, will try to refresh anyway", ip_id, ip_title, err))
			} else if !exists {
				logger.Info(fmt.Sprintf("Index-pattern %s:%s not found in Kibana, skipping refresh", ip_id, ip_title))
				continue
			}
			if cfg.GetDryRun() {
				logger.Info(fmt.Sprintf("DRY RUN: Would refresh index-pattern %s:%s (matches %d indices)", ip_id, ip_title, len(indices)))
				refreshedPatterns = append(refreshedPatterns, ip_title)
			} else {
				logger.Info(fmt.Sprintf("Refreshing index-pattern %s:%s (matches %d indices)", ip_id, ip_title, len(indices)))
				if err := kb.RefreshIndexPattern(ip_id, ip_title); err == nil {
					logger.Info(fmt.Sprintf("Successfully refreshed index-pattern %s:%s", ip_id, ip_title))
					refreshedPatterns = append(refreshedPatterns, ip_title)
				} else {
					logger.Error(fmt.Sprintf("Failed to refresh index-pattern %s:%s: %s", ip_id, ip_title, err))
					failedRefreshedPatterns = append(failedRefreshedPatterns, ip_title)
				}
			}
		}

	}
	var createdPatterns []string

	if cfg.GetIndexPatternsKibanaMultitenancy() {
		if cfg.GetIndexPatternsRecovererEnabled() {
			return fmt.Errorf("recoverer_enabled must be false in multitenancy mode")
		}
		if cfg.GetKibanaIndexRegex() != "" {
			logger.Info("kibana_index_regex is ignored in multitenancy mode")
		}
		tf, err := config.GetConfig().GetTenantsConfig()
		if err != nil {
			return err
		}
		for _, t := range tf.Tenants {
			normalizedName := utils.NormalizeTenantName(t.Name)
			aliasPattern := ".kibana*_" + normalizedName
			aliases, err := osClient.GetAliases(aliasPattern)
			if err != nil {
				return err
			}
			tenantIndex := ""
			if len(aliases) > 0 {
				tenantIndex = aliases[0].Alias
			}
			if tenantIndex == "" {
				logger.Info(fmt.Sprintf("Skip tenant %s: .kibana alias not found", t.Name))
				continue
			}
			existing, existingTitles, _, err := getExistingIndexPatternTitles(osClient, tenantIndex)
			if err != nil {
				return err
			}
			logger.Info(fmt.Sprintf("Tenant %s existing index patterns (%d): %s", t.Name, len(existingTitles), strings.Join(existingTitles, ", ")))
			toCreate := []string{}
			seen := map[string]struct{}{}
			logger.Info(fmt.Sprintf("Tenant %s: checking patterns from config (%d): %s", t.Name, len(t.Patterns), strings.Join(t.Patterns, ", ")))
			for _, p := range t.Patterns {
				pp := strings.TrimSpace(p)
				if pp == "" {
					continue
				}
				if _, ok := seen[pp]; ok {
					logger.Info(fmt.Sprintf("Tenant %s: pattern %s already seen, skipping", t.Name, pp))
					continue
				}
				seen[pp] = struct{}{}
				if _, ok := existing[pp]; !ok {
					logger.Info(fmt.Sprintf("Tenant %s: pattern %s not found in existing, will create", t.Name, pp))
					toCreate = append(toCreate, pp)
				} else {
					logger.Info(fmt.Sprintf("Tenant %s: pattern %s already exists, skipping", t.Name, pp))
				}
			}
			if len(toCreate) == 0 {
				logger.Info(fmt.Sprintf("Tenant %s: no new index patterns to create", t.Name))
			} else {
				logger.Info(fmt.Sprintf("Tenant %s: will create index patterns: %s", t.Name, strings.Join(toCreate, ", ")))
			}
			for _, p := range toCreate {
				payload := map[string]any{
					"type": "index-pattern",
					"index-pattern": map[string]any{
						"title":         p,
						"timeFieldName": "@timestamp",
					},
				}
				id := fmt.Sprintf("index-pattern:%s", uuid.NewString())
				if cfg.GetDryRun() {
					logger.Info(fmt.Sprintf("DRY RUN: Would create index pattern %s in tenant %s", p, t.Name))
					createdPatterns = append(createdPatterns, fmt.Sprintf("%s (tenant=%s)", p, t.Name))
					continue
				}
				if err := osClient.CreateDoc(tenantIndex, id, payload); err != nil {
					return err
				}
				logger.Info(fmt.Sprintf("Created index pattern %s in tenant %s", p, t.Name))
				createdPatterns = append(createdPatterns, fmt.Sprintf("%s (tenant=%s)", p, t.Name))
			}
		}
	} else {
		if cfg.GetKibanaIndexRegex() == "" {
			return fmt.Errorf("kibana-index-regex must be provided in single-tenant mode")
		}
		if p := config.GetConfig().GetOSCTLTenantsConfig(); p != "" {
			if _, err := os.Stat(p); err == nil {
				logger.Info("kibana-tenants-config is ignored in single-tenant mode")
			}
		}

		re := regexp.MustCompile(cfg.GetKibanaIndexRegex())
		today := utils.FormatDate(time.Now(), cfg.GetDateFormat())
		idxToday, err := osClient.GetIndicesWithFields(fmt.Sprintf("*-%s*,-.*", today), "index", "i")
		if err != nil {
			return err
		}
		needed := []string{}
		for _, ii := range idxToday {
			m := re.FindStringSubmatch(ii.Index)
			if len(m) > 1 {
				needed = append(needed, m[1]+"-*")
			}
		}

		logger.Info(fmt.Sprintf("Required patterns (%d): %s", len(needed), strings.Join(needed, ", ")))
		existing, existingTitles, _, err := getExistingIndexPatternTitles(osClient, ".kibana")
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Existing index patterns in .kibana (%d): %s", len(existingTitles), strings.Join(existingTitles, ", ")))
		toCreate := []string{}
		seen := map[string]struct{}{}
		for _, p := range needed {
			if _, ok := seen[p]; ok {
				continue
			}
			seen[p] = struct{}{}
			if _, ok := existing[p]; !ok {
				toCreate = append(toCreate, p)
			}
		}
		if len(toCreate) == 0 {
			logger.Info("No new index patterns to create in single-tenant mode")
		} else {
			logger.Info(fmt.Sprintf("Will create index patterns: %s", strings.Join(toCreate, ", ")))
		}
		for _, p := range toCreate {
			payload := map[string]any{
				"type": "index-pattern",
				"index-pattern": map[string]any{
					"title":         p,
					"timeFieldName": "@timestamp",
				},
			}
			id := fmt.Sprintf("index-pattern:%s", uuid.NewString())
			if cfg.GetDryRun() {
				logger.Info(fmt.Sprintf("DRY RUN: Would create index pattern %s", p))
				createdPatterns = append(createdPatterns, p)
				continue
			}
			if err := osClient.CreateDoc(".kibana", id, payload); err != nil {
				return err
			}
			logger.Info(fmt.Sprintf("Created index pattern %s", p))
			createdPatterns = append(createdPatterns, p)
		}
		if cfg.GetIndexPatternsRecovererEnabled() {
			frDS, err := osClient.Search(".kibana", "q=type=data-source&size=1000")
			if err == nil {
				var dsId string
				for _, h := range frDS.Hits.Hits {
					if src, ok := h.Source["data-source"].(map[string]any); ok {
						if t, ok := src["title"].(string); ok && t == config.GetConfig().GetDataSourceName() {
							dsId = strings.TrimPrefix(h.ID, "data-source:")
							break
						}
					}
				}
				if dsId != "" {
					logger.Info(fmt.Sprintf("Found data-source reference id=%s for title=%s", dsId, config.GetConfig().GetDataSourceName()))
					payload := map[string]any{
						"type": "index-pattern",
						"index-pattern": map[string]any{
							"title":         "extracted_*",
							"timeFieldName": "@timestamp",
						},
						"references": []map[string]string{{
							"id":   dsId,
							"type": "data-source",
							"name": "dataSource",
						}},
					}
					if cfg.GetDryRun() {
						logger.Info("DRY RUN: Would create index pattern extracted_* with data-source reference")
						createdPatterns = append(createdPatterns, "extracted_*")
					} else if err := osClient.CreateDoc(".kibana", "index-pattern:recoverer-extracted", payload); err == nil {
						logger.Info("Created index pattern extracted_* with data-source reference")
						createdPatterns = append(createdPatterns, "extracted_*")
					}
				}
			}
		}
	}

	if !cfg.GetDryRun() {
		logger.Info(strings.Repeat("=", 60))
		logger.Info("INDEX PATTERNS SUMMARY")
		logger.Info(strings.Repeat("=", 60))

		if len(refreshedPatterns) > 0 {
			logger.Info(fmt.Sprintf("Refreshed: %d index patterns", len(refreshedPatterns)))
			for _, name := range refreshedPatterns {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		} else {
			logger.Info("No index patterns were refreshed")
		}

		if len(failedRefreshedPatterns) > 0 {
			logger.Info(fmt.Sprintf("Failed while refresh: %d index patterns", len(failedRefreshedPatterns)))
			for _, name := range failedRefreshedPatterns {
				logger.Info(fmt.Sprintf("  ✕ %s", name))
			}
		}

		if len(createdPatterns) > 0 {
			logger.Info(fmt.Sprintf("Created: %d index patterns", len(createdPatterns)))
			for _, name := range createdPatterns {
				logger.Info(fmt.Sprintf("  ✓ %s", name))
			}
		} else {
			logger.Info("No index patterns were added")
		}
		logger.Info(strings.Repeat("=", 60))
	}

	return nil
}

func getExistingIndexPatternTitles(osClient *opensearch.Client, index string) (map[string]struct{}, []string, map[string]string, error) {
	sr, err := osClient.Search(index, "q=type:index-pattern&size=1000")
	if err != nil {
		return nil, nil, nil, err
	}
	existing := map[string]struct{}{}
	exist_map_id_title := map[string]string{}
	titles := []string{}
	for _, h := range sr.Hits.Hits {
		if src, ok := h.Source["index-pattern"].(map[string]any); ok {
			if t, ok := src["title"].(string); ok {
				if _, seen := existing[t]; !seen {
					existing[t] = struct{}{}
					titles = append(titles, t)

				}
				if _, seen := exist_map_id_title[h.ID]; !seen {
					p_id := strings.Split(h.ID, ":")
					exist_map_id_title[p_id[1]] = t
				}
			}
		}
	}
	return existing, titles, exist_map_id_title, nil
}
