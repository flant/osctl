package commands

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"osctl/pkg/utils"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// Every settings update is a cluster state change, and a too long request URL is
// rejected as a too long frame (the default HTTP line limit is 4 KiB).
const (
	translogBatchSize   = 50
	translogBatchLength = 3000
)

var translogCmd = &cobra.Command{
	Use:   "translog",
	Short: "Enable async translog for indices and index templates",
	Long: `Switch every index of the cluster to an asynchronous translog
(index.translog.durability=async with a fixed sync_interval) and add the same
settings to every existing index template, so newly created indices are async
from the start. The command only adds settings and never turns them back to
request durability.`,
	RunE: runTranslog,
}

func init() {
	addFlags(translogCmd)
}

type translogNameFilter struct {
	include *regexp.Regexp
	exclude *regexp.Regexp
}

func (f translogNameFilter) skip(name string) bool {
	if f.include != nil && !f.include.MatchString(name) {
		return true
	}
	if f.exclude != nil && f.exclude.MatchString(name) {
		return true
	}
	return false
}

type translogChange struct {
	kind string
	name string
}

func runTranslog(cmd *cobra.Command, args []string) error {
	cfg := config.GetConfig()
	logger := logging.NewLogger()

	if !cfg.GetTranslogAsyncEnabled() {
		logger.Info("Async translog is disabled (translog-async-enabled=false), nothing to do")
		return nil
	}

	syncInterval := fmt.Sprintf("%ds", cfg.GetTranslogSyncIntervalSeconds())
	filter, err := newTranslogNameFilter(cfg.GetTranslogIncludeRegex(), cfg.GetTranslogExcludeRegex())
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("Starting translog process durability=%s syncInterval=%s allIndices=%t templates=%t skipCatchAll=%t include=%q exclude=%q dryRun=%t",
		opensearch.TranslogDurabilityAsync, syncInterval, cfg.GetTranslogAllIndices(), cfg.GetTranslogTemplatesEnabled(),
		cfg.GetTranslogSkipCatchAllTemplates(), cfg.GetTranslogIncludeRegex(), cfg.GetTranslogExcludeRegex(), cfg.GetDryRun()))

	client, err := utils.NewOSClientWithURL(cfg, cfg.GetOpenSearchURL())
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	changed, failed, err := processTranslogIndices(client, cfg, logger, filter, syncInterval)
	if err != nil {
		return err
	}

	if cfg.GetTranslogTemplatesEnabled() {
		templateChanged, templateFailed, err := processTranslogTemplates(client, cfg, logger, filter, syncInterval)
		if err != nil {
			return err
		}
		changed = append(changed, templateChanged...)
		failed = append(failed, templateFailed...)
	} else {
		logger.Info("Templates processing is disabled (translog-templates-enabled=false)")
	}

	logTranslogSummary(logger, cfg.GetDryRun(), changed, failed)

	if len(failed) > 0 {
		return fmt.Errorf("failed to process %d objects", len(failed))
	}
	return nil
}

func newTranslogNameFilter(include, exclude string) (translogNameFilter, error) {
	var filter translogNameFilter
	if include != "" {
		re, err := regexp.Compile(include)
		if err != nil {
			return filter, fmt.Errorf("invalid translog-include-regex %q: %v", include, err)
		}
		filter.include = re
	}
	if exclude != "" {
		re, err := regexp.Compile(exclude)
		if err != nil {
			return filter, fmt.Errorf("invalid translog-exclude-regex %q: %v", exclude, err)
		}
		filter.exclude = re
	}
	return filter, nil
}

func processTranslogIndices(client *opensearch.Client, cfg *config.Config, logger *logging.Logger, filter translogNameFilter, syncInterval string) ([]translogChange, []translogChange, error) {
	pattern := "*"
	if !cfg.GetTranslogAllIndices() {
		pattern = fmt.Sprintf("*%s*", utils.FormatDate(time.Now(), cfg.GetDateFormat()))
	}
	logger.Info(fmt.Sprintf("Indices pattern: %s", pattern))

	indices, err := client.GetIndicesWithFields(pattern, "index,status")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get indices: %v", err)
	}

	current, err := client.GetIndicesTranslogSettings(pattern)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get translog settings: %v", err)
	}

	// ES 5.x accepts sync_interval only at index creation, so there it comes from
	// the template and existing indices get durability alone.
	indexSyncInterval := syncInterval
	if client.ES5Compatibility() {
		indexSyncInterval = ""
		logger.Info("Elasticsearch 5.x compatibility mode: sync_interval for existing indices is left to templates")
	}

	var targets []string
	alreadyAsync := 0
	skipped := 0
	for _, index := range indices {
		name := index.Index
		if strings.HasPrefix(name, ".") {
			skipped++
			continue
		}
		if index.Status != "" && index.Status != "open" {
			logger.Info(fmt.Sprintf("Skip not open index=%s status=%s", name, index.Status))
			skipped++
			continue
		}
		if filter.skip(name) {
			skipped++
			continue
		}
		settings := current[name]
		if settings.Durability == opensearch.TranslogDurabilityAsync &&
			(indexSyncInterval == "" || settings.SyncInterval == indexSyncInterval) {
			alreadyAsync++
			continue
		}
		targets = append(targets, name)
	}

	logger.Info(fmt.Sprintf("Indices discovered: total=%d skipped=%d alreadyAsync=%d toUpdate=%d",
		len(indices), skipped, alreadyAsync, len(targets)))

	var changed, failed []translogChange
	if len(targets) == 0 {
		return changed, failed, nil
	}

	if cfg.GetDryRun() {
		for _, name := range targets {
			settings := current[name]
			logger.Info(fmt.Sprintf("DRY RUN: Would set translog durability=%s sync_interval=%q index=%s (current durability=%q sync_interval=%q)",
				opensearch.TranslogDurabilityAsync, indexSyncInterval, name, settings.Durability, settings.SyncInterval))
			changed = append(changed, translogChange{kind: "index", name: name})
		}
		return changed, failed, nil
	}

	interval := indexSyncInterval
	for _, batch := range translogBatches(targets) {
		err := client.SetIndicesTranslog(batch, opensearch.TranslogDurabilityAsync, interval)
		if err != nil && interval != "" && opensearch.IsNonDynamicSettingError(err) {
			logger.Warn(fmt.Sprintf("Cluster does not accept sync_interval on open indices, applying durability only: %v", err))
			interval = ""
			err = client.SetIndicesTranslog(batch, opensearch.TranslogDurabilityAsync, interval)
		}
		if err == nil {
			for _, name := range batch {
				logger.Info(fmt.Sprintf("Successfully set async translog index=%s", name))
				changed = append(changed, translogChange{kind: "index", name: name})
			}
			continue
		}

		logger.Warn(fmt.Sprintf("Batch translog update failed, retrying one by one indices=%d error=%v", len(batch), err))
		for _, name := range batch {
			if err := client.SetIndicesTranslog([]string{name}, opensearch.TranslogDurabilityAsync, interval); err != nil {
				logger.Error(fmt.Sprintf("Failed to set async translog index=%s error=%v", name, err))
				failed = append(failed, translogChange{kind: "index", name: name})
				continue
			}
			logger.Info(fmt.Sprintf("Successfully set async translog index=%s", name))
			changed = append(changed, translogChange{kind: "index", name: name})
		}
	}

	return changed, failed, nil
}

func translogBatches(indices []string) [][]string {
	var batches [][]string
	var batch []string
	length := 0
	for _, name := range indices {
		if len(batch) > 0 && (len(batch) >= translogBatchSize || length+len(name)+1 > translogBatchLength) {
			batches = append(batches, batch)
			batch = nil
			length = 0
		}
		batch = append(batch, name)
		length += len(name) + 1
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

func processTranslogTemplates(client *opensearch.Client, cfg *config.Config, logger *logging.Logger, filter translogNameFilter, syncInterval string) ([]translogChange, []translogChange, error) {
	var changed, failed []translogChange

	if client.ES5Compatibility() {
		logger.Info("Skip composable index templates: they do not exist in Elasticsearch 5.x")
	} else {
		templates, err := client.GetAllIndexTemplatesRaw()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get index templates: %v", err)
		}
		logger.Info(fmt.Sprintf("Composable index templates discovered: %d", len(templates)))

		for _, template := range templates {
			if skipTranslogTemplate(template.Name, filter) {
				continue
			}
			if cfg.GetTranslogSkipCatchAllTemplates() && isCatchAllTemplate(template.Body) {
				logger.Info(fmt.Sprintf("Skip catch-all index template %s", template.Name))
				continue
			}
			section := utils.GetOrCreateSection(template.Body, "template")
			if !translogSettingsChanged(section, syncInterval) {
				continue
			}
			change := translogChange{kind: "index_template", name: template.Name}
			if cfg.GetDryRun() {
				logger.Info(fmt.Sprintf("DRY RUN: Would add async translog settings to index template %s", template.Name))
				changed = append(changed, change)
				continue
			}
			if err := client.PutIndexTemplate(template.Name, template.Body); err != nil {
				logger.Error(fmt.Sprintf("Failed to update index template template=%s error=%v", template.Name, err))
				failed = append(failed, change)
				continue
			}
			logger.Info(fmt.Sprintf("Added async translog settings to index template %s", template.Name))
			changed = append(changed, change)
		}
	}

	legacyTemplates, err := client.GetAllLegacyTemplates()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get legacy templates: %v", err)
	}
	logger.Info(fmt.Sprintf("Legacy templates discovered: %d", len(legacyTemplates)))

	for name, body := range legacyTemplates {
		if skipTranslogTemplate(name, filter) {
			continue
		}
		if cfg.GetTranslogSkipCatchAllTemplates() && isCatchAllTemplate(body) {
			logger.Info(fmt.Sprintf("Skip catch-all legacy template %s", name))
			continue
		}
		if !translogSettingsChanged(body, syncInterval) {
			continue
		}
		change := translogChange{kind: "legacy_template", name: name}
		if cfg.GetDryRun() {
			logger.Info(fmt.Sprintf("DRY RUN: Would add async translog settings to legacy template %s", name))
			changed = append(changed, change)
			continue
		}
		if err := client.PutLegacyTemplate(name, body); err != nil {
			logger.Error(fmt.Sprintf("Failed to update legacy template template=%s error=%v", name, err))
			failed = append(failed, change)
			continue
		}
		logger.Info(fmt.Sprintf("Added async translog settings to legacy template %s", name))
		changed = append(changed, change)
	}

	return changed, failed, nil
}

func skipTranslogTemplate(name string, filter translogNameFilter) bool {
	return strings.HasPrefix(name, ".") || filter.skip(name)
}

// isCatchAllTemplate reports whether the template matches every index of the
// cluster. Such a template (default-template and friends) is shared by everything
// and is usually owned by the chart, so by default we stay out of it.
func isCatchAllTemplate(body map[string]any) bool {
	for _, pattern := range templateIndexPatterns(body) {
		if pattern == "*" {
			return true
		}
	}
	return false
}

func templateIndexPatterns(body map[string]any) []string {
	var patterns []string
	if raw, ok := body["index_patterns"].([]any); ok {
		for _, pattern := range raw {
			if s, ok := pattern.(string); ok {
				patterns = append(patterns, s)
			}
		}
	}
	// ES 5.x keeps the single pattern of a legacy template in "template".
	if s, ok := body["template"].(string); ok {
		patterns = append(patterns, s)
	}
	return patterns
}

// translogSettingsChanged adds the settings to the template body and reports
// whether anything was missing.
func translogSettingsChanged(body map[string]any, syncInterval string) bool {
	settings := utils.GetOrCreateSection(body, "settings")
	durabilityChanged := utils.EnsureSetting(settings, opensearch.TranslogDurabilityKey, opensearch.TranslogDurabilityAsync)
	intervalChanged := utils.EnsureSetting(settings, opensearch.TranslogSyncIntervalKey, syncInterval)
	return durabilityChanged || intervalChanged
}

func logTranslogSummary(logger *logging.Logger, dryRun bool, changed, failed []translogChange) {
	logger.Info(strings.Repeat("=", 60))
	if dryRun {
		logger.Info("TRANSLOG DRY RUN SUMMARY")
	} else {
		logger.Info("TRANSLOG SUMMARY")
	}
	logger.Info(strings.Repeat("=", 60))

	if len(changed) > 0 {
		verb := "Successfully updated"
		if dryRun {
			verb = "Would update"
		}
		logger.Info(fmt.Sprintf("%s: %d objects", verb, len(changed)))
		for _, change := range changed {
			logger.Info(fmt.Sprintf("  ✓ %s %s", change.kind, change.name))
		}
	}
	if len(failed) > 0 {
		logger.Info("")
		logger.Info(fmt.Sprintf("Failed to update: %d objects", len(failed)))
		for _, change := range failed {
			logger.Info(fmt.Sprintf("  ✗ %s %s", change.kind, change.name))
		}
	}
	if len(changed) == 0 && len(failed) == 0 {
		logger.Info("Everything already has async translog, nothing to do")
	}
	logger.Info(strings.Repeat("=", 60))
}
