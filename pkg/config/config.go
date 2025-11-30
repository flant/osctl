package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Action                             string
	OpenSearchURL                      string
	OpenSearchRecovererURL             string
	CertFile                           string
	KeyFile                            string
	CAFile                             string
	Timeout                            string
	RetryAttempts                      string
	DateFormat                         string
	RecovererDateFormat                string
	MadisonURL                         string
	OSDURL                             string
	MadisonKey                         string
	SnapshotRepo                       string
	RetentionThreshold                 string
	RetentionDaysCount                 string
	RetentionCheckSnapshots            string
	RetentionCheckNodesDown            string
	IndicesDeleteCheckSnapshots        string
	DereplicatorDaysCount              string
	DereplicatorUseSnapshot            string
	DataSourceName                     string
	KibanaUser                         string
	KibanaPass                         string
	HotCount                           string
	ColdAttribute                      string
	ExtractedPattern                   string
	ExtractedDays                      string
	DryRun                             string
	ShardingTargetSizeGiB              string
	ShardingExcludeRegex               string
	ShardingRoutingAllocationTemp      string
	KibanaIndexRegex                   string
	KubeNamespace                      string
	SnapshotManualKind                 string
	SnapshotManualValue                string
	SnapshotManualName                 string
	SnapshotManualSystem               string
	SnapshotManualDaysCount            string
	SnapshotManualCountS3              string
	SnapshotManualRepo                 string
	OSCTLConfig                        string
	OSCTLIndicesConfig                 string
	OsctlIndicesConfig                 *OsctlIndicesConfig
	OSCTLTenantsConfig                 string
	KibanaMultidomainEnabled           string
	DataSourceKibanaMultitenancy       string
	DataSourceKibanaMultidomainEnabled string
	DataSourceRemoteCRT                string
	DataSourceEndpoint                 string
	IndexPatternsKibanaMultitenancy    string
	IndexPatternsKibanaTenantsConfig   string
	IndexPatternsRecovererEnabled      string
	SnapshotsBackfillIndicesList       string
	MaxConcurrentSnapshots             string
}

type CommandConfig = Config

var (
	configInstance *Config
)

func LoadConfig(cmd *cobra.Command, commandName string) error {
	if commandName == "completion" || commandName == "help" {
		return nil
	}

	viper.Reset()
	viper.SetConfigType("yaml")

	setDefaults()

	configPath := getValue(cmd, "config", "OSCTL_CONFIG", viper.GetString("osctl_config"))

	if configPath != "" {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				fmt.Printf("Config file not found at %s; using flags and environment variables\n", configPath)
			} else {
				return fmt.Errorf("error reading config file %s: %w", configPath, err)
			}
		}
	}

	var osctlIndicesConfig *OsctlIndicesConfig
	osctlIndicesPath := getValue(cmd, "osctl-indices-config", "OSCTL_INDICES_CONFIG", viper.GetString("osctl_indices_config"))
	tenantsPath := getValue(cmd, "kibana-tenants-config", "KIBANA_TENANTS_CONFIG", viper.GetString("kibana_tenants_config"))

	requireIndicesConfig := commandName == "snapshots" || commandName == "indicesdelete" || commandName == "snapshotsdelete" || commandName == "snapshotschecker" || commandName == "snapshotsbackfill"

	if requireIndicesConfig {

		if osctlIndicesPath == "" {
			return fmt.Errorf("osctl-indices-config is required for %s", commandName)
		}
		if _, err := os.Stat(osctlIndicesPath); os.IsNotExist(err) {
			return fmt.Errorf("osctl-indices-config file not found: %s", osctlIndicesPath)
		}
		var err error
		osctlIndicesConfig, err = LoadOsctlIndicesConfig(osctlIndicesPath)
		if err != nil {
			return fmt.Errorf("error loading osctl indices config: %w", err)
		}
		dateFormat := getValue(cmd, "date-format", "DATE_FORMAT", viper.GetString("date_format"))
		if dateFormat == "" {
			dateFormat = "%Y.%m.%d"
		}
		if err = ValidateOsctlIndicesConfig(osctlIndicesConfig, dateFormat); err != nil {
			return fmt.Errorf("error validating osctl indices config: %w", err)
		}
	}

	configInstance = &Config{
		Action:                        getValue(cmd, "action", "OSCTL_ACTION", viper.GetString("action")),
		OpenSearchURL:                 getValue(cmd, "os-url", "OPENSEARCH_URL", viper.GetString("opensearch_url")),
		OpenSearchRecovererURL:        getValue(cmd, "os-recoverer-url", "OPENSEARCH_RECOVERER_URL", viper.GetString("opensearch_recoverer_url")),
		CertFile:                      getValue(cmd, "cert-file", "OPENSEARCH_CERT_FILE", viper.GetString("cert_file")),
		KeyFile:                       getValue(cmd, "key-file", "OPENSEARCH_KEY_FILE", viper.GetString("key_file")),
		CAFile:                        getValue(cmd, "ca-file", "OPENSEARCH_CA_FILE", viper.GetString("ca_file")),
		Timeout:                       getValue(cmd, "timeout", "OPENSEARCH_TIMEOUT", viper.GetString("timeout")),
		RetryAttempts:                 getValue(cmd, "retry-attempts", "OPENSEARCH_RETRY_ATTEMPTS", viper.GetString("retry_attempts")),
		DateFormat:                    getValue(cmd, "date-format", "OPENSEARCH_DATE_FORMAT", viper.GetString("date_format")),
		RecovererDateFormat:           getValue(cmd, "recoverer-date-format", "RECOVERER_DATE_FORMAT", viper.GetString("recoverer_date_format")),
		MadisonURL:                    getValue(cmd, "madison-url", "MADISON_URL", viper.GetString("madison_url")),
		OSDURL:                        getValue(cmd, "osd-url", "OPENSEARCH_DASHBOARDS_URL", viper.GetString("osd_url")),
		KibanaUser:                    getValue(cmd, "kibana-user", "KIBANA_API_USER", viper.GetString("kibana_user")),
		KibanaPass:                    getValue(cmd, "kibana-pass", "KIBANA_API_PASS", viper.GetString("kibana_pass")),
		MadisonKey:                    getValue(cmd, "madison-key", "MADISON_KEY", viper.GetString("madison_key")),
		SnapshotRepo:                  getValue(cmd, "snap-repo", "SNAPSHOT_REPOSITORY", viper.GetString("snapshot_repo")),
		RetentionThreshold:            getValue(cmd, "retention-threshold", "RETENTION_THRESHOLD", viper.GetString("retention_threshold")),
		RetentionDaysCount:            getValue(cmd, "retention-days-count", "RETENTION_DAYS_COUNT", viper.GetString("retention_days_count")),
		RetentionCheckSnapshots:       getValue(cmd, "retention-check-snapshots", "RETENTION_CHECK_SNAPSHOTS", viper.GetString("retention_check_snapshots")),
		RetentionCheckNodesDown:       getValue(cmd, "retention-check-nodes-down", "RETENTION_CHECK_NODES_DOWN", viper.GetString("retention_check_nodes_down")),
		IndicesDeleteCheckSnapshots:   getValue(cmd, "indicesdelete-check-snapshots", "INDICESDELETE_CHECK_SNAPSHOTS", viper.GetString("indicesdelete_check_snapshots")),
		DereplicatorDaysCount:         getValue(cmd, "dereplicator-days-count", "DEREPLICATOR_DAYS", viper.GetString("dereplicator_days_count")),
		DereplicatorUseSnapshot:       getValue(cmd, "dereplicator-use-snapshot", "DEREPLICATOR_USE_SNAPSHOT", viper.GetString("dereplicator_use_snapshot")),
		HotCount:                      getValue(cmd, "hot-count", "HOT_COUNT", viper.GetString("hot_count")),
		ColdAttribute:                 getValue(cmd, "cold-attribute", "COLD_ATTRIBUTE", viper.GetString("cold_attribute")),
		ExtractedPattern:              getValue(cmd, "extracted-pattern", "EXTRACTED_PATTERN", viper.GetString("extracted_pattern")),
		ExtractedDays:                 getValue(cmd, "days", "EXTRACTED_DAYS", viper.GetString("extracted_days")),
		DryRun:                        getValue(cmd, "dry-run", "DRY_RUN", viper.GetString("dry_run")),
		ShardingTargetSizeGiB:         getValue(cmd, "sharding-target-size-gib", "SHARDING_TARGET_SIZE_GIB", viper.GetString("sharding_target_size_gib")),
		ShardingExcludeRegex:          getValue(cmd, "exclude-sharding", "EXCLUDE_SHARDING", viper.GetString("exclude_sharding")),
		ShardingRoutingAllocationTemp: getValue(cmd, "sharding-routing-allocation-temp", "SHARDING_ROUTING_ALLOCATION_TEMP", viper.GetString("sharding_routing_allocation_temp")),
		KibanaIndexRegex:              getValue(cmd, "kibana-index-regex", "KIBANA_INDEX_REGEX", viper.GetString("kibana_index_regex")),
		KubeNamespace:                 getValue(cmd, "kube-namespace", "KUBE_NAMESPACE", viper.GetString("kube_namespace")),
		SnapshotManualKind:            getValue(cmd, "snapshot-manual-kind", "SNAPSHOT_KIND", viper.GetString("snapshot_manual_kind")),
		SnapshotManualValue:           getValue(cmd, "snapshot-manual-value", "SNAPSHOT_VALUE", viper.GetString("snapshot_manual_value")),
		SnapshotManualName:            getValue(cmd, "snapshot-manual-name", "SNAPSHOT_NAME", viper.GetString("snapshot_manual_name")),
		SnapshotManualSystem:          getValue(cmd, "snapshot-manual-system", "SNAPSHOT_SYSTEM", viper.GetString("snapshot_manual_system")),
		SnapshotManualDaysCount:       getValue(cmd, "snapshot-manual-days-count", "SNAPSHOT_DAYS_COUNT", viper.GetString("snapshot_manual_days_count")),
		SnapshotManualCountS3:         getValue(cmd, "snapshot-manual-count-s3", "SNAPSHOT_COUNT_S3", viper.GetString("snapshot_manual_count_s3")),
		SnapshotManualRepo:            getValue(cmd, "snapshot-manual-repo", "SNAPSHOT_MANUAL_REPO", viper.GetString("snapshot_manual_repo")),
		OSCTLConfig:                   getValue(cmd, "config", "OSCTL_CONFIG", viper.GetString("osctl_config")),
		OSCTLIndicesConfig:            getValue(cmd, "osctl-indices-config", "OSCTL_INDICES_CONFIG", viper.GetString("osctl_indices_config")),
		OsctlIndicesConfig:            osctlIndicesConfig,
		OSCTLTenantsConfig:            getValue(cmd, "kibana-tenants-config", "KIBANA_TENANTS_CONFIG", tenantsPath),
		KibanaMultidomainEnabled:      getValue(cmd, "kibana-multidomain-enabled", "KIBANA_MULTIDOMAIN_ENABLED", viper.GetString("kibana_multidomain_enabled")),
		DataSourceName:                getValue(cmd, "datasource-name", "DATA_SOURCE_NAME", viper.GetString("datasource_name")),

		DataSourceKibanaMultitenancy:       getValue(cmd, "datasource-kibana-multitenancy", "DATASOURCE_KIBANA_MULTITENANCY", viper.GetString("datasource_kibana_multitenancy")),
		DataSourceKibanaMultidomainEnabled: getValue(cmd, "datasource-kibana-multidomain-enabled", "DATASOURCE_KIBANA_MULTIDOMAIN_ENABLED", viper.GetString("datasource_kibana_multidomain_enabled")),
		DataSourceRemoteCRT:                getValue(cmd, "datasource-remote-crt", "DATASOURCE_REMOTE_CRT", viper.GetString("datasource_remote_crt")),
		DataSourceEndpoint:                 getValue(cmd, "datasource-endpoint", "DATASOURCE_ENDPOINT", viper.GetString("datasource_endpoint")),
		IndexPatternsKibanaMultitenancy:    getValue(cmd, "indexpatterns-kibana-multitenancy", "INDEXPATTERNS_KIBANA_MULTITENANCY", viper.GetString("indexpatterns_kibana_multitenancy")),
		IndexPatternsKibanaTenantsConfig:   getValue(cmd, "indexpatterns-kibana-tenants-config", "INDEXPATTERNS_KIBANA_TENANTS_CONFIG", viper.GetString("indexpatterns_kibana_tenants_config")),
		IndexPatternsRecovererEnabled:      getValue(cmd, "indexpatterns-recoverer-enabled", "INDEXPATTERNS_RECOVERER_ENABLED", viper.GetString("indexpatterns_recoverer_enabled")),
		SnapshotsBackfillIndicesList:       getValue(cmd, "indices-list", "SNAPSHOTS_BACKFILL_INDICES_LIST", viper.GetString("snapshots_backfill_indices_list")),
		MaxConcurrentSnapshots:             getValue(cmd, "max-concurrent-snapshots", "MAX_CONCURRENT_SNAPSHOTS", viper.GetString("max_concurrent_snapshots")),
	}

	switch commandName {
	case "snapshots", "snapshotsdelete", "snapshotsbackfill":
		if configInstance.SnapshotRepo == "" {
			return fmt.Errorf("snap-repo is required for %s", commandName)
		}
	case "snapshot-manual":
		repoToUse := configInstance.SnapshotRepo
		if configInstance.SnapshotManualRepo != "" {
			repoToUse = configInstance.SnapshotManualRepo
		}
		if repoToUse == "" {
			return fmt.Errorf("snap-repo is required (or set snapshot-manual-repo) for %s", commandName)
		}
	}

	return nil
}

func setDefaults() {
	viper.SetDefault("action", "")
	viper.SetDefault("opensearch_url", "https://opendistro:9200")
	viper.SetDefault("opensearch_recoverer_url", "https://opendistro-recoverer:9200")
	viper.SetDefault("cert_file", "/etc/ssl/certs/admin-crt.pem")
	viper.SetDefault("key_file", "/etc/ssl/certs/admin-key.pem")
	viper.SetDefault("ca_file", "")
	viper.SetDefault("timeout", "300s")
	viper.SetDefault("retry_attempts", 3)
	viper.SetDefault("date_format", "%Y.%m.%d")
	viper.SetDefault("recoverer_date_format", "%d-%m-%Y")
	viper.SetDefault("madison_url", "https://madison.flant.com/api/events/custom/")
	viper.SetDefault("osd_url", "")
	viper.SetDefault("kibana_user", "")
	viper.SetDefault("kibana_pass", "")
	viper.SetDefault("madison_key", "")
	viper.SetDefault("snapshot_repo", "s3-backup")
	viper.SetDefault("retention_threshold", 75.0)
	viper.SetDefault("retention_days_count", 2)
	viper.SetDefault("retention_check_snapshots", true)
	viper.SetDefault("retention_check_nodes_down", true)
	viper.SetDefault("indicesdelete_check_snapshots", true)
	viper.SetDefault("dereplicator_days_count", 2)
	viper.SetDefault("dereplicator_use_snapshot", false)
	viper.SetDefault("hot_count", 4)
	viper.SetDefault("cold_attribute", "cold")
	viper.SetDefault("extracted_pattern", "extracted_")
	viper.SetDefault("extracted_days", 7)
	viper.SetDefault("snapshot_manual_kind", "prefix")
	viper.SetDefault("snapshot_manual_value", "")
	viper.SetDefault("snapshot_manual_name", "")
	viper.SetDefault("snapshot_manual_system", false)
	viper.SetDefault("snapshot_manual_days_count", 7)
	viper.SetDefault("snapshot_manual_count_s3", 14)
	viper.SetDefault("snapshot_manual_repo", "")
	viper.SetDefault("dry_run", false)
	viper.SetDefault("osctl_config", "config.yaml")
	viper.SetDefault("osctl_indices_config", "osctlindicesconfig.yaml")
	viper.SetDefault("sharding_target_size_gib", 25)
	viper.SetDefault("exclude_sharding", "")
	viper.SetDefault("kibana_index_regex", `^([\w-]+)-([\w-]*)(\d{4}[\.-]\d{2}[\.-]\d{2}(?:[\.-]\d{2})*)$`)
	viper.SetDefault("recoverer_enabled", false)
	viper.SetDefault("kube_namespace", "infra-elklogs")
	viper.SetDefault("kibana_tenants_config", "osctltenants.yaml")
	viper.SetDefault("kibana_multidomain_enabled", false)
	viper.SetDefault("datasource_name", "recoverer")
	viper.SetDefault("datasource_endpoint", "https://opendistro-recoverer:9200")
	viper.SetDefault("max_concurrent_snapshots", 3)
}

func GetAvailableActions() []string {
	return []string{
		"snapshots",
		"snapshot-manual",
		"snapshotsdelete",
		"snapshotschecker",
		"snapshotsbackfill",
		"indicesdelete",
		"retention",
		"dereplicator",
		"coldstorage",
		"extracteddelete",
		"danglingchecker",
		"sharding",
		"indexpatterns",
		"datasource",
	}
}

func ValidateAction(action string) error {
	if action == "" {
		return nil
	}

	availableActions := GetAvailableActions()
	for _, validAction := range availableActions {
		if action == validAction {
			return nil
		}
	}

	return fmt.Errorf("invalid action '%s'. Available actions: %s", action, strings.Join(availableActions, ", "))
}

func GetConfig() *Config {
	if configInstance == nil {
		panic("Config has not been initialized. Call LoadConfig first.")
	}
	return configInstance
}

func getValue(cmd *cobra.Command, flagName, envVar, configValue string) string {
	if flag := cmd.Flags().Lookup(flagName); flag != nil && flag.Changed {
		return flag.Value.String()
	}
	if envValue := os.Getenv(envVar); envValue != "" {
		return envValue
	}
	return configValue
}

func parseDurationWithDefault(value, key string) time.Duration {
	if value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	if d := viper.GetDuration(key); d != 0 {
		return d
	}
	if raw := viper.GetString(key); raw != "" {
		if duration, err := time.ParseDuration(raw); err == nil {
			return duration
		}
	}
	return 0
}

func parseIntWithDefault(value, key string) int {
	if value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return viper.GetInt(key)
}

func parseFloatWithDefault(value, key string) float64 {
	if value != "" {
		if parsed, err := strconv.ParseFloat(value, 64); err == nil {
			return parsed
		}
	}
	return viper.GetFloat64(key)
}

func parseBoolWithDefault(value, key string) bool {
	if value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
	}
	if raw := strings.TrimSpace(viper.GetString(key)); raw != "" {
		if parsed, err := strconv.ParseBool(raw); err == nil {
			return parsed
		}
	}
	return viper.GetBool(key)
}

func clampInt(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func (c *Config) GetTimeout() time.Duration {
	return parseDurationWithDefault(c.Timeout, "timeout")
}

func (c *Config) GetRetryAttempts() int {
	return parseIntWithDefault(c.RetryAttempts, "retry_attempts")
}

func (c *Config) GetRetentionThreshold() float64 {
	return parseFloatWithDefault(c.RetentionThreshold, "retention_threshold")
}

func (c *Config) GetRetentionDaysCount() int {
	return parseIntWithDefault(c.RetentionDaysCount, "retention_days_count")
}

func (c *Config) GetRetentionCheckSnapshots() bool {
	return parseBoolWithDefault(c.RetentionCheckSnapshots, "retention_check_snapshots")
}

func (c *Config) GetRetentionCheckNodesDown() bool {
	return parseBoolWithDefault(c.RetentionCheckNodesDown, "retention_check_nodes_down")
}

func (c *Config) GetIndicesDeleteCheckSnapshots() bool {
	return parseBoolWithDefault(c.IndicesDeleteCheckSnapshots, "indicesdelete_check_snapshots")
}

func (c *Config) GetDereplicatorDaysCount() int {
	return parseIntWithDefault(c.DereplicatorDaysCount, "dereplicator_days_count")
}

func (c *Config) GetDereplicatorUseSnapshot() bool {
	return parseBoolWithDefault(c.DereplicatorUseSnapshot, "dereplicator_use_snapshot")
}

func (c *Config) GetHotCount() int {
	return parseIntWithDefault(c.HotCount, "hot_count")
}

func (c *Config) GetExtractedDays() int {
	return parseIntWithDefault(c.ExtractedDays, "extracted_days")
}

func (c *Config) GetDryRun() bool {
	return parseBoolWithDefault(c.DryRun, "dry_run")
}

func (c *Config) GetDataSourceKibanaMultitenancy() bool {
	return parseBoolWithDefault(c.DataSourceKibanaMultitenancy, "datasource_kibana_multitenancy")
}

func (c *Config) GetDataSourceKibanaMultidomainEnabled() bool {
	return parseBoolWithDefault(c.DataSourceKibanaMultidomainEnabled, "datasource_kibana_multidomain_enabled")
}

func (c *Config) GetIndexPatternsKibanaMultitenancy() bool {
	return parseBoolWithDefault(c.IndexPatternsKibanaMultitenancy, "indexpatterns_kibana_multitenancy")
}

func (c *Config) GetIndexPatternsRecovererEnabled() bool {
	return parseBoolWithDefault(c.IndexPatternsRecovererEnabled, "indexpatterns_recoverer_enabled")
}

func (c *Config) GetShardingTargetSizeGiB() int {
	value := parseIntWithDefault(c.ShardingTargetSizeGiB, "sharding_target_size_gib")
	if value < 1 || value > 50 {
		fmt.Fprintf(os.Stderr, "invalid sharding_target_size_gib=%d; expected value between 1 and 50\n", value)
		os.Exit(1)
	}
	return value
}

func (c *Config) GetSnapshotManualSystem() bool {
	return parseBoolWithDefault(c.SnapshotManualSystem, "snapshot_manual_system")
}

func (c *Config) GetKibanaMultidomainEnabled() bool {
	return parseBoolWithDefault(c.KibanaMultidomainEnabled, "kibana_multidomain_enabled")
}

func (c *Config) GetAction() string {
	return c.Action
}

func (c *Config) GetOpenSearchURL() string {
	return c.OpenSearchURL
}

func (c *Config) GetOpenSearchRecovererURL() string {
	return c.OpenSearchRecovererURL
}

func (c *Config) GetCertFile() string {
	return c.CertFile
}

func (c *Config) GetKeyFile() string {
	return c.KeyFile
}

func (c *Config) GetCAFile() string {
	return c.CAFile
}

func (c *Config) GetDateFormat() string {
	return c.DateFormat
}

func (c *Config) GetRecovererDateFormat() string {
	return c.RecovererDateFormat
}

func (c *Config) GetMadisonURL() string {
	return c.MadisonURL
}

func (c *Config) GetOSDURL() string {
	return c.OSDURL
}

func (c *Config) GetMadisonKey() string {
	return c.MadisonKey
}

func (c *Config) GetSnapshotRepo() string {
	return c.SnapshotRepo
}

func (c *Config) GetDataSourceName() string {
	return c.DataSourceName
}

func (c *Config) GetKibanaUser() string {
	return c.KibanaUser
}

func (c *Config) GetKibanaPass() string {
	return c.KibanaPass
}

func (c *Config) GetColdAttribute() string {
	return c.ColdAttribute
}

func (c *Config) GetExtractedPattern() string {
	return c.ExtractedPattern
}

func (c *Config) GetShardingExcludeRegex() string {
	return c.ShardingExcludeRegex
}

func (c *Config) GetShardingRoutingAllocationTemp() string {
	return c.ShardingRoutingAllocationTemp
}

func (c *Config) GetKibanaIndexRegex() string {
	return c.KibanaIndexRegex
}

func (c *Config) GetKubeNamespace() string {
	return c.KubeNamespace
}

func (c *Config) GetSnapshotManualKind() string {
	return c.SnapshotManualKind
}

func (c *Config) GetSnapshotManualValue() string {
	return c.SnapshotManualValue
}

func (c *Config) GetSnapshotManualName() string {
	return c.SnapshotManualName
}

func (c *Config) GetSnapshotManualRepo() string {
	return c.SnapshotManualRepo
}

func (c *Config) GetOSCTLConfig() string {
	return c.OSCTLConfig
}

func (c *Config) GetOSCTLIndicesConfig() string {
	return c.OSCTLIndicesConfig
}

func (c *Config) GetOSCTLTenantsConfig() string {
	return c.OSCTLTenantsConfig
}

func (c *Config) GetDataSourceRemoteCRT() string {
	return c.DataSourceRemoteCRT
}

func (c *Config) GetDataSourceEndpoint() string {
	return c.DataSourceEndpoint
}

func (c *Config) GetIndexPatternsKibanaTenantsConfig() string {
	return c.IndexPatternsKibanaTenantsConfig
}

func (c *Config) GetSnapshotsBackfillIndicesList() string {
	return c.SnapshotsBackfillIndicesList
}

func (c *Config) GetMaxConcurrentSnapshots() int {
	return parseIntWithDefault(c.MaxConcurrentSnapshots, "max_concurrent_snapshots")
}

type FlagDefinition struct {
	Name        string
	Type        string
	Default     interface{}
	Description string
	Validation  []string
}

var CommandFlags = map[string][]FlagDefinition{
	"common": {
		{"osctl-indices-config", "string", "", "Path to osctl indices configuration file", []string{}},
		{"max-concurrent-snapshots", "int", 3, "Maximum number of snapshots to create simultaneously", []string{"min:1", "max:10"}},
	},
	"snapshots": {
		{"dry-run", "bool", false, "Show what would be created without actually creating", []string{}},
	},
	"snapshot-manual": {
		{"snapshot-manual-kind", "string", "", "Pattern type: prefix or regex", []string{}},
		{"snapshot-manual-value", "string", "", "Pattern value", []string{}},
		{"snapshot-manual-name", "string", "", "Name for snapshot (required for regex)", []string{}},
		{"snapshot-manual-system", "bool", false, "System index flag", []string{}},
		{"snapshot-manual-days-count", "int", 0, "Days to keep index", []string{}},
		{"snapshot-manual-count-s3", "int", 0, "Days to keep snapshot in S3 (0 = use default)", []string{}},
		{"snapshot-manual-repo", "string", "", "Override repository for manual snapshot (empty = use snapshot_repo)", []string{}},
	},
	"retention": {
		{"retention-threshold", "int", 75, "Disk usage threshold percentage", []string{"min:0", "max:100"}},
		{"retention-days-count", "int", 2, "Number of days to keep indices (indices newer than this will not be deleted). Minimum 2 days.", []string{"min:2", "max:365"}},
		{"retention-check-snapshots", "bool", true, "Check for valid snapshots before deleting indices", []string{}},
		{"retention-check-nodes-down", "bool", true, "Check if nodes are down before running retention", []string{}},
		{"snap-repo", "string", "", "Snapshot repository name", []string{"required"}},
		{"dry-run", "bool", false, "Show what would be deleted without actually deleting", []string{}},
	},
	"dereplicator": {
		{"dereplicator-days-count", "int", 2, "Number of days to keep with replicas", []string{"min:1", "max:365"}},
		{"dereplicator-use-snapshot", "bool", false, "Check for snapshots before reducing replicas", []string{}},
		{"snap-repo", "string", "", "Snapshot repository name", []string{}},
		{"dry-run", "bool", false, "Show what would be changed without actually changing", []string{}},
	},
	"snapshotschecker": {
		// Uses --osctl-indices-config for configuration
		{"dry-run", "bool", false, "Show what would be done without sending alerts", []string{}},
	},
	"snapshotsbackfill": {
		// Uses --osctl-indices-config for configuration
		{"indices-list", "string", "", "Comma-separated list of indices to backfill snapshots for", []string{}},
		{"dry-run", "bool", false, "Show what would be created without actually creating", []string{}},
	},
	"coldstorage": {
		{"hot-count", "int", 3, "Number of days to keep indices hot", []string{"min:1", "max:30"}},
		{"cold-attribute", "string", "", "Node attribute for cold storage", []string{}},
		{"dry-run", "bool", false, "Show what would be changed without actually changing", []string{}},
	},
	"datasource": {
		{"kibana-user", "string", "", "Kibana API user", []string{}},
		{"kibana-pass", "string", "", "Kibana API password", []string{}},
		{"datasource-name", "string", "recoverer", "Data source title", []string{}},
		{"datasource-endpoint", "string", "https://opendistro-recoverer:9200", "OpenSearch endpoint URL for data source", []string{}},
		{"kube-namespace", "string", "default", "Kubernetes namespace for secrets", []string{}},
		{"datasource-kibana-multidomain-enabled", "bool", false, "Enable Kibana multidomain cert management", []string{}},
		{"datasource-remote-crt", "string", "", "Concatenated base64 certs separated by | for multidomain", []string{}},
		{"datasource-kibana-multitenancy", "bool", false, "Enable multitenancy mode (uses datasource-kibana-tenants-config)", []string{}},
		{"datasource-kibana-tenants-config", "string", "osctltenants.yaml", "Path to YAML tenants and patterns", []string{}},
		{"dry-run", "bool", false, "Show what would be created/updated without changing Kibana/K8s", []string{}},
	},
	"extracteddelete": {
		{"os-recoverer-url", "string", "", "OpenSearch recoverer cluster URL", []string{}},
		{"recoverer-date-format", "string", "%Y.%m.%d", "Date format for recoverer index names", []string{}},
		{"extracted-pattern", "string", "extracted_", "Prefix for extracted indices", []string{}},
		{"days", "int", 7, "Number of days to keep extracted indices", []string{"min:1", "max:365"}},
		{"dry-run", "bool", false, "Show what would be deleted without actually deleting", []string{}},
	},
	"indicesdelete": {
		{"indicesdelete-check-snapshots", "bool", true, "Check for valid snapshots before deleting indices that should have snapshots. If true and snapshots cannot be retrieved or snap-repo is not configured, job exits with error.", []string{}},
		{"snap-repo", "string", "", "Snapshot repository name (required if indicesdelete-check-snapshots is true)", []string{}},
		// Uses --osctl-indices-config for configuration
		{"dry-run", "bool", false, "Show what would be deleted without actually deleting", []string{}},
	},
	"sharding": {
		{"sharding-target-size-gib", "int", 25, "Target max shard size GiB (<=50)", []string{"min:1", "max:50"}},
		{"exclude-sharding", "string", "", "Regex to exclude patterns from sharding", []string{}},
		{"sharding-routing-allocation-temp", "string", "", "Routing allocation temp value (e.g., 'hot')", []string{}},
		{"dry-run", "bool", false, "Show what templates would be created/updated without applying", []string{}},
	},
	"indexpatterns": {
		{"kibana-index-regex", "string", "^(.*?)-\\d{4}\\.\\d{2}\\.\\d{2}.*$", "Regex to extract pattern from today's indices", []string{}},
		{"indexpatterns-kibana-multitenancy", "bool", false, "Enable multitenancy mode", []string{}},
		{"indexpatterns-kibana-tenants-config", "string", "osctltenants.yaml", "Path to YAML tenants and patterns", []string{}},
		{"indexpatterns-recoverer-enabled", "bool", false, "Enable recoverer extracted_* pattern creation", []string{}},
		{"dry-run", "bool", false, "Show what index patterns would be created without creating", []string{}},
	},
	"snapshotsdelete": {
		// Uses --osctl-indices-config for configuration
	},
	"danglingchecker": {
		{"dry-run", "bool", false, "Show what alerts would be sent without sending", []string{}},
	},
}

func AddCommandFlags(cmd *cobra.Command, commandName string) {
	if commonFlags, exists := CommandFlags["common"]; exists {
		for _, flag := range commonFlags {
			addFlag(cmd, flag)
		}
	}

	flags, exists := CommandFlags[commandName]
	if !exists {
		return
	}

	for _, flag := range flags {
		addFlag(cmd, flag)
	}
}

func addFlag(cmd *cobra.Command, flag FlagDefinition) {
	switch flag.Type {
	case "string":
		cmd.Flags().String(flag.Name, flag.Default.(string), flag.Description)
	case "int":
		cmd.Flags().Int(flag.Name, flag.Default.(int), flag.Description)
	case "bool":
		cmd.Flags().Bool(flag.Name, flag.Default.(bool), flag.Description)
	case "stringSlice":
		cmd.Flags().StringSlice(flag.Name, flag.Default.([]string), flag.Description)
	case "duration":
		cmd.Flags().Duration(flag.Name, flag.Default.(time.Duration), flag.Description)
	case "float64":
		cmd.Flags().Float64(flag.Name, flag.Default.(float64), flag.Description)
	}
}

func LogConfigSource(cmd *cobra.Command, commandName string) {
	fmt.Printf("Configuration source priority (CLI flags > Environment variables > Command config > Defaults):\n")

	configPath, _ := cmd.Flags().GetString("config")

	if configPath != "" {
		fmt.Printf("  Command config: %s\n", configPath)
	} else {
		fmt.Printf("  Command config: none (using defaults)\n")
	}

	fmt.Printf("  Environment variables: OPENSEARCH_URL, MADISON_URL, SNAPSHOT_REPOSITORY, etc.\n")
	fmt.Printf("  CLI flags: available for all commands\n")
	fmt.Printf("  Defaults: built-in values\n")
}
