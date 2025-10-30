package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Action                   string
	OpenSearchURL            string
	OpenSearchRecovererURL   string
	CertFile                 string
	KeyFile                  string
	CAFile                   string
	Timeout                  string
	RetryAttempts            string
	DateFormat               string
	RecovererDateFormat      string
	MadisonURL               string
	OSDURL                   string
	MadisonKey               string
	MadisonProject           string
	SnapshotRepo             string
	RetentionThreshold       string
	DereplicatorDaysCount    string
	DereplicatorUseSnapshot  string
	DataSourceName           string
	KibanaUser               string
	KibanaPass               string
	HotCount                 string
	ColdAttribute            string
	ExtractedPattern         string
	ExtractedDays            string
	DryRun                   string
	ShardingTargetSizeGiB    string
	ShardingExcludeRegex     string
	KibanaIndexRegex         string
	KibanaMultitenancy       string
	KibanaTenants            string
	RecovererEnabled         string
	KubeNamespace            string
	SnapshotManualKind       string
	SnapshotManualValue      string
	SnapshotManualName       string
	SnapshotManualSystem     string
	SnapshotManualDaysCount  string
	SnapshotManualCountS3    string
	OSCTLConfig              string
	OSCTLIndicesConfig       string
	OsctlIndicesConfig       *OsctlIndicesConfig
	OSCTLTenantsConfig       string
	KibanaMultidomainEnabled string
}

var (
	configInstance *Config
)

func LoadConfig(cmd *cobra.Command, commandName string) error {
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

	requireIndicesConfig := commandName == "snapshots" || commandName == "indicesdelete" || commandName == "snapshotsdelete" || commandName == "snapshotschecker"

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
		Action:                   getValue(cmd, "action", "OSCTL_ACTION", viper.GetString("action")),
		OpenSearchURL:            getValue(cmd, "os-url", "OPENSEARCH_URL", viper.GetString("opensearch_url")),
		OpenSearchRecovererURL:   getValue(cmd, "os-recoverer-url", "OPENSEARCH_RECOVERER_URL", viper.GetString("opensearch_recoverer_url")),
		CertFile:                 getValue(cmd, "cert-file", "OPENSEARCH_CERT_FILE", viper.GetString("cert_file")),
		KeyFile:                  getValue(cmd, "key-file", "OPENSEARCH_KEY_FILE", viper.GetString("key_file")),
		CAFile:                   getValue(cmd, "ca-file", "OPENSEARCH_CA_FILE", viper.GetString("ca_file")),
		Timeout:                  getValue(cmd, "timeout", "OPENSEARCH_TIMEOUT", viper.GetString("timeout")),
		RetryAttempts:            getValue(cmd, "retry-attempts", "OPENSEARCH_RETRY_ATTEMPTS", viper.GetString("retry_attempts")),
		DateFormat:               getValue(cmd, "date-format", "OPENSEARCH_DATE_FORMAT", viper.GetString("date_format")),
		RecovererDateFormat:      getValue(cmd, "recoverer-date-format", "RECOVERER_DATE_FORMAT", viper.GetString("recoverer_date_format")),
		MadisonURL:               getValue(cmd, "madison-url", "MADISON_URL", viper.GetString("madison_url")),
		OSDURL:                   getValue(cmd, "osd-url", "OPENSEARCH_DASHBOARDS_URL", viper.GetString("osd_url")),
		KibanaUser:               getValue(cmd, "kibana-user", "KIBANA_API_USER", viper.GetString("kibana_user")),
		KibanaPass:               getValue(cmd, "kibana-pass", "KIBANA_API_PASS", viper.GetString("kibana_pass")),
		MadisonKey:               getValue(cmd, "madison-key", "MADISON_KEY", viper.GetString("madison_key")),
		MadisonProject:           getValue(cmd, "madison-project", "MADISON_PROJECT", viper.GetString("madison_project")),
		SnapshotRepo:             getValue(cmd, "snap-repo", "SNAPSHOT_REPOSITORY", viper.GetString("snapshot_repo")),
		RetentionThreshold:       getValue(cmd, "retention-threshold", "RETENTION_THRESHOLD", viper.GetString("retention_threshold")),
		DereplicatorDaysCount:    getValue(cmd, "dereplicator-days-count", "DEREPLICATOR_DAYS", viper.GetString("dereplicator_days_count")),
		DereplicatorUseSnapshot:  getValue(cmd, "dereplicator-use-snapshot", "DEREPLICATOR_USE_SNAPSHOT", viper.GetString("dereplicator_use_snapshot")),
		HotCount:                 getValue(cmd, "hot-count", "HOT_COUNT", viper.GetString("hot_count")),
		ColdAttribute:            getValue(cmd, "cold-attribute", "COLD_ATTRIBUTE", viper.GetString("cold_attribute")),
		ExtractedPattern:         getValue(cmd, "extracted-pattern", "EXTRACTED_PATTERN", viper.GetString("extracted_pattern")),
		ExtractedDays:            getValue(cmd, "days", "EXTRACTED_DAYS", viper.GetString("extracted_days")),
		DryRun:                   getValue(cmd, "dry-run", "DRY_RUN", viper.GetString("dry_run")),
		ShardingTargetSizeGiB:    getValue(cmd, "sharding-target-size-gib", "SHARDING_TARGET_SIZE_GIB", viper.GetString("sharding_target_size_gib")),
		ShardingExcludeRegex:     getValue(cmd, "exclude-sharding", "EXCLUDE_SHARDING", viper.GetString("exclude_sharding")),
		KibanaIndexRegex:         getValue(cmd, "kibana-index-regex", "KIBANA_INDEX_REGEX", viper.GetString("kibana_index_regex")),
		KibanaMultitenancy:       getValue(cmd, "kibana-multitenancy", "KIBANA_MULTITENANCY", viper.GetString("kibana_multitenancy")),
		KibanaTenants:            getValue(cmd, "kibana-tenants", "KIBANA_TENANTS", viper.GetString("kibana_tenants")),
		RecovererEnabled:         getValue(cmd, "recoverer-enabled", "RECOVERER_ENABLED", viper.GetString("recoverer_enabled")),
		KubeNamespace:            getValue(cmd, "kube-namespace", "KUBE_NAMESPACE", viper.GetString("kube_namespace")),
		SnapshotManualKind:       getValue(cmd, "snapshot-manual-kind", "SNAPSHOT_KIND", viper.GetString("snapshot_manual_kind")),
		SnapshotManualValue:      getValue(cmd, "snapshot-manual-value", "SNAPSHOT_VALUE", viper.GetString("snapshot_manual_value")),
		SnapshotManualName:       getValue(cmd, "snapshot-manual-name", "SNAPSHOT_NAME", viper.GetString("snapshot_manual_name")),
		SnapshotManualSystem:     getValue(cmd, "snapshot-manual-system", "SNAPSHOT_SYSTEM", viper.GetString("snapshot_manual_system")),
		SnapshotManualDaysCount:  getValue(cmd, "snapshot-manual-days-count", "SNAPSHOT_DAYS_COUNT", viper.GetString("snapshot_manual_days_count")),
		SnapshotManualCountS3:    getValue(cmd, "snapshot-manual-count-s3", "SNAPSHOT_COUNT_S3", viper.GetString("snapshot_manual_count_s3")),
		OSCTLConfig:              getValue(cmd, "config", "OSCTL_CONFIG", viper.GetString("osctl_config")),
		OSCTLIndicesConfig:       getValue(cmd, "osctl-indices-config", "OSCTL_INDICES_CONFIG", viper.GetString("osctl_indices_config")),
		OsctlIndicesConfig:       osctlIndicesConfig,
		OSCTLTenantsConfig:       tenantsPath,
		KibanaMultidomainEnabled: getValue(cmd, "kibana-multidomain-enabled", "KIBANA_MULTIDOMAIN_ENABLED", viper.GetString("kibana_multidomain_enabled")),
		DataSourceName:           getValue(cmd, "datasource-name", "DATA_SOURCE_NAME", viper.GetString("datasource_name")),
	}

	return nil
}

func setDefaults() {
	viper.SetDefault("action", "")
	viper.SetDefault("opensearch_url", "https://opendistro:9200")
	viper.SetDefault("opensearch_recoverer_url", "https://opendistro-recoverer:9200")
	viper.SetDefault("cert_file", "/etc/ssl/certs/admin-crt.pem")
	viper.SetDefault("key_file", "/etc/ssl/certs/admin-key.pem")
	viper.SetDefault("ca_file", "/etc/ssl/certs/elk-root-ca.pem")
	viper.SetDefault("timeout", "300s")
	viper.SetDefault("retry_attempts", 3)
	viper.SetDefault("date_format", "%Y.%m.%d")
	viper.SetDefault("recoverer_date_format", "%d-%m-%Y")
	viper.SetDefault("madison_url", "https://madison.flant.com/api/events/custom/")
	viper.SetDefault("osd_url", "")
	viper.SetDefault("kibana_user", "")
	viper.SetDefault("kibana_pass", "")
	viper.SetDefault("madison_key", "")
	viper.SetDefault("madison_project", "lm-elk")
	viper.SetDefault("snapshot_repo", "s3-backup")
	viper.SetDefault("retention_threshold", 75.0)
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
	viper.SetDefault("dry_run", false)
	viper.SetDefault("osctl_config", "config.yaml")
	viper.SetDefault("osctl_indices_config", "osctlindicesconfig.yaml")
	viper.SetDefault("sharding_target_size_gib", 25)
	viper.SetDefault("exclude_sharding", "")
	viper.SetDefault("kibana_index_regex", "^([\\w-]+)-([\\w-]*)(\\d{4}[\\.-]\\d{2}[\\.-]\\d{2}(?:[\\.-]\\d{2})*)$")
	viper.SetDefault("kibana_multitenancy", false)
	viper.SetDefault("recoverer_enabled", false)
	viper.SetDefault("kube_namespace", "infra-elklogs")
	viper.SetDefault("kibana_tenants_config", "osctltenants.yaml")
	viper.SetDefault("kibana_multidomain_enabled", false)
	viper.SetDefault("datasource_name", "recoverer")
}

func GetAvailableActions() []string {
	return []string{
		"snapshots",
		"snapshot-manual",
		"snapshotsdelete",
		"snapshotschecker",
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

type ValidationError struct {
	Field   string
	Message string
}

type Validator struct {
	field string
	value interface{}
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error in field '%s': %s", e.Field, e.Message)
}

func NewValidator(field string, value interface{}) *Validator {
	return &Validator{field: field, value: value}
}

func (v *Validator) Required() *Validator {
	if v.value == nil || v.value == "" {
		panic(ValidationError{Field: v.field, Message: "is required"})
	}
	return v
}

func (v *Validator) URL() *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		if _, err := url.Parse(str); err != nil {
			panic(ValidationError{Field: v.field, Message: "must be a valid URL"})
		}
	}
	return v
}

func (v *Validator) OneOf(options ...string) *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		for _, option := range options {
			if str == option {
				return v
			}
		}
		panic(ValidationError{Field: v.field, Message: fmt.Sprintf("must be one of: %s", strings.Join(options, ", "))})
	}
	return v
}

func (v *Validator) Min(min int) *Validator {
	if num, ok := v.value.(int); ok {
		if num < min {
			panic(ValidationError{Field: v.field, Message: fmt.Sprintf("must be at least %d", min)})
		}
	}
	return v
}

func (v *Validator) Max(max int) *Validator {
	if num, ok := v.value.(int); ok {
		if num > max {
			panic(ValidationError{Field: v.field, Message: fmt.Sprintf("must be at most %d", max)})
		}
	}
	return v
}

func (v *Validator) DateFormat() *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		validFormats := []string{
			"%Y.%m.%d", "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
			"%Y.%m.%d.%H", "%Y-%m-%d %H:%M:%S", "%Y%m%d",
		}
		for _, format := range validFormats {
			if str == format {
				return v
			}
		}
		panic(ValidationError{Field: v.field, Message: fmt.Sprintf("must be a valid date format, supported: %s", strings.Join(validFormats, ", "))})
	}
	return v
}

func (v *Validator) FileExists() *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		if _, err := os.Stat(str); os.IsNotExist(err) {
			panic(ValidationError{Field: v.field, Message: "file does not exist"})
		}
	}
	return v
}

func (v *Validator) FileExistsOptional() *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		if _, err := os.Stat(str); os.IsNotExist(err) {
			panic(ValidationError{Field: v.field, Message: "file does not exist"})
		}
	}
	return v
}

func (v *Validator) Regex(pattern string) *Validator {
	if str, ok := v.value.(string); ok && str != "" {
		if matched, _ := regexp.MatchString(pattern, str); !matched {
			panic(ValidationError{Field: v.field, Message: fmt.Sprintf("must match pattern: %s", pattern)})
		}
	}
	return v
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

func (c *Config) GetTimeout() time.Duration {
	if c.Timeout == "" {
		return 30 * time.Second
	}
	if duration, err := time.ParseDuration(c.Timeout); err == nil {
		return duration
	}
	return 30 * time.Second
}

func (c *Config) GetRetryAttempts() int {
	if c.RetryAttempts == "" {
		return 3
	}
	if value, err := strconv.Atoi(c.RetryAttempts); err == nil {
		return value
	}
	return 3
}

func (c *Config) GetRetentionThreshold() float64 {
	if c.RetentionThreshold == "" {
		return 75.0
	}
	if value, err := strconv.ParseFloat(c.RetentionThreshold, 64); err == nil {
		return value
	}
	return 75.0
}

func (c *Config) GetDereplicatorDaysCount() int {
	if c.DereplicatorDaysCount == "" {
		return 2
	}
	if value, err := strconv.Atoi(c.DereplicatorDaysCount); err == nil {
		return value
	}
	return 2
}

func (c *Config) GetDereplicatorUseSnapshot() bool {
	if c.DereplicatorUseSnapshot == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.DereplicatorUseSnapshot); err == nil {
		return value
	}
	return false
}

func (c *Config) GetHotCount() int {
	if c.HotCount == "" {
		return 1
	}
	if value, err := strconv.Atoi(c.HotCount); err == nil {
		return value
	}
	return 1
}

func (c *Config) GetExtractedDays() int {
	if c.ExtractedDays == "" {
		return 7
	}
	if value, err := strconv.Atoi(c.ExtractedDays); err == nil {
		return value
	}
	return 7
}

func (c *Config) GetDryRun() bool {
	if c.DryRun == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.DryRun); err == nil {
		return value
	}
	return false
}

func (c *Config) GetRecovererEnabled() bool {
	if c.RecovererEnabled == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.RecovererEnabled); err == nil {
		return value
	}
	return false
}

func (c *CommandConfig) GetTimeout() time.Duration {
	if c.Timeout == "" {
		return 30 * time.Second
	}
	if duration, err := time.ParseDuration(c.Timeout); err == nil {
		return duration
	}
	return 30 * time.Second
}

func (c *CommandConfig) GetRetryAttempts() int {
	if c.RetryAttempts == "" {
		return 3
	}
	if value, err := strconv.Atoi(c.RetryAttempts); err == nil {
		return value
	}
	return 3
}

func (c *CommandConfig) GetRetentionThreshold() float64 {
	if c.RetentionThreshold == "" {
		return 75.0
	}
	if value, err := strconv.ParseFloat(c.RetentionThreshold, 64); err == nil {
		return value
	}
	return 75.0
}

func (c *CommandConfig) GetDereplicatorDaysCount() int {
	if c.DereplicatorDaysCount == "" {
		return 2
	}
	if value, err := strconv.Atoi(c.DereplicatorDaysCount); err == nil {
		return value
	}
	return 2
}

func (c *CommandConfig) GetDereplicatorUseSnapshot() bool {
	if c.DereplicatorUseSnapshot == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.DereplicatorUseSnapshot); err == nil {
		return value
	}
	return false
}

func (c *CommandConfig) GetHotCount() int {
	if c.HotCount == "" {
		return 1
	}
	if value, err := strconv.Atoi(c.HotCount); err == nil {
		return value
	}
	return 1
}

func (c *CommandConfig) GetExtractedDays() int {
	if c.ExtractedDays == "" {
		return 7
	}
	if value, err := strconv.Atoi(c.ExtractedDays); err == nil {
		return value
	}
	return 7
}

func (c *CommandConfig) GetDryRun() bool {
	if c.DryRun == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.DryRun); err == nil {
		return value
	}
	return false
}

func (c *CommandConfig) GetKibanaMultitenancy() bool {
	if c.KibanaMultitenancy == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.KibanaMultitenancy); err == nil {
		return value
	}
	return false
}

func (c *CommandConfig) GetRecovererEnabled() bool {
	if c.RecovererEnabled == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.RecovererEnabled); err == nil {
		return value
	}
	return false
}

func (c *Config) GetSnapshotManualSystem() bool {
	if c.SnapshotManualSystem == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.SnapshotManualSystem); err == nil {
		return value
	}
	return false
}

func (c *Config) GetKibanaMultidomainEnabled() bool {
	if c.KibanaMultidomainEnabled == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.KibanaMultidomainEnabled); err == nil {
		return value
	}
	return false
}

func (c *CommandConfig) GetShardingTargetSizeGiB() int {
	if c.ShardingTargetSizeGiB == "" {
		return 25
	}
	if n, err := strconv.Atoi(c.ShardingTargetSizeGiB); err == nil {
		if n > 50 {
			return 50
		}
		if n < 1 {
			return 1
		}
		return n
	}
	return 25
}

func (c *CommandConfig) GetKibanaMultidomainEnabled() bool {
	if c.KibanaMultidomainEnabled == "" {
		return false
	}
	if value, err := strconv.ParseBool(c.KibanaMultidomainEnabled); err == nil {
		return value
	}
	return false
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
	},
	"retention": {
		{"retention-threshold", "int", 75, "Disk usage threshold percentage", []string{"min:0", "max:100"}},
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
		{"kube-namespace", "string", "default", "Kubernetes namespace for secrets", []string{}},
		{"kibana-multidomain-enabled", "bool", false, "Enable Kibana multidomain cert management", []string{}},
	},
	"extracteddelete": {
		{"os-recoverer-url", "string", "", "OpenSearch recoverer cluster URL", []string{}},
		{"recoverer-date-format", "string", "%Y.%m.%d", "Date format for recoverer index names", []string{}},
		{"extracted-pattern", "string", "extracted_", "Prefix for extracted indices", []string{}},
		{"days", "int", 7, "Number of days to keep extracted indices", []string{"min:1", "max:365"}},
		{"dry-run", "bool", false, "Show what would be deleted without actually deleting", []string{}},
	},
	"indicesdelete": {
		// Uses --osctl-indices-config for configuration
	},
	"sharding": {
		{"sharding-target-size-gib", "int", 25, "Target max shard size GiB (<=50)", []string{"min:1", "max:50"}},
		{"exclude-sharding", "string", "", "Regex to exclude patterns from sharding", []string{}},
	},
	"indexpatterns": {
		{"kibana-index-regex", "string", "^(.*?)-\\d{4}\\.\\d{2}\\.\\d{2}.*$", "Regex to extract pattern from today's indices", []string{}},
		{"kibana-multitenancy", "bool", false, "Enable multitenancy mode", []string{}},
		// Tenants are always sourced from YAML config; no CLI list
		{"kibana-tenants-config", "string", "osctltenants.yaml", "Path to YAML tenants and patterns", []string{}},
		{"recoverer-enabled", "bool", false, "Enable recoverer extracted_* pattern creation", []string{}},
	},
	"snapshotsdelete": {
		// Uses --osctl-indices-config for configuration
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

type CommandConfig struct {
	OpenSearchURL            string
	OpenSearchRecovererURL   string
	CertFile                 string
	KeyFile                  string
	CAFile                   string
	Timeout                  string
	RetryAttempts            string
	DateFormat               string
	RecovererDateFormat      string
	MadisonURL               string
	OSDURL                   string
	MadisonKey               string
	MadisonProject           string
	SnapshotRepo             string
	RetentionThreshold       string
	DereplicatorDaysCount    string
	DereplicatorUseSnapshot  string
	DataSourceName           string
	KibanaUser               string
	KibanaPass               string
	HotCount                 string
	ColdAttribute            string
	ExtractedPattern         string
	ExtractedDays            string
	DryRun                   string
	ShardingTargetSizeGiB    string
	ShardingExcludeRegex     string
	KibanaIndexRegex         string
	KibanaMultitenancy       string
	KibanaTenants            string
	RecovererEnabled         string
	KubeNamespace            string
	KibanaMultidomainEnabled string
}

func GetCommandConfig(cmd *cobra.Command) *CommandConfig {
	cfg := GetConfig()
	return &CommandConfig{
		OpenSearchURL:            cfg.OpenSearchURL,
		OpenSearchRecovererURL:   cfg.OpenSearchRecovererURL,
		CertFile:                 cfg.CertFile,
		KeyFile:                  cfg.KeyFile,
		CAFile:                   cfg.CAFile,
		Timeout:                  cfg.Timeout,
		RetryAttempts:            cfg.RetryAttempts,
		DateFormat:               cfg.DateFormat,
		RecovererDateFormat:      cfg.RecovererDateFormat,
		MadisonURL:               cfg.MadisonURL,
		OSDURL:                   cfg.OSDURL,
		MadisonKey:               cfg.MadisonKey,
		MadisonProject:           cfg.MadisonProject,
		SnapshotRepo:             cfg.SnapshotRepo,
		RetentionThreshold:       cfg.RetentionThreshold,
		DereplicatorDaysCount:    cfg.DereplicatorDaysCount,
		DereplicatorUseSnapshot:  cfg.DereplicatorUseSnapshot,
		DataSourceName:           cfg.DataSourceName,
		KibanaUser:               cfg.KibanaUser,
		KibanaPass:               cfg.KibanaPass,
		HotCount:                 cfg.HotCount,
		ColdAttribute:            cfg.ColdAttribute,
		ExtractedPattern:         cfg.ExtractedPattern,
		ExtractedDays:            cfg.ExtractedDays,
		DryRun:                   cfg.DryRun,
		ShardingTargetSizeGiB:    cfg.ShardingTargetSizeGiB,
		ShardingExcludeRegex:     cfg.ShardingExcludeRegex,
		KibanaIndexRegex:         cfg.KibanaIndexRegex,
		KibanaMultitenancy:       cfg.KibanaMultitenancy,
		KibanaTenants:            cfg.KibanaTenants,
		RecovererEnabled:         cfg.RecovererEnabled,
		KubeNamespace:            cfg.KubeNamespace,
		KibanaMultidomainEnabled: cfg.KibanaMultidomainEnabled,
	}
}
