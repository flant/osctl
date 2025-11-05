package config

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type OsctlIndicesConfig struct {
	S3Snapshots S3SnapshotsConfig `yaml:"s3_snapshots"`
	Unknown     UnknownConfig     `yaml:"unknown"`
	Indices     []IndexConfig     `yaml:"indices"`
}

type S3SnapshotsConfig struct {
	UnitCount UnitCountConfig `yaml:"unit_count"`
}

type UnitCountConfig struct {
	All     int `yaml:"all"`
	Unknown int `yaml:"unknown"`
}

type UnknownConfig struct {
	DaysCount      int  `yaml:"days_count"`
	Snapshot       bool `yaml:"snapshot"`
	ManualSnapshot bool `yaml:"manual_snapshot,omitempty"`
}

type IndexConfig struct {
	Kind            string `yaml:"kind"`
	Value           string `yaml:"value"`
	Name            string `yaml:"name"`
	System          bool   `yaml:"system,omitempty"`
	Repository      string `yaml:"repository,omitempty"`
	Schedule        string `yaml:"schedule,omitempty"`
	DaysCount       int    `yaml:"days_count"`
	Snapshot        bool   `yaml:"snapshot"`
	SnapshotCountS3 int    `yaml:"snapshot_count_s3,omitempty"`
	ManualSnapshot  bool   `yaml:"manual_snapshot,omitempty"`
}

func LoadOsctlIndicesConfig(path string) (*OsctlIndicesConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read osctl indices config file: %w", err)
	}

	content := strings.TrimPrefix(string(data), "---")

	var config OsctlIndicesConfig
	err = yaml.Unmarshal([]byte(content), &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal osctl indices config: %w", err)
	}

	return &config, nil
}

func ValidateOsctlIndicesConfig(config *OsctlIndicesConfig, dateFormat string) error {
	for i, indexConfig := range config.Indices {
		if indexConfig.Kind == "regex" {
			if !containsDatePattern(indexConfig.Value, dateFormat) {
				return fmt.Errorf("index config #%d: regex pattern '%s' must contain date pattern in format '%s'", i+1, indexConfig.Value, dateFormat)
			}
			if indexConfig.Name == "" {
				return fmt.Errorf("index config #%d: regex pattern '%s' must have 'name' field specified", i+1, indexConfig.Value)
			}
		}
	}
	return nil
}

func containsDatePattern(pattern, dateFormat string) bool {
	dateRegex := convertDateFormatToRegex(dateFormat)

	return strings.Contains(pattern, dateRegex)
}

func convertDateFormatToRegex(dateFormat string) string {
	pattern := dateFormat
	pattern = strings.ReplaceAll(pattern, "%Y", `\d{4}`)
	pattern = strings.ReplaceAll(pattern, "%m", `\d{2}`)
	pattern = strings.ReplaceAll(pattern, "%d", `\d{2}`)
	pattern = strings.ReplaceAll(pattern, ".", `\.`)
	pattern = strings.ReplaceAll(pattern, "-", `-`)
	pattern = strings.ReplaceAll(pattern, "_", `_`)

	return pattern
}

func (c *Config) GetOsctlIndices() ([]IndexConfig, error) {
	if c.OsctlIndicesConfig == nil {
		return nil, fmt.Errorf("osctl indices config is not loaded")
	}

	indices := c.OsctlIndicesConfig.Indices

	sort.Slice(indices, func(i, j int) bool {
		return indices[i].Value < indices[j].Value
	})

	return indices, nil
}

func (c *Config) GetOsctlIndicesUnknownConfig() UnknownConfig {
	if c.OsctlIndicesConfig == nil {
		return UnknownConfig{
			DaysCount: 7,
			Snapshot:  true,
		}
	}

	return c.OsctlIndicesConfig.Unknown
}

func (c *Config) GetOsctlIndicesS3SnapshotsConfig() S3SnapshotsConfig {
	if c.OsctlIndicesConfig == nil {
		return S3SnapshotsConfig{
			UnitCount: UnitCountConfig{
				All:     60,
				Unknown: 7,
			},
		}
	}

	return c.OsctlIndicesConfig.S3Snapshots
}

func (c *Config) IsOsctlIndicesMode() bool {
	return c.OsctlIndicesConfig != nil
}
