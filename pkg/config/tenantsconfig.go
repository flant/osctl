package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type TenantSpec struct {
	Name     string   `yaml:"name"`
	Patterns []string `yaml:"indices"`
}

type TenantsFile struct {
	Tenants []TenantSpec `yaml:"tenants"`
}

func LoadTenantsConfig(path string) (*TenantsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read tenants config: %w", err)
	}
	var tf TenantsFile
	if err := yaml.Unmarshal(data, &tf); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tenants config: %w", err)
	}
	return &tf, nil
}

func (c *Config) GetTenantsConfig() (*TenantsFile, error) {
	configPath := c.GetOSCTLTenantsConfig()
	if configPath == "" {
		return nil, fmt.Errorf("kibana-tenants-config must be provided in multitenancy mode")
	}
	st, err := os.Stat(configPath)
	if err != nil || st.IsDir() {
		return nil, fmt.Errorf("kibana-tenants-config '%s' not found or is a directory", configPath)
	}
	return LoadTenantsConfig(configPath)
}

func (c *Config) GetIndexPatternsTenantsConfig() (*TenantsFile, error) {
	configPath := c.GetIndexPatternsKibanaTenantsConfig()
	if configPath == "" {
		configPath = "osctltenants.yaml"
	}
	st, err := os.Stat(configPath)
	if err != nil || st.IsDir() {
		return nil, fmt.Errorf("indexpatterns-kibana-tenants-config '%s' not found or is a directory", configPath)
	}
	return LoadTenantsConfig(configPath)
}

func (tf *TenantsFile) GetTenantNames() []string {
	out := []string{}
	for _, t := range tf.Tenants {
		name := strings.TrimSpace(t.Name)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func (tf *TenantsFile) GetTenantPatternsMap() map[string][]string {
	res := map[string][]string{}
	for _, t := range tf.Tenants {
		seen := map[string]struct{}{}
		for _, p := range t.Patterns {
			pp := strings.TrimSpace(p)
			if pp == "" {
				continue
			}
			if _, ok := seen[pp]; ok {
				continue
			}
			seen[pp] = struct{}{}
			res[t.Name] = append(res[t.Name], pp)
		}
	}
	return res
}
