package utils

import (
	"fmt"
	"osctl/pkg/config"
	"osctl/pkg/opensearch"
	"strings"
)

func NewOSClientFromCommandConfig(cfg *config.CommandConfig) (*opensearch.Client, error) {
	return opensearch.NewClient(cfg.GetOpenSearchURL(), cfg.GetCertFile(), cfg.GetKeyFile(), cfg.GetCAFile(), cfg.GetTimeout(), cfg.GetRetryAttempts())
}

func NewOSClientFromCommandConfigWithError(cfg *config.CommandConfig, url string) (*opensearch.Client, error) {
	client, err := opensearch.NewClient(url, cfg.GetCertFile(), cfg.GetKeyFile(), cfg.GetCAFile(), cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	return client, nil
}

func NewOSClientFromConfig(cfg *config.Config) (*opensearch.Client, error) {
	client, err := opensearch.NewClient(cfg.GetOpenSearchURL(), cfg.GetCertFile(), cfg.GetKeyFile(), cfg.GetCAFile(), cfg.GetTimeout(), cfg.GetRetryAttempts())
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	return client, nil
}

func NormalizeURL(url string) string {
	if url != "" && !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return "https://" + url
	}
	return url
}
