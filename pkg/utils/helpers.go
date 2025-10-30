package utils

import (
	"osctl/pkg/config"
	"osctl/pkg/opensearch"
)

func NewOSClientFromCommandConfig(cfg *config.CommandConfig) (*opensearch.Client, error) {
	return opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
}
