package utils

import (
	b64 "encoding/base64"
	"fmt"

	"osctl/pkg/config"
	"osctl/pkg/opensearch"
	"strings"

	"github.com/google/uuid"
)

func NewOSClientWithURL(cfg *config.Config, url string) (*opensearch.Client, error) {
	client, err := opensearch.NewClient(url, cfg.GetCertFile(), cfg.GetKeyFile(), cfg.GetCAFile(), cfg.GetTimeout(), cfg.GetRetryAttempts())
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

func GenerateRandomAlphanumericString(length int) string {
	id := uuid.New().String()
	id = strings.ReplaceAll(id, "-", "")
	if length > len(id) {
		length = len(id)
	}
	return id[:length]
}

func PatternVersion() string {
	return b64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("[%d,1]", 100)))
}
