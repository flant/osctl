package opensearch

import (
	"fmt"
	"strings"
)

const (
	TranslogDurabilityKey   = "index.translog.durability"
	TranslogSyncIntervalKey = "index.translog.sync_interval"
	TranslogDurabilityAsync = "async"
)

type TranslogSettings struct {
	Durability   string
	SyncInterval string
}

func (c *Client) GetIndicesTranslogSettings(pattern string) (map[string]TranslogSettings, error) {
	url := fmt.Sprintf("%s/%s/_settings/index.translog.*?flat_settings=true&expand_wildcards=open&ignore_unavailable=true&allow_no_indices=true",
		c.baseURL, escapePathSegment(pattern))

	var raw map[string]struct {
		Settings map[string]any `json:"settings"`
	}
	if err := c.getJSON(url, &raw); err != nil {
		return nil, err
	}

	result := make(map[string]TranslogSettings, len(raw))
	for index, data := range raw {
		result[index] = TranslogSettings{
			Durability:   settingToString(data.Settings[TranslogDurabilityKey]),
			SyncInterval: settingToString(data.Settings[TranslogSyncIntervalKey]),
		}
	}
	return result, nil
}

func (c *Client) SetIndicesTranslog(indices []string, durability, syncInterval string) error {
	if len(indices) == 0 {
		return nil
	}

	translog := map[string]any{
		"durability": durability,
	}
	if syncInterval != "" {
		translog["sync_interval"] = syncInterval
	}
	settings := map[string]any{
		"index": map[string]any{
			"translog": translog,
		},
	}

	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, escapePathList(indices))
	return c.putJSON(url, settings)
}

func IsNonDynamicSettingError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "non dynamic setting") ||
		strings.Contains(message, "final setting") ||
		strings.Contains(message, "can't update non dynamic")
}

func settingToString(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}
