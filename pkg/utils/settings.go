package utils

import (
	"fmt"
	"strings"
)

// LookupSetting finds a setting regardless of the notation used by the map: flat
// ("index.translog.durability"), nested ({"index":{"translog":{...}}}) or mixed.
func LookupSetting(settings map[string]any, key string) (string, bool) {
	return lookupSetting(settings, strings.Split(key, "."))
}

// EnsureSetting makes sure key holds value, keeping the notation already used by
// the map. Returns true when the map was changed.
func EnsureSetting(settings map[string]any, key, value string) bool {
	return ensureSetting(settings, strings.Split(key, "."), value)
}

func GetOrCreateSection(settings map[string]any, name string) map[string]any {
	if section, ok := settings[name].(map[string]any); ok {
		return section
	}
	section := map[string]any{}
	settings[name] = section
	return section
}

func lookupSetting(settings map[string]any, parts []string) (string, bool) {
	if settings == nil || len(parts) == 0 {
		return "", false
	}
	for i := len(parts); i > 0; i-- {
		value, ok := settings[strings.Join(parts[:i], ".")]
		if !ok {
			continue
		}
		if i == len(parts) {
			return settingToString(value), true
		}
		if nested, ok := value.(map[string]any); ok {
			if found, ok := lookupSetting(nested, parts[i:]); ok {
				return found, true
			}
		}
	}
	return "", false
}

func ensureSetting(settings map[string]any, parts []string, value string) bool {
	for i := len(parts); i > 0; i-- {
		key := strings.Join(parts[:i], ".")
		current, ok := settings[key]
		if !ok {
			continue
		}
		if i == len(parts) {
			if settingToString(current) == value {
				return false
			}
			settings[key] = value
			return true
		}
		nested, ok := current.(map[string]any)
		if !ok {
			continue
		}
		if _, found := lookupSetting(nested, parts[i:]); found {
			return ensureSetting(nested, parts[i:], value)
		}
	}

	section := settings
	for i, part := range parts {
		if i == len(parts)-1 {
			section[part] = value
			break
		}
		section = GetOrCreateSection(section, part)
	}
	return true
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
