package utils

import (
	"encoding/json"
	"testing"
)

func TestEnsureSettingNested(t *testing.T) {
	settings := map[string]any{
		"index": map[string]any{
			"number_of_shards": "1",
		},
	}

	if changed := EnsureSetting(settings, "index.translog.durability", "async"); !changed {
		t.Fatal("expected the missing setting to be added")
	}
	if changed := EnsureSetting(settings, "index.translog.durability", "async"); changed {
		t.Fatal("expected the second call to be a no-op")
	}

	got, _ := json.Marshal(settings)
	want := `{"index":{"number_of_shards":"1","translog":{"durability":"async"}}}`
	if string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestEnsureSettingKeepsExistingNotation(t *testing.T) {
	cases := map[string]map[string]any{
		"flat":  {"index.translog.durability": "request"},
		"mixed": {"index": map[string]any{"translog.durability": "request"}},
	}

	for name, settings := range cases {
		if changed := EnsureSetting(settings, "index.translog.durability", "async"); !changed {
			t.Fatalf("%s: expected the setting to be changed", name)
		}
		value, ok := LookupSetting(settings, "index.translog.durability")
		if !ok || value != "async" {
			t.Fatalf("%s: got %q (found=%t), want async", name, value, ok)
		}
		if len(settings) != 1 {
			t.Fatalf("%s: expected the value to be updated in place, got %v", name, settings)
		}
	}
}

func TestLookupSettingNonString(t *testing.T) {
	settings := map[string]any{"index": map[string]any{"number_of_shards": float64(3)}}

	value, ok := LookupSetting(settings, "index.number_of_shards")
	if !ok || value != "3" {
		t.Fatalf("got %q (found=%t), want 3", value, ok)
	}

	if _, ok := LookupSetting(settings, "index.translog.durability"); ok {
		t.Fatal("expected a missing setting to be reported as missing")
	}
}
