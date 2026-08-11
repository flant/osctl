package commands

import (
	"fmt"
	"strings"
	"testing"
)

func TestTranslogBatchesSplitsByCountAndLength(t *testing.T) {
	var short []string
	for i := 0; i < 120; i++ {
		short = append(short, fmt.Sprintf("index-%d", i))
	}

	batches := translogBatches(short)
	if len(batches) != 3 {
		t.Fatalf("got %d batches, want 3", len(batches))
	}
	for _, batch := range batches {
		if len(batch) > translogBatchSize {
			t.Fatalf("batch of %d indices exceeds %d", len(batch), translogBatchSize)
		}
	}

	long := []string{strings.Repeat("a", 400), strings.Repeat("b", 400), strings.Repeat("c", 400)}
	for _, batch := range translogBatches(append(long, short...)) {
		length := 0
		for _, name := range batch {
			length += len(name) + 1
		}
		if length > translogBatchLength {
			t.Fatalf("batch url length %d exceeds %d", length, translogBatchLength)
		}
	}
}

func TestTranslogNameFilter(t *testing.T) {
	filter, err := newTranslogNameFilter("^app-", "-tmp$")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for name, want := range map[string]bool{"app-logs": false, "app-logs-tmp": true, "other-logs": true} {
		if got := filter.skip(name); got != want {
			t.Fatalf("skip(%q) = %t, want %t", name, got, want)
		}
	}

	if _, err := newTranslogNameFilter("(", ""); err == nil {
		t.Fatal("expected an invalid regex to be reported")
	}
}

func TestIsCatchAllTemplate(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		want bool
	}{
		{"composable catch-all", map[string]any{"index_patterns": []any{"*"}}, true},
		{"composable service", map[string]any{"index_patterns": []any{"*jaeger-span-*"}}, false},
		{"one of many", map[string]any{"index_patterns": []any{"logs-*", "*"}}, true},
		{"es5 legacy catch-all", map[string]any{"template": "*"}, true},
		{"composable template section", map[string]any{"index_patterns": []any{"logs-*"}, "template": map[string]any{"settings": map[string]any{}}}, false},
		{"no patterns", map[string]any{}, false},
	}

	for _, c := range cases {
		if got := isCatchAllTemplate(c.body); got != c.want {
			t.Fatalf("%s: got %t, want %t", c.name, got, c.want)
		}
	}
}
