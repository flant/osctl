package utils

import (
	"osctl/pkg/config"
	"osctl/pkg/opensearch"
	"strconv"
)

func ResolveOpenIndicesForPrefix(client *opensearch.Client, indexConfig config.IndexConfig) ([]string, map[string]int64, error) {
	pattern := "*"
	if indexConfig.Kind == "prefix" && indexConfig.Value != "" {
		pattern = indexConfig.Value + "*"
	}

	infos, err := client.GetIndicesWithFields(pattern, "index,status,ss", "ss:desc")
	if err != nil {
		return nil, nil, err
	}

	var indices []string
	sizes := make(map[string]int64)
	for _, idx := range infos {
		if idx.Status != "" && idx.Status != "open" {
			continue
		}
		if !MatchesIndex(idx.Index, indexConfig) {
			continue
		}
		indices = append(indices, idx.Index)
		if size, err := strconv.ParseInt(idx.Size, 10, 64); err == nil {
			sizes[idx.Index] = size
		}
	}

	return indices, sizes, nil
}
