package utils

import (
	"fmt"
	"osctl/pkg/opensearch"
	"strconv"
	"strings"
)

func TemplateExists(client *opensearch.Client, templateName string) (bool, error) {
	_, err := client.GetIndexTemplate(templateName)
	if err != nil {
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "resource_not_found_exception") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func GetTemplateShardCount(tpl *opensearch.IndexTemplate) (int, error) {
	if len(tpl.IndexTemplates) == 0 {
		return 0, fmt.Errorf("template has no index templates")
	}

	template := tpl.IndexTemplates[0].IndexTemplate.Template
	settings, ok := template["settings"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("template settings not found")
	}

	index, ok := settings["index"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("template index settings not found")
	}

	shards, ok := index["number_of_shards"]
	if !ok {
		return 0, fmt.Errorf("number_of_shards not found")
	}

	shardsStr, ok := shards.(string)
	if !ok {
		return 0, fmt.Errorf("number_of_shards is not a string: %T", shards)
	}

	s, err := strconv.Atoi(shardsStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse shards as int: %v", err)
	}
	return s, nil
}
