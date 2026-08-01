package opensearch

import (
	"fmt"
	"strings"
)

type IndexTemplate struct {
	IndexTemplates []struct {
		Name          string `json:"name"`
		IndexTemplate struct {
			IndexPatterns []string       `json:"index_patterns"`
			Template      map[string]any `json:"template"`
			Priority      int            `json:"priority"`
			ComposedOf    []string       `json:"composed_of,omitempty"`
		} `json:"index_template"`
	} `json:"index_templates"`
}

func (c *Client) FindIndexTemplateByPattern(pattern string) (string, error) {
	url := fmt.Sprintf("%s/_index_template", c.baseURL)
	var it IndexTemplate
	if err := c.getJSON(url, &it); err != nil {
		return "", err
	}
	normalizedPattern := strings.TrimSuffix(pattern, "*")
	normalizedPattern = strings.TrimSuffix(normalizedPattern, "-")
	for _, t := range it.IndexTemplates {
		for _, p := range t.IndexTemplate.IndexPatterns {
			normalizedP := strings.TrimSuffix(p, "*")
			normalizedP = strings.TrimSuffix(normalizedP, "-")
			if normalizedP == normalizedPattern {
				return t.Name, nil
			}
		}
	}
	return "", nil
}

func (c *Client) PutIndexTemplate(name string, body map[string]any) error {
	url := fmt.Sprintf("%s/_index_template/%s", c.baseURL, name)
	return c.putJSON(url, body)
}

func (c *Client) GetIndexTemplate(name string) (*IndexTemplate, error) {
	url := fmt.Sprintf("%s/_index_template/%s", c.baseURL, name)
	var it IndexTemplate
	if err := c.getJSON(url, &it); err != nil {
		return nil, err
	}
	return &it, nil
}

func (c *Client) GetAllIndexTemplates() (*IndexTemplate, error) {
	url := fmt.Sprintf("%s/_index_template", c.baseURL)
	var it IndexTemplate
	if err := c.getJSON(url, &it); err != nil {
		return nil, err
	}
	return &it, nil
}
