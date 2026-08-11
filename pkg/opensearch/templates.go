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

type RawIndexTemplate struct {
	Name string
	Body map[string]any
}

// GetAllIndexTemplatesRaw returns templates as raw maps, so a template can be
// modified and sent back without losing fields osctl does not know about
// (version, _meta, data_stream and so on).
func (c *Client) GetAllIndexTemplatesRaw() ([]RawIndexTemplate, error) {
	url := fmt.Sprintf("%s/_index_template", c.baseURL)

	var response struct {
		IndexTemplates []struct {
			Name          string         `json:"name"`
			IndexTemplate map[string]any `json:"index_template"`
		} `json:"index_templates"`
	}
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}

	templates := make([]RawIndexTemplate, 0, len(response.IndexTemplates))
	for _, t := range response.IndexTemplates {
		body := t.IndexTemplate
		if body == nil {
			body = map[string]any{}
		}
		templates = append(templates, RawIndexTemplate{Name: t.Name, Body: body})
	}
	return templates, nil
}

// GetAllLegacyTemplates returns templates of the legacy _template API: they still
// exist in modern clusters (Jaeger creates them) and are the only kind in ES 5.x.
func (c *Client) GetAllLegacyTemplates() (map[string]map[string]any, error) {
	url := fmt.Sprintf("%s/_template", c.baseURL)

	var templates map[string]map[string]any
	if err := c.getJSON(url, &templates); err != nil {
		return nil, err
	}
	return templates, nil
}

func (c *Client) PutLegacyTemplate(name string, body map[string]any) error {
	url := fmt.Sprintf("%s/_template/%s", c.baseURL, escapePathSegment(name))
	return c.putJSON(url, body)
}

func (c *Client) GetAllIndexTemplates() (*IndexTemplate, error) {
	url := fmt.Sprintf("%s/_index_template", c.baseURL)
	var it IndexTemplate
	if err := c.getJSON(url, &it); err != nil {
		return nil, err
	}
	return &it, nil
}
