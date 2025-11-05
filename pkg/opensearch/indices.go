package opensearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type IndexInfo struct {
	Index        string `json:"index"`
	Rep          string `json:"rep"`
	Size         string `json:"ss"`
	PriStoreSize string `json:"pri.store.size"`
	CreationDate string `json:"cd"`
}

type OSSearchHit struct {
	ID     string         `json:"_id"`
	Source map[string]any `json:"_source"`
}

type OSSearchHits struct {
	Total struct {
		Value int `json:"value"`
	} `json:"total"`
	Hits []OSSearchHit `json:"hits"`
}

type OSSearchResponse struct {
	Hits OSSearchHits `json:"hits"`
}

type DanglingIndex struct {
	IndexName string `json:"index_name"`
	IndexUUID string `json:"index_uuid"`
}

type DanglingResponse struct {
	DanglingIndices []DanglingIndex `json:"dangling_indices"`
}

func (c *Client) GetIndicesWithFields(pattern, fields string, sortBy ...string) ([]IndexInfo, error) {
	sortParam := ""
	if len(sortBy) > 0 && sortBy[0] != "" {
		sortParam = sortBy[0]
	}

	url := fmt.Sprintf("%s/_cat/indices/%s?format=json&bytes=b&h=%s", c.baseURL, pattern, fields)
	if sortParam != "" {
		url += fmt.Sprintf("&s=%s", sortParam)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var indices []IndexInfo
	if err := json.Unmarshal(body, &indices); err != nil {
		return nil, err
	}

	return indices, nil
}

func (c *Client) Search(index, query string) (*OSSearchResponse, error) {
	url := fmt.Sprintf("%s/%s/_search?%s", c.baseURL, strings.TrimLeft(index, "/"), query)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("SEARCH %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	var sr OSSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		return nil, err
	}
	return &sr, nil
}

func (c *Client) CreateDoc(index, id string, payload interface{}) error {
	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, strings.TrimLeft(index, "/"), id)
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("opensearch create doc failed: %s — %s", resp.Status, readErrorSnippet(resp))
	}
	return nil
}

func (c *Client) DeleteIndex(index string) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)
	return c.delete(url)
}

func (c *Client) DeleteIndices(indices []string) error {
	if len(indices) == 0 {
		return nil
	}

	indicesList := strings.Join(indices, ",")
	url := fmt.Sprintf("%s/%s", c.baseURL, indicesList)
	return c.delete(url)
}

func (c *Client) GetDanglingIndices() ([]DanglingIndex, error) {
	url := fmt.Sprintf("%s/_dangling?pretty", c.baseURL)

	var result DanglingResponse
	if err := c.getJSON(url, &result); err != nil {
		return nil, err
	}

	return result.DanglingIndices, nil
}

func (c *Client) SetReplicas(index string, replicas int) error {
	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, index)

	settings := map[string]any{
		"index": map[string]any{
			"number_of_replicas": replicas,
		},
	}

	return c.putJSON(url, settings)
}

func (c *Client) SetColdStorage(index, coldAttribute string) error {
	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, index)

	settings := map[string]any{
		"index": map[string]any{
			"routing.allocation.require.temp": coldAttribute,
			"number_of_replicas":              0,
		},
	}

	return c.putJSON(url, settings)
}

func (c *Client) GetIndexColdRequirement(index string) (string, error) {
	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, index)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("GET %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var raw map[string]struct {
		Settings map[string]any `json:"settings"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", err
	}
	for _, data := range raw {
		settings, ok := data.Settings["index"].(map[string]any)
		if !ok {
			return "", nil
		}
		routing, ok := settings["routing"].(map[string]any)
		if !ok {
			return "", nil
		}
		allocation, ok := routing["allocation"].(map[string]any)
		if !ok {
			return "", nil
		}
		require, ok := allocation["require"].(map[string]any)
		if !ok {
			return "", nil
		}
		if v, ok := require["temp"]; ok {
			if s, ok := v.(string); ok {
				return s, nil
			}
		}
	}
	return "", nil
}
