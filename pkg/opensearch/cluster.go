package opensearch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type AllocationInfo struct {
	Name            string `json:"name"`
	NodeRole        string `json:"node.role"`
	DiskUsedPercent string `json:"diskUsedPercent"`
}

type NodesResponse struct {
	Nodes map[string]struct {
		Roles      []string       `json:"roles"`
		Attributes map[string]any `json:"attributes"`
	} `json:"nodes"`
}

type AliasInfo struct {
	Alias string `json:"alias"`
}

func (c *Client) GetAllocation() ([]AllocationInfo, error) {
	url := fmt.Sprintf("%s/_cat/nodes?h=name,node.role,diskUsedPercent&format=json", c.baseURL)

	var allocation []AllocationInfo
	if err := c.getJSON(url, &allocation); err != nil {
		return nil, err
	}

	return allocation, nil
}

func (c *Client) GetDataNodeCount(coldAttribute string) (int, error) {
	url := fmt.Sprintf("%s/_nodes", c.baseURL)
	var nodes NodesResponse
	if err := c.getJSON(url, &nodes); err != nil {
		return 0, err
	}
	count := 0
	for _, n := range nodes.Nodes {
		isData := false
		isMaster := false
		for _, r := range n.Roles {
			if r == "data" {
				isData = true
			}
			if r == "master" {
				isMaster = true
			}
		}
		if !isData || isMaster {
			continue
		}
		count++
	}
	if count == 0 {
		count = 1
	}
	return count, nil
}

func (c *Client) GetAliases(pattern string) ([]AliasInfo, error) {
	url := fmt.Sprintf("%s/_cat/aliases/%s?format=json", c.baseURL, escapePathSegment(pattern))
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
		return nil, fmt.Errorf("GET %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	var out []AliasInfo
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return out, nil
}
