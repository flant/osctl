package opensearch

import (
	"fmt"
	"net/http"
	"strings"
)

func (c *Client) GetSnapshotsDetailed(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?ignore_unavailable=true", c.baseURL, escapePathSegment(repo), escapePathSegment(pattern))

	var response SnapshotResponse
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}
	return response.Snapshots, nil
}

func (c *Client) RestoreSnapshot(repo, snapshot string, body map[string]any) error {
	url := fmt.Sprintf("%s/_snapshot/%s/%s/_restore?wait_for_completion=false", c.baseURL, escapePathSegment(repo), escapePathSegment(snapshot))
	return c.postJSON(url, body)
}

func (c *Client) IndexExists(index string) (bool, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, escapePathSegment(index))
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	defer resp.Body.Close()
	return resp.StatusCode == 200, nil
}

type IndexHealth struct {
	Status              string
	NumberOfShards      int
	ActivePrimaryShards int
}

func (c *Client) GetIndicesHealth(indices []string) (map[string]IndexHealth, error) {
	list := escapePathList(indices)
	url := fmt.Sprintf("%s/_cluster/health/%s?level=indices", c.baseURL, list)

	var data struct {
		Status  string `json:"status"`
		Indices map[string]struct {
			Status              string `json:"status"`
			NumberOfShards      int    `json:"number_of_shards"`
			ActivePrimaryShards int    `json:"active_primary_shards"`
		} `json:"indices"`
	}
	if err := c.getJSON(url, &data); err != nil {
		return nil, err
	}

	out := make(map[string]IndexHealth, len(data.Indices))
	for name, idx := range data.Indices {
		out[name] = IndexHealth{
			Status:              idx.Status,
			NumberOfShards:      idx.NumberOfShards,
			ActivePrimaryShards: idx.ActivePrimaryShards,
		}
	}
	return out, nil
}
