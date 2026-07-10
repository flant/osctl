package opensearch

import (
	"bytes"
	"encoding/json"
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

type catRecoveryRow struct {
	Index string `json:"index"`
	Type  string `json:"type"`
	Stage string `json:"stage"`
}

func (c *Client) ActiveSnapshotRecoveryIndices() ([]string, error) {
	url := fmt.Sprintf("%s/_cat/recovery?format=json&active_only=true&h=index,type,stage", c.baseURL)
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
		return nil, fmt.Errorf("GET _cat/recovery failed: %s — %s", resp.Status, readErrorSnippet(resp))
	}
	var rows []catRecoveryRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var out []string
	for _, r := range rows {
		if strings.EqualFold(r.Type, "snapshot") && !strings.EqualFold(r.Stage, "done") {
			if !seen[r.Index] {
				seen[r.Index] = true
				out = append(out, r.Index)
			}
		}
	}
	return out, nil
}

type catShardRow struct {
	Index            string `json:"index"`
	Prirep           string `json:"prirep"`
	State            string `json:"state"`
	UnassignedReason string `json:"unassigned.reason"`
}

func (c *Client) GetShardRows(pattern string) ([]catShardRow, error) {
	url := fmt.Sprintf("%s/_cat/shards/%s?format=json&h=index,prirep,state,unassigned.reason", c.baseURL, escapePathSegment(pattern))
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
		return nil, fmt.Errorf("GET _cat/shards failed: %s — %s", resp.Status, readErrorSnippet(resp))
	}
	var rows []catShardRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *Client) RestoreFailedPrimaryIndices() ([]string, error) {
	rows, err := c.GetShardRows("*")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var out []string
	for _, r := range rows {
		if strings.EqualFold(r.Prirep, "p") && strings.EqualFold(r.State, "UNASSIGNED") && r.UnassignedReason == "NEW_INDEX_RESTORED" {
			if !seen[r.Index] {
				seen[r.Index] = true
				out = append(out, r.Index)
			}
		}
	}
	return out, nil
}

func (c *Client) RestoreSourceOfIndex(index string) (repo, snapshot string, ok bool, err error) {
	body, _ := json.Marshal(map[string]any{"index": index, "shard": 0, "primary": true})
	url := fmt.Sprintf("%s/_cluster/allocation/explain", c.baseURL)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return "", "", false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.executeRequest(req)
	if err != nil {
		return "", "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", "", false, fmt.Errorf("POST _cluster/allocation/explain failed: %s — %s", resp.Status, readErrorSnippet(resp))
	}
	var data struct {
		UnassignedInfo struct {
			Details string `json:"details"`
		} `json:"unassigned_info"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", "", false, err
	}
	const marker = "restore_source["
	i := strings.Index(data.UnassignedInfo.Details, marker)
	if i < 0 {
		return "", "", false, nil
	}
	rest := data.UnassignedInfo.Details[i+len(marker):]
	j := strings.Index(rest, "]")
	if j < 0 {
		return "", "", false, nil
	}
	src := rest[:j]
	k := strings.Index(src, "/")
	if k < 0 {
		return "", "", false, nil
	}
	return src[:k], src[k+1:], true, nil
}
