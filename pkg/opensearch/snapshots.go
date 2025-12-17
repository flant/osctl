package opensearch

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Snapshot struct {
	Snapshot          string   `json:"snapshot"`
	State             string   `json:"state"`
	Indices           []string `json:"indices"`
	StartTimeInMillis int64    `json:"start_time_in_millis"`
	DurationInMillis  int64    `json:"duration_in_millis"`
}

type SnapshotResponse struct {
	Snapshots []Snapshot `json:"snapshots"`
}

type SnapshotStatus struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
}

type SnapshotInfo struct {
	Snapshot   string `json:"snapshot"`
	Repository string `json:"repository"`
	State      string `json:"state"`
}

type SnapshotDetailStatus struct {
	Snapshots []SnapshotDetail `json:"snapshots"`
}

type SnapshotDetail struct {
	Snapshot           string                 `json:"snapshot"`
	Repository         string                 `json:"repository"`
	UUID               string                 `json:"uuid"`
	State              string                 `json:"state"`
	IncludeGlobalState bool                   `json:"include_global_state"`
	ShardsStats        ShardsStats            `json:"shards_stats"`
	Stats              SnapshotStats          `json:"stats"`
	Indices            map[string]IndexDetail `json:"indices"`
}

type ShardsStats struct {
	Initializing int `json:"initializing"`
	Started      int `json:"started"`
	Finalizing   int `json:"finalizing"`
	Done         int `json:"done"`
	Failed       int `json:"failed"`
	Total        int `json:"total"`
}

type SnapshotStats struct {
	Incremental struct {
		FileCount   int   `json:"file_count"`
		SizeInBytes int64 `json:"size_in_bytes"`
	} `json:"incremental"`
	Processed struct {
		FileCount   int   `json:"file_count"`
		SizeInBytes int64 `json:"size_in_bytes"`
	} `json:"processed"`
	Total struct {
		FileCount   int   `json:"file_count"`
		SizeInBytes int64 `json:"size_in_bytes"`
	} `json:"total"`
	StartTimeInMillis int64 `json:"start_time_in_millis"`
	TimeInMillis      int64 `json:"time_in_millis"`
}

type IndexDetail struct {
	ShardsStats ShardsStats            `json:"shards_stats"`
	Stats       SnapshotStats          `json:"stats"`
	Shards      map[string]ShardDetail `json:"shards"`
}

type ShardDetail struct {
	Stage string `json:"stage"`
}

func (c *Client) GetSnapshots(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?verbose=false", c.baseURL, escapePathSegment(repo), escapePathSegment(pattern))

	var response SnapshotResponse
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}

	return response.Snapshots, nil
}

func (c *Client) CreateSnapshot(repo, snapshot string, body map[string]any) error {
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, escapePathSegment(repo), escapePathSegment(snapshot))

	return c.putJSON(url, body)
}

func (c *Client) DeleteSnapshots(snapRepo string, snapshotNames []string) error {
	if len(snapshotNames) == 0 {
		return nil
	}

	snapshotsList := escapePathList(snapshotNames)
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, escapePathSegment(snapRepo), snapshotsList)
	return c.delete(url)
}

func (c *Client) GetSnapshotStatus() (*SnapshotStatus, error) {
	url := fmt.Sprintf("%s/_snapshot/_status", c.baseURL)
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

	var status SnapshotStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

func (c *Client) GetSnapshotStatusDetail(repo, snapshot string) (*SnapshotDetailStatus, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s/_status", c.baseURL, escapePathSegment(repo), escapePathSegment(snapshot))
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

	var status SnapshotDetailStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}
