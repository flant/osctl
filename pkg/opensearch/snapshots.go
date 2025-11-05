package opensearch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

func (c *Client) GetSnapshots(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?verbose=false", c.baseURL, repo, pattern)

	var response SnapshotResponse
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}

	return response.Snapshots, nil
}

func (c *Client) CreateSnapshot(repo, snapshot string, body map[string]any) error {
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, repo, snapshot)

	return c.putJSON(url, body)
}

func (c *Client) DeleteSnapshots(snapRepo string, snapshotNames []string) error {
	if len(snapshotNames) == 0 {
		return nil
	}

	snapshotsList := strings.Join(snapshotNames, ",")
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, snapRepo, snapshotsList)
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
