package opensearch

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	baseURL       string
	certFile      string
	keyFile       string
	caFile        string
	timeout       time.Duration
	retryAttempts int
	httpClient    *http.Client
}

type Snapshot struct {
	Snapshot string   `json:"snapshot"`
	State    string   `json:"state"`
	Indices  []string `json:"indices"`
}

type SnapshotResponse struct {
	Snapshots []Snapshot `json:"snapshots"`
}

type IndexInfo struct {
	Index string `json:"index"`
	Rep   string `json:"rep"`
	Size  string `json:"ss"`
}

type NodeInfo struct {
	DiskPercent string `json:"disk.percent"`
}

type AllocationInfo struct {
	DiskPercent string `json:"disk.percent"`
}

func NewClient(baseURL, certFile, keyFile, caFile string, timeout time.Duration, retryAttempts int) (*Client, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: timeout,
	}

	return &Client{
		baseURL:       baseURL,
		certFile:      certFile,
		keyFile:       keyFile,
		caFile:        caFile,
		timeout:       timeout,
		retryAttempts: retryAttempts,
		httpClient:    httpClient,
	}, nil
}

func (c *Client) executeRequest(req *http.Request) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.retryAttempts; attempt++ {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.retryAttempts {
				time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
				continue
			}
			return nil, err
		}

		if resp.StatusCode >= 500 {

			resp.Body.Close()
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			if attempt < c.retryAttempts {
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
		}

		return resp, nil
	}

	return nil, lastErr
}

func (c *Client) GetIndicesWithFields(pattern, fields string) ([]IndexInfo, error) {
	url := fmt.Sprintf("%s/_cat/indices/%s?format=json&h=%s", c.baseURL, pattern, fields)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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

func (c *Client) getJSON(url string, result interface{}) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(body, result)
}

func (c *Client) putJSON(url string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *Client) SetReplicas(index string, replicas int) error {
	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, index)

	settings := map[string]interface{}{
		"index": map[string]interface{}{
			"number_of_replicas": replicas,
		},
	}

	return c.putJSON(url, settings)
}

func (c *Client) SetColdStorage(index, coldAttribute string) error {
	url := fmt.Sprintf("%s/%s/_settings", c.baseURL, index)

	settings := map[string]interface{}{
		"index": map[string]interface{}{
			"routing.allocation.require.temp": coldAttribute,
			"number_of_replicas":              0,
		},
	}

	return c.putJSON(url, settings)
}

func (c *Client) GetSnapshots(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?verbose=false", c.baseURL, repo, pattern)

	var response SnapshotResponse
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}

	return response.Snapshots, nil
}

func (c *Client) GetNodes() ([]NodeInfo, error) {
	url := fmt.Sprintf("%s/_cat/nodes?format=json&h=disk.percent", c.baseURL)

	var nodes []NodeInfo
	if err := c.getJSON(url, &nodes); err != nil {
		return nil, err
	}

	return nodes, nil
}

func (c *Client) GetAllocation() ([]AllocationInfo, error) {
	url := fmt.Sprintf("%s/_cat/allocation?format=json&h=disk.percent", c.baseURL)

	var allocation []AllocationInfo
	if err := c.getJSON(url, &allocation); err != nil {
		return nil, err
	}

	return allocation, nil
}

func (c *Client) DeleteIndex(index string) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, index)
	return c.delete(url)
}

type DanglingIndex struct {
	IndexName string `json:"index_name"`
	IndexUUID string `json:"index_uuid"`
}

type DanglingResponse struct {
	DanglingIndices []DanglingIndex `json:"dangling_indices"`
}

func (c *Client) GetDanglingIndices() ([]DanglingIndex, error) {
	url := fmt.Sprintf("%s/_dangling?pretty", c.baseURL)

	var result DanglingResponse
	if err := c.getJSON(url, &result); err != nil {
		return nil, err
	}

	return result.DanglingIndices, nil
}

func (c *Client) delete(url string) error {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (c *Client) DeleteSnapshot(repo, snapshot string) error {
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, repo, snapshot)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

type SnapshotStatus struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
}

type SnapshotInfo struct {
	State string `json:"state"`
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

	var status SnapshotStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

type TasksResponse struct {
	Nodes map[string]TaskNodeInfo `json:"nodes"`
}

type TaskNodeInfo struct {
	Tasks map[string]TaskInfo `json:"tasks"`
}

type TaskInfo struct {
	Action string `json:"action"`
}

func (c *Client) GetTasks() (*TasksResponse, error) {
	url := fmt.Sprintf("%s/_tasks", c.baseURL)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var tasks TasksResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}

	return &tasks, nil
}

func (c *Client) CreateSnapshot(repo, snapshot string, body map[string]interface{}) error {
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, repo, snapshot)

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
