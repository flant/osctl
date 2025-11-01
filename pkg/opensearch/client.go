package opensearch

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
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
	Snapshot          string   `json:"snapshot"`
	State             string   `json:"state"`
	Indices           []string `json:"indices"`
	StartTimeInMillis int64    `json:"start_time_in_millis"`
	DurationInMillis  int64    `json:"duration_in_millis"`
}

type SnapshotResponse struct {
	Snapshots []Snapshot `json:"snapshots"`
}

type IndexInfo struct {
	Index        string `json:"index"`
	Rep          string `json:"rep"`
	Size         string `json:"ss"`
	PriStoreSize string `json:"pri.store.size"`
	CreationDate string `json:"cd"`
}

type AliasInfo struct {
	Alias string `json:"alias"`
}

type AllocationInfo struct {
	DiskPercent string `json:"disk.percent"`
}

func NewClient(baseURL, certFile, keyFile, caFile string, timeout time.Duration, retryAttempts int) (*Client, error) {
	var transport *http.Transport
	if certFile == "" && keyFile == "" && caFile == "" {
		transport = &http.Transport{}
	} else {
		tlsConfig := &tls.Config{}
		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		if caFile != "" {
			caData, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file: %v", err)
			}
			pool := x509.NewCertPool()
			if ok := pool.AppendCertsFromPEM(caData); !ok {
				return nil, fmt.Errorf("failed to parse CA file: %s", caFile)
			}
			tlsConfig.RootCAs = pool
			tlsConfig.InsecureSkipVerify = false
		} else {
			tlsConfig.InsecureSkipVerify = true
		}
		transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	httpClient := &http.Client{Transport: transport, Timeout: timeout}

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
				time.Sleep(time.Duration(attempt+1) * time.Second)
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
			return nil, lastErr
		}

		return resp, nil
	}

	return nil, lastErr
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

type NodesResponse struct {
	Nodes map[string]struct {
		Roles      []string       `json:"roles"`
		Attributes map[string]any `json:"attributes"`
	} `json:"nodes"`
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

func (c *Client) GetAliases(pattern string) ([]AliasInfo, error) {
	url := fmt.Sprintf("%s/_cat/aliases/%s?format=json", c.baseURL, pattern)
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

func (c *Client) DeleteSnapshotsBatch(snapRepo string, snapshotNames []string) error {
	if len(snapshotNames) == 0 {
		return nil
	}

	snapshotsList := strings.Join(snapshotNames, ",")
	url := fmt.Sprintf("%s/_snapshot/%s/%s", c.baseURL, snapRepo, snapshotsList)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := c.executeRequest(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	return nil
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	return nil
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

func (c *Client) GetSnapshots(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?verbose=false", c.baseURL, repo, pattern)

	var response SnapshotResponse
	if err := c.getJSON(url, &response); err != nil {
		return nil, err
	}

	return response.Snapshots, nil
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

func (c *Client) DeleteIndicesBatch(indices []string) error {
	if len(indices) == 0 {
		return nil
	}

	indicesList := strings.Join(indices, ",")
	url := fmt.Sprintf("%s/%s", c.baseURL, indicesList)
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	return nil
}

type SnapshotStatus struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
}

type SnapshotInfo struct {
	Snapshot   string `json:"snapshot"`
	Repository string `json:"repository"`
	State      string `json:"state"`
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

type TasksResponse struct {
	Nodes map[string]TaskNodeInfo `json:"nodes"`
}

type TaskNodeInfo struct {
	Tasks map[string]TaskInfo `json:"tasks"`
}

type TaskInfo struct {
	Action      string `json:"action"`
	Description string `json:"description"`
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
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}

	var tasks TasksResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}

	return &tasks, nil
}

func (c *Client) CreateSnapshot(repo, snapshot string, body map[string]any) error {
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
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s failed: %s — %s", req.URL.Path, resp.Status, readErrorSnippet(resp))
	}
	return nil
}

func readErrorSnippet(resp *http.Response) string {
	const limit = 4096
	b, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	return s
}
