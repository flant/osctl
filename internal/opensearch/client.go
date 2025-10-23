package opensearch

import (
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
}

func NewClient(baseURL, certFile, keyFile, caFile string, timeout time.Duration, retryAttempts int) *Client {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to load certificate: %v", err))
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
	}
}

func (c *Client) GetIndices(pattern string) ([]string, error) {
	url := fmt.Sprintf("%s/_cat/indices/%s?format=json&h=index", c.baseURL, pattern)

	resp, err := c.httpClient.Get(url)
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

	result := make([]string, len(indices))
	for i, idx := range indices {
		result[i] = idx.Index
	}

	return result, nil
}

func (c *Client) GetSnapshots(repo, pattern string) ([]Snapshot, error) {
	url := fmt.Sprintf("%s/_snapshot/%s/%s?verbose=false", c.baseURL, repo, pattern)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response SnapshotResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	return response.Snapshots, nil
}

// TODO:
// Добавить метод для получения списка репо для снапшотов и их бакетов
