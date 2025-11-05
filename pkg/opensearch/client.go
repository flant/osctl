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

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			snippet := readErrorSnippet(resp)
			resp.Body.Close()
			return nil, fmt.Errorf("client error: %d — %s", resp.StatusCode, snippet)
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

func readErrorSnippet(resp *http.Response) string {
	const limit = 4096
	b, _ := io.ReadAll(io.LimitReader(resp.Body, limit))
	s := strings.TrimSpace(string(b))
	if s == "" {
		return ""
	}
	return s
}
