package opensearch

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Client struct {
	baseURL            string
	certFile           string
	keyFile            string
	caFile             string
	insecureSkipVerify bool
	basicAuthUser      string
	basicAuthPass      string
	timeout            time.Duration
	retryAttempts      int
	httpClient         *http.Client
}

func escapePathSegment(s string) string {
	return url.PathEscape(strings.TrimLeft(s, "/"))
}

func escapePathList(items []string) string {
	escaped := make([]string, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		escaped = append(escaped, escapePathSegment(it))
	}
	return strings.Join(escaped, ",")
}

type ClientOptions struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
	BasicAuthUser      string
	BasicAuthPass      string
	Timeout            time.Duration
	RetryAttempts      int
}

func NewClient(baseURL, certFile, keyFile, caFile string, timeout time.Duration, retryAttempts int) (*Client, error) {
	return NewClientWithOptions(baseURL, ClientOptions{
		CertFile:      certFile,
		KeyFile:       keyFile,
		CAFile:        caFile,
		Timeout:       timeout,
		RetryAttempts: retryAttempts,
	})
}

func NewClientWithOptions(baseURL string, opts ClientOptions) (*Client, error) {
	hasCert := opts.CertFile != "" && opts.KeyFile != ""
	hasCA := opts.CAFile != ""
	needsTLS := strings.HasPrefix(baseURL, "https://") || hasCert || hasCA || opts.InsecureSkipVerify

	var transport *http.Transport
	if !needsTLS {
		transport = &http.Transport{}
	} else {
		tlsConfig := &tls.Config{}
		if hasCert {
			cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load certificate: %v", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		if hasCA {
			caData, err := os.ReadFile(opts.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file: %v", err)
			}
			pool := x509.NewCertPool()
			if ok := pool.AppendCertsFromPEM(caData); !ok {
				return nil, fmt.Errorf("failed to parse CA file: %s", opts.CAFile)
			}
			tlsConfig.RootCAs = pool
		}
		if opts.InsecureSkipVerify || (!hasCA && !hasCert) {
			tlsConfig.InsecureSkipVerify = true
		}
		transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	httpClient := &http.Client{Transport: transport, Timeout: opts.Timeout}

	return &Client{
		baseURL:            baseURL,
		certFile:           opts.CertFile,
		keyFile:            opts.KeyFile,
		caFile:             opts.CAFile,
		insecureSkipVerify: opts.InsecureSkipVerify,
		basicAuthUser:      opts.BasicAuthUser,
		basicAuthPass:      opts.BasicAuthPass,
		timeout:            opts.Timeout,
		retryAttempts:      opts.RetryAttempts,
		httpClient:         httpClient,
	}, nil
}

func (c *Client) executeRequest(req *http.Request) (*http.Response, error) {
	var lastErr error

	if c.basicAuthUser != "" || c.basicAuthPass != "" {
		req.SetBasicAuth(c.basicAuthUser, c.basicAuthPass)
	}

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
