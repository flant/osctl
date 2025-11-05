package kibana

import (
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL       string
	httpClient    *http.Client
	basicUser     string
	basicPassword string
}

func NewClient(baseURL, user, password string, timeout time.Duration) *Client {
	return &Client{
		baseURL:       strings.TrimRight(baseURL, "/"),
		httpClient:    &http.Client{Timeout: timeout},
		basicUser:     user,
		basicPassword: password,
	}
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.basicUser != "" || c.basicPassword != "" {
		req.SetBasicAuth(c.basicUser, c.basicPassword)
	}
	req.Header.Set("osd-xsrf", "osd-fetch")
	return c.httpClient.Do(req)
}
