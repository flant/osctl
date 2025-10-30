package kibana

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
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

type SavedObject struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Attributes map[string]interface{} `json:"attributes"`
}

type FindResponse struct {
	Total        int           `json:"total"`
	SavedObjects []SavedObject `json:"saved_objects"`
}

func (c *Client) FindSavedObjects(tenant string, objType string, perPage int) (*FindResponse, error) {
	params := url.Values{}
	params.Set("type", objType)
	params.Set("per_page", fmt.Sprintf("%d", perPage))
	params.Add("fields", "id")
	params.Add("fields", "title")
	params.Add("fields", "description")
	u := fmt.Sprintf("%s/api/saved_objects/_find?%s", c.baseURL, params.Encode())
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	if tenant != "" && tenant != "global" {
		req.Header.Set("securitytenant", tenant)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var fr FindResponse
	if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
		return nil, err
	}
	return &fr, nil
}

func (c *Client) CreateDataSource(tenant, title, endpoint, user, password string) error {
	u := fmt.Sprintf("%s/api/saved_objects/data-source", c.baseURL)
	body := map[string]interface{}{
		"attributes": map[string]interface{}{
			"title":       title,
			"description": "",
			"endpoint":    endpoint,
			"auth": map[string]interface{}{
				"type": "username_password",
				"credentials": map[string]string{
					"username": user,
					"password": password,
				},
			},
		},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", u, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if tenant != "" && tenant != "global" {
		req.Header.Set("securitytenant", tenant)
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kibana create data-source failed: %s", resp.Status)
	}
	return nil
}
