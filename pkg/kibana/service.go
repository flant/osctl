package kibana

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"osctl/pkg/utils"
	"strings"
)

type SavedObject struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Attributes map[string]any `json:"attributes"`
}

type FindResponse struct {
	Total        int           `json:"total"`
	SavedObjects []SavedObject `json:"saved_objects"`
}

type Fields map[string]any

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
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kibana find saved objects failed: %s — %s", resp.Status, strings.TrimSpace(string(snippet)))
	}
	var fr FindResponse
	if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
		return nil, err
	}
	return &fr, nil
}

func (c *Client) CreateDataSource(tenant, title, endpoint, user, password string) error {
	u := fmt.Sprintf("%s/api/saved_objects/data-source", c.baseURL)
	body := map[string]any{
		"attributes": map[string]any{
			"title":       title,
			"description": "",
			"endpoint":    endpoint,
			"auth": map[string]any{
				"type": "username_password",
				"credentials": map[string]string{
					"username": user,
					"password": password,
				},
			},
		},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequest("POST", u, bytes.NewReader(b))
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
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("kibana create data-source failed: %s — %s", resp.Status, strings.TrimSpace(string(snippet)))
	}
	return nil
}

func (c *Client) GetActualMappingForIndexPattern(title string) ([]Fields, error) {

	u := fmt.Sprintf("%s/api/index_patterns/_fields_for_wildcard?pattern=%s&meta_fields=_source&meta_fields=_id&meta_fields=_type&meta_fields=_index&meta_fields=_score", c.baseURL, title)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("kibana get index mapping failed with: %s — %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var fields []Fields
	err = json.Unmarshal(body, &fields)
	return fields, nil
}

func (c *Client) RefreshIndexPattern(id string, title string) error {
	var fields []Fields
	fields, err := c.GetActualMappingForIndexPattern(title)

	if err != nil {
		return err
	}

	u := fmt.Sprintf("%s/api/saved_objects/index-pattern/%s", c.baseURL, id)
	body := map[string]any{
		"attributes": map[string]any{
			"title":         title,
			"version":       utils.PatternVersion(),
			"timeFieldName": "@timestamp",
			"fields":        fields,
		},
	}

	b, _ := json.Marshal(body)
	req, err := http.NewRequest("PUT", u, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
