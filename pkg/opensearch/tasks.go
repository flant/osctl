package opensearch

import (
	"encoding/json"
	"fmt"
	"net/http"
)

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
