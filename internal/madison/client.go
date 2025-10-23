package madison

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	apiKey     string
	project    string
	kibanaHost string
	httpClient *http.Client
}

type Alert struct {
	Labels      Labels      `json:"labels"`
	Annotations Annotations `json:"annotations"`
}

type Labels struct {
	Trigger       string `json:"trigger"`
	Project       string `json:"project"`
	SeverityLevel string `json:"severity_level"`
	SnapshotsList string `json:"SnapshotsList"`
	Kibana        string `json:"kibana"`
}

type Annotations struct {
	Summary     string `json:"summary"`
	Description string `json:"description"`
}

func NewClient(apiKey, project, kibanaHost string) *Client {
	return &Client{
		apiKey:     apiKey,
		project:    project,
		kibanaHost: kibanaHost,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) SendSnapshotMissingAlert(missingSnapshots []string) error {
	if len(missingSnapshots) == 0 {
		return nil
	}

	// Prepare display lists
	var displayList, snapshotsList string
	if len(missingSnapshots) <= 3 {
		displayList = strings.Join(missingSnapshots, ",")
		snapshotsList = displayList
	} else {
		displayList = strings.Join(missingSnapshots[:3], ",") + ",... полный список индексов в описании."
		snapshotsList = strings.Join(missingSnapshots[:3], ",") + ",..."
	}

	summary := fmt.Sprintf("Снапшоты не найдены для индексов: %s", displayList)
	fullList := strings.Join(missingSnapshots, ",")
	description := fmt.Sprintf("Снапшоты для индексов (%s) — не обнаружены, хотя ожидаются. Алерт одноразовый, просьба не закрывать без создания нужных снапшотов.", fullList)

	payload := Alert{
		Labels: Labels{
			Trigger:       "SnapshotsMissing",
			Project:       c.project,
			SeverityLevel: "5",
			SnapshotsList: snapshotsList,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:     summary,
			Description: description,
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal alert: %v", err)
	}

	madisonURL := fmt.Sprintf("https://madison.flant.com/api/events/custom/%s", c.apiKey)

	req, err := http.NewRequest("POST", madisonURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return fmt.Errorf("madison API returned 403 Forbidden - check key and permissions")
	}

	if resp.StatusCode >= 400 {
		body, _ := json.Marshal(resp.Body)
		return fmt.Errorf("madison API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
