package alerts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	apiKey     string
	kibanaHost string
	madisonURL string
	httpClient *http.Client
}

type Alert struct {
	Labels      Labels      `json:"labels"`
	Annotations Annotations `json:"annotations"`
}

type Labels struct {
	Trigger       string `json:"trigger"`
	SeverityLevel string `json:"severity_level"`
	IndicesList   string `json:"IndicesList"`
	Kibana        string `json:"kibana"`
}

type Annotations struct {
	Summary     string `json:"summary"`
	Description string `json:"description"`
}

func NewMadisonClient(apiKey, kibanaHost, madisonURL string) *Client {
	return &Client{
		apiKey:     apiKey,
		kibanaHost: kibanaHost,
		madisonURL: madisonURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) SendMadisonSnapshotMissingAlert(missingSnapshots []string) (string, error) {
	if len(missingSnapshots) == 0 {
		return "", nil
	}

	var displayList, indicesList string
	if len(missingSnapshots) <= 3 {
		displayList = strings.Join(missingSnapshots, ",")
		indicesList = displayList
	} else {
		displayList = strings.Join(missingSnapshots[:3], ",") + ",... полный список индексов в описании."
		indicesList = strings.Join(missingSnapshots[:3], ",") + ",..."
	}

	summary := fmt.Sprintf("Снапшоты не найдены для индексов: %s", displayList)
	fullList := strings.Join(missingSnapshots, ",")
	description := fmt.Sprintf("Снапшоты для индексов (%s) — не обнаружены, хотя ожидаются. Алерт одноразовый, просьба не закрывать без создания нужных снапшотов.", fullList)

	payload := Alert{
		Labels: Labels{
			Trigger:       "SnapshotsMissing",
			SeverityLevel: "5",
			IndicesList:   indicesList,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:     summary,
			Description: description,
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal alert: %v", err)
	}

	if c.madisonURL == "" {
		return "", fmt.Errorf("madison URL is required")
	}

	requestURL := fmt.Sprintf("%s/%s", c.madisonURL, c.apiKey)

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return "", fmt.Errorf("madison API returned 403 Forbidden - check key and permissions")
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("madison API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

func (c *Client) SendMadisonDanglingIndicesAlert(danglingIndices []string) (string, error) {
	if len(danglingIndices) == 0 {
		return "", nil
	}

	summary := "Кластер содержит dangling индексы"
	description := fmt.Sprintf("Кластер содержит dangling индексы.\nПроверьте индексы в %s\nGET _dangling?pretty", c.kibanaHost)

	payload := map[string]any{
		"severity_level": 4,
		"type":           "Events::Continuous",
		"source_type":    "custom",
		"labels": map[string]string{
			"trigger":      "dangling_indices_mon",
			"os-dashboard": c.kibanaHost,
		},
		"annotations": map[string]string{
			"summary":                      summary,
			"description":                  description,
			"polk_flant_com_markup_format": "markdown",
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal alert: %v", err)
	}

	if c.madisonURL == "" {
		return "", fmt.Errorf("madison URL is required")
	}

	requestURL := fmt.Sprintf("%s/%s", c.madisonURL, c.apiKey)

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return "", fmt.Errorf("madison API returned 403 Forbidden - check key and permissions")
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("madison API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

func (c *Client) SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName string) (string, error) {
	summary := fmt.Sprintf("Не удалось создать снапшот %s для индекса %s", snapshotName, indexName)
	description := fmt.Sprintf("Снапшот %s для индекса %s не удалось создать после 5 попыток. Проверьте состояние кластера и доступность индекса.", snapshotName, indexName)

	payload := Alert{
		Labels: Labels{
			Trigger:       "SnapshotCreationFailed",
			SeverityLevel: "4",
			IndicesList:   snapshotName,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:     summary,
			Description: description,
		},
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal alert: %v", err)
	}

	if c.madisonURL == "" {
		return "", fmt.Errorf("madison URL is required")
	}

	requestURL := fmt.Sprintf("%s/%s", c.madisonURL, c.apiKey)

	req, err := http.NewRequest("POST", requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 403 {
		return "", fmt.Errorf("madison API returned 403 Forbidden - check key and permissions")
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("madison API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}
