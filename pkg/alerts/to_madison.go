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
	Summary                                 string `json:"summary"`
	Description                             string `json:"description"`
	PlkCreateGroupIfNotExistsElkFieldsGroup string `json:"plk_create_group_if_not_exists__elk_fields_group,omitempty"`
	PlkGroupedByElkFieldsGroup              string `json:"plk_grouped_by__elk_fields_group,omitempty"`
	PlkMarkupFormat                         string `json:"plk_markup_format,omitempty"`
	PlkProtocolVersion                      string `json:"plk_protocol_version,omitempty"`
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

func (c *Client) SendMadisonSnapshotMissingAlert(missingSnapshotIndicesList []string, snapRepo, namespace, dateStr string) (string, error) {
	if len(missingSnapshotIndicesList) == 0 {
		return "", nil
	}

	var displayList, indicesList string
	if len(missingSnapshotIndicesList) <= 3 {
		displayList = strings.Join(missingSnapshotIndicesList, ",")
		indicesList = displayList
	} else {
		displayList = strings.Join(missingSnapshotIndicesList[:3], ",") + ",... полный список индексов в описании."
		indicesList = strings.Join(missingSnapshotIndicesList[:3], ",") + ",..."
	}

	summary := fmt.Sprintf("Снапшоты не найдены для индексов: %s", displayList)
	fullList := strings.Join(missingSnapshotIndicesList, ",")
	description := fmt.Sprintf("Снапшоты для индексов (%s) — не обнаружены, хотя ожидаются. Необходимо выборочно проверить действиельно ли нет снапшотов для этих индексов через GET _cat/snapshots/%s/<snapshot_name> , поскольку их могла уже создать джоба создания пропущенных снапшотов. Дальше можно попробовать запустить Job создания пропущенных снапшотов через kubectl -n %s create job --from=cronjob/osctl-snapshotsbackfill osctl-snapshotsbackfill-%s или создать их всех вручную. Алерт одноразовый, просьба не закрывать без создания нужных снапшотов.", fullList, snapRepo, namespace, dateStr)

	payload := Alert{
		Labels: Labels{
			Trigger:       "SnapshotsMissing",
			SeverityLevel: "5",
			IndicesList:   indicesList,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:                                 summary,
			Description:                             description,
			PlkCreateGroupIfNotExistsElkFieldsGroup: "ElkSnapshotMissingGroup,kibana=~kibana",
			PlkGroupedByElkFieldsGroup:              "ElkSnapshotMissingGroup,kibana=~kibana",
			PlkMarkupFormat:                         "markdown",
			PlkProtocolVersion:                      "1",
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

	var displayList, indicesList string
	if len(danglingIndices) <= 3 {
		displayList = strings.Join(danglingIndices, ",")
		indicesList = displayList
	} else {
		indicesList = strings.Join(danglingIndices[:3], ",") + ",..."
	}

	summary := "Кластер содержит dangling индексы"
	description := fmt.Sprintf("Кластер содержит dangling индексы. Проверьте индексы в %s GET _dangling?pretty и удалите их если они не нужны.", c.kibanaHost)

	payload := Alert{
		Labels: Labels{
			Trigger:       "dangling_indices_mon",
			SeverityLevel: "4",
			IndicesList:   indicesList,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:                                 summary,
			Description:                             description,
			PlkCreateGroupIfNotExistsElkFieldsGroup: "ElkDanglingIndicesGroup,kibana=~kibana",
			PlkGroupedByElkFieldsGroup:              "ElkDanglingIndicesGroup,kibana=~kibana",
			PlkMarkupFormat:                         "markdown",
			PlkProtocolVersion:                      "1",
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

func (c *Client) SendMadisonSnapshotCreationFailedAlert(snapshotName, indexName, snapRepo, namespace, dateStr string) (string, error) {
	summary := fmt.Sprintf("Не удалось создать снапшот %s для индекса %s", snapshotName, indexName)
	description := fmt.Sprintf("Снапшот %s для индекса %s не удалось создать после 7 попыток. Надо проверить наличие соответствующего снапшота через GET _cat/snapshots/%s/%s - возможно его уже создала джоба snapshotsbackfill, но если его нет - сначала попробуйте запустить Job создания пропущенных снапшотов через kubectl -n %s create job --from=cronjob/osctl-snapshotsbackfill osctl-snapshotsbackfill-%s или ещё вариант - создать его вручную", snapshotName, indexName, snapRepo, snapshotName, namespace, dateStr)

	payload := Alert{
		Labels: Labels{
			Trigger:       "SnapshotCreationFailed",
			SeverityLevel: "4",
			IndicesList:   indexName,
			Kibana:        c.kibanaHost,
		},
		Annotations: Annotations{
			Summary:                                 summary,
			Description:                             description,
			PlkCreateGroupIfNotExistsElkFieldsGroup: "ElkSnapshotCreationFailedGroup,kibana=~kibana",
			PlkGroupedByElkFieldsGroup:              "ElkSnapshotCreationFailedGroup,kibana=~kibana",
			PlkMarkupFormat:                         "markdown",
			PlkProtocolVersion:                      "1",
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
