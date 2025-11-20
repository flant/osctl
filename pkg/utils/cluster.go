package utils

import (
	"encoding/json"
	"fmt"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"strconv"
	"strings"
)

type NodeUtilizationInfo struct {
	Name            string  `json:"name"`
	Role            string  `json:"role"`
	DiskUsedPercent float64 `json:"diskUsedPercent"`
}

func GetAverageUtilization(client *opensearch.Client, logger *logging.Logger, showDetails bool) (int, error) {
	allocation, err := client.GetAllocation()
	if err != nil {
		return 0, err
	}
	if len(allocation) == 0 {
		return 0, fmt.Errorf("no allocation data")
	}

	var dataNodes []opensearch.AllocationInfo
	for _, node := range allocation {
		if strings.Contains(node.NodeRole, "d") {
			dataNodes = append(dataNodes, node)
		}
	}

	if len(dataNodes) == 0 {
		return 0, fmt.Errorf("no data nodes found")
	}

	if showDetails {
		var nodesInfo []NodeUtilizationInfo
		for _, node := range dataNodes {
			percent, err := strconv.ParseFloat(node.DiskUsedPercent, 64)
			if err == nil {
				nodesInfo = append(nodesInfo, NodeUtilizationInfo{
					Name:            node.Name,
					Role:            node.NodeRole,
					DiskUsedPercent: percent,
				})
			}
		}
		nodesJSON, _ := json.Marshal(nodesInfo)
		logger.Info(fmt.Sprintf("Data nodes used for utilization calculation: %s", string(nodesJSON)))
	}

	sum := 0.0
	count := 0
	for _, node := range dataNodes {
		if percent, err := strconv.ParseFloat(node.DiskUsedPercent, 64); err == nil {
			sum += percent
			count++
		}
	}

	if count == 0 {
		return 0, fmt.Errorf("no valid disk utilization data")
	}

	avgUtil := int(sum / float64(count))
	if showDetails {
		logger.Info(fmt.Sprintf("Average disk utilization calculated from %d data nodes: %d%%", count, avgUtil))
	}

	return avgUtil, nil
}
