package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"regexp"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
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

func CheckNodesDown(client *opensearch.Client, logger *logging.Logger, checkEnabled bool, kubeNamespace string, showDetails bool) (int, error) {
	allocation, err := client.GetAllocation()
	if err != nil {
		return 0, err
	}

	if len(allocation) == 0 {
		return 0, fmt.Errorf("no allocation data")
	}

	nodeCount := len(allocation)
	var nodePrefixes []string
	prefixSet := make(map[string]bool)
	var invalidNodeNames []string

	re := regexp.MustCompile(`-\d+$`)

	for _, node := range allocation {
		nodeName := node.Name
		if !re.MatchString(nodeName) {
			invalidNodeNames = append(invalidNodeNames, nodeName)
		}

		prefix := re.ReplaceAllString(nodeName, "")
		if !prefixSet[prefix] {
			prefixSet[prefix] = true
			nodePrefixes = append(nodePrefixes, prefix)
		}
	}

	if checkEnabled && len(invalidNodeNames) > 0 {
		return 0, fmt.Errorf("found nodes without numeric suffix in name (expected format: name-number): %s", strings.Join(invalidNodeNames, ", "))
	}

	if showDetails {
		logger.Info(fmt.Sprintf("Nodes in cluster: %d", nodeCount))
		logger.Info(fmt.Sprintf("Node prefixes found: %s", strings.Join(nodePrefixes, ", ")))
	}

	rc, err := rest.InClusterConfig()
	if err != nil {
		return 0, fmt.Errorf("failed to get Kubernetes in-cluster config: %v", err)
	}

	k8sClient, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return 0, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	ctx := context.Background()
	stsList, err := k8sClient.AppsV1().StatefulSets(kubeNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to list StatefulSets: %v", err)
	}

	totalReplicas := 0
	var matchingSts []string

	for _, sts := range stsList.Items {
		for _, prefix := range nodePrefixes {
			if strings.HasPrefix(sts.Name, prefix) || strings.Contains(sts.Name, prefix) {
				replicas := int(*sts.Spec.Replicas)
				totalReplicas += replicas
				matchingSts = append(matchingSts, fmt.Sprintf("%s (replicas=%d)", sts.Name, replicas))
				break
			}
		}
	}

	if showDetails {
		if len(matchingSts) > 0 {
			logger.Info(fmt.Sprintf("StatefulSets found: %s", strings.Join(matchingSts, ", ")))
			logger.Info(fmt.Sprintf("Total replicas in StatefulSets: %d", totalReplicas))
		} else {
			logger.Warn("No matching StatefulSets found for node prefixes")
		}
		logger.Info(fmt.Sprintf("Nodes in cluster: %d, Expected replicas: %d, Difference: %d", nodeCount, totalReplicas, totalReplicas-nodeCount))
	}

	return totalReplicas - nodeCount, nil
}
