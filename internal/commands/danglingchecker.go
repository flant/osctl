package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/madison"
	"curator-go/internal/opensearch"
	"fmt"

	"github.com/spf13/cobra"
)

var danglingCheckerCmd = &cobra.Command{
	Use:   "danglingchecker",
	Short: "Check for dangling indices and send alerts",
	Long: `Check for dangling indices that are not referenced by any index pattern
and send alerts to Madison if found.`,
	RunE: runDanglingChecker,
}

func init() {
	addCommonFlags(danglingCheckerCmd)
}

func runDanglingChecker(cmd *cobra.Command, args []string) error {
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	madisonKey, _ := cmd.Flags().GetString("madison-key")
	madisonProject, _ := cmd.Flags().GetString("madison-project")
	osdURL, _ := cmd.Flags().GetString("osd-url")
	madisonURL, _ := cmd.Flags().GetString("madison-url")

	if madisonKey == "" || madisonProject == "" || osdURL == "" || madisonURL == "" {
		return fmt.Errorf("madison-key, madison-project, osd-url and madison-url parameters are required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}

	danglingIndices, err := client.GetDanglingIndices()
	if err != nil {
		return fmt.Errorf("failed to get dangling indices: %v", err)
	}

	if len(danglingIndices) == 0 {
		logger.Info("No dangling indices found")
		return nil
	}

	var indexNames []string
	for _, idx := range danglingIndices {
		indexNames = append(indexNames, idx.IndexName)
	}

	madisonClient := madison.NewClient(madisonKey, madisonProject, osdURL, madisonURL)
	if err := madisonClient.SendDanglingIndicesAlert(indexNames); err != nil {
		return fmt.Errorf("failed to send Madison alert: %v", err)
	}

	logger.Info("Sent Madison alert for dangling indices", "count", len(danglingIndices))
	return nil
}
