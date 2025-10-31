package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/opensearch"
	"strings"

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
	addFlags(danglingCheckerCmd)
}

func runDanglingChecker(cmd *cobra.Command, args []string) error {
	cfg := config.GetCommandConfig(cmd)
	dryRun := cfg.GetDryRun()

	madisonKey := cfg.MadisonKey
	osdURL := cfg.OSDURL

	if madisonKey == "" || osdURL == "" || cfg.MadisonURL == "" {
		return fmt.Errorf("madison-key, osd-url and madison-url parameters are required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(cfg.OpenSearchURL, cfg.CertFile, cfg.KeyFile, cfg.CAFile, cfg.GetTimeout(), cfg.GetRetryAttempts())
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

	var names []string
	for _, di := range danglingIndices {
		names = append(names, di.IndexName)
	}
	logger.Info(fmt.Sprintf("Dangling indices %s", strings.Join(names, ", ")))

	var indexNames []string
	for _, idx := range danglingIndices {
		indexNames = append(indexNames, idx.IndexName)
	}

	if dryRun {
		logger.Info("DRY RUN: Would send Madison alert for dangling indices")
	} else {
		madisonClient := alerts.NewMadisonClient(madisonKey, osdURL, cfg.MadisonURL)
		response, err := madisonClient.SendMadisonDanglingIndicesAlert(indexNames)
		if err != nil {
			return fmt.Errorf("failed to send Madison alert: %v", err)
		}
		logger.Info(fmt.Sprintf("Madison alert sent successfully: type=DanglingIndices response=%s", response))
	}

	if dryRun {
		logger.Info(fmt.Sprintf("DRY RUN: Would send Madison alert for dangling indices count=%d", len(danglingIndices)))
	} else {
		logger.Info(fmt.Sprintf("Sent Madison alert for dangling indices count=%d", len(danglingIndices)))
	}
	return nil
}
