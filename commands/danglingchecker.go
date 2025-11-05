package commands

import (
	"fmt"
	"osctl/pkg/alerts"
	"osctl/pkg/config"
	"osctl/pkg/logging"
	"osctl/pkg/utils"
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
	cfg := config.GetConfig()
	dryRun := cfg.GetDryRun()

	madisonKey := cfg.GetMadisonKey()
	osdURL := cfg.GetOSDURL()

	if madisonKey == "" || osdURL == "" || cfg.GetMadisonURL() == "" {
		return fmt.Errorf("madison-key, osd-url and madison-url parameters are required")
	}

	logger := logging.NewLogger()
	client, err := utils.NewOSClientFromCommandConfig(cfg)
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

	indexNames := make([]string, len(danglingIndices))
	for i, di := range danglingIndices {
		indexNames[i] = di.IndexName
	}
	logger.Info(fmt.Sprintf("Dangling indices found (%d): %s", len(indexNames), strings.Join(indexNames, ", ")))

	if dryRun {
		logger.Info(fmt.Sprintf("DRY RUN: Would send Madison alert for dangling indices count=%d", len(danglingIndices)))
	} else {
		madisonClient := alerts.NewMadisonClient(madisonKey, osdURL, cfg.GetMadisonURL())
		response, err := madisonClient.SendMadisonDanglingIndicesAlert(indexNames)
		if err != nil {
			return fmt.Errorf("failed to send Madison alert: %v", err)
		}
		logger.Info(fmt.Sprintf("Madison alert sent successfully: type=DanglingIndices count=%d response=%s", len(danglingIndices), response))
	}
	return nil
}
