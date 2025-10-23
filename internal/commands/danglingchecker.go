package commands

import (
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
	danglingCheckerCmd.Flags().String("kibana-host", "", "Kibana host URL")
	danglingCheckerCmd.Flags().String("madison-host", "", "Madison host URL")
	danglingCheckerCmd.Flags().String("madison-token", "", "Madison authentication token")
	danglingCheckerCmd.Flags().String("alert-title", "Dangling indices found", "Alert title")
	danglingCheckerCmd.Flags().String("alert-message", "", "Alert message template")

	addCommonFlags(danglingCheckerCmd)
}

func runDanglingChecker(cmd *cobra.Command, args []string) error {
	return nil
}
