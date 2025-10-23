package commands

import (
	"github.com/spf13/cobra"
)

var dereplicatorCmd = &cobra.Command{
	Use:   "dereplicator",
	Short: "Reduce replicas for old indices",
	Long: `Reduce replicas to 0 for indices older than specified days.
Optionally checks for snapshots before reducing replicas.`,
	RunE: runDereplicator,
}

func init() {
	dereplicatorCmd.Flags().Int("days-count", 14, "Number of days to keep with replicas")
	dereplicatorCmd.Flags().String("date-format", "%Y.%m.%d", "Date format for index names")
	dereplicatorCmd.Flags().Bool("use-snapshot", false, "Check for snapshots before reducing replicas")
	dereplicatorCmd.Flags().String("snap-repo", "", "Snapshot repository name")

	addCommonFlags(dereplicatorCmd)
}

func runDereplicator(cmd *cobra.Command, args []string) error {
	return nil
}
