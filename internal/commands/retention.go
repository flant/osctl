package commands

import (
	"github.com/spf13/cobra"
)

var retentionCmd = &cobra.Command{
	Use:   "retention",
	Short: "Manage disk space by deleting old indices",
	Long: `Manage disk space by deleting old indices when disk utilization exceeds threshold.
Only deletes indices that have valid snapshots in both old and new repositories.`,
	RunE: runRetention,
}

func init() {
	retentionCmd.Flags().Int("threshold", 80, "Disk usage threshold percentage")
	retentionCmd.Flags().String("old-repo", "", "Old snapshot repository name")
	retentionCmd.Flags().String("new-repo", "", "New snapshot repository name")
	retentionCmd.Flags().String("endpoint", "opendistro", "OpenSearch endpoint")

	addCommonFlags(retentionCmd)
}

func runRetention(cmd *cobra.Command, args []string) error {
	return nil
}
