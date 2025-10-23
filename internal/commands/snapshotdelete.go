package commands

import (
	"github.com/spf13/cobra"
)

var snapshotDeleteCmd = &cobra.Command{
	Use:   "snapshotdelete",
	Short: "Delete snapshots",
	Long:  `Delete snapshots`,
	RunE:  runSnapshotDelete,
}

func init() {
	snapshotDeleteCmd.Flags().String("repo", "", "Snapshot repository name")
	snapshotDeleteCmd.Flags().String("pattern", "", "Snapshot name pattern to match")
	snapshotDeleteCmd.Flags().Int("days", 180, "Number of days to keep snapshots")

	addCommonFlags(snapshotDeleteCmd)
}

func runSnapshotDelete(cmd *cobra.Command, args []string) error {
	return nil
}
