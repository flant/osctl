package commands

import (
	"github.com/spf13/cobra"
)

var extractedDeleteCmd = &cobra.Command{
	Use:   "extracteddelete",
	Short: "Delete extracted indices",
	Long:  `Delete extracted indices that are no longer needed.`,
	RunE:  runExtractedDelete,
}

func init() {
	extractedDeleteCmd.Flags().Int("days", 7, "Number of days to keep extracted indices")
	extractedDeleteCmd.Flags().String("snap-repo", "", "Snapshot repository name")
	extractedDeleteCmd.Flags().Bool("dry-run", false, "Show what would be deleted without actually deleting")

	addCommonFlags(extractedDeleteCmd)
}

func runExtractedDelete(cmd *cobra.Command, args []string) error {
	return nil
}
