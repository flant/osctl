package commands

import (
	"curator-go/internal/logging"
	"curator-go/internal/opensearch"
	"fmt"

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
	osURL, _ := cmd.Flags().GetString("os-url")
	certFile, _ := cmd.Flags().GetString("cert-file")
	keyFile, _ := cmd.Flags().GetString("key-file")
	caFile, _ := cmd.Flags().GetString("ca-file")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	retryAttempts, _ := cmd.Flags().GetInt("retry-attempts")
	repo, _ := cmd.Flags().GetString("repo")

	if repo == "" {
		return fmt.Errorf("repo parameter is required")
	}

	logger := logging.NewLogger()
	client, err := opensearch.NewClient(osURL, certFile, keyFile, caFile, timeout, retryAttempts)
	if err != nil {
		return fmt.Errorf("failed to create OpenSearch client: %v", err)
	}
	_ = client

	logger.Info("Snapshot delete command not implemented yet")
	return nil
}
