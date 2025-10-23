package commands

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "osctl",
	Short: "OpenSearch indices lifecycle management tool",
	Long: `osctl is a comprehensive tool for managing OpenSearch cluster indices lifecycle.

Available Commands:
  snapshot          Create snapshots of indices
  snapshotdelete    Delete old snapshots based on retention policy
  indicesdelete     Delete old indices based on retention policy
  retention         Manage disk space by deleting old indices when threshold exceeded
  dereplicator      Reduce replicas for old indices to save disk space
  snapshotchecker   Check for missing snapshots and send alerts
  danglingchecker   Check for dangling indices and send alerts
  sharding          Create index templates with optimal shard counts
  indexpatterns     Manage Kibana index patterns
  datasource        Create Kibana data sources
  coldstorage       Migrate indices to cold storage nodes
  extracteddelete   Delete extracted indices that are no longer needed

Common Flags:
  --os-url string          OpenSearch URL (default: https://opendistro:9200)
  --cert-file string       Certificate file path (default: /etc/ssl/certs/admin-crt.pem)
  --key-file string        Key file path (default: /etc/ssl/certs/admin-key.pem)
  --ca-file string         CA file path (default: /etc/ssl/certs/elk-root-ca.pem)
  --timeout duration     Request timeout (default: 30s)
  --retry-attempts int     Number of retry attempts (default: 3)
  --date-format string     Date format for index names (default: %Y.%m.%d)
  --madison-url string     Madison API URL (default: https://madison.flant.com/api/events/custom/)
  --osd-url string         OpenSearch Dashboards URL
  --madison-key string     Madison API key
  --madison-project string Madison project name

Examples:
  # Check for missing snapshots
  osctl snapshotchecker --snap-repo=s3-backup --madison-key=xxx --madison-project=yyy --osd-url=https://kibana.example.com

  # Manage disk space retention
  osctl retention --snap-repo=s3-backup --threshold=75

  # Reduce replicas for old indices
  osctl dereplicator --days-count=2 --use-snapshot --snap-repo=s3-backup

  # Check for dangling indices
  osctl danglingchecker --madison-key=xxx --madison-project=yyy --osd-url=https://kibana.example.com`,
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(snapshotDeleteCmd)
	rootCmd.AddCommand(indicesDeleteCmd)
	rootCmd.AddCommand(retentionCmd)
	rootCmd.AddCommand(shardingCmd)
	rootCmd.AddCommand(indexPatternsCmd)
	rootCmd.AddCommand(dataSourceCmd)
	rootCmd.AddCommand(dereplicatorCmd)
	rootCmd.AddCommand(snapshotCheckerCmd)
	rootCmd.AddCommand(danglingCheckerCmd)
	rootCmd.AddCommand(coldStorageCmd)
	rootCmd.AddCommand(extractedDeleteCmd)
}

func addCommonFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("os-url", "https://opendistro:9200", "OpenSearch URL")
	cmd.PersistentFlags().String("cert-file", "/etc/ssl/certs/admin-crt.pem", "Certificate file path")
	cmd.PersistentFlags().String("key-file", "/etc/ssl/certs/admin-key.pem", "Key file path")
	cmd.PersistentFlags().String("ca-file", "/etc/ssl/certs/elk-root-ca.pem", "CA file path")
	cmd.PersistentFlags().Duration("timeout", 30, "Request timeout")
	cmd.PersistentFlags().Int("retry-attempts", 3, "Number of retry attempts")
	cmd.PersistentFlags().String("date-format", "%Y.%m.%d", "Date format for index names")
	cmd.PersistentFlags().String("madison-url", "https://madison.flant.com/api/events/custom/", "Madison API URL")
	cmd.PersistentFlags().String("osd-url", "", "OpenSearch Dashboards URL")
	cmd.PersistentFlags().String("madison-key", "", "Madison API key")
	cmd.PersistentFlags().String("madison-project", "", "Madison project name")
}
