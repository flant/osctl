package commands

import (
	"fmt"
	"osctl/pkg/config"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "osctl",
	Short: "OpenSearch indices lifecycle management tool",
	Long: `osctl is a comprehensive tool for managing OpenSearch cluster indices lifecycle.

Available Commands:
  snapshots         Create snapshots of indices
  snapshot-manual   Create manual snapshots for specific indices using CLI flags
  snapshotsdelete   Delete old snapshots based on retention policy
  indicesdelete     Delete old indices based on retention policy
  retention         Manage disk space by deleting old indices when threshold exceeded
  dereplicator      Reduce replicas for old indices to save disk space
  snapshotschecker  Check for missing snapshots and send alerts
  danglingchecker   Check for dangling indices and send alerts
  sharding          Create index templates with optimal shard counts
  indexpatterns     Manage Kibana index patterns
  datasource        Create Kibana data sources
  coldstorage       Migrate indices to cold storage nodes
  extracteddelete   Delete extracted indices that are no longer needed`,
	RunE: func(cmd *cobra.Command, args []string) error {

		actionFlag, _ := cmd.Flags().GetString("action")
		if actionFlag != "" {
			if err := config.ValidateAction(actionFlag); err != nil {
				fmt.Printf("Error: %v\n", err)
				return err
			}

			if err := config.LoadConfig(cmd, actionFlag); err != nil {
				fmt.Printf("Error: Could not load config: %v\n", err)
				return err
			}
			return executeActionCommand(actionFlag, args)
		}

		if err := config.LoadConfig(cmd, "root"); err != nil {
			fmt.Printf("Error: Could not load config: %v\n", err)
			return err
		}
		cfg := config.GetConfig()
		if cfg.Action != "" {
			if err := config.ValidateAction(cfg.Action); err != nil {
				fmt.Printf("Error: %v\n", err)
				return err
			}

			if err := config.LoadConfig(cmd, cfg.Action); err != nil {
				fmt.Printf("Error: Could not load config: %v\n", err)
				return err
			}
			return executeActionCommand(cfg.Action, args)
		}

		return cmd.Help()
	},
}

func Execute() error {
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		commandName := cmd.Name()
		if commandName == "osctl" {
			commandName = "root"
		}

		if err := config.LoadConfig(cmd, commandName); err != nil {
			fmt.Printf("Error: Could not load config: %v\n", err)
			return err
		}

		return nil
	}

	return rootCmd.Execute()
}

func executeActionCommand(action string, args []string) error {

	var targetCmd *cobra.Command

	switch action {
	case "snapshots":
		targetCmd = snapshotCmd
	case "snapshot-manual":
		targetCmd = snapshotManualCmd
	case "snapshotsdelete":
		targetCmd = snapshotDeleteCmd
	case "snapshotschecker":
		targetCmd = snapshotCheckerCmd
	case "indicesdelete":
		targetCmd = indicesDeleteCmd
	case "retention":
		targetCmd = retentionCmd
	case "dereplicator":
		targetCmd = dereplicatorCmd
	case "coldstorage":
		targetCmd = coldStorageCmd
	case "extracteddelete":
		targetCmd = extractedDeleteCmd
	case "danglingchecker":
		targetCmd = danglingCheckerCmd
	default:
		return fmt.Errorf("unknown action: %s", action)
	}

	targetCmd.SetArgs(args)
	return targetCmd.RunE(targetCmd, args)
}

func init() {
	addFlags(rootCmd)
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(snapshotManualCmd)
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

func addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String("action", "", "Action to execute (snapshot, retention, dereplicator, etc.)")
	cmd.PersistentFlags().String("config", "", "Path to configuration file")
	cmd.PersistentFlags().String("os-url", "", "OpenSearch URL")
	cmd.PersistentFlags().String("cert-file", "", "Certificate file path")
	cmd.PersistentFlags().String("key-file", "", "Key file path")
	cmd.PersistentFlags().String("ca-file", "", "CA file path")
	cmd.PersistentFlags().Duration("timeout", 0, "Request timeout")
	cmd.PersistentFlags().Int("retry-attempts", 0, "Number of retry attempts")
	cmd.PersistentFlags().String("date-format", "", "Date format for index names")
	cmd.PersistentFlags().String("madison-url", "", "Madison API URL")
	cmd.PersistentFlags().String("osd-url", "", "OpenSearch Dashboards URL")
	cmd.PersistentFlags().String("madison-key", "", "Madison API key")
	cmd.PersistentFlags().String("madison-project", "", "Madison project name")
	cmd.PersistentFlags().Bool("dry-run", false, "Show what would be done without executing")

	commandName := cmd.Name()
	config.AddCommandFlags(cmd, commandName)
}
