package cmd

import (
	"github.com/kweheliye/json2parquet/internal/parse"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/cobra"
)

var (
	genericConfigFile string
)

var genericCmd = &cobra.Command{
	Use:   "generic",
	Short: "Parse JSON to Parquet using generic configuration",
	Long: `Parse nested JSON files to multiple Parquet tables based on a YAML configuration file.
	
The configuration file defines:
- Source JSON file location
- Table definitions with field mappings
- Nested structure navigation
- Parent-child relationships

Example:
  json2parquet generic --config parse_config.yaml`,
	Run: func(cmd *cobra.Command, args []string) {
		runGenericParse()
	},
}

func init() {

	genericCmd.Flags().StringVarP(&genericConfigFile, "config", "c", "parse_config.yaml", "Path to parse configuration file")
}

func runGenericParse() {
	log := utils.GetLogger()
	log.Infof("Starting generic JSON to Parquet conversion")
	log.Infof("Configuration file: %s", genericConfigFile)

	if err := parse.ParseGeneric(genericConfigFile); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}

	log.Infof("Generic parsing completed successfully")
}
