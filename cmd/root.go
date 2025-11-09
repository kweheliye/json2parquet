package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "json2parquet",
	Short: "A CLI tool to convert JSON to Parquet",
	Long:  `json2parquet is a versatile tool for converting JSON files to Parquet format, with support for nested structures and custom configurations.`,
}

func init() {
	rootCmd.AddCommand(genericCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
