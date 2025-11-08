package cmd

import (
	"os"
	"runtime/pprof"

	"github.com/kweheliye/json2parquet/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	log            *logrus.Logger
	memProfileFile string
	cpuProfileFile string
)

var rootCmd = &cobra.Command{
	Use:   "json-pipeline",
	Short: "Process JSON datasets into Parquet",
	Long:  `JSON Pipeline is a Go tool for processing massive JSON/NDJSON datasets into analytics-ready Parquet format.`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if cpuProfileFile != "" {
			f, err := os.Create(cpuProfileFile)
			if err != nil {
				log.Fatal(err)
			}
			if err := pprof.StartCPUProfile(f); err != nil {
				log.Fatal(err)
			}
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if cpuProfileFile != "" {
			pprof.StopCPUProfile()
		}
		if memProfileFile != "" {
			f, err := os.Create(memProfileFile)
			if err != nil {
				log.Fatal(err)
			}
			if err := pprof.WriteHeapProfile(f); err != nil {
				log.Fatal(err)
			}
			f.Close()
		}
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&memProfileFile, "memprofile", "", "Write memory profile to this file")
	rootCmd.PersistentFlags().StringVar(&cpuProfileFile, "cpuprofile", "", "Write CPU profile to this file")

	// Add subcommands
	rootCmd.AddCommand(runCmd)

}

func initConfig() {

	// Initialize logger
	log = utils.GetLogger()
}
