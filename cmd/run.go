package cmd

import (
	"github.com/kweheliye/json2parquet/internal/pipeline"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/cobra"
)

var (
	inputPath  string
	outputPath string
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the JSON → Parquet pipeline",
	Run:   runPipeline,
}

func init() {
	runCmd.Flags().StringVar(&inputPath, "input", "", "Input file path or URL")
	runCmd.Flags().StringVar(&outputPath, "output", "", "Output Parquet file path")
	runCmd.Flags().Bool("overwrite", false, "overwrite contents of output path if it exists")

}

func runPipeline(cmd *cobra.Command, args []string) {

	if inputPath == "" || outputPath == "" {
		log.Fatal("Input and output paths cannot be empty")
	}

	log.Infof("Running pipeline with input: %s, output: %s", inputPath, outputPath)

	inputPath, err := cmd.Flags().GetString("input")
	utils.ExitOnError(err)

	outputPath, err := cmd.Flags().GetString("output")
	utils.ExitOnError(err)

	p := pipeline.NewParsePipeline(inputPath, outputPath, "", 0)
	p.Run()

}
