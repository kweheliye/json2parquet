package cmd

import (
	"os"
	"path/filepath"

	"github.com/kweheliye/json2parquet/internal/pipeline"
	"github.com/kweheliye/json2parquet/pkg/utils"
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
}

func runPipeline(cmd *cobra.Command, args []string) {

	if inputPath == "" || outputPath == "" {
		log.Fatal("Input and output paths cannot be empty")
	}

	log.Infof("Running pipeline with input: %s, output: %s", inputPath, outputPath)

	tmpDir := os.TempDir()

	steps := []pipeline.Step{
		&pipeline.DownloadStep{
			InputPath:  inputPath,
			OutputPath: filepath.Join(tmpDir, "data.json"),
		},
		&pipeline.SplitStep{
			InputPath:  inputPath,
			OutputPath: filepath.Join(tmpDir, "split"),
		},
		&pipeline.ParseStep{
			InputPath:  filepath.Join(tmpDir, "split"),
			OutputPath: outputPath,
		},
		&pipeline.CleanStep{
			TmpPath: tmpDir,
		},
	}

	p := pipeline.New(steps...)
	err := p.Run()
	utils.ExitOnError(err)

}
