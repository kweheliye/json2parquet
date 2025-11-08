package pipeline

import (
	"os"
	"path/filepath"
	"strings"

	utils2 "github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/viper"
)

var log = utils2.GetLogger()

type Pipeline struct {
	Steps []Step
}

func NewParsePipeline(inputPath, outputPath, serviceFile string, planID int64) *Pipeline {
	var (
		err          error
		tmpPath      string
		srcFilePath  string
		tmpPathSrc   string
		tmpPathSplit string
		steps        []Step
		cfgTmpPath   = viper.GetString("tmp.path")
	)

	if cfgTmpPath != "" {
		tmpPath, err = os.MkdirTemp(cfgTmpPath, "mrfparse")
	} else {
		tmpPath, err = os.MkdirTemp("", "mrfparse")
	}

	utils2.ExitOnError(err)

	tmpPathSrc = filepath.Join(tmpPath, "src")
	tmpPathSplit = filepath.Join(tmpPath, "split")

	srcFilePath = filepath.Join(tmpPathSrc, filepath.Base(inputPath))
	srcFilePath = strings.Split(srcFilePath, "?")[0]

	steps = []Step{
		&DownloadStep{
			URL:        inputPath,
			OutputPath: srcFilePath,
		},
		&SplitStep{
			InputPath:  srcFilePath,
			OutputPath: tmpPathSplit,
			Overwrite:  true,
		},
		&ParseStep{
			InputPath:   tmpPathSplit,
			OutputPath:  outputPath,
			ServiceFile: serviceFile,
			PlanID:      planID,
		},
		&CleanStep{
			TmpPath: tmpPath,
		},
	}

	return New(steps...)
}

func New(steps ...Step) *Pipeline {
	return &Pipeline{Steps: steps}
}

func (p *Pipeline) Run() {
	var fn func()

	for _, step := range p.Steps {
		log.Infof("Running step: %s", step.Name())

		fn = func() {
			step.Run()
		}
		elapsed := utils2.Timed(fn)
		log.Infof("Step %s completed in %d seconds", step.Name(), elapsed)
	}
}
