package pipeline

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/kweheliye/json2parquet/internal/parse"
	"github.com/kweheliye/json2parquet/models"
	"github.com/kweheliye/json2parquet/utils"
	"gopkg.in/yaml.v3"
)

var log = utils.GetLogger()

// NewGenericParsePipeline constructs a Downloader → Parse → Clean pipeline for the generic parser
func NewGenericParsePipeline(configPath string) (*Pipeline, error) {
	// Load config to know source and output
	cfgData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var cfg models.ParseConfig
	if err := yaml.Unmarshal(cfgData, &cfg); err != nil {
		return nil, err
	}

	// Create tmp dir
	tmpDir, err := os.MkdirTemp("", "json2parquet")
	if err != nil {
		return nil, err
	}

	// Instantiate parser from configPath (it reads full config including output settings)
	gp, err := parse.NewGenericParser(configPath)
	if err != nil {
		return nil, err
	}

	// Compute destination file name inside tmp/src
	base := filepath.Base(cfg.Source.Path)
	if i := strings.Index(base, "?"); i >= 0 {
		base = base[:i]
	}
	localPath := filepath.Join(tmpDir, "src", base)

	dl := &GenericDownloadStep{
		Source: parseSource{Type: cfg.Source.Type, Path: cfg.Source.Path},
		TmpDir: tmpDir,
	}

	ps := &GenericParseStep{
		Parser:         gp,
		InputLocalPath: localPath,
	}

	clean := &CleanStep{TmpPath: tmpDir}

	steps := []Step{dl, ps, clean}
	return New(steps...), nil
}
