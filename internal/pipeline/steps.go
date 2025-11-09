package pipeline

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kweheliye/json2parquet/internal/downloader"
	"github.com/kweheliye/json2parquet/internal/split"
	"github.com/kweheliye/json2parquet/utils"
)

// Step interface
type Step interface {
	Name() string
	Run()
}

type DownloadStep struct {
	URL        string
	OutputPath string
}

func (s *DownloadStep) Run() {
	log.Infof("[DownloadStep] DownloadStep file: %s → %s", s.URL, s.OutputPath)

	o := filepath.Dir(s.OutputPath)

	config := &downloader.DownloadConfig{
		ChunkSize:  1024 * 1024 * 50, // 50MB chunks
		Timeout:    2 * time.Minute,  // 2 minute timeout
		MaxRetries: 5,                // 5 retry attempts
	}

	err := os.MkdirAll(o, 0o755)
	utils.ExitOnError(err)

	d, err := downloader.DownloaderFactory(s.URL, config)
	utils.ExitOnError(err)

	rd, err := d.DownloadReader(s.URL)
	utils.ExitOnError(err)

	defer rd.Close()

	wr, err := os.Create(s.OutputPath)
	utils.ExitOnError(err)

	defer wr.Close()

	n, err := io.Copy(wr, rd)
	utils.ExitOnError(err)

	log.Infof("Downloaded %d bytes from %s to %s", n, s.URL, s.OutputPath)

}

func (s *DownloadStep) Name() string {
	return "Download"
}

type SplitStep struct {
	InputPath  string
	OutputPath string
	Overwrite  bool
}

func (s *SplitStep) Name() string {
	return "Split"
}

func (s *SplitStep) Run() {
	log.Infof("[SplitStep] Splitting file: %s → %s", s.InputPath, s.OutputPath)
	split.File(s.InputPath, s.OutputPath, s.Overwrite)
}

type ParseStep struct {
	InputPath   string
	OutputPath  string
	ServiceFile string
	PlanID      int64
}

func (p *ParseStep) Run() {
	log.Infof("[ParseStep] Parsing chunks from %s → %s\n", p.InputPath, p.OutputPath)
}

func (s *ParseStep) Name() string {
	return "Parse"
}

// CleanStep removes the tmp directory used to store the split files
type CleanStep struct {
	TmpPath string
}

func (s *CleanStep) Run() {
	err := os.RemoveAll(s.TmpPath)
	utils.ExitOnError(err)
}

func (s *CleanStep) Name() string {
	return "Clean"
}
