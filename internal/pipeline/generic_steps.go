package pipeline

import (
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kweheliye/json2parquet/internal/fetch"
	fetchhttp "github.com/kweheliye/json2parquet/internal/fetch/http"
	"github.com/kweheliye/json2parquet/internal/parse"
)

// GenericDownloadStep downloads or copies the source into tmp/src and exposes the local path
// Name: Downloader
// Behavior:
// - If Source.Type is http/https/url or the path has http/https scheme → HTTP download
// - If Source.Type is file or empty → copy local file to tmp/src
// - Creates tmp/src directory if missing
// OutputLocalPath is the resulting local file path to be used by the parser step

type GenericDownloadStep struct {
	Source          parseSource
	TmpDir          string
	OutputLocalPath string
}

type parseSource struct {
	Type string
	Path string
}

func (s *GenericDownloadStep) Name() string { return "Downloader" }

func (s *GenericDownloadStep) Run() {
	// Ensure destination dir exists
	dstDir := filepath.Join(s.TmpDir, "src")
	err := os.MkdirAll(dstDir, 0o755)
	if err != nil {
		log.Fatalf("failed to create tmp src dir: %v", err)
	}

	base := filepath.Base(s.Source.Path)
	// strip query if any
	if i := strings.Index(base, "?"); i >= 0 {
		base = base[:i]
	}

	s.OutputLocalPath = filepath.Join(dstDir, base)
	log.Infof("[Downloader] Resolving source: %s → %s", s.Source.Path, s.OutputLocalPath)

	// Decide download vs copy
	if isHTTPSource(s.Source) {
		cfg := fetch.DefaultFetchConfig()
		// be a little more generous by default here
		cfg.Timeout = 2 * time.Minute
		cfg.MaxRetries = 5
		dl := fetchhttp.NewHTTPFetcher(cfg)
		rc, err := dl.FetchReader(s.Source.Path)
		if err != nil {
			log.Fatalf("failed to download source: %v", err)
		}
		defer rc.Close()
		f, err := os.Create(s.OutputLocalPath)
		if err != nil {
			log.Fatalf("failed to create tmp file: %v", err)
		}
		if _, err := io.Copy(f, rc); err != nil {
			f.Close()
			log.Fatalf("failed to save downloaded content: %v", err)
		}
		if err := f.Close(); err != nil {
			log.Fatalf("failed to close tmp file: %v", err)
		}
		log.Infof("[Downloader] Downloaded %s (%s)", s.Source.Path, s.OutputLocalPath)
		return
	}

	// Treat as local file
	src := s.Source.Path
	in, err := os.Open(src)
	if err != nil {
		log.Fatalf("failed to open source file: %v", err)
	}
	defer in.Close()
	out, err := os.Create(s.OutputLocalPath)
	if err != nil {
		log.Fatalf("failed to create destination file: %v", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		log.Fatalf("failed to copy source file: %v", err)
	}
	if err := out.Close(); err != nil {
		log.Fatalf("failed to close destination file: %v", err)
	}
	log.Infof("[Downloader] Copied local file %s → %s", src, s.OutputLocalPath)
}

// isHTTPSource determines if the source should be fetched via HTTP(S)
func isHTTPSource(src parseSource) bool {
	// Check explicit type first
	t := strings.ToLower(strings.TrimSpace(src.Type))
	switch t {
	case "http", "https", "url":
		return true
	}
	// Fallback to detecting from URL scheme in the path
	if u, err := url.Parse(src.Path); err == nil && u != nil {
		scheme := strings.ToLower(u.Scheme)
		if scheme == "http" || scheme == "https" {
			return true
		}
	}
	return false
}

// GenericParseStep runs the generic parser using a provided local input path
// Name: Parse

type GenericParseStep struct {
	Parser         *parse.GenericParser
	InputLocalPath string
}

func (s *GenericParseStep) Name() string {
	return "Parse"
}

func (s *GenericParseStep) Run() {
	if s.Parser == nil {
		log.Fatalf("parser is nil in GenericParseStep")
	}
	if s.InputLocalPath == "" {
		log.Fatalf("input path is empty in GenericParseStep")
	}
	log.Infof("[Parse] Parsing JSON from %s", s.InputLocalPath)
	if err := s.Parser.ParseFile(s.InputLocalPath); err != nil {
		log.Fatalf("failed to parse file: %v", err)
	}
	log.Infof("[Parse] Completed parsing")
}

// CleanStep removes the temporary working directory
// Name: Clean

type CleanStep struct {
	TmpPath string
}

func (s *CleanStep) Name() string {
	return "Clean"
}

func (s *CleanStep) Run() {
	if s.TmpPath == "" {
		log.Warnf("[Clean] TmpPath is empty; nothing to clean")
		return
	}
	if err := os.RemoveAll(s.TmpPath); err != nil {
		log.Errorf("[Clean] failed to remove tmp dir %s: %v", s.TmpPath, err)
		return
	}
	log.Infof("[Clean] Removed tmp dir: %s", s.TmpPath)
}
