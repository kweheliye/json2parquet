package downloader

import (
	"fmt"
	"io"
	"net/url"
	"strings"
)

type FileDownloader interface {
	DownloadReader(fileURL string) (io.ReadCloser, error)
}

func DownloaderFactory(sourcePath string, config *DownloadConfig) (FileDownloader, error) {
	sourceType := detectSourceType(sourcePath)

	switch sourceType {

	case "s3":
		return NewS3Downloader(config), nil // Updated for no error return
	case "http", "https":
		return NewHTTPDownloader(config), nil
	default:
		return nil, fmt.Errorf("unsupported source type for path: %s", sourcePath)
	}
}

// detectSourceType determines the source type from the path
func detectSourceType(sourcePath string) string {
	if u, err := url.Parse(sourcePath); err == nil && u.Scheme != "" {
		switch strings.ToLower(u.Scheme) {
		case "http", "https":
			return u.Scheme
		case "s3":
			return "s3"
		}
	}

	if strings.HasPrefix(strings.ToLower(sourcePath), "s3://") {
		return "s3"
	}

	return "file"
}
