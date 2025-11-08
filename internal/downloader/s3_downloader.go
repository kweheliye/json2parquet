package downloader

import (
	"io"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Downloader handles S3 downloads
type S3Downloader struct {
	config   *DownloadConfig
	s3Client *s3.Client
}

func NewS3Downloader(config *DownloadConfig) *S3Downloader {
	return &S3Downloader{
		config:   config,
		s3Client: nil,
	}
}
func (d *S3Downloader) DownloadReader(fileURL string) (io.ReadCloser, error) {
	log.Infof("[S3Downloader] Downloading from S3")
	return nil, nil
}
