package downloader

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// DownloadConfig holds common configuration for all downloaders
type DownloadConfig struct {
	ChunkSize  int64
	Timeout    time.Duration
	MaxRetries uint
	UserAgent  string
	AWSConfig  *aws.Config
}

// DefaultDownloadConfig returns default configuration
func DefaultDownloadConfig() *DownloadConfig {
	return &DownloadConfig{
		ChunkSize:  1024 * 1024 * 10, // 10MB
		Timeout:    30 * time.Second,
		MaxRetries: 3,
	}
}
