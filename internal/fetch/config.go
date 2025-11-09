package fetch

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// FetchConfig holds common configuration for all fetchers
type FetchConfig struct {
	ChunkSize  int64
	Timeout    time.Duration
	MaxRetries uint
	UserAgent  string
	AWSConfig  *aws.Config
}

// DefaultFetchConfig returns default configuration
func DefaultFetchConfig() *FetchConfig {
	return &FetchConfig{
		ChunkSize:  1024 * 1024 * 10, // 10MB
		Timeout:    30 * time.Second,
		MaxRetries: 3,
	}
}
