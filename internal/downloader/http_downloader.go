package downloader

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/viper"
)

var log = utils.GetLogger()

// HTTPDownloader handles HTTP/HTTPS downloads
type HTTPDownloader struct {
	config *DownloadConfig
	client *http.Client
}

func NewHTTPDownloader(config *DownloadConfig) *HTTPDownloader {
	return &HTTPDownloader{
		config: config,
		client: &http.Client{Timeout: config.Timeout},
	}
}

func (d *HTTPDownloader) DownloadReader(fileURL string) (io.ReadCloser, error) {
	var (
		err         error
		r           *http.Response
		HTTPTimeOut time.Duration
	)

	HTTPTimeOut = time.Duration(viper.GetInt("pipeline.download_timeout")) * time.Minute

	var httpClient = &http.Client{
		Timeout: HTTPTimeOut,
	}

	err = retry.Do(func() error {
		r, err = httpClient.Get(fileURL)
		if err != nil {
			return err
		}
		return nil
	}, retry.DelayType(RetryAfterDelay),
		retry.Attempts(d.config.MaxRetries),
	)
	if err != nil {
		r.Body.Close()

		return nil, fmt.Errorf("unable to downloader file from %s: %s", fileURL, errors.Unwrap(err))
	}

	if r.StatusCode != http.StatusOK {
		errorText := fmt.Errorf("bad status downloading %s: %s", fileURL, r.Status)
		log.Error(errorText)

		return nil, errorText
	}

	return r.Body, nil
}
