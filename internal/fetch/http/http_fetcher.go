package http

import (
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/kweheliye/json2parquet/internal/fetch"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/spf13/viper"
)

var log = utils.GetLogger()

// HTTPFetcher handles HTTP/HTTPS fetching
type HTTPFetcher struct {
	config *fetch.FetchConfig
	client *stdhttp.Client
}

func NewHTTPFetcher(config *fetch.FetchConfig) *HTTPFetcher {
	return &HTTPFetcher{
		config: config,
		client: &stdhttp.Client{Timeout: config.Timeout},
	}
}

func (d *HTTPFetcher) FetchReader(fileURL string) (io.ReadCloser, error) {
	var (
		err         error
		r           *stdhttp.Response
		HTTPTimeOut time.Duration
	)

	HTTPTimeOut = time.Duration(viper.GetInt("pipeline.download_timeout")) * time.Minute

	var httpClient = &stdhttp.Client{
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
		if r != nil && r.Body != nil {
			r.Body.Close()
		}
		return nil, fmt.Errorf("unable to fetch file from %s: %s", fileURL, errors.Unwrap(err))
	}

	if r.StatusCode != stdhttp.StatusOK {
		errorText := fmt.Errorf("bad status fetching %s: %s", fileURL, r.Status)
		log.Error(errorText)

		return nil, errorText
	}

	return r.Body, nil
}
