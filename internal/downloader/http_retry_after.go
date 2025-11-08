package downloader

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/avast/retry-go/v4"
)

var (
	// ErrNegativeSecondsNotAllowed is parsing error that represents seconds value is negative.
	ErrNegativeSecondsNotAllowed = errors.New("negative seconds not allowed")

	// ErrInvalidFormat is parsing error that represents given Retry-After neither valid seconds nor valid HTTP date.
	ErrInvalidFormat = errors.New("Retry-After value must be seconds integer or HTTP date string")
)

func RetryAfterDelay(n uint, err error, config *retry.Config) time.Duration {
	var (
		t time.Time

		e = new(RetryAfterError)
	)

	if errors.As(err, e) {
		if t, err = ParseRetryAfter(e.response.Header.Get("Retry-After")); err == nil {
			log.Warnf("Got Retry-After header: %s", t)
			return time.Until(t)
		}
	}

	delay := retry.BackOffDelay(n, err, config)

	if n > 10/2 {
		log.Warnf("Retrying in %s after error %s", delay, err)
	}

	return delay
}

type RetryAfterError struct {
	response http.Response
}

func (err RetryAfterError) Error() string {
	return fmt.Sprintf(
		"Request to %s fail %s (%d)",
		err.response.Request.RequestURI,
		err.response.Status,
		err.response.StatusCode,
	)
}

// ParseRetryAfter tries to parse the value as seconds or HTTP date.
func ParseRetryAfter(retryAfter string) (time.Time, error) {
	if dur, err := ParseSeconds(retryAfter); err == nil {
		now := time.Now()
		return now.Add(dur), nil
	}

	if dt, err := ParseHTTPDate(retryAfter); err == nil {
		return dt, nil
	}

	return time.Time{}, ErrInvalidFormat
}

// ParseSeconds parses the value as seconds.
func ParseSeconds(retryAfter string) (time.Duration, error) {
	seconds, err := strconv.ParseInt(retryAfter, 10, 64)

	if err != nil {
		return time.Duration(0), err
	}

	if seconds < 0 {
		return time.Duration(0), ErrNegativeSecondsNotAllowed
	}

	return time.Second * time.Duration(seconds), nil
}

// ParseHTTPDate parses the value as HTTP date.
func ParseHTTPDate(retryAfter string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC1123, retryAfter)
	if err != nil {
		return time.Time{}, err
	}

	return parsed, nil
}
