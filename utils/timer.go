package utils

import (
	"time"
)

// Time execution of a function
type wrapped func()

func Timed(fn wrapped) time.Duration {
	start := time.Now()

	fn()

	return time.Since(start)
}
