package utils

import (
	"os"
	"runtime"
)

func ExitOnError(err error) {
	if err != nil {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			log.Errorf("Fatal error in %s#%d: %s", file, no, err.Error())
		} else {
			log.Errorf("Fatal error: %s", err.Error())
		}

		os.Exit(1)
	}
}
