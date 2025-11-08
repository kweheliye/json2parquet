package utils

import (
	"fmt"
	"os"
)

// FileExists checks if a file exists
func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

// ValidateFile ensures file exists and is readable
func ValidateFile(filename string) error {
	if !FileExists(filename) {
		return fmt.Errorf("file does not exist: %s", filename)
	}
	return nil
}
