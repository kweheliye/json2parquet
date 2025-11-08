package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// test Sha256Sum
func TestSha256Sum(t *testing.T) {
	s := "filename_test.gz"
	hash_expected := "cc13984a42a92b86c46c861655e91bda947325361fe6427a611be61053366877"

	hash := Sha256Sum(s)
	assert.Equal(t, hash_expected, hash)
}
