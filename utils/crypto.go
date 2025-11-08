package utils

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/rs/xid"
)

// GetUniqueID generates an xid, a fast, sortable globally unique id that is only 20 characters long.
func GetUniqueID() string {
	guid := xid.New()

	return guid.String()
}

// Generate sha256sum for a string. Not intended to be cryptographically secure.
func Sha256Sum(s string) string {
	h := sha256.New()
	h.Write([]byte(s))

	return hex.EncodeToString(h.Sum(nil))
}
