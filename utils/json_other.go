//go:build !amd64 || !(linux || darwin)

package utils

import (
	"github.com/kiwicom/fakesimdjson"
	"github.com/minio/simdjson-go"
)

func simdParse(b []byte, _ *simdjson.ParsedJson) (*simdjson.ParsedJson, error) {
	return fakesimdjson.Parse(b)
}

func simdParseND(b []byte, _ *simdjson.ParsedJson) (*simdjson.ParsedJson, error) {
	return fakesimdjson.ParseND(b)
}
