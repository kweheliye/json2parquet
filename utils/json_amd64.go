//go:build amd64 && (linux || darwin)

package utils

import "github.com/minio/simdjson-go"

func simdParse(b []byte, r *simdjson.ParsedJson) (*simdjson.ParsedJson, error) {
	return simdjson.Parse(b, r)
}

func simdParseND(b []byte, r *simdjson.ParsedJson) (*simdjson.ParsedJson, error) {
	return simdjson.ParseND(b, r)
}
