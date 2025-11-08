package split

import (
	"github.com/kweheliye/json2parquet/utils"
	"github.com/kweheliye/jsplit/pkg/jsplit"
)

// File splits a JSON document into multiple files.
// It produces a root.json file for field elements in the root of the document, and
// a file for each array element in the document root. Files are limited to 4GB each.
func File(inputURI, outputURI string, overwrite bool) {
	err := jsplit.Split(inputURI, outputURI, overwrite)
	utils.ExitOnError(err)
}
