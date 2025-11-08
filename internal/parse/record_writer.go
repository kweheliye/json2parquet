package parse

import "github.com/kweheliye/json2parquet/models"

var WriteRecords func(records []*models.Mrf) error

// NewRecordWriter returns a function that writes Mrf records to the writer channel
// This allows us to avoid passing the channel to every function that needs to write
func NewRecordWriter(wc chan []*models.Mrf) func(records []*models.Mrf) error {
	return func(records []*models.Mrf) error {
		wc <- records

		return nil
	}
}
