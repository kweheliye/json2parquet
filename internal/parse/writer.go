package parse

import (
	"context"

	"github.com/kweheliye/json2parquet/internal/parquet"
	"github.com/kweheliye/json2parquet/models"
	"github.com/kweheliye/json2parquet/utils"
)

// Writer is intended to run as a goroutine, writing data to parquet files. The wc channel
// receives slices of Mrf structs. Send true to the done channel to signal that no more
// data will be sent to wc and that the writer should close the current file and exit.
//
// Writer will create a new file when the number of rows written to the current file
// exceeds the WriterFactory's MaxRowsPerFile.
func Writer(filePrefix, outputURI string, wc <-chan []*models.Mrf, done <-chan bool) {
	var (
		data   []*models.Mrf
		i      int
		rowCnt int
		err    error
		wf     = parquet.NewPqWriterFactory(filePrefix, outputURI)
		writer *parquet.PqWriteCloser
		ctx    = context.Background()
	)

W:
	for {
		select {
		case data = <-wc:
			if i%wf.MaxRowsPerFile == 0 {
				if writer != nil {
					err = writer.Close()
					utils.ExitOnError(err)

					log.Debugf("Closed writer for %s", writer.URI())
				}

				writer, err = wf.CreateWriter(ctx)
				utils.ExitOnError(err)
			}

			rowCnt, err = writer.Write(data)
			utils.ExitOnError(err)

			if i%50_000 == 0 {
				log.Debug("Wrote ", i, " rows.")
				// We see slightly less memory usage and faster run times when periodically
				// flushing the writer.
				err = writer.Flush()
				utils.ExitOnError(err)
			}
			i += rowCnt

		case <-done:
			err = writer.Close()
			utils.ExitOnError(err)
			break W
		}
	}
}
