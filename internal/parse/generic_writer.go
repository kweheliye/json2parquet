package parse

import (
	"fmt"

	"github.com/kweheliye/json2parquet/models"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// GenericWriter handles writing generic records to Parquet
type GenericWriter struct {
	file        *local.LocalFile
	writer      *writer.ParquetWriter
	tableConfig models.TableConfig
	schema      string
	recordCount int64
}

// NewGenericWriter creates a new generic Parquet writer
func NewGenericWriter(outputPath string, tableConfig models.TableConfig, compression string) (*GenericWriter, error) {
	// Create output file
	fw, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	// Generate schema from table config
	schema := generateSchema(tableConfig)

	// Create Parquet writer
	pw, err := writer.NewParquetWriter(fw, nil, 4)
	if err != nil {
		fw.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression
	switch compression {
	case "zstd":
		pw.CompressionType = parquet.CompressionCodec_ZSTD
	case "snappy":
		pw.CompressionType = parquet.CompressionCodec_SNAPPY
	case "gzip":
		pw.CompressionType = parquet.CompressionCodec_GZIP
	default:
		pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	}

	return &GenericWriter{
		file:        fw.(*local.LocalFile),
		writer:      pw,
		tableConfig: tableConfig,
		schema:      schema,
	}, nil
}

// Write writes a generic record to Parquet
func (gw *GenericWriter) Write(record models.GenericRecord) error {
	// Convert record to slice for writing
	if err := gw.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	gw.recordCount++
	return nil
}

// Close closes the writer and file
func (gw *GenericWriter) Close() error {
	if err := gw.writer.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop writer: %w", err)
	}

	if err := gw.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	log.Infof("Wrote %d records to table: %s", gw.recordCount, gw.tableConfig.Name)
	return nil
}

// generateSchema generates a Parquet schema from table configuration
func generateSchema(tableConfig models.TableConfig) string {
	// This is a simplified schema generation
	// In production, you'd want to generate proper Parquet schema struct tags
	return fmt.Sprintf("Table: %s with %d fields", tableConfig.Name, len(tableConfig.Fields))
}
