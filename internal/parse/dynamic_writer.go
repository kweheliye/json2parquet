package parse

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/kweheliye/json2parquet/models"
	"github.com/kweheliye/json2parquet/utils"
	"github.com/segmentio/parquet-go"
)

var log = utils.GetLogger()

// DynamicWriter handles writing records with dynamically generated schemas
type DynamicWriter struct {
	file        *os.File
	writer      *parquet.Writer
	tableConfig models.TableConfig
	structType  reflect.Type
	recordCount int64
}

// NewDynamicWriter creates a new writer with dynamic schema
func NewDynamicWriter(outputPath string, tableConfig models.TableConfig, compression string) (*DynamicWriter, error) {
	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	// Generate struct type dynamically
	structType := generateStructType(tableConfig)

	// Create schema from the struct type
	schema := parquet.SchemaOf(reflect.New(structType).Interface())

	// Create Parquet writer with compression
	writerConfig, _ := parquet.NewWriterConfig()
	writerConfig.Schema = schema

	// Set compression
	switch compression {
	case "zstd":
		writerConfig.Compression = &parquet.Zstd
	case "snappy":
		writerConfig.Compression = &parquet.Snappy
	case "gzip":
		writerConfig.Compression = &parquet.Gzip
	default:
		writerConfig.Compression = &parquet.Uncompressed
	}

	writer := parquet.NewWriter(file, writerConfig)

	return &DynamicWriter{
		file:        file,
		writer:      writer,
		tableConfig: tableConfig,
		structType:  structType,
	}, nil
}

// Write writes a generic record
func (dw *DynamicWriter) Write(record models.GenericRecord) error {
	// Create new struct instance
	instance := reflect.New(dw.structType).Elem()

	// Populate fields
	allFields := getAllFields(dw.tableConfig)

	for i, field := range allFields {
		if i >= instance.NumField() {
			break
		}

		fieldValue := instance.Field(i)

		if value, ok := record[field.Name]; ok && value != nil {
			dw.setFieldValue(fieldValue, value, field.Type)
		}
	}

	// Write to Parquet
	row := parquet.Row{}
	for i := 0; i < instance.NumField(); i++ {
		field := instance.Field(i)
		row = append(row, parquet.ValueOf(field.Interface()))
	}

	if _, err := dw.writer.WriteRows([]parquet.Row{row}); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	dw.recordCount++
	return nil
}

// setFieldValue sets a reflect.Value based on the target type
func (dw *DynamicWriter) setFieldValue(field reflect.Value, value interface{}, targetType string) {
	if !field.CanSet() {
		return
	}

	switch targetType {
	case "string":
		field.SetString(fmt.Sprintf("%v", value))
	case "int64":
		switch v := value.(type) {
		case float64:
			field.SetInt(int64(v))
		case int:
			field.SetInt(int64(v))
		case int64:
			field.SetInt(v)
		case int32:
			field.SetInt(int64(v))
		default:
			field.SetInt(0)
		}
	case "float64":
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case int:
			field.SetFloat(float64(v))
		case int64:
			field.SetFloat(float64(v))
		case float32:
			field.SetFloat(float64(v))
		default:
			field.SetFloat(0)
		}
	case "bool":
		if b, ok := value.(bool); ok {
			field.SetBool(b)
		} else {
			field.SetBool(false)
		}
	}
}

// Close closes the writer
func (dw *DynamicWriter) Close() error {
	if err := dw.writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	if err := dw.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	log.Infof("Wrote %d records to table: %s", dw.recordCount, dw.tableConfig.Name)
	return nil
}

// generateStructType dynamically generates a struct type from table config
func generateStructType(tableConfig models.TableConfig) reflect.Type {
	allFields := getAllFields(tableConfig)

	var fields []reflect.StructField

	for _, fieldConfig := range allFields {
		field := reflect.StructField{
			Name: toExportedName(fieldConfig.Name),
			Type: getReflectType(fieldConfig.Type),
			Tag:  reflect.StructTag(generateParquetTag(fieldConfig)),
		}
		fields = append(fields, field)
	}

	return reflect.StructOf(fields)
}

// getAllFields combines parent ref fields and table fields
func getAllFields(tableConfig models.TableConfig) []models.FieldConfig {
	var allFields []models.FieldConfig

	// Add parent reference fields first
	for _, parentRef := range tableConfig.ParentRefs {
		allFields = append(allFields, parentRef.Fields...)
	}

	// Add table's own fields
	allFields = append(allFields, tableConfig.Fields...)

	return allFields
}

// toExportedName converts a field name to an exported Go name
func toExportedName(name string) string {
	parts := strings.Split(name, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
		}
	}
	result := strings.Join(parts, "")

	// Ensure first character is uppercase
	if len(result) > 0 && result[0] >= 'a' && result[0] <= 'z' {
		result = strings.ToUpper(result[:1]) + result[1:]
	}

	return result
}

// getReflectType returns the reflect.Type for a type string
func getReflectType(typeStr string) reflect.Type {
	switch typeStr {
	case "string":
		return reflect.TypeOf("")
	case "int64":
		return reflect.TypeOf(int64(0))
	case "float64":
		return reflect.TypeOf(float64(0))
	case "bool":
		return reflect.TypeOf(false)
	default:
		return reflect.TypeOf("")
	}
}

// generateParquetTag generates the parquet struct tag
func generateParquetTag(field models.FieldConfig) string {
	return fmt.Sprintf(`parquet:"%s"`, field.Name)
}
