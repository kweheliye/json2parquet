package parse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/kweheliye/json2parquet/models"
	"gopkg.in/yaml.v3"
)

// GenericParser handles parsing of nested JSON based on configuration
type GenericParser struct {
	config     *models.ParseConfig
	writers    map[string]*DynamicWriter
	writersMux sync.RWMutex
}

// NewGenericParser creates a new generic parser from config file
func NewGenericParser(configPath string) (*GenericParser, error) {
	config, err := loadParseConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	return &GenericParser{
		config:  config,
		writers: make(map[string]*DynamicWriter),
	}, nil
}

// loadParseConfig loads the parse configuration from YAML file
func loadParseConfig(configPath string) (*models.ParseConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config models.ParseConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &config, nil
}

// ParseFile processes the provided local JSON file according to configuration
func (gp *GenericParser) ParseFile(localPath string) error {
	// Initialize writers for each table
	if err := gp.initializeWriters(); err != nil {
		return fmt.Errorf("failed to initialize writers: %w", err)
	}
	defer gp.closeWriters()

	// Read the JSON file
	log.Infof("Reading JSON from: %s", localPath)
	data, err := os.ReadFile(localPath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}
	log.Infof("Read %d bytes from JSON file", len(data))

	// Parse JSON into generic structure
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}
	log.Infof("Successfully parsed JSON")

	// Convert to array if needed
	var records []interface{}
	switch v := jsonData.(type) {
	case []interface{}:
		records = v
	case map[string]interface{}:
		if gp.config.Source.RootArray != "" {
			if arr, ok := v[gp.config.Source.RootArray].([]interface{}); ok {
				records = arr
			} else {
				return fmt.Errorf("root_array '%s' is not an array", gp.config.Source.RootArray)
			}
		} else {
			records = []interface{}{v}
		}
	default:
		return fmt.Errorf("unsupported JSON structure")
	}

	log.Infof("Processing %d root records", len(records))

	// Process each record
	for i, record := range records {
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			log.Warnf("Skipping non-object record at index %d", i)
			continue
		}

		if err := gp.processRecord(recordMap, nil, ""); err != nil {
			log.Errorf("Failed to process record %d: %v", i, err)
		}
	}

	log.Infof("Successfully parsed %d root records", len(records))
	return nil
}

// processRecord recursively processes a record and its nested structures
func (gp *GenericParser) processRecord(record map[string]interface{}, parentContext map[string]interface{}, currentPath string) error {
	// Process each table configuration
	for _, tableConfig := range gp.config.Tables {
		if shouldProcessTable(tableConfig.JSONPath, currentPath) {
			if err := gp.processTable(tableConfig, record, parentContext); err != nil {
				return err
			}
		}
	}

	return nil
}

// shouldProcessTable determines if a table should be processed at the current path
func shouldProcessTable(tablePath, currentPath string) bool {
	if tablePath == "" && currentPath == "" {
		return true // Root level table
	}

	// For nested paths, we'll handle them in processTable
	return true
}

// processTable processes a single table configuration
func (gp *GenericParser) processTable(tableConfig models.TableConfig, record map[string]interface{}, parentContext map[string]interface{}) error {
	log.Debugf("Processing table: %s with json_path: %s", tableConfig.Name, tableConfig.JSONPath)

	// Build parent context including current record
	fullContext := cloneMap(parentContext)
	fullContext["__current__"] = record

	// Handle root level table (empty json_path)
	if tableConfig.JSONPath == "" {
		flatRecord := gp.createFlatRecord(tableConfig, record, fullContext)
		writer := gp.getWriter(tableConfig.Name)
		if err := writer.Write(flatRecord); err != nil {
			return fmt.Errorf("failed to write record to %s: %w", tableConfig.Name, err)
		}
		return nil
	}

	pathParts := strings.Split(tableConfig.JSONPath, ".")

	// Recursive traversal that supports arrays at any level
	var traverse func(data interface{}, idx int, ctx map[string]interface{}) error
	traverse = func(data interface{}, idx int, ctx map[string]interface{}) error {
		if idx >= len(pathParts) {
			// We reached the target path; data should be an array of items to write
			switch arr := data.(type) {
			case []interface{}:
				for _, item := range arr {
					itemMap, ok := item.(map[string]interface{})
					if !ok {
						log.Warnf("Skipping non-object item at path %s", tableConfig.JSONPath)
						continue
					}
					flatRecord := gp.createFlatRecord(tableConfig, itemMap, ctx)
					writer := gp.getWriter(tableConfig.Name)
					if err := writer.Write(flatRecord); err != nil {
						return fmt.Errorf("failed to write record to %s: %w", tableConfig.Name, err)
					}

					// Prepare nested context with current entity bound under table name and its singular form
					nextCtx := contextWithEntity(ctx, tableConfig.Name, itemMap)

					if err := gp.processRecord(itemMap, nextCtx, tableConfig.JSONPath); err != nil {
						return err
					}
				}
				return nil
			default:
				return fmt.Errorf("expected array at target path %s, got %T", tableConfig.JSONPath, data)
			}
		}

		// If data is an array at this level, iterate and continue with same idx
		switch cur := data.(type) {
		case []interface{}:
			for _, item := range cur {
				// Carry forward context with the current array item as the entity under the PREVIOUS path part
				// Because when we encounter an array, idx has already advanced past the part that yielded this array.
				itemCtx := ctx
				if m, ok := item.(map[string]interface{}); ok {
					part := ""
					if idx > 0 {
						part = pathParts[idx-1]
					}
					if part != "" {
						itemCtx = contextWithEntity(ctx, part, m)
					}
				}
				if err := traverse(item, idx, itemCtx); err != nil {
					return err
				}
			}
			return nil
		case map[string]interface{}:
			part := pathParts[idx]
			if part == "" {
				return traverse(cur, idx+1, ctx)
			}
			next, ok := cur[part]
			if !ok || next == nil {
				// Path doesn't exist in this branch; skip
				return nil
			}
			return traverse(next, idx+1, ctx)
		default:
			// Unexpected type; nothing to do
			return nil
		}
	}

	return traverse(record, 0, fullContext)
}

// createFlatRecord creates a flattened record from the configuration
func (gp *GenericParser) createFlatRecord(tableConfig models.TableConfig, record map[string]interface{}, parentContext map[string]interface{}) models.GenericRecord {
	flatRecord := make(models.GenericRecord)

	// Add fields from parent entities
	for _, parentRef := range tableConfig.ParentRefs {
		var parentData map[string]interface{}

		if parentRef.EntityName == "user" {
			if current, ok := parentContext["__current__"].(map[string]interface{}); ok {
				parentData = current
			}
		} else {
			if data, ok := parentContext[parentRef.EntityName].(map[string]interface{}); ok {
				parentData = data
			}
		}

		if parentData != nil {
			for _, field := range parentRef.Fields {
				value := getValueFromPath(parentData, field.JSONPath)
				flatRecord[field.Name] = convertValue(value, field.Type)
			}
		}
	}

	// Add fields from current record
	for _, field := range tableConfig.Fields {
		value := getValueFromPath(record, field.JSONPath)

		if value == nil && field.DefaultValue != "" {
			value = field.DefaultValue
		}

		flatRecord[field.Name] = convertValue(value, field.Type)
	}

	return flatRecord
}

// getValueFromPath extracts value from a map using dot notation path
func getValueFromPath(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		dataMap, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current = dataMap[part]
		if current == nil {
			return nil
		}
	}

	return current
}

// convertValue converts interface{} to the specified type
func convertValue(value interface{}, targetType string) interface{} {
	if value == nil {
		switch targetType {
		case "string":
			return ""
		case "int64":
			return int64(0)
		case "float64":
			return float64(0)
		case "bool":
			return false
		default:
			return nil
		}
	}

	switch targetType {
	case "string":
		return fmt.Sprintf("%v", value)
	case "int64":
		switch v := value.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		default:
			return int64(0)
		}
	case "float64":
		switch v := value.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		default:
			return float64(0)
		}
	case "bool":
		if b, ok := value.(bool); ok {
			return b
		}
		return false
	default:
		return value
	}
}

// initializeWriters creates writers for all configured tables
func (gp *GenericParser) initializeWriters() error {
	for _, tableConfig := range gp.config.Tables {
		outputPath := filepath.Join(gp.config.OutputPath, fmt.Sprintf("%s.parquet", tableConfig.Name))

		writer, err := NewDynamicWriter(outputPath, tableConfig, gp.config.Compression)
		if err != nil {
			return fmt.Errorf("failed to create writer for table %s: %w", tableConfig.Name, err)
		}

		gp.writers[tableConfig.Name] = writer
		log.Infof("Initialized writer for table: %s", tableConfig.Name)
	}

	return nil
}

// getWriter retrieves a writer for a table
func (gp *GenericParser) getWriter(tableName string) *DynamicWriter {
	gp.writersMux.RLock()
	defer gp.writersMux.RUnlock()
	return gp.writers[tableName]
}

// closeWriters closes all writers
func (gp *GenericParser) closeWriters() {
	gp.writersMux.Lock()
	defer gp.writersMux.Unlock()

	for name, writer := range gp.writers {
		if err := writer.Close(); err != nil {
			log.Errorf("Failed to close writer for table %s: %v", name, err)
		} else {
			log.Infof("Closed writer for table: %s", name)
		}
	}
}

// ParseGeneric is the main entry point for generic parsing
func ParseGeneric(configPath string) error {
	parser, err := NewGenericParser(configPath)
	if err != nil {
		return err
	}

	// Backward-compat: parse directly from configured source path (no downloading here)
	return parser.ParseFile(parser.config.Source.Path)
}

// ---- internal helpers for context management ----
// cloneMap creates a shallow copy of a map (nil-safe).
func cloneMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return make(map[string]interface{})
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// singularize returns a naive singular form by trimming a trailing 's'.
func singularize(name string) string {
	if strings.HasSuffix(name, "s") && len(name) > 1 {
		return name[:len(name)-1]
	}
	return name
}

// contextWithEntity returns a cloned context with the entity bound under both
// the provided plural name and its naive singular form.
func contextWithEntity(ctx map[string]interface{}, pluralName string, entity map[string]interface{}) map[string]interface{} {
	next := cloneMap(ctx)
	next[pluralName] = entity
	sg := singularize(pluralName)
	if sg != pluralName {
		next[sg] = entity
	}
	return next
}
