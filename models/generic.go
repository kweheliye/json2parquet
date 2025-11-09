package models

// GenericRecord represents a generic flattened record
// that can be dynamically created based on configuration
type GenericRecord map[string]interface{}

// TableConfig defines how to extract and flatten data from nested JSON
type TableConfig struct {
	Name        string        `yaml:"name"`        // Table name (e.g., "projects", "tasks")
	Description string        `yaml:"description"` // Table description
	JSONPath    string        `yaml:"json_path"`   // Path to the array in JSON (e.g., "projects", "projects[*].tasks")
	Fields      []FieldConfig `yaml:"fields"`      // Field mappings
	ParentRefs  []ParentRef   `yaml:"parent_refs"` // References to parent entities
}

// FieldConfig defines how to map a JSON field to a Parquet column
type FieldConfig struct {
	Name         string `yaml:"name"`           // Parquet column name
	JSONPath     string `yaml:"json_path"`      // Path in JSON (e.g., "project_id", "title")
	Type         string `yaml:"type"`           // Data type: string, int64, float64, bool
	ParquetType  string `yaml:"parquet_type"`   // Parquet encoding: plain, enum, etc.
	Required     bool   `yaml:"required"`       // Is this field required?
	DefaultValue string `yaml:"default_value"`  // Default value if missing
}

// ParentRef defines a reference to a parent entity
type ParentRef struct {
	EntityName string        `yaml:"entity_name"` // Name of parent entity (e.g., "user", "project")
	Fields     []FieldConfig `yaml:"fields"`      // Fields to copy from parent
}

// ParseConfig defines the overall parsing configuration
type ParseConfig struct {
	Source      SourceConfig  `yaml:"source"`       // Source data configuration
	Tables      []TableConfig `yaml:"tables"`       // Table definitions
	OutputPath  string        `yaml:"output_path"`  // Output directory for Parquet files
	Compression string        `yaml:"compression"`  // Compression type: zstd, snappy, gzip, none
	RowGroup    int           `yaml:"row_group"`    // Rows per group
}

// SourceConfig defines the source data
type SourceConfig struct {
	Type        string `yaml:"type"`         // Type: file, url, s3
	Path        string `yaml:"path"`         // Path to source
	RootArray   string `yaml:"root_array"`   // Root array name if JSON is array at root
	TypeField   string `yaml:"type_field"`   // Field name that contains entity type (optional)
}
