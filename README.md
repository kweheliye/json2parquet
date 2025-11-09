# json2parquet

A **configuration-driven tool** for converting massive, nested JSON datasets into flattened Parquet files. It's designed to handle everything from simple JSON to deeply nested structures, making it ideal for preparing data for analytics, data lakes, and ETL pipelines.

The tool operates based on a **YAML configuration file**, allowing you to define how JSON data is mapped to one or more Parquet tables without writing any code. It supports automatic flattening, parent-child relationship preservation, and dynamic schema generation.

---

## 🎯 Key Features

- ✅ **Configuration-Driven**: Define your entire JSON-to-Parquet transformation in a YAML file.
- ✅ **No Code Required**: No need to write custom scripts for different JSON structures.
- ✅ **Deep Nesting Support**: Handles arbitrarily complex and deeply nested JSON.
- ✅ **Automatic Flattening**: Preserves parent-child relationships by adding parent keys to child records.
- ✅ **Multiple Output Tables**: Extract different entities from a single JSON file into separate Parquet tables.
- ✅ **Dynamic Schema Generation**: Parquet schemas are created on-the-fly based on your configuration.
- ✅ **Type Safety**: Automatic type conversion from JSON to Parquet types (`string`, `int64`, `float64`, `bool`).
- ✅ **High Performance**: Built in Go, with support for `zstd` compression and optimized writing.

---

## 🚀 Quick Start

### 1. Install

```bash
git clone https://github.com/yourusername/json2parquet.git
cd json2parquet
go build -o json2parquet main.go
```

### 2. Create a Configuration File

Create a `parse_config.yaml` file to define how your JSON should be processed.

```yaml
# parse_config.yaml
source:
  type: file
  path: "data/nested_20.json" # Path to your JSON file

output_path: "output"             # Directory for Parquet files
compression: "zstd"               # zstd, snappy, gzip, or none

tables:
  # Root-level table for users
  - name: "users"
    json_path: "" # An empty path means this is a root-level object
    fields:
      - { name: "user_id", json_path: "user_id", type: "int64" }
      - { name: "user_name", json_path: "name", type: "string" }

  # Nested table for projects
  - name: "projects"
    json_path: "projects" # "projects" is an array inside each user object
    fields:
      - { name: "project_id", json_path: "project_id", type: "string" }
      - { name: "project_title", json_path: "title", type: "string" }
    parent_refs:
      - entity_name: "users" # Link to the 'users' table
        fields:
          - { name: "user_id", json_path: "user_id", type: "int64" }
```

### 3. Run the Parser

Execute the `generic` command with your configuration file.

```bash
./json2parquet generic --config parse_config.yaml
```

The tool will generate `users.parquet` and `projects.parquet` in the `output/` directory.

### 4. Query the Results

You can use tools like **DuckDB**, **pandas**, or **Spark** to query your Parquet files.

```bash
# Example with DuckDB
duckdb -c "SELECT user_name, project_title FROM 'output/users.parquet' u JOIN 'output/projects.parquet' p ON u.user_id = p.user_id LIMIT 5;"
```

---

## 🔧 Configuration Reference

The YAML configuration has three main sections: `source`, `output_path`, and `tables`.

### `source`

Defines where to find the input JSON data.

```yaml
source:
  type: file          # Currently supports 'file'
  path: "data.json"   # Path to the source JSON file
  root_array: ""      # Optional: If the root of the JSON is an object containing the main array, specify its key here.
```

### `tables`

An array of tables to be extracted. Each table corresponds to a Parquet file.

```yaml
tables:
  - name: "table_name"        # Name of the output Parquet file (and the entity)
    description: "..."        # Optional description
    json_path: "path.to.data" # Dot-notation path to the array in the JSON. Use "" for the root.
    fields:                   # List of columns for this table
      - name: "column_name"
        json_path: "field_in_json"
        type: "int64"         # Supported types: string, int64, float64, bool
        default_value: ""     # Optional: Value to use if the field is null or missing
    parent_refs:              # Optional: Defines the parent-child relationship
      - entity_name: "parent_table_name"
        fields:
          - name: "parent_id_in_child_table"
            json_path: "id_field_in_parent_json"
            type: "int64"
```

For more detailed examples, see `SOLUTION_SUMMARY.md` and `QUICK_REFERENCE.md`.

---

## 📊 How It Works

The parser recursively traverses the JSON structure. For each object, it checks the configuration to see if a table should be created.

1.  **Load Config**: The YAML file is parsed to understand the desired schema.
2.  **Initialize Writers**: A Parquet writer is created for each table defined in the config.
3.  **Process JSON**: The tool reads the JSON and recursively walks through its structure.
4.  **Flatten Data**: When processing a nested object, it keeps track of the parent's data. This "context" is used to add parent keys (like `user_id`) to child records (like `projects`).
5.  **Write Parquet**: Flattened records are written to the corresponding Parquet writer.
6.  **Finalize**: After processing, all writers are closed, and the Parquet files are saved.

---

## 🐛 Troubleshooting

- **Empty Parquet Files**: This often means the `json_path` in your configuration is incorrect or no records matched. Double-check your paths against the JSON structure.
- **Missing Parent Data**: Ensure the `entity_name` in `parent_refs` exactly matches the `name` of the parent table.
- **Errors**: Check the console output for detailed error messages, which often point to configuration issues.

For more tips, see the detailed guides in the project.

---

## Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

---

## License

MIT
