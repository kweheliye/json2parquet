# json2parquet

An **open-source Go tool** for processing massive structured datasets (e.g., Transparency in Coverage MRF files) with streaming, filtering, and transformation, enabling efficient preparation of data for analytics, data lakes, and graph database ingestion.

Designed to handle **datasets from gigabytes to terabytes**. The tool processes files in **5MB chunks**, streaming and converting data efficiently into **Parquet format**, enabling analytics workflows without loading the entire dataset into memory. On a 12-core machine, an 80GB dataset can be converted to Parquet in under 5 minutes.

Supports **deeply nested JSON/NDJSON files**, allowing filtering, flattening, and transformation of complex structures into analytics-ready formats.

---

## Key Features

- ✅ Stream large JSON/NDJSON datasets efficiently
- ✅ Convert input to **Parquet format** for analytics and data lakes
- ✅ Read and write **gzip-compressed files** automatically
- ✅ Filter records by field/value (e.g., subset of CPT/HCPCS codes)
- ✅ Filter providers with pricing data, dropping extraneous records
- ✅ Flatten and transform **deeply nested JSON structures**
- ✅ Parallel processing using worker pool for chunked data
- ✅ Read from HTTP, S3, GCS; write to S3, GCS
- ✅ Output schema optimized for **graph database ingestion**

---

## Quick Start

### Install
```bash
git clone https://github.com/yourusername/json2parquet.git
cd json2parquet
go build -o json2parquet .
```

### Usage
```bash
 ./json-pipeline run \                                                   
  --input data/example1.json \
  --output out.parquet \
  --cpuprofile cpu.prof \
  --memprofile mem.prof
```


**Command:**
```bash
 ./json2parquet run \
  --input example.json \
  --codes ./cpt-codes.csv \
  --output out.parquet
```


## Roadmap
- [ ] Configuration file support (YAML/TOML)
- [ ] Advanced filtering and expressions
- [ ] Schema-based transformations
- [ ] Additional output formats: CSV, Avro
- [ ] Cloud connectors (S3, GCS, HTTP URLs)
- [ ] Worker pool for parallelism
- [ ] Metrics, logging, and error handling improvements

---

## Contributing
Pull requests are welcome! Please open an issue first to discuss major changes.

---

## License
MIT License
