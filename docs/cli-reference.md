# CLI Reference

Complete reference for the `data-sync` command-line interface.

## Global Options

```bash
data-sync [OPTIONS] COMMAND [ARGS]...
```

### Options

| Option | Description |
|--------|-------------|
| `--version` | Show version number and exit |
| `--help` | Show help message and exit |

## Commands

### sync

Sync a CSV or CDF file to the database using a configuration.

```bash
data-sync sync FILE_PATH CONFIG JOB [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILE_PATH` | Path | Yes | Path to the CSV or CDF file to sync |
| `CONFIG` | Path | Yes | Path to the YAML configuration file |
| `JOB` | String | Yes | Name of the job to run from config |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--db-url TEXT` | String | `$DATABASE_URL` | PostgreSQL connection string |
| `--dry-run` | Flag | False | Simulate sync without making database changes |
| `--max-records INTEGER` | Integer | None (all) | Maximum number of records to extract per variable from CDF files |

#### Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string (alternative to `--db-url`) |

#### Examples

**Basic CSV sync:**

```bash
data-sync sync data.csv config.yaml my_job --db-url postgresql://localhost/mydb
```

**Sync CDF file (automatic extraction):**

```bash
data-sync sync science_data.cdf config.yaml vectors --db-url postgresql://localhost/mydb
```

**Sync CDF with limited records (for testing):**

```bash
data-sync sync science_data.cdf config.yaml vectors --db-url postgresql://localhost/mydb --max-records 200
```

**Using environment variable:**

```bash
export DATABASE_URL=postgresql://localhost/mydb
data-sync sync data.csv config.yaml my_job
```

**Dry-run mode:**

```bash
data-sync sync data.csv config.yaml my_job --dry-run
```

**Dry-run CDF with limited records:**

```bash
data-sync sync data.cdf config.yaml my_job --dry-run --max-records 100
```

#### Output

**Normal mode:**

```
Syncing data.csv using job 'my_job'...
  Extracted values: {'date': '2024-01-15'}
✓ Successfully synced 100 rows
  Table: my_table
  File: data.csv
  Extracted values: {'date': '2024-01-15'}
```

**Dry-run mode:**

```
DRY RUN: Simulating sync of data.csv using job 'my_job'...

Dry-run Summary
────────────────────────────────────────────────────────────
  • Table 'my_table' would be CREATED

Data Changes:
  • 100 row(s) would be inserted/updated
  • No stale rows to delete

✓ Dry-run complete - no changes made to database
  File: data.csv
  Extracted values: {'date': '2024-01-15'}
```

#### Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error occurred |

---

### prepare

Analyze a CSV or CDF file and generate or update a configuration file.

```bash
data-sync prepare FILE_PATH... CONFIG [JOB] [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILE_PATH` | Path(s) | Yes | Path to the CSV or CDF file(s) to analyze |
| `CONFIG` | Path | Yes | Path to the YAML configuration file (created if doesn't exist) |
| `JOB` | String | No | Name for the job (auto-generated from filename if omitted) |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--force` | Flag | False | Overwrite existing job if it exists |

#### Behavior

1. **Analyzes CSV file**:
   - Detects column types (integer, float, text, date, etc.)
   - Identifies nullable columns
   - Suggests an ID column

2. **Detects filename patterns**:
   - Looks for date patterns (YYYYMMDD, YYYY-MM-DD, YYYY_MM_DD)
   - Suggests `filename_to_column` configuration if found

3. **Suggests indexes**:
   - Date/datetime columns → descending indexes
   - Columns ending in `_id` or `_key` → ascending indexes

4. **Generates job name** (if not provided):
   - Removes file extension
   - Removes numbers
   - Collapses multiple underscores/hyphens
   - Strips trailing separators
   - Converts to lowercase
   - Example: `Sales_Data_2024.csv` → `sales_data`

#### Examples

**Auto-generate job name from CSV:**

```bash
data-sync prepare users.csv --config config.yaml
```

**Prepare CDF file:**

```bash
data-sync prepare science_data.cdf --config config.yaml
```

**Specify job name:**

```bash
data-sync prepare users.csv --config config.yaml --job my_custom_job
```

**Multiple files (auto-names each):**

```bash
data-sync prepare file1.csv file2.csv --config config.yaml
```

**Update existing job:**

```bash
data-sync prepare users.csv --config config.yaml --job users_sync --force
```

#### Output

```
Analyzing users_2024.csv...
  Found 5 columns
  Suggested ID column: user_id
  Detected date pattern in filename: users_[date].csv
  Suggested 2 index(es)

┌──────────────────────────────────────────────┐
│ Column Analysis                              │
├──────────────┬─────────────┬──────────────┤
│ Column       │ Type        │ Nullable     │
├──────────────┼─────────────┼──────────────┤
│ user_id      │ INTEGER     │ NOT NULL     │
│ name         │ TEXT        │ NOT NULL     │
│ email        │ TEXT        │ NOT NULL     │
│ created_at   │ DATE        │ NULL         │
│ status       │ TEXT        │ NULL         │
└──────────────┴─────────────┴──────────────┘

┌──────────────────────────────────────────────┐
│ Suggested Indexes                            │
├──────────────┬─────────────┬──────────────┤
│ Index Name   │ Column      │ Order        │
├──────────────┼─────────────┼──────────────┤
│ idx_created_at│ created_at │ DESC         │
└──────────────┴─────────────┴──────────────┘

✓ Created job 'users' in config.yaml
  Target table: users
  ID column: user_id → id
  Filename pattern detected: users_[date].csv
```

#### Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | Error occurred (e.g., job exists and --force not used) |

---

### inspect

Inspect CSV or CDF files and display summary information.

```bash
data-sync inspect FILES... [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILES` | Path(s) | Yes | One or more file paths to inspect |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--max-records`, `-n` | Integer | 10 | Number of sample records to display |

#### Examples

**Inspect a single CSV file:**

```bash
data-sync inspect users.csv
```

**Inspect a CDF file:**

```bash
data-sync inspect science_data.cdf
```

**Inspect with custom record count:**

```bash
data-sync inspect data.csv --max-records 20
data-sync inspect data.cdf -n 5
```

**Inspect multiple files:**

```bash
data-sync inspect file1.csv file2.cdf file3.csv
```

#### Output

Displays:
- File format and size
- For CSV: column names, types, row count, sample data
- For CDF: variables, record counts, dimensions, attributes

---

### extract

Extract data from CDF files to CSV format.

Supports two modes:
1. **Raw extraction** (default): Extracts all CDF variables to CSV with automatic column naming
2. **Config-based extraction**: Uses job configuration to select, rename, and transform columns (same as `sync` command but outputs to CSV)

```bash
data-sync extract FILES... [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILES` | Path(s) | Yes | One or more CDF files to extract |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--output-path`, `-o` | Path | Current directory | Output directory for CSV files |
| `--filename` | String | `[SOURCE_FILE]-[VARIABLE_NAME].csv` | Template for output filenames (raw mode only) |
| `--automerge` | Flag | True | Merge variables with same record count (raw mode only) |
| `--no-automerge` | Flag | - | Create separate CSV for each variable (raw mode only) |
| `--append` | Flag | False | Append to existing CSV files (raw mode only) |
| `--variables`, `-v` | String(s) | All | Specific variable names to extract (raw mode only) |
| `--max-records` | Integer | None (all) | Maximum number of records to extract per variable |
| `--config`, `-c` | Path | None | YAML configuration file (requires `--job`) |
| `--job`, `-j` | String | None | Job name from config (requires `--config`) |

#### Examples

**Raw Extraction Mode:**

**Extract all variables:**

```bash
data-sync extract science_data.cdf
```

**Extract to specific directory:**

```bash
data-sync extract data.cdf --output-path ./output
```

**Extract with limited records (for testing):**

```bash
data-sync extract data.cdf --max-records 100
```

**Extract specific variables:**

```bash
data-sync extract data.cdf --variables Epoch --variables B_field
```

**Extract without automerge:**

```bash
data-sync extract data.cdf --no-automerge
```

**Config-Based Extraction Mode:**

**Extract with column mapping (same transformations as sync):**

```bash
data-sync extract science_data.cdf --config config.yaml --job vectors_job
```

**Extract to specific directory with config:**

```bash
data-sync extract data.cdf -o output/ --config config.yaml --job my_job
```

**Config-based with limited records:**

```bash
data-sync extract data.cdf --config config.yaml --job my_job --max-records 100
```

**Multiple files with config:**

```bash
data-sync extract *.cdf --config config.yaml --job my_job -o output/
```

#### Output

**Raw mode** creates CSV files with:
- One CSV per group of variables (with automerge)
- Or one CSV per variable (without automerge)
- Column names derived from variable labels or names
- Array variables expanded into multiple columns

**Config mode** creates CSV files with:
- Columns selected and renamed according to job configuration
- Same transformations (lookup, expression, function) as `sync` command
- Metadata from filename extraction (if configured)
- One CSV file per CDF file (named after source file)

---

## Connection String Format

The `--db-url` option accepts standard PostgreSQL connection strings:

### Basic Format

```
postgresql://[user[:password]@][host][:port][/dbname]
```

### Examples

**Local database:**

```bash
postgresql://localhost/mydb
postgresql://localhost:5432/mydb
```

**With authentication:**

```bash
postgresql://user:password@localhost/mydb
postgresql://user:password@localhost:5432/mydb
```

**Cloud providers:**

```bash
# AWS RDS
postgresql://user:pass@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb

# Google Cloud SQL
postgresql://user:pass@10.1.2.3:5432/mydb

# Supabase
postgresql://postgres:pass@db.abc123.supabase.co:5432/postgres
```

**SQLite (alternative):**

```bash
sqlite:///path/to/database.db
```

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | Default database connection string | `postgresql://localhost/mydb` |

## Common Workflows

### Initial Setup

```bash
# 1. Analyze CSV and create config
data-sync prepare data.csv config.yaml my_job

# 2. Review generated config.yaml
cat config.yaml

# 3. Test with dry-run
data-sync sync data.csv config.yaml my_job --dry-run

# 4. Run actual sync
data-sync sync data.csv config.yaml my_job
```

### Daily Updates

```bash
# Setup (once)
export DATABASE_URL="postgresql://localhost/mydb"

# Daily sync (idempotent)
data-sync sync sales_$(date +%Y-%m-%d).csv config.yaml daily_sales
```

### Batch Processing

```bash
# Process multiple files
for file in data/*.csv; do
  data-sync sync "$file" config.yaml my_job
done
```

### Configuration Updates

```bash
# Update existing job with --force
data-sync prepare new_data.csv config.yaml my_job --force

# Review changes
git diff config.yaml

# Test new config
data-sync sync new_data.csv config.yaml my_job --dry-run
```

## Troubleshooting

### Job not found

```
Error: Job 'my_job' not found in config
Available jobs: users_sync, daily_sales
```

**Solution**: Check job name spelling or use `prepare` to create it.

### Filename pattern mismatch

```
Error: Could not extract values from filename 'data.csv'
  Pattern: sales_[date].csv
```

**Solution**: Rename file to match pattern or update `filename_to_column` configuration.

### Database connection failed

```
Error: could not connect to server
```

**Solution**: Verify `DATABASE_URL` is correct and database is running.

## Next Steps

- [Configuration Guide](configuration.md) - Learn about YAML configuration
- [Features](features.md) - Detailed feature documentation
- [API Reference](api-reference.md) - Use data-sync programmatically
