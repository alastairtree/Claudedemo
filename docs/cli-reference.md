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

Sync a CSV file to the database using a configuration.

```bash
data-sync sync FILE_PATH CONFIG JOB [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILE_PATH` | Path | Yes | Path to the CSV file to sync |
| `CONFIG` | Path | Yes | Path to the YAML configuration file |
| `JOB` | String | Yes | Name of the job to run from config |

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--db-url TEXT` | String | `$DATABASE_URL` | PostgreSQL connection string |
| `--dry-run` | Flag | False | Simulate sync without making database changes |

#### Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string (alternative to `--db-url`) |

#### Examples

**Basic sync:**

```bash
data-sync sync data.csv config.yaml my_job --db-url postgresql://localhost/mydb
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

Analyze a CSV file and generate or update a configuration file.

```bash
data-sync prepare FILE_PATH CONFIG [JOB] [OPTIONS]
```

#### Arguments

| Argument | Type | Required | Description |
|----------|------|----------|-------------|
| `FILE_PATH` | Path | Yes | Path to the CSV file to analyze |
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

**Auto-generate job name:**

```bash
data-sync prepare users.csv config.yaml
```

**Specify job name:**

```bash
data-sync prepare users.csv config.yaml my_custom_job
```

**Multiple files (auto-names each):**

```bash
data-sync prepare file1.csv file2.csv config.yaml
```

**Update existing job:**

```bash
data-sync prepare users.csv config.yaml users_sync --force
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
