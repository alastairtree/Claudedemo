# Features

Detailed documentation of all data-sync features.

## Idempotent Operations

Running sync multiple times is safe - it uses PostgreSQL's `INSERT ... ON CONFLICT DO UPDATE` (upsert).

### How it Works

```bash
# First run: inserts 3 rows
data-sync sync users.csv config.yaml sync_users

# Second run: updates existing rows, no duplicates
data-sync sync users.csv config.yaml sync_users
```

The same rows are updated, not duplicated. This makes data-sync safe for:

- Automated scripts
- Cron jobs
- CI/CD pipelines
- Re-running after failures

### Benefits

- **No duplicate data**: Primary key conflicts are handled automatically
- **Safe retries**: Failures can be retried without cleanup
- **Incremental updates**: Update only changed rows
- **Atomic operations**: Each sync is a single transaction

## Column Mapping

Map CSV columns to different database column names.

### Basic Mapping

```yaml
columns:
  name: full_name      # CSV: name → DB: full_name
  email: email_address # CSV: email → DB: email_address
```

### Use Cases

- **Database conventions**: Map to snake_case or camelCase
- **Legacy schemas**: Adapt to existing table structures
- **Conflict resolution**: Rename columns that conflict with SQL keywords
- **Data normalization**: Standardize column names across sources

### Example

**CSV file** (`users.csv`):
```csv
user_id,name,email
1,Alice,alice@example.com
```

**Configuration**:
```yaml
id_mapping:
  user_id: id
columns:
  name: full_name
  email: email_address
```

**Database table**:
```sql
SELECT id, full_name, email_address FROM users;
```

## Selective Syncing

Choose which columns to sync, ignoring others in the CSV.

### Configuration

```yaml
columns:
  name: full_name
  email: email
  # internal_notes column is NOT synced
```

### Use Cases

- **Privacy**: Exclude sensitive columns
- **Optimization**: Reduce data size by excluding unused columns
- **Partial updates**: Update only specific fields
- **Data filtering**: Skip temporary or internal columns

### Sync All Columns

To sync all columns with their original names, omit the `columns` field:

```yaml
jobs:
  my_job:
    target_table: users
    id_mapping:
      user_id: id
    # No columns field = sync all columns
```

## Filename-Based Value Extraction

Extract values from filenames (dates, versions, mission names, etc.) and store them in database columns.

### Template Syntax

Use `[column_name]` placeholders:

```yaml
filename_to_column:
  template: "sales_[date].csv"
  columns:
    date:
      db_column: sync_date
      type: date
      use_to_delete_old_rows: true
```

**Matches**: `sales_2024-01-15.csv`
**Extracts**: `date = '2024-01-15'`

### Regex Syntax

For complex patterns, use regex with named groups:

```yaml
filename_to_column:
  regex: '(?P<mission>[a-z]+)_level2_(?P<sensor>[a-z]+)_(?P<date>\d{8})_v(?P<version>\d+)\.cdf'
  columns:
    mission:
      db_column: mission_name
      type: varchar(10)
    sensor:
      db_column: sensor_type
      type: varchar(20)
    date:
      db_column: observation_date
      type: date
      use_to_delete_old_rows: true
    version:
      db_column: file_version
      type: varchar(10)
```

**Matches**: `imap_level2_primary_20240115_v002.cdf`
**Extracts**:
- `mission = 'imap'`
- `sensor = 'primary'`
- `date = '20240115'`
- `version = '002'`

### Use Cases

- **Time-series data**: Extract dates from daily/weekly/monthly files
- **Versioned data**: Track file versions in the database
- **Multi-tenant**: Extract customer/tenant IDs from filenames
- **Data provenance**: Record source information
- **Partitioned data**: Extract partition keys

## Automatic Stale Record Cleanup

Automatically delete records that are no longer in the current CSV.

### How it Works

1. **Extract value** from filename (e.g., date)
2. **Add to all rows** being synced
3. **After sync**, delete rows where:
   - The extracted value(s) match
   - But the ID is not in the current CSV

### Configuration

Mark columns with `use_to_delete_old_rows: true`:

```yaml
filename_to_column:
  template: "sales_[date].csv"
  columns:
    date:
      db_column: sync_date
      type: date
      use_to_delete_old_rows: true
```

### Example Workflow

**Day 1**: Sync sales for 2024-01-15

```bash
data-sync sync sales_2024-01-15.csv config.yaml daily_sales
```

**Database**:
```sql
SELECT * FROM sales WHERE sync_date = '2024-01-15';
-- 100 rows
```

**Day 2**: Re-sync with corrections (only 95 rows now)

```bash
data-sync sync sales_2024-01-15-corrected.csv config.yaml daily_sales
```

**Result**:
- Updates 95 existing rows
- Deletes 5 stale rows (no longer in CSV)
- Preserves rows for other dates

### Compound Delete Keys

Use multiple columns to identify stale records:

```yaml
filename_to_column:
  template: "[mission]_[sensor]_[date].cdf"
  columns:
    mission:
      db_column: mission_name
      type: varchar(10)
      use_to_delete_old_rows: true
    sensor:
      db_column: sensor_type
      type: varchar(20)
      use_to_delete_old_rows: true
    date:
      db_column: observation_date
      type: date
      use_to_delete_old_rows: true
```

Deletes stale rows only when **all three values** match.

### Benefits

- **Safe incremental syncs**: Replace partitioned data without affecting others
- **Automatic cleanup**: No manual deletion needed
- **Data integrity**: Ensures database matches current source
- **Versioning support**: Update specific partitions while preserving history

## Compound Primary Keys

Support for multi-column primary keys when a single column isn't unique.

### Configuration

```yaml
id_mapping:
  store_id: store_id
  product_id: product_id
```

This creates a compound primary key on `(store_id, product_id)`.

### Use Cases

- **Many-to-many relationships**: Store-product, user-role mappings
- **Time-series with dimensions**: Metric-timestamp-host
- **Multi-tenant data**: Tenant-record combinations
- **Hierarchical data**: Parent-child relationships

### Example

**CSV file**:
```csv
store_id,product_id,quantity,price
1,100,50,9.99
1,101,30,14.99
2,100,25,9.99
```

**Configuration**:
```yaml
id_mapping:
  store_id: store_id
  product_id: product_id
columns:
  quantity: qty
  price: price
```

**Database**:
```sql
-- Compound primary key on (store_id, product_id)
SELECT * FROM store_sales;
```

## Database Indexes

Define indexes to improve query performance.

### Single Column Index

```yaml
indexes:
  - name: idx_created_at
    columns:
      - column: created_at
        order: DESC
```

### Multi-Column Index

```yaml
indexes:
  - name: idx_user_date
    columns:
      - column: user_id
        order: ASC
      - column: created_at
        order: DESC
```

### Index Features

- **Single or multi-column**: Support for composite indexes
- **Sort order**: Ascending (ASC) or descending (DESC)
- **Automatic creation**: Created if they don't exist
- **Idempotent**: Safe to run multiple times
- **Cross-database**: Works with PostgreSQL and SQLite

### Automatic Index Suggestions

The `prepare` command suggests indexes automatically:

```bash
data-sync prepare activity_log.csv config.yaml user_activity
```

**Rules**:

| Column Type | Index Type | Reason |
|-------------|------------|--------|
| Date/datetime columns | DESC | Recent-first queries |
| Columns ending in `_id` or `_key` | ASC | Foreign key lookups |
| ID column | Skipped | Already a primary key |

### Performance Impact

**Without index**:
```sql
-- Seq Scan on sales (cost=0.00..1234.56 rows=100)
SELECT * FROM sales WHERE created_at > '2024-01-01';
```

**With index**:
```sql
-- Index Scan using idx_created_at on sales (cost=0.42..8.44 rows=100)
SELECT * FROM sales WHERE created_at > '2024-01-01';
```

## Dry-Run Mode

Preview all changes without modifying the database.

### Usage

```bash
data-sync sync data.csv config.yaml my_job --dry-run
```

### What it Shows

**Schema Changes**:
- Tables to be created
- Columns to be added
- Indexes to be created

**Data Changes**:
- Number of rows to insert/update
- Number of stale rows to delete

**Example Output**:

```
DRY RUN: Simulating sync of sales_2024-01-15.csv using job 'daily_sales'...

Dry-run Summary
────────────────────────────────────────────────────────────
  • Table 'sales' exists
  • 2 column(s) would be ADDED:
      - product_name (TEXT)
      - category (TEXT)
  • 1 index(es) would be CREATED:
      - idx_sync_date

Data Changes:
  • 150 row(s) would be inserted/updated
  • 10 stale row(s) would be deleted

✓ Dry-run complete - no changes made to database
  File: sales_2024-01-15.csv
  Extracted values: {'date': '2024-01-15'}
```

### Use Cases

- **Test configurations**: Verify config before running
- **Preview schema changes**: See what columns/indexes will be added
- **Estimate impact**: Check how many rows will be affected
- **Debug issues**: Identify problems without side effects
- **Documentation**: Show expected behavior

## Multi-Database Support

Works with PostgreSQL and SQLite.

### PostgreSQL

Primary target with full feature support:

```bash
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync data.csv config.yaml my_job
```

**Features**:
- Full upsert support
- Compound primary keys
- Advanced indexing
- Row value constructors for compound keys
- Transaction isolation

### SQLite

Alternative for testing and lightweight use:

```bash
export DATABASE_URL="sqlite:///mydb.db"
data-sync sync data.csv config.yaml my_job
```

**Limitations**:
- Some index features work differently
- Compound key deletion uses AND conditions instead of row value constructors

## Type Detection

Automatically detects column types from CSV data.

### Supported Types

| Type | Example Values |
|------|---------------|
| INTEGER | `1`, `42`, `-100` |
| FLOAT | `3.14`, `-0.5`, `1.23e-4` |
| DATE | `2024-01-15`, `2024-12-31` |
| TEXT | `Alice`, `alice@example.com` |

### Nullable Detection

Columns with empty values are marked as nullable:

```csv
user_id,name,email,notes
1,Alice,alice@example.com,
2,Bob,bob@example.com,Admin
```

Result:
- `notes`: `TEXT NULL` (has empty value)
- `name`: `TEXT NOT NULL` (always has value)

### Override Types

Specify types explicitly in configuration:

```yaml
filename_to_column:
  template: "data_[version].csv"
  columns:
    version:
      db_column: file_version
      type: varchar(10)  # Override detected type
```

## Next Steps

- [Configuration Guide](configuration.md) - YAML configuration reference
- [CLI Reference](cli-reference.md) - Command-line options
- [API Reference](api-reference.md) - Python API documentation
