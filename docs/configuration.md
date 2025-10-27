# Configuration Guide

The configuration file is a YAML file that defines sync jobs. Each job specifies how to sync a CSV file to a database table.

## Basic Configuration

A minimal configuration looks like this:

```yaml
jobs:
  my_job:
    target_table: users
    id_mapping:
      user_id: id
```

## Configuration Reference

### Job Structure

Each job has the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target_table` | string | Yes | Name of the database table |
| `id_mapping` | dict | Yes | Mapping for primary key column(s) |
| `columns` | dict | No | Column mappings (CSV → DB). If omitted, syncs all columns |
| `filename_to_column` | object | No | Extract values from filename to database columns |
| `indexes` | list | No | Database indexes to create |

### ID Mapping

The `id_mapping` defines which column(s) form the primary key:

#### Single Primary Key

```yaml
id_mapping:
  user_id: id  # CSV column: DB column
```

#### Compound Primary Key

```yaml
id_mapping:
  store_id: store_id
  product_id: product_id
```

### Column Mapping

The `columns` field maps CSV columns to database columns:

```yaml
columns:
  name: full_name          # Rename: name → full_name
  email: email_address     # Rename: email → email_address
  status: status           # Keep same name
```

!!! tip
    If you omit the `columns` field entirely, all CSV columns are synced with their original names.

### Filename to Column

Extract values from filenames and store them in database columns:

#### Template Syntax

Use `[column_name]` placeholders in the template:

```yaml
filename_to_column:
  template: "sales_[date].csv"
  columns:
    date:
      db_column: sync_date
      type: date
      use_to_delete_old_rows: true
```

This matches files like `sales_2024-01-15.csv` and extracts `2024-01-15` into the `sync_date` column.

#### Regex Syntax

For more complex patterns, use regex with named groups:

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

This matches files like `imap_level2_primary_20240115_v002.cdf`.

#### Column Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `db_column` | string | No | Database column name (defaults to column name) |
| `type` | string | No | SQL data type (e.g., `date`, `varchar(10)`, `integer`) |
| `use_to_delete_old_rows` | boolean | No | Use this column to identify stale records for deletion |

#### Automatic Cleanup

When `use_to_delete_old_rows: true` is set, data-sync will:

1. Extract the value from the filename
2. Add it to all synced rows
3. Delete rows with matching values but IDs not in the current CSV
4. Preserve rows with different values

**Example Workflow:**

```bash
# Day 1: Sync sales for 2024-01-15
data-sync sync sales_2024-01-15.csv config.yaml daily_sales
# Result: Inserts 100 rows with sync_date = '2024-01-15'

# Day 2: Re-sync same date with corrections (only 95 rows)
data-sync sync sales_2024-01-15-corrected.csv config.yaml daily_sales
# Result: Updates 95 rows, deletes 5 stale rows for 2024-01-15
#         Rows for other dates are untouched
```

#### Compound Delete Keys

You can use multiple columns for identifying stale records:

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

This deletes stale rows only when ALL three values match.

### Indexes

Define database indexes to improve query performance:

```yaml
indexes:
  - name: idx_user_id
    columns:
      - column: user_id
        order: ASC
  - name: idx_created_at
    columns:
      - column: created_at
        order: DESC
  - name: idx_user_date
    columns:
      - column: user_id
        order: ASC
      - column: created_at
        order: DESC
```

#### Index Features

- Single or multi-column indexes
- Ascending (ASC) or descending (DESC) sort order
- Automatically created if they don't exist
- Works with both PostgreSQL and SQLite

#### Automatic Index Suggestions

The `prepare` command automatically suggests indexes:

```bash
data-sync prepare activity_log.csv config.yaml user_activity
```

Suggestion rules:

- **Date/datetime columns**: Get descending indexes (for recent-first queries)
- **Columns ending in `_id` or `_key`**: Get ascending indexes (for foreign key lookups)
- **ID column**: Excluded (already a primary key)

## Complete Example

Here's a comprehensive configuration demonstrating all features:

```yaml
jobs:
  # Simple job - sync all columns
  users_sync:
    target_table: users
    id_mapping:
      user_id: id

  # Selective sync with renaming
  customers_sync:
    target_table: customers
    id_mapping:
      customer_id: id
    columns:
      first_name: fname
      last_name: lname
      email: email_address

  # Daily sales with date extraction and cleanup
  daily_sales:
    target_table: sales
    id_mapping:
      sale_id: id
    filename_to_column:
      template: "sales_[date].csv"
      columns:
        date:
          db_column: sync_date
          type: date
          use_to_delete_old_rows: true
    columns:
      product_id: product_id
      amount: amount
      quantity: qty
    indexes:
      - name: idx_sync_date
        columns:
          - column: sync_date
            order: DESC
      - name: idx_product
        columns:
          - column: product_id
            order: ASC

  # Compound key with indexes
  sales_by_store:
    target_table: store_sales
    id_mapping:
      store_id: store_id
      product_id: product_id
    columns:
      quantity: qty
      price: price
    indexes:
      - name: idx_store_product
        columns:
          - column: store_id
            order: ASC
          - column: product_id
            order: ASC

  # Science data with complex filename pattern
  observation_data:
    target_table: observations
    id_mapping:
      obs_id: id
    filename_to_column:
      template: "[mission]_level2_[sensor]_[date]_v[version].cdf"
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
    indexes:
      - name: idx_obs_date
        columns:
          - column: observation_date
            order: DESC
      - name: idx_mission_sensor
        columns:
          - column: mission_name
            order: ASC
          - column: sensor_type
            order: ASC
```

## Best Practices

1. **Use the prepare command**: Let data-sync analyze your CSV and generate configuration automatically
2. **Start simple**: Begin with basic config, add features as needed
3. **Use dry-run mode**: Test configuration before running actual syncs
4. **Add indexes**: Define indexes for columns you'll query frequently
5. **Use filename extraction**: For time-series or versioned data, extract metadata from filenames
6. **Compound keys carefully**: Only use when a single column isn't unique
7. **Document your jobs**: Use descriptive job names that explain what they do

## Next Steps

- [CLI Reference](cli-reference.md) - Learn about command-line options
- [Features](features.md) - Detailed feature documentation
- [API Reference](api-reference.md) - Use data-sync programmatically
