# API Reference

Use data-sync programmatically as a Python library.

## Overview

The `data-sync` package can be imported and used in your Python applications. This is useful for:

- Building custom ETL pipelines
- Integrating with existing applications
- Automating data synchronization workflows
- Creating custom tools on top of data-sync

## Installation

```bash
pip install data-sync
```

## Quick Example

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres, SyncConfig

# Load configuration
config = SyncConfig.from_yaml(Path("config.yaml"))
job = config.get_job("my_job")

# Sync a CSV file
rows_synced = sync_csv_to_postgres(
    file_path=Path("data.csv"),
    sync_job=job,
    db_url="postgresql://user:pass@localhost/mydb"
)
print(f"Synced {rows_synced} rows")
```

## Core Functions

### sync_csv_to_postgres

Sync a CSV file to PostgreSQL.

```python
def sync_csv_to_postgres(
    file_path: Path,
    sync_job: SyncJob,
    db_url: str,
    filename_values: dict[str, str] | None = None
) -> int
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `file_path` | `Path` | Path to the CSV file to sync |
| `sync_job` | `SyncJob` | Configuration for the sync job |
| `db_url` | `str` | Database connection string |
| `filename_values` | `dict[str, str] \| None` | Extracted values from filename (optional) |

**Returns**: Number of rows synced (int)

**Raises**:
- `FileNotFoundError`: CSV file doesn't exist
- `ValueError`: Invalid configuration or data
- `DatabaseError`: Database connection or query errors

**Example**:

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres, SyncConfig

config = SyncConfig.from_yaml(Path("config.yaml"))
job = config.get_job("users_sync")

# Basic sync
rows = sync_csv_to_postgres(
    file_path=Path("users.csv"),
    sync_job=job,
    db_url="postgresql://localhost/mydb"
)

# With filename extraction
filename_values = {"date": "2024-01-15"}
rows = sync_csv_to_postgres(
    file_path=Path("sales_2024-01-15.csv"),
    sync_job=job,
    db_url="postgresql://localhost/mydb",
    filename_values=filename_values
)
```

### sync_csv_to_postgres_dry_run

Preview sync without making database changes.

```python
def sync_csv_to_postgres_dry_run(
    file_path: Path,
    sync_job: SyncJob,
    db_url: str,
    filename_values: dict[str, str] | None = None
) -> DryRunSummary
```

**Parameters**: Same as `sync_csv_to_postgres`

**Returns**: `DryRunSummary` object with:

| Attribute | Type | Description |
|-----------|------|-------------|
| `table_name` | `str` | Name of the target table |
| `table_exists` | `bool` | Whether table already exists |
| `new_columns` | `list[tuple[str, str]]` | Columns to be added (name, type) |
| `new_indexes` | `list[str]` | Indexes to be created |
| `rows_to_sync` | `int` | Number of rows to insert/update |
| `rows_to_delete` | `int` | Number of stale rows to delete |

**Example**:

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres_dry_run, SyncConfig

config = SyncConfig.from_yaml(Path("config.yaml"))
job = config.get_job("my_job")

summary = sync_csv_to_postgres_dry_run(
    file_path=Path("data.csv"),
    sync_job=job,
    db_url="postgresql://localhost/mydb"
)

print(f"Table exists: {summary.table_exists}")
print(f"New columns: {summary.new_columns}")
print(f"Rows to sync: {summary.rows_to_sync}")
print(f"Rows to delete: {summary.rows_to_delete}")

# Check before proceeding
if summary.new_columns:
    print("Warning: New columns will be added!")
    for col_name, col_type in summary.new_columns:
        print(f"  - {col_name}: {col_type}")
```

### analyze_csv_types_and_nullable

Analyze CSV file to detect column types.

```python
def analyze_csv_types_and_nullable(
    file_path: Path
) -> dict[str, tuple[str, bool]]
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `file_path` | `Path` | Path to the CSV file |

**Returns**: Dictionary mapping column names to (type, nullable) tuples

**Example**:

```python
from pathlib import Path
from data_sync import analyze_csv_types_and_nullable

column_info = analyze_csv_types_and_nullable(Path("data.csv"))

for col_name, (data_type, nullable) in column_info.items():
    null_str = "NULL" if nullable else "NOT NULL"
    print(f"{col_name}: {data_type} {null_str}")
```

Output:
```
user_id: INTEGER NOT NULL
name: TEXT NOT NULL
email: TEXT NOT NULL
age: INTEGER NULL
created_at: DATE NULL
```

### suggest_id_column

Suggest an ID column from a list of columns.

```python
def suggest_id_column(columns: list[str]) -> str
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `columns` | `list[str]` | List of column names |

**Returns**: Suggested ID column name (str)

**Logic**:
1. Looks for columns ending with `_id` or `_key`
2. Prefers shorter names
3. Falls back to first column if none found

**Example**:

```python
from data_sync import suggest_id_column

columns = ["user_id", "name", "email", "created_at"]
id_col = suggest_id_column(columns)
print(f"Suggested ID: {id_col}")  # Output: user_id

columns = ["name", "email"]
id_col = suggest_id_column(columns)
print(f"Suggested ID: {id_col}")  # Output: name (first column)
```

## Configuration Classes

### SyncConfig

Main configuration container.

```python
class SyncConfig:
    def __init__(self, jobs: dict[str, SyncJob]) -> None

    @classmethod
    def from_yaml(cls, config_path: Path) -> SyncConfig

    def save_to_yaml(self, config_path: Path) -> None

    def get_job(self, job_name: str) -> SyncJob | None

    def add_or_update_job(self, job: SyncJob, force: bool = False) -> None
```

**Example**:

```python
from pathlib import Path
from data_sync import SyncConfig

# Load from file
config = SyncConfig.from_yaml(Path("config.yaml"))

# Get a job
job = config.get_job("my_job")
if job:
    print(f"Target table: {job.target_table}")

# Create new config
from data_sync import SyncJob, ColumnMapping

new_config = SyncConfig(jobs={})
job = SyncJob(
    name="users",
    target_table="users",
    id_mapping=[ColumnMapping("user_id", "id", "integer")]
)
new_config.add_or_update_job(job)
new_config.save_to_yaml(Path("new_config.yaml"))
```

### SyncJob

Configuration for a single sync job.

```python
class SyncJob:
    def __init__(
        self,
        name: str,
        target_table: str,
        id_mapping: list[ColumnMapping],
        columns: list[ColumnMapping] | None = None,
        filename_to_column: FilenameToColumn | None = None,
        indexes: list[Index] | None = None,
    ) -> None
```

**Attributes**:

| Attribute | Type | Description |
|-----------|------|-------------|
| `name` | `str` | Job name |
| `target_table` | `str` | Target database table |
| `id_mapping` | `list[ColumnMapping]` | Primary key mapping |
| `columns` | `list[ColumnMapping]` | Column mappings (None = sync all) |
| `filename_to_column` | `FilenameToColumn \| None` | Filename extraction config |
| `indexes` | `list[Index]` | Database indexes to create |

**Example**:

```python
from data_sync import SyncJob, ColumnMapping, Index, IndexColumn

job = SyncJob(
    name="users_sync",
    target_table="users",
    id_mapping=[
        ColumnMapping("user_id", "id", "integer")
    ],
    columns=[
        ColumnMapping("name", "full_name", "text"),
        ColumnMapping("email", "email", "text"),
    ],
    indexes=[
        Index(
            name="idx_email",
            columns=[IndexColumn("email", "ASC")]
        )
    ]
)
```

### ColumnMapping

Mapping for a single column.

```python
class ColumnMapping:
    def __init__(
        self,
        csv_column: str,
        db_column: str,
        data_type: str | None = None
    ) -> None
```

**Example**:

```python
from data_sync import ColumnMapping

mapping = ColumnMapping(
    csv_column="user_id",
    db_column="id",
    data_type="integer"
)
```

### FilenameToColumn

Configuration for extracting values from filenames.

```python
class FilenameToColumn:
    def __init__(
        self,
        columns: dict[str, FilenameColumnMapping],
        template: str | None = None,
        regex: str | None = None,
    ) -> None

    def extract_values_from_filename(
        self,
        filename: str | Path
    ) -> dict[str, str] | None

    def get_delete_key_columns(self) -> list[str]
```

**Example**:

```python
from data_sync import FilenameToColumn, FilenameColumnMapping

ftc = FilenameToColumn(
    template="sales_[date].csv",
    columns={
        "date": FilenameColumnMapping(
            name="date",
            db_column="sync_date",
            data_type="date",
            use_to_delete_old_rows=True
        )
    }
)

# Extract values
values = ftc.extract_values_from_filename("sales_2024-01-15.csv")
print(values)  # {'date': '2024-01-15'}

# Get delete key columns
delete_cols = ftc.get_delete_key_columns()
print(delete_cols)  # ['sync_date']
```

### Index

Database index configuration.

```python
class Index:
    def __init__(
        self,
        name: str,
        columns: list[IndexColumn]
    ) -> None
```

**Example**:

```python
from data_sync import Index, IndexColumn

# Single column index
idx1 = Index(
    name="idx_created_at",
    columns=[IndexColumn("created_at", "DESC")]
)

# Multi-column index
idx2 = Index(
    name="idx_user_date",
    columns=[
        IndexColumn("user_id", "ASC"),
        IndexColumn("created_at", "DESC")
    ]
)
```

## Complete Example

Here's a complete example demonstrating various API features:

```python
from pathlib import Path
from data_sync import (
    SyncConfig,
    SyncJob,
    ColumnMapping,
    FilenameToColumn,
    FilenameColumnMapping,
    Index,
    IndexColumn,
    sync_csv_to_postgres,
    sync_csv_to_postgres_dry_run,
    analyze_csv_types_and_nullable,
    suggest_id_column,
)

# Analyze a CSV file
csv_path = Path("sales.csv")
column_info = analyze_csv_types_and_nullable(csv_path)
columns = list(column_info.keys())
id_column = suggest_id_column(columns)

print(f"Detected columns: {columns}")
print(f"Suggested ID: {id_column}")

# Create a job programmatically
job = SyncJob(
    name="daily_sales",
    target_table="sales",
    id_mapping=[
        ColumnMapping(id_column, "id", "integer")
    ],
    columns=[
        ColumnMapping("product_id", "product_id", "integer"),
        ColumnMapping("amount", "amount", "float"),
    ],
    filename_to_column=FilenameToColumn(
        template="sales_[date].csv",
        columns={
            "date": FilenameColumnMapping(
                name="date",
                db_column="sync_date",
                data_type="date",
                use_to_delete_old_rows=True
            )
        }
    ),
    indexes=[
        Index(
            name="idx_sync_date",
            columns=[IndexColumn("sync_date", "DESC")]
        )
    ]
)

# Create config and save
config = SyncConfig(jobs={})
config.add_or_update_job(job)
config.save_to_yaml(Path("config.yaml"))

# Extract filename values
filename_values = job.filename_to_column.extract_values_from_filename(csv_path)

# Dry-run first
summary = sync_csv_to_postgres_dry_run(
    file_path=csv_path,
    sync_job=job,
    db_url="postgresql://localhost/mydb",
    filename_values=filename_values
)

print(f"\nDry-run results:")
print(f"  Table exists: {summary.table_exists}")
print(f"  Rows to sync: {summary.rows_to_sync}")
print(f"  Rows to delete: {summary.rows_to_delete}")

# Confirm and sync
if input("Proceed with sync? (y/n): ").lower() == "y":
    rows = sync_csv_to_postgres(
        file_path=csv_path,
        sync_job=job,
        db_url="postgresql://localhost/mydb",
        filename_values=filename_values
    )
    print(f"\nSynced {rows} rows successfully!")
```

## Error Handling

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres, SyncConfig

try:
    config = SyncConfig.from_yaml(Path("config.yaml"))
    job = config.get_job("my_job")

    if not job:
        print("Job not found in configuration")
        exit(1)

    rows = sync_csv_to_postgres(
        file_path=Path("data.csv"),
        sync_job=job,
        db_url="postgresql://localhost/mydb"
    )
    print(f"Success: {rows} rows synced")

except FileNotFoundError as e:
    print(f"File not found: {e}")
except ValueError as e:
    print(f"Configuration error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Type Hints

All functions include full type hints for IDE autocomplete and type checking:

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres, SyncJob

# Type checker knows the types
def sync_data(csv_file: Path, job: SyncJob) -> None:
    rows: int = sync_csv_to_postgres(
        file_path=csv_file,
        sync_job=job,
        db_url="postgresql://localhost/mydb"
    )
    # rows is guaranteed to be int
    print(f"Synced {rows} rows")
```

## Next Steps

- [Configuration Guide](configuration.md) - Learn about YAML configuration
- [Features](features.md) - Detailed feature documentation
- [Development](development.md) - Contributing to data-sync
