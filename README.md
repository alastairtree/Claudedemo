# data-sync

Sync CSV and CDF science files into PostgreSQL database using configuration-based jobs. A robust CLI application built with Python 3.11+, demonstrating best practices for maintainable Python software.

[![CI](https://github.com/yourusername/data-sync/workflows/CI/badge.svg)](https://github.com/yourusername/data-sync/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Features

- **Dual Interface**: Use as a CLI tool or import as a Python library
- **Configuration-Based**: Define sync jobs in YAML with column mappings
- **Column Mapping**: Rename columns between CSV and database
- **Selective Sync**: Choose which columns to sync or sync all
- **Date-Based Syncing**: Extract dates from filenames and store in database
- **Automatic Cleanup**: Delete stale records for specific dates after sync
- **Compound Primary Keys**: Support for multi-column primary keys
- **Database Indexes**: Define indexes with custom sort orders
- **Automatic Index Suggestions**: Prepare command suggests indexes based on column types
- **Dry-Run Mode**: Preview all changes without modifying the database
- **Idempotent Operations**: Safe to run multiple times, uses upsert
- **Multi-Database Support**: Works with PostgreSQL and SQLite
- **Modern Python**: Built for Python 3.11+ with full type hints
- **CLI Interface**: User-friendly command-line interface using Click
- **Programmatic API**: Full Python API for integration into applications
- **Rich Output**: Beautiful terminal output with Rich library
- **Code Quality**: Automated linting with Ruff and type checking with MyPy
- **CI/CD**: GitHub Actions workflow with automated PyPI publishing
- **Cross-Platform**: Tested on Linux, Windows, and macOS

## Installation

### Prerequisites

- Python 3.11 or higher
- PostgreSQL database (for database operations)
- Docker (only for running integration tests during development)

### Install from PyPI (Recommended)

Install the package using pip or uv:

```bash
# Using pip
pip install data-sync

# OR using uv
uv pip install data-sync
```

This installs the `data-sync` CLI tool and makes the package available for programmatic use.

### Install from source

For development or to get the latest unreleased features:

```bash
# Clone the repository
git clone https://github.com/yourusername/data-sync.git
cd data-sync

# Install with uv (recommended)
uv sync --all-extras

# OR install with pip
pip install -e ".[dev]"
```

### VSCode Dev Container (Recommended)

For the best development experience, open the project in VSCode with the Dev Containers extension:

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop)
2. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
3. Open the project folder in VSCode
4. Click "Reopen in Container" when prompted (or use Command Palette: "Dev Containers: Reopen in Container")

The devcontainer includes:
- Python 3.11 with all dependencies pre-installed via uv
- Docker-in-Docker for running integration tests
- VSCode extensions for Python, Ruff, and Docker
- Proper test and linting configuration

## Usage

### Quick Start

1. **Create a configuration file** (`config.yaml`):

```yaml
jobs:
  sync_users:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name: full_name
      email: email_address
```

OR use the tool to examine your csv file and suggest a config file job entry:

```
data-sync prepare test.csv config.yaml new_import_job
```

2. **Have a CSV file you want to sync to the database** (`users.csv`):

```csv
user_id,name,email,notes,thing
1,Alice,alice@example.com,Admin user,123
2,Bob,bob@example.com,Regular user,5.88
```

3. **Run the sync**:

```bash
# Set database URL
export DATABASE_URL="postgresql://user:password@localhost:5432/mydb"

# Sync the file
data-sync sync users.csv config.yaml sync_users
```

### Configuration File Format

The configuration file defines named jobs, each with:

- `target_table`: The PostgreSQL table name
- `id_mapping`: Mapping for the ID column (CSV column → DB column)
- `columns`: Optional list of columns to sync (syncs all if omitted)

#### Example: Sync All Columns

```yaml
jobs:
  sync_products:
    target_table: products
    id_mapping:
      product_id: id
    # No columns specified = sync all columns
```

#### Example: Selective Sync with Renaming

```yaml
jobs:
  sync_with_rename:
    target_table: customers
    id_mapping:
      customer_id: id
    columns:
      first_name: fname
      last_name: lname
      # Note: other CSV columns like 'internal_notes' won't be synced
```

#### Example: Date-Based Sync from a Daily File with Automatic Cleanup

```yaml
jobs:
  daily_sales_sync:
    target_table: sales
    id_mapping:
      sale_id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'  # Extract YYYY-MM-DD from filename
      db_column: sync_date                    # Store date in this column
    columns:
      product_id: product_id
      amount: amount
```

This configuration extracts the date from the filename (e.g., `sales_2024-01-15.csv` → `2024-01-15`) and:
1. Stores the date in the `sync_date` column for all synced rows
2. Automatically deletes stale records (same date, but IDs not in current CSV)
3. Allows safe incremental syncs where you can replace all data for a specific date

### Command Line Interface

```bash
# Basic usage
data-sync sync <csv_file> <config_file> <job_name> --db-url <connection_string>

# Using environment variable for database
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync data.csv config.yaml my_job

# Dry-run mode (preview changes without modifying database)
data-sync sync data.csv config.yaml my_job --dry-run

# Show help
data-sync --help
data-sync sync --help

# Show version
data-sync --version
```

#### Dry-Run Mode

The `--dry-run` flag allows you to preview what changes would be made without actually modifying the database:

```bash
data-sync sync sales_2024-01-15.csv config.yaml daily_sales --dry-run
```

**What it reports:**
- Schema changes (new tables, columns, indexes)
- Number of rows that would be inserted/updated
- Number of stale rows that would be deleted
- All without making any database modifications

**Use cases:**
- Test configuration files before running actual syncs
- Preview schema changes when adding new columns
- Estimate data impact before large syncs
- Debug sync jobs safely

### Key Features

#### Idempotent Operations

Running the sync multiple times is safe - it uses PostgreSQL's `INSERT ... ON CONFLICT DO UPDATE` (upsert):

```bash
# First run: inserts 3 rows
data-sync sync users.csv config.yaml sync_users

# Second run: updates existing rows, no duplicates
data-sync sync users.csv config.yaml sync_users
```

#### Column Mapping

Map CSV columns to different database column names:

```yaml
id_mapping:
  user_id: id    # CSV column: Database column

columns:
  name: full_name      # CSV column: Database column
  email: email_address  # Renamed in database
```

#### Selective Syncing

Only sync specific columns, ignoring others in the CSV:

```csv
user_id,name,email,internal_notes
1,Alice,alice@example.com,Do not sync this column
```

```yaml
columns:
  name: full_name
  email: email
  # internal_notes column is NOT synced
```

#### Date-Based Syncing with Automatic Cleanup

Extract dates from filenames and automatically clean up stale records:

```yaml
date_mapping:
  filename_regex: '(\d{4}-\d{2}-\d{2})'  # Regex to extract date
  db_column: sync_date                    # Column to store date
```

**How it works:**
1. **Date Extraction**: Extracts date from filename using regex pattern
2. **Date Storage**: Stores the extracted date in all synced rows
3. **Automatic Cleanup**: After syncing, deletes records with the same date whose IDs are no longer in the CSV

**Example workflow:**

```bash
# Day 1: Sync sales data for 2024-01-15
data-sync sync sales_2024-01-15.csv config.yaml daily_sales
# Result: Inserts 100 sales records with sync_date = '2024-01-15'

# Day 2: Re-sync same date with updated data (only 95 records)
data-sync sync sales_2024-01-15_corrected.csv config.yaml daily_sales
# Result: Updates existing 95 records, deletes 5 stale records
#         Other dates in database remain unchanged
```

**Benefits:**
- Safe incremental syncs for time-series data
- Automatic cleanup of removed records for specific dates
- Preserves data from other dates
- Perfect for daily/weekly/monthly data updates

### Programmatic API Usage

The `data-sync` package can be used programmatically as a Python library, allowing you to integrate it into your own applications:

#### Basic Sync Operation

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

#### Dry-Run Mode

```python
from data_sync import sync_csv_to_postgres_dry_run, SyncConfig
from pathlib import Path

config = SyncConfig.from_yaml(Path("config.yaml"))
job = config.get_job("my_job")

# Preview changes without modifying database
summary = sync_csv_to_postgres_dry_run(
    file_path=Path("data.csv"),
    sync_job=job,
    db_url="postgresql://user:pass@localhost/mydb"
)

print(f"Table exists: {summary.table_exists}")
print(f"New columns: {summary.new_columns}")
print(f"Rows to sync: {summary.rows_to_sync}")
print(f"Rows to delete: {summary.rows_to_delete}")
```

#### Analyze CSV Files

```python
from data_sync import analyze_csv_types_and_nullable, suggest_id_column
from pathlib import Path

# Analyze a CSV file to detect column types
column_info = analyze_csv_types_and_nullable(Path("data.csv"))

for col_name, (data_type, nullable) in column_info.items():
    null_str = "NULL" if nullable else "NOT NULL"
    print(f"{col_name}: {data_type} {null_str}")

# Suggest an ID column
columns = list(column_info.keys())
id_column = suggest_id_column(columns)
print(f"Suggested ID column: {id_column}")
```

#### Create Configuration Programmatically

```python
from data_sync import SyncConfig, SyncJob, ColumnMapping, Index, IndexColumn
from pathlib import Path

# Create a sync job programmatically
job = SyncJob(
    name="users_sync",
    target_table="users",
    id_mapping=[
        ColumnMapping(csv_column="user_id", db_column="id", data_type="integer")
    ],
    columns=[
        ColumnMapping(csv_column="name", db_column="full_name", data_type="text"),
        ColumnMapping(csv_column="email", db_column="email", data_type="text"),
    ],
    indexes=[
        Index(
            name="idx_email",
            columns=[IndexColumn(column="email", order="ASC")]
        )
    ]
)

# Create config and add job
config = SyncConfig(jobs={})
config.add_or_update_job(job, force=False)

# Save to file
config.save_to_yaml(Path("config.yaml"))
```

#### Available API Functions

The package exports the following main functions:

- `sync_csv_to_postgres()` - Sync a CSV file to PostgreSQL
- `sync_csv_to_postgres_dry_run()` - Preview sync without changes
- `analyze_csv_types_and_nullable()` - Analyze CSV column types
- `suggest_id_column()` - Suggest an ID column from a list of columns
- `SyncConfig.from_yaml()` - Load configuration from YAML
- `SyncConfig.save_to_yaml()` - Save configuration to YAML
- `SyncJob` - Configuration class for sync jobs
- `ColumnMapping` - Configuration class for column mappings
- `Index` - Configuration class for database indexes
- `DryRunSummary` - Result object from dry-run operations

For complete API documentation, see the inline docstrings in the source code or use Python's `help()` function:

```python
from data_sync import sync_csv_to_postgres
help(sync_csv_to_postgres)
```

#### Compound Primary Keys

Support for multi-column primary keys when a single column isn't unique:

```yaml
jobs:
  sales_by_store:
    target_table: sales
    id_mapping:
      store_id: store_id
      product_id: product_id
    columns:
      quantity: qty
      price: price
```

This creates a compound primary key on `(store_id, product_id)`, ensuring uniqueness across both columns.

#### Database Indexes

Define indexes to improve query performance:

```yaml
jobs:
  user_activity:
    target_table: activity_log
    id_mapping:
      activity_id: id
    columns:
      user_id: user_id
      created_at: created_at
      action_type: action_type
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

**Index Features:**
- Single or multi-column indexes
- Ascending (ASC) or descending (DESC) sort order
- Automatically created if they don't exist
- Works with both PostgreSQL and SQLite

**Automatic Index Suggestions:**

The `prepare` command automatically suggests indexes based on column types and names:

```bash
data-sync prepare activity_log.csv config.yaml user_activity
```

**Index Suggestion Rules:**
- **Date/datetime columns**: Get descending indexes (for recent-first queries)
- **Columns ending in `_id` or `_key`**: Get ascending indexes (for foreign key lookups)
- **ID column**: Excluded (already a primary key)

Example output:
```
Analyzing activity_log.csv...
  Found 5 columns
  Suggested ID column: activity_id
  Suggested 3 index(es)

┌──────────────────┬─────────────┬────────┐
│ Suggested Indexes                       │
├──────────────────┼─────────────┼────────┤
│ idx_user_id      │ user_id     │ ASC    │
│ idx_created_at   │ created_at  │ DESC   │
│ idx_updated_at   │ updated_at  │ DESC   │
└──────────────────┴─────────────┴────────┘
```

You can customize these suggestions in the generated config file before running the sync.

## Development

### Setup Development Environment

```bash
# With uv (recommended)
uv sync --all-extras

# OR with pip
pip install -e ".[dev]"
```

### Running Tests

```bash
# With uv (recommended)
uv run pytest -v

# OR with direct pytest (if using pip)
pytest -v

# Run unit tests only (no Docker required)
uv run pytest tests/test_cli.py tests/test_config.py -v

# Run with coverage
uv run pytest --cov=data_sync

# Run specific test file
uv run pytest tests/test_config.py -v
```

**Note:** Integration tests (`test_database_integration.py`) require Docker to be running, as they use testcontainers to spin up a real PostgreSQL instance.

### Code Quality

```bash
# With uv (recommended)
uv run ruff format .    # Format code
uv run ruff check .     # Lint code
uv run ruff check --fix .  # Fix linting issues
uv run mypy src/data_sync  # Type checking

# OR directly (if using pip)
ruff format .
ruff check .
ruff check --fix .
mypy src/data_sync
```

### Project Structure

```
data-sync/
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions CI/CD
├── src/
│   └── data_sync/
│       ├── __init__.py               # Package initialization
│       ├── cli.py                    # CLI commands
│       ├── config.py                 # YAML configuration parser
│       └── database.py               # PostgreSQL operations
├── tests/
│   ├── __init__.py
│   ├── conftest.py                   # Pytest configuration
│   ├── test_cli.py                   # CLI tests (8 tests)
│   ├── test_config.py                # Config parser and date extraction tests (15 tests)
│   └── test_database_integration.py  # Integration tests (7 tests, requires Docker)
├── pyproject.toml                    # Project configuration
├── README.md                         # This file
└── LICENSE                           # License file
```

## Configuration

The project uses `pyproject.toml` for all configuration:

- **Build system**: Hatchling
- **Testing**: pytest with coverage, testcontainers for integration tests
- **Linting**: Ruff
- **Type checking**: MyPy
- **Dependencies**: PyYAML, psycopg3, Click, Rich

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## Testing Philosophy

This project prioritizes:

- **High coverage**: Aim for >90% code coverage
- **Meaningful tests**: Test behavior, not implementation
- **Fast tests**: Keep test suite fast for quick feedback
- **Real integration tests**: Use testcontainers for authentic database testing
- **Readable tests**: Tests serve as documentation

## Test Results

Current test suite:
- 23 unit tests (CLI, config parsing, date extraction)
- 7 integration tests (real PostgreSQL via testcontainers)
- 100% passing
- Tests verify idempotency, column mapping, date-based syncing, stale record cleanup, and error handling

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Click](https://click.palletsprojects.com/) - Command line interface creation
- [Rich](https://rich.readthedocs.io/) - Beautiful terminal output
- [pytest](https://pytest.org/) - Testing framework
- [Ruff](https://github.com/astral-sh/ruff) - Fast Python linter
- [psycopg3](https://www.psycopg.org/psycopg3/) - PostgreSQL adapter
- [PyYAML](https://pyyaml.org/) - YAML parser
- [testcontainers](https://github.com/testcontainers/testcontainers-python) - Integration testing with real services

## Roadmap

- [x] YAML configuration support
- [x] Column mapping and renaming
- [x] Selective column syncing
- [x] Idempotent upsert operations
- [x] Integration tests with real PostgreSQL
- [x] Dry-run mode
- [ ] Support for CDF science files
- [ ] Data validation and transformation
- [ ] Support for batch processing multiple files
- [ ] Add progress bars for large files
- [ ] Transaction management and rollback
- [ ] Schema migration support

## Support

If you have any questions or run into issues, please [open an issue](https://github.com/yourusername/data-sync/issues).
