# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Dependencies and Environment
```bash
# Install dependencies with development extras
uv sync --all-extras

# OR using pip
pip install -e ".[dev]"
```

### Testing
```bash
# Start with unit tests and sqlite only as these are fast to run (and no Docker required)
uv run pytest tests -k "not [postgres]" -v

# Run all tests
uv run pytest -v

# Run all tests with coverage
uv run pytest --cov=src --cov-report=term-missing

# database integration tests for sqlite only so VERY FAST (and no Docker required)
uv run pytest tests -k database -k "[sqlite]" -v

# database integration tests for postgres only (slow) (Docker required)
uv run pytest tests -k database -k "[postgres]" -v

# Integration tests only (requires Docker)
uv run pytest tests/test_database_integration.py -v

# Run specific test
uv run pytest tests/test_config.py::TestSyncConfig::test_load_from_yaml -v
```

### Code Quality
```bash
# Format code
uv run ruff format .

# Check and fix linting
uv run ruff check --fix .

# Type checking
uv run mypy src

# Run all quality checks
uv run ruff format . && uv run ruff check --fix . && uv run mypy src && uv run pytest
```

### Documentation
```bash
# Generate and serve documentation locally
./generate-docs.sh

# OR manually
uv run mkdocs serve
```

## Project Architecture

### Core Components

**CLI Interface** (`cli.py`, `cli_*.py`):
- Main entry point with Click-based commands
- Commands: `sync`, `prepare`, `inspect`, `extract`
- Each command has dedicated module (e.g., `cli_sync.py`)

**Configuration System** (`config.py`):
- YAML-based job configuration with `SyncConfig` and `SyncJob` classes
- Column mappings between CSV and database
- Filename extraction patterns for metadata (dates, versions, etc.)
- Compound primary key support via `id_mapping`

**Database Operations** (`database.py`):
- PostgreSQL sync with `sync_csv_to_postgres()`
- Dry-run mode for previewing changes
- Automatic table creation and schema updates
- Stale record cleanup based on filename-extracted values

**Type Detection** (`type_detection.py`):
- Automatic CSV analysis for data types and nullable columns
- Primary key suggestion based on column characteristics

**CDF Support** (`cdf_*.py`):
- Reading and extracting CDF (Common Data Format) science files
- Conversion to CSV for database sync

### Key Features

**Filename to Column Extraction**:
- Extract metadata (dates, versions) from filenames using templates or regex
- Store extracted values in database columns
- Use extracted values to clean up stale records automatically

**Idempotent Operations**:
- Safe to run multiple times
- Updates existing records rather than duplicating
- Handles schema changes gracefully

**Compound Primary Keys**:
- Support multi-column primary keys via `id_mapping` configuration
- Handles complex data relationships

### Configuration Structure

```yaml
jobs:
  job_name:
    target_table: "table_name"
    id_mapping:                    # Compound primary key
      csv_col1: db_col1
      csv_col2: db_col2
    filename_to_column:            # Extract from filename
      template: "data_[date]_[version].csv"
      columns:
        date:
          db_column: sync_date
          type: date
          use_to_delete_old_rows: true
    columns:                       # Column mappings
      csv_col: db_col
```

### Testing Strategy

- **Unit Tests**: CLI, config parsing, type detection
- **Integration Tests**: Real PostgreSQL via testcontainers
- **Real Database Testing**: Uses Docker containers for authentic PostgreSQL testing
- **High Coverage**: Aim for >90% coverage
- **Test Markers**: `integration` marker for tests requiring Docker

### Dependencies

- **Click**: CLI framework
- **Rich**: Terminal output formatting
- **PyYAML**: Configuration parsing
- **psycopg**: PostgreSQL adapter
- **cdflib**: CDF file reading
- **testcontainers**: Integration testing with real databases