# data-sync

WARNING: This is a demo and all code is entirely untested. Use at your own risk!

Examines and syncs CSV and CDF science files into PostgreSQL or SQLite databases in batched files using easy to edit configuration files.

[![CI](https://github.com/alastairtree/clauddemo/workflows/CI/badge.svg)](https://github.com/alastairtree/clauddemo/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Overview

**data-sync** is a command-line tool and Python library for easy syncing CSV and CDF (Common Data Format) files to a PostgreSQL database. It provides a declarative, configuration-based approach to data synchronization with some additional features that make it very fast to get up and running syncing big complex data files into a db quickly.

## Quick Start

### CSV Files

```bash
# Install
uv install data-sync

# or pip
pip install data-sync

# Create configuration by analyzing your CSV
data-sync prepare users.csv --config config.yaml

# Preview changes (dry-run)
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync users.csv config.yaml users_sync --dry-run

# Sync to database
data-sync sync users.csv config.yaml users_sync
```

### CDF (Science Data) Files

```bash
# Inspect CDF file contents
data-sync inspect data.cdf --max-records 10

# Extract CDF to CSV (optional, for preview)
data-sync extract data.cdf --output-path ./output --max-records 100

# Create configuration from CDF file
data-sync prepare data.cdf --config config.yaml

# Sync CDF directly to database (automatic extraction)
data-sync sync data.cdf config.yaml vectors --db-url postgresql://localhost/mydb
```

## Key Features

- **CSV & CDF Support**: Work with both CSV files and NASA CDF (Common Data Format) science data files
- **Direct CDF Sync**: Sync CDF files directly to database without manual extraction
- **Configuration-Based**: Define sync jobs in YAML
- **Column Mapping**: Rename columns between CSV and database
- **Filename Extraction**: Extract values from filenames (dates, versions, etc.)
- **Automatic Cleanup**: Delete stale records based on extracted values
- **Compound Primary Keys**: Support for multi-column primary keys
- **Dry-Run Mode**: Preview changes without modifying database
- **Record Limiting**: Limit extraction with `--max-records` for testing and quick syncs
- **Idempotent**: Safe to run multiple times
- **Type Hints**: Full type hints for IDE support
- **Well Tested**: Comprehensive test suite with real database tests

## Example Configuration

```yaml
jobs:
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
```

This configuration:
- Syncs `sales_YYYY-MM-DD.csv` files to the `sales` table
- Extracts the date from filename and stores it in `sync_date` column
- Automatically deletes stale records for the same date after sync
- Maps CSV columns to database columns

## Documentation

📚 **[Read the full documentation](https://yourusername.github.io/data-sync)**

- [Installation Guide](https://yourusername.github.io/data-sync/installation/) - Install data-sync
- [Quick Start](https://yourusername.github.io/data-sync/quick-start/) - Get started in 5 minutes
- [Configuration](https://yourusername.github.io/data-sync/configuration/) - YAML configuration reference
- [CLI Reference](https://yourusername.github.io/data-sync/cli-reference/) - Command-line documentation
- [Features](https://yourusername.github.io/data-sync/features/) - Detailed feature documentation
- [API Reference](https://yourusername.github.io/data-sync/api-reference/) - Python API documentation
- [Development](https://yourusername.github.io/data-sync/development/) - Contributing guide

## Use Cases

- **Daily Data Updates**: Sync daily CSV exports with automatic date extraction and cleanup
- **Science Data Processing**: Process NASA CDF (Common Data Format) science files directly to database
- **Mission Data Pipelines**: Automated syncing of spacecraft telemetry and instrument data from CDF files
- **Data Warehousing**: Load CSV data into PostgreSQL with column transformations
- **Incremental Updates**: Replace partitioned data (by date, version, etc.) while preserving other partitions
- **Testing & Development**: Use `--max-records` to quickly test with subset of data

## Installation

```bash
# Using pip
pip install data-sync

# Using uv
uv pip install data-sync
```

Requires Python 3.11+ and PostgreSQL (or SQLite for testing).

## CLI Usage

```bash
# Analyze CSV and generate configuration
data-sync prepare data.csv config.yaml my_job

# Sync with database
export DATABASE_URL="postgresql://user:pass@localhost/mydb"
data-sync sync data.csv config.yaml my_job

# Preview changes without modifying database
data-sync sync data.csv config.yaml my_job --dry-run
```

## Programmatic Usage

```python
from pathlib import Path
from data_sync import sync_csv_to_postgres, SyncConfig

# Load configuration
config = SyncConfig.from_yaml(Path("config.yaml"))
job = config.get_job("my_job")

# Sync CSV to database
rows_synced = sync_csv_to_postgres(
    file_path=Path("data.csv"),
    sync_job=job,
    db_url="postgresql://localhost/mydb"
)
print(f"Synced {rows_synced} rows")
```

## Development

```bash
# Clone repository
git clone https://github.com/yourusername/data-sync.git
cd data-sync

# Install with development dependencies
uv sync --all-extras

# Run tests
uv run pytest -v

# Generate documentation locally
./generate-docs.sh
```

See the [Development Guide](https://yourusername.github.io/data-sync/development/) for detailed instructions.

## Contributing

Contributions are welcome! Please see the [Contributing Guide](https://yourusername.github.io/data-sync/contributing/) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://yourusername.github.io/data-sync)
- 🐛 [Issue Tracker](https://github.com/yourusername/data-sync/issues)
- 💬 [Discussions](https://github.com/yourusername/data-sync/discussions)

## Acknowledgments

Built with [Click](https://click.palletsprojects.com/), [Rich](https://rich.readthedocs.io/), [psycopg3](https://www.psycopg.org/psycopg3/), and [pytest](https://pytest.org/).
