# Welcome to data-sync

Sync CSV and CDF science files into PostgreSQL database using configuration-based jobs. A robust CLI application built with Python 3.11+, demonstrating best practices for maintainable Python software.

[![CI](https://github.com/yourusername/data-sync/workflows/CI/badge.svg)](https://github.com/yourusername/data-sync/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Overview

**data-sync** is a powerful command-line tool and Python library for syncing CSV and CDF files to PostgreSQL databases. It provides a declarative, configuration-based approach to data synchronization with advanced features for production use.

## Key Features

- **Dual Interface**: Use as a CLI tool or import as a Python library
- **Configuration-Based**: Define sync jobs in YAML with column mappings
- **Column Mapping**: Rename columns between CSV and database
- **Selective Sync**: Choose which columns to sync or sync all
- **Filename-Based Extraction**: Extract values from filenames (dates, versions, etc.) and store in database columns
- **Automatic Cleanup**: Delete stale records based on extracted filename values after sync
- **Compound Primary Keys**: Support for multi-column primary keys
- **Database Indexes**: Define indexes with custom sort orders
- **Automatic Index Suggestions**: Prepare command suggests indexes based on column types
- **Dry-Run Mode**: Preview all changes without modifying the database
- **Idempotent Operations**: Safe to run multiple times, uses upsert
- **Multi-Database Support**: Works with PostgreSQL and SQLite
- **Modern Python**: Built for Python 3.11+ with full type hints
- **Rich Output**: Beautiful terminal output with Rich library

## Quick Example

```bash
# Create a configuration file
data-sync prepare users.csv config.yaml users_sync

# Sync the file to database
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync users.csv config.yaml users_sync

# Or preview changes first
data-sync sync users.csv config.yaml users_sync --dry-run
```

## Use Cases

- **Daily Data Updates**: Sync daily CSV exports with automatic date extraction and cleanup
- **Science Data Processing**: Process CDF science files with metadata extraction
- **Data Warehousing**: Load CSV data into PostgreSQL with column transformations
- **Incremental Updates**: Replace partitioned data (by date, version, etc.) while preserving other partitions
- **Configuration-Driven ETL**: Define data pipelines in YAML without writing code

## Next Steps

- [Installation Guide](installation.md) - Install data-sync
- [Quick Start](quick-start.md) - Get started in 5 minutes
- [Configuration](configuration.md) - Learn about YAML configuration
- [CLI Reference](cli-reference.md) - Command-line interface documentation
- [Features](features.md) - Detailed feature documentation
- [API Reference](api-reference.md) - Use data-sync as a Python library

## Support

If you have any questions or run into issues, please [open an issue](https://github.com/yourusername/data-sync/issues) on GitHub.

## License

This project is licensed under the MIT License - see the [License](license.md) page for details.
