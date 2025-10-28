# Welcome to data-sync

WARNING: This is a demo and all code is entirely untested. Use at your own risk! 

Examines and syncs CSV and CDF science files into PostgreSQL or SQLite databases in batched files using easy to edit configuration files.

[![CI](https://github.com/yourusername/data-sync/workflows/CI/badge.svg)](https://github.com/yourusername/data-sync/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Overview

**data-sync** is a command-line tool and Python library for easy syncing CSV and CDF files to a PostgreSQL database. It provides a declarative, configuration-based approach to data synchronization with some additional features that make it very fast to get up and running syncing big complex data files into a db quickly.

## Key Features

- **Configuration-Based**: Examines your CSV files with the prepare command, and defines sync jobs in YAML with sensible column mappings
- **Creates missing database schema**: Automatically creates target tables if they don't exist, and updates schema as needed. Never deletes columns.
- **Column Mapping**: Sync all columns, rename them or only sync a subset
- **Dual Interface**: Use as a CLI tool or import as a Python library
- **Filename-Based Extraction**: Extract values from filenames (dates, versions, etc.) and store in database columns
- **Automatic Cleanup**: Delete stale records based on extracted filename values after sync
- **Compound Primary Keys**: Support for multi-column primary keys
- **Suggests and manages Database Indexes**: Prepare command suggests indexes based on column types and puts them in the config file, you then adjust and deploy to the database with the sync command. Define indexes with custom sort orders
- **Dry-Run Mode**: Preview all changes without modifying the database
- **Idempotent Operations**: Safe to run multiple times, uses upsert
- **Rich Output**: Beautiful terminal output with Rich library

## Quick Example

```bash
# Create a configuration file
data-sync prepare users_2025-01-01_v01.csv config.yaml users_sync

# preview changes first
data-sync sync users_2025-01-01_v01.csv config.yaml users_sync --dry-run

# Sync the file to database
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync users_2025-01-01_v01.csv config.yaml users_sync

# Later that day the v2 of the file arrives
# Sync the new file, old records from v1 are removed automatically, updates are applied to rows that match base on primary key
data-sync sync users_2025-01-01_v02.csv config.yaml users_sync
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

