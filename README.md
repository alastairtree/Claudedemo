# data-sync

Sync CSV and CDF science files into PostgreSQL database using configuration-based jobs. A robust CLI application built with Python 3.11+, demonstrating best practices for maintainable Python software.

[![CI](https://github.com/yourusername/data-sync/workflows/CI/badge.svg)](https://github.com/yourusername/data-sync/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Features

- **Configuration-Based**: Define sync jobs in YAML with column mappings
- **Column Mapping**: Rename columns between CSV and database
- **Selective Sync**: Choose which columns to sync or sync all
- **Idempotent Operations**: Safe to run multiple times, uses upsert
- **Modern Python**: Built for Python 3.11+ with full type hints
- **Robust Testing**: Comprehensive test suite with pytest (15 unit tests + 4 integration tests)
- **Real Database Testing**: Integration tests with PostgreSQL via testcontainers
- **CLI Interface**: User-friendly command-line interface using Click
- **Rich Output**: Beautiful terminal output with Rich library
- **Code Quality**: Automated linting with Ruff and type checking with MyPy
- **CI/CD**: GitHub Actions workflow for automated testing and builds
- **Cross-Platform**: Tested on Linux, Windows, and macOS

## Installation

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- PostgreSQL database
- Docker (for running integration tests)

### Install from source

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

2. **Prepare your CSV file** (`users.csv`):

```csv
user_id,name,email,notes
1,Alice,alice@example.com,Admin user
2,Bob,bob@example.com,Regular user
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

### Command Line Interface

```bash
# Basic usage
data-sync sync <csv_file> <config_file> <job_name> --db-url <connection_string>

# Using environment variable for database
export DATABASE_URL="postgresql://localhost/mydb"
data-sync sync data.csv config.yaml my_job

# Show help
data-sync --help
data-sync sync --help

# Show version
data-sync --version
```

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
│   ├── test_config.py                # Config parser tests (7 tests)
│   └── test_database_integration.py  # Integration tests (4 tests, requires Docker)
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
- 15 unit tests (CLI, config parsing)
- 4 integration tests (real PostgreSQL via testcontainers)
- 100% passing
- Tests verify idempotency, column mapping, and error handling

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
- [ ] Support for CDF science files
- [ ] Data validation and transformation
- [ ] Support for batch processing multiple files
- [ ] Add progress bars for large files
- [ ] Transaction management and rollback
- [ ] Schema migration support
- [ ] Dry-run mode

## Support

If you have any questions or run into issues, please [open an issue](https://github.com/yourusername/data-sync/issues).
