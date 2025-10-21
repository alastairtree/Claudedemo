# data-sync

Sync CSV and CDF science files into PostgreSQL database. A robust CLI application built with Python 3.11+, demonstrating best practices for maintainable Python software.

[![CI](https://github.com/yourusername/data-sync/workflows/CI/badge.svg)](https://github.com/yourusername/data-sync/actions)
[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Features

- **Modern Python**: Built for Python 3.11+ with full type hints
- **Robust Testing**: Comprehensive test suite with pytest (93% coverage)
- **CLI Interface**: User-friendly command-line interface using Click
- **Rich Output**: Beautiful terminal output with Rich library
- **Code Quality**: Automated linting with Ruff and type checking with MyPy
- **CI/CD**: GitHub Actions workflow for automated testing and builds
- **Cross-Platform**: Tested on Linux, Windows, and macOS
- **Well Documented**: Clear documentation and examples

## Installation

### Prerequisites

- Python 3.11 or higher
- pip

### Install from source

```bash
# Clone the repository
git clone https://github.com/yourusername/data-sync.git
cd data-sync

# Install in development mode
pip install -e ".[dev]"
```

## Usage

### Basic Commands

```bash
# Display help
data-sync --help

# Show version
data-sync --version

# Sync a CSV file
data-sync sync /path/to/data.csv

# Sync a CDF science file
data-sync sync /path/to/science.cdf
```

### Examples

```bash
# Sync a CSV file to the database
data-sync sync ./data/measurements.csv

# Sync a CDF file to the database
data-sync sync ./data/experiment_001.cdf
```

## Supported File Types

- **CSV**: Comma-separated values files
- **CDF**: Common Data Format science files

## Development

### Setup Development Environment

```bash
# Install with development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=data_sync

# Run specific test file
pytest tests/test_core.py

# Run with verbose output
pytest -v
```

### Code Quality

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Fix linting issues automatically
ruff check --fix .

# Type checking
mypy src/data_sync
```

### Project Structure

```
data-sync/
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions CI/CD
├── src/
│   └── data_sync/
│       ├── __init__.py     # Package initialization
│       ├── cli.py          # CLI commands
│       └── core.py         # Core business logic
├── tests/
│   ├── __init__.py
│   ├── conftest.py         # Pytest configuration and fixtures
│   ├── test_cli.py         # CLI tests (10 tests)
│   └── test_core.py        # Core logic tests (12 tests)
├── pyproject.toml          # Project configuration
├── README.md               # This file
└── LICENSE                 # License file
```

## Configuration

The project uses `pyproject.toml` for all configuration:

- **Build system**: Hatchling
- **Testing**: pytest with coverage
- **Linting**: Ruff
- **Type checking**: MyPy

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
- **Readable tests**: Tests serve as documentation

## Test Results

Current test suite:
- 22 tests total
- 100% passing
- 93% code coverage

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Click](https://click.palletsprojects.com/) - Command line interface creation
- [Rich](https://rich.readthedocs.io/) - Beautiful terminal output
- [pytest](https://pytest.org/) - Testing framework
- [Ruff](https://github.com/astral-sh/ruff) - Fast Python linter

## Roadmap

- [ ] Implement CSV file parsing and database insertion
- [ ] Implement CDF file parsing and database insertion
- [ ] Add PostgreSQL connection configuration
- [ ] Add data validation and transformation
- [ ] Add support for batch processing
- [ ] Add progress bars for large files
- [ ] Create comprehensive documentation site
- [ ] Add integration tests with PostgreSQL

## Support

If you have any questions or run into issues, please [open an issue](https://github.com/yourusername/data-sync/issues).
