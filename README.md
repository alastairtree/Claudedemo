# MyApp

A robust CLI application built with Python 3.13, demonstrating best practices for maintainable Python software.

[![CI](https://github.com/yourusername/myapp/workflows/CI/badge.svg)](https://github.com/yourusername/myapp/actions)
[![Python Version](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

## Features

- **Modern Python**: Built for Python 3.13 with full type hints
- **Robust Testing**: Comprehensive test suite with pytest
- **CLI Interface**: User-friendly command-line interface using Click
- **Rich Output**: Beautiful terminal output with Rich library
- **Code Quality**: Automated linting with Ruff and type checking with MyPy
- **CI/CD**: GitHub Actions workflow for automated testing and builds
- **Well Documented**: Clear documentation and examples

## Installation

### Prerequisites

- Python 3.13 or higher
- pip

### Install from source

```bash
# Clone the repository
git clone https://github.com/yourusername/myapp.git
cd myapp

# Install in development mode
pip install -e ".[dev]"
```

## Usage

### Basic Commands

```bash
# Display help
myapp --help

# Show version
myapp --version

# Greet command
myapp greet "World"
# Output: Hello, World!

# Greet with uppercase
myapp greet "World" --uppercase
# Output: HELLO, WORLD!

# Greet with repeat
myapp greet "Python" --repeat 3
# Output: Hello, Python! Hello, Python! Hello, Python!

# Display application info
myapp info

# Display info as JSON
myapp info --format json
```

### Examples

```bash
# Combine options
myapp greet "CLI" -u -r 2

# Get application information
myapp info -f json
```

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
pytest --cov=myapp

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
mypy src/myapp
```

### Project Structure

```
myapp/
├── .github/
│   └── workflows/
│       └── ci.yml          # GitHub Actions CI/CD
├── src/
│   └── myapp/
│       ├── __init__.py     # Package initialization
│       ├── cli.py          # CLI commands
│       └── core.py         # Core business logic
├── tests/
│   ├── __init__.py
│   ├── conftest.py         # Pytest configuration and fixtures
│   ├── test_cli.py         # CLI tests
│   └── test_core.py        # Core logic tests
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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Click](https://click.palletsprojects.com/) - Command line interface creation
- [Rich](https://rich.readthedocs.io/) - Beautiful terminal output
- [pytest](https://pytest.org/) - Testing framework
- [Ruff](https://github.com/astral-sh/ruff) - Fast Python linter

## Roadmap

- [ ] Add configuration file support
- [ ] Implement YAML output format
- [ ] Add more CLI commands
- [ ] Create comprehensive documentation site
- [ ] Add integration tests
- [ ] Implement plugin system

## Support

If you have any questions or run into issues, please [open an issue](https://github.com/yourusername/myapp/issues).
