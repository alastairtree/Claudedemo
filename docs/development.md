# Development Guide

Guide for contributing to data-sync development.

## Setup Development Environment

### Prerequisites

- Python 3.11 or higher
- Docker (for integration tests)
- Git

### Quick Setup

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

For the best development experience:

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop)
2. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
3. Open the project folder in VSCode
4. Click "Reopen in Container" when prompted

The devcontainer includes:

- Python 3.11 with all dependencies pre-installed via uv
- Docker-in-Docker for running integration tests
- VSCode extensions for Python, Ruff, and Docker
- Proper test and linting configuration

## Project Structure

```
data-sync/
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions CI/CD
├── .devcontainer/
│   └── devcontainer.json             # VSCode dev container config
├── docs/                             # MkDocs documentation
│   ├── index.md
│   ├── installation.md
│   └── ...
├── src/
│   └── data_sync/
│       ├── __init__.py               # Package exports
│       ├── cli.py                    # Main CLI entry point
│       ├── cli_sync.py               # Sync command
│       ├── cli_prepare.py            # Prepare command
│       ├── config.py                 # Configuration classes
│       └── database.py               # Database operations
├── tests/
│   ├── __init__.py
│   ├── conftest.py                   # Pytest configuration
│   ├── helpers.py                    # Test helpers
│   ├── test_cli.py                   # CLI tests
│   ├── test_config.py                # Config parser tests
│   ├── test_prepare.py               # Prepare command tests
│   └── test_database_integration.py  # Integration tests (requires Docker)
├── pyproject.toml                    # Project configuration
├── mkdocs.yml                        # Documentation configuration
├── generate-docs.sh                  # Local docs generation script
├── README.md                         # Concise readme with link to docs
└── LICENSE                           # MIT License
```

## Running Tests

### All Tests

```bash
# With uv (recommended)
uv run pytest -v

# OR with direct pytest (if using pip)
pytest -v
```

### Unit Tests Only

Run without Docker:

```bash
uv run pytest tests/test_cli.py tests/test_config.py tests/test_prepare.py -v
```

### Integration Tests Only

Requires Docker to be running:

```bash
uv run pytest tests/test_database_integration.py -v
```

### With Coverage

```bash
uv run pytest --cov=data_sync --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Specific Test

```bash
# Run specific test file
uv run pytest tests/test_config.py -v

# Run specific test class
uv run pytest tests/test_config.py::TestSyncConfig -v

# Run specific test
uv run pytest tests/test_config.py::TestSyncConfig::test_load_from_yaml -v
```

## Code Quality

### Formatting

Format code with Ruff:

```bash
uv run ruff format .
```

### Linting

Check and fix linting issues:

```bash
# Check for issues
uv run ruff check .

# Fix automatically
uv run ruff check --fix .
```

### Type Checking

Run MyPy type checker:

```bash
uv run mypy src/data_sync
```

### Pre-Commit Checks

Run all checks before committing:

```bash
# Format code
uv run ruff format .

# Fix linting issues
uv run ruff check --fix .

# Type check
uv run mypy src/data_sync

# Run tests
uv run pytest
```

## Testing Philosophy

This project prioritizes:

### High Coverage

Aim for >90% code coverage:

```bash
uv run pytest --cov=data_sync --cov-report=term-missing
```

### Meaningful Tests

Test behavior, not implementation:

```python
# Good: Tests behavior
def test_sync_updates_existing_rows(self):
    # First sync
    sync_csv_to_postgres(...)
    # Update CSV
    # Second sync
    # Verify rows are updated, not duplicated

# Avoid: Tests implementation details
def test_sync_calls_execute_with_correct_sql(self):
    # Too tied to implementation
```

### Fast Tests

Keep test suite fast for quick feedback:

- Unit tests: < 5 seconds
- Integration tests: < 30 seconds
- Full suite: < 1 minute

### Real Integration Tests

Use testcontainers for authentic database testing:

```python
import pytest
from testcontainers.postgres import PostgresContainer

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres
```

### Readable Tests

Tests serve as documentation:

```python
def test_filename_to_column_extracts_date_from_filename(self):
    """Test that dates are correctly extracted from filenames."""
    # Arrange: Create config with date pattern
    # Act: Extract values
    # Assert: Date matches expected value
```

## Test Results

Current test suite:

- **30+ tests total**
- Unit tests: CLI, config parsing, filename detection
- Integration tests: Real PostgreSQL via testcontainers
- **100% passing**
- Tests verify: idempotency, column mapping, filename extraction, stale record cleanup, error handling

## Adding New Features

### 1. Write Tests First (TDD)

```python
# tests/test_new_feature.py
def test_new_feature_works(self):
    """Test that new feature does X."""
    # Arrange
    # Act
    # Assert
```

### 2. Implement Feature

```python
# src/data_sync/module.py
def new_feature():
    """Docstring explaining what it does."""
    pass
```

### 3. Update Documentation

- Add to relevant docs page
- Update API reference if needed
- Add examples

### 4. Run Quality Checks

```bash
uv run ruff format .
uv run ruff check --fix .
uv run mypy src/data_sync
uv run pytest
```

## Documentation

### Build Documentation Locally

```bash
# Generate and serve documentation
./generate-docs.sh

# Or manually
uv run mkdocs serve
```

Then open http://127.0.0.1:8000

### Documentation Structure

- `docs/index.md`: Homepage and overview
- `docs/installation.md`: Installation instructions
- `docs/quick-start.md`: Quick start guide
- `docs/configuration.md`: Configuration reference
- `docs/cli-reference.md`: CLI documentation
- `docs/features.md`: Feature documentation
- `docs/api-reference.md`: Python API reference
- `docs/development.md`: This file
- `docs/contributing.md`: Contribution guidelines

### Writing Documentation

Use MkDocs Material features:

**Admonitions**:

```markdown
!!! note
    This is a note

!!! warning
    This is a warning

!!! tip
    This is a tip
```

**Code Tabs**:

```markdown
=== "Python"
    ```python
    print("Hello")
    ```

=== "Bash"
    ```bash
    echo "Hello"
    ```
```

**Tables**:

```markdown
| Column | Type | Description |
|--------|------|-------------|
| name   | str  | User name   |
```

## Git Workflow

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation changes
- `refactor/description` - Code refactoring

### Commit Messages

Follow conventional commits:

```
feat: add support for CDF files
fix: handle empty CSV files correctly
docs: update API reference
refactor: simplify column mapping logic
test: add tests for filename extraction
```

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run quality checks
6. Commit with clear messages
7. Push to your fork
8. Open a Pull Request

## CI/CD Pipeline

GitHub Actions runs on every push:

### Jobs

1. **Test** (Python 3.11, 3.12, 3.13, 3.14)
   - Install dependencies
   - Run linting (ruff)
   - Run type checking (mypy)
   - Run tests with coverage
   - Upload coverage to Codecov

2. **Build**
   - Build wheel
   - Verify installation

3. **Publish** (on release)
   - Publish to PyPI

4. **Deploy Docs** (on main branch)
   - Build documentation
   - Deploy to GitHub Pages

### Running CI Locally

```bash
# Run the same checks as CI
uv run ruff format .
uv run ruff check .
uv run mypy src/data_sync
uv run pytest --cov=data_sync
```

## Release Process

### Version Bumping

Update version in `pyproject.toml`:

```toml
[project]
version = "0.2.0"
```

### Create Release

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Commit changes
4. Create git tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
5. CI automatically publishes to PyPI

## Troubleshooting

### Integration Tests Failing

**Problem**: Tests fail with "Docker daemon not running"

**Solution**: Start Docker Desktop

### Import Errors

**Problem**: `ModuleNotFoundError: No module named 'data_sync'`

**Solution**: Install in editable mode:
```bash
pip install -e .
```

### Type Checking Errors

**Problem**: MyPy reports errors

**Solution**: Ensure all functions have type hints:
```python
def my_function(param: str) -> int:
    return len(param)
```

## Resources

- [Python Type Hints](https://docs.python.org/3/library/typing.html)
- [Pytest Documentation](https://docs.pytest.org/)
- [Ruff Documentation](https://docs.astral.sh/ruff/)
- [MkDocs Material](https://squidfunk.github.io/mkdocs-material/)
- [Testcontainers Python](https://testcontainers-python.readthedocs.io/)

## Next Steps

- [Contributing Guidelines](contributing.md) - How to contribute
- [API Reference](api-reference.md) - Python API documentation
- [Roadmap](roadmap.md) - Future plans
