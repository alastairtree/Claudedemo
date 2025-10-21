# Contributing to MyApp

Thank you for your interest in contributing to MyApp! This document provides guidelines and instructions for contributing.

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code. Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid duplicates. When creating a bug report, include:

- A clear and descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Python version and OS
- Any relevant logs or error messages

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

- A clear and descriptive title
- Detailed description of the proposed functionality
- Any potential drawbacks or considerations
- Examples of how the feature would be used

### Pull Requests

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/yourusername/myapp.git
   cd myapp
   ```

2. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Set up development environment**
   ```bash
   pip install -e ".[dev]"
   ```

4. **Make your changes**
   - Write clear, readable code
   - Follow the existing code style
   - Add tests for new functionality
   - Update documentation as needed

5. **Run tests and linting**
   ```bash
   # Format code
   ruff format .

   # Lint code
   ruff check .

   # Type check
   mypy src/myapp

   # Run tests
   pytest
   ```

6. **Commit your changes**
   ```bash
   git add .
   git commit -m "Add feature: brief description"
   ```

7. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Open a Pull Request**
   - Provide a clear description of the changes
   - Reference any related issues
   - Ensure all CI checks pass

## Development Guidelines

### Code Style

- Follow PEP 8 guidelines
- Use Ruff for linting and formatting
- Maximum line length: 100 characters
- Use type hints for all functions
- Write docstrings for public APIs

### Testing

- Write tests for all new features
- Maintain or improve code coverage
- Tests should be fast and independent
- Use descriptive test names
- Follow AAA pattern: Arrange, Act, Assert

Example:
```python
def test_feature_name() -> None:
    """Test description of what is being tested."""
    # Arrange
    input_data = "test"

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected_value
```

### Type Hints

All functions should have type hints:

```python
def process_data(text: str, uppercase: bool = False) -> str:
    """Process the input text.

    Args:
        text: The input text to process
        uppercase: Whether to convert to uppercase

    Returns:
        Processed text string
    """
    return text.upper() if uppercase else text
```

### Documentation

- Update README.md for user-facing changes
- Add docstrings to all public functions and classes
- Include examples in docstrings where helpful
- Keep CHANGELOG.md updated

### Commit Messages

Follow conventional commits format:

- `feat: add new feature`
- `fix: resolve bug in function`
- `docs: update README`
- `test: add tests for feature`
- `refactor: improve code structure`
- `style: format code`
- `chore: update dependencies`

## Project Structure

Understanding the project structure:

```
myapp/
├── src/myapp/        # Main package source code
├── tests/            # Test files
├── .github/          # GitHub Actions and templates
└── pyproject.toml    # Project configuration
```

## Release Process

1. Update version in `pyproject.toml` and `src/myapp/__init__.py`
2. Update CHANGELOG.md
3. Create a git tag
4. Push to GitHub
5. GitHub Actions will build and publish the package

## Getting Help

- Read the README.md
- Check existing issues and pull requests
- Open an issue for questions or problems

## Recognition

Contributors will be recognized in the project README and release notes.

Thank you for contributing to MyApp!
