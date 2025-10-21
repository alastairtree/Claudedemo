"""Pytest configuration and shared fixtures."""

import pytest
from click.testing import CliRunner


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide a Click CLI test runner.

    Returns:
        CliRunner instance for testing CLI commands
    """
    return CliRunner()


@pytest.fixture
def sample_text() -> str:
    """Provide sample text for testing.

    Returns:
        A sample text string
    """
    return "test input"
