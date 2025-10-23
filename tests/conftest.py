"""Pytest configuration and shared fixtures."""

import platform

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


def should_skip_postgres_tests():
    """Check if PostgreSQL tests should be skipped.

    Testcontainers has issues on Windows/macOS with Docker socket mounting.
    Only run PostgreSQL tests on Linux (locally or in CI).
    """
    system = platform.system()

    # Skip on Windows and macOS - testcontainers doesn't work reliably
    if system in ("Windows", "Darwin"):
        return True, f"PostgreSQL tests not supported on {system} (testcontainers limitation)"

    # On Linux, check if Docker is available
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return False, None
    except Exception as e:
        return True, f"Docker is not available: {e}"


@pytest.fixture(params=["sqlite", "postgres"])
def db_url(request, tmp_path):
    """Provide database connection URL for both SQLite and PostgreSQL."""
    if request.param == "sqlite":
        # SQLite: use file-based database
        db_file = tmp_path / "test.db"
        return f"sqlite:///{db_file}"
    else:
        # PostgreSQL: use testcontainers
        skip, reason = should_skip_postgres_tests()
        if skip:
            pytest.skip(reason)

        from testcontainers.postgres import PostgresContainer

        # Create container for this test
        container = PostgresContainer("postgres:16-alpine")
        container.start()

        # Store container in request so we can clean it up
        request.addfinalizer(container.stop)

        return container.get_connection_url(driver=None)
