"""Pytest configuration and shared fixtures."""

import csv
import platform
from pathlib import Path

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


def create_csv_file(file_path: Path, fieldnames: list[str], rows: list[dict]) -> Path:
    """Create a CSV file with the given fieldnames and rows.

    Args:
        file_path: Path where the CSV file should be created
        fieldnames: List of column names
        rows: List of dictionaries representing rows

    Returns:
        Path to the created CSV file
    """
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return file_path


def create_config_file(
    file_path: Path,
    job_name: str,
    target_table: str,
    id_mapping: dict[str, str],
    columns: dict[str, str] | None = None,
    date_mapping: dict[str, str] | None = None,
) -> Path:
    """Create a YAML config file with the specified job configuration.

    Args:
        file_path: Path where the config file should be created
        job_name: Name of the job
        target_table: Target database table name
        id_mapping: Dictionary mapping CSV columns to database ID columns
        columns: Optional dictionary mapping CSV columns to database columns
        date_mapping: Optional date mapping configuration

    Returns:
        Path to the created config file
    """
    config_lines = ["jobs:", f"  {job_name}:", f"    target_table: {target_table}", "    id_mapping:"]

    for csv_col, db_col in id_mapping.items():
        config_lines.append(f"      {csv_col}: {db_col}")

    if columns:
        config_lines.append("    columns:")
        for csv_col, db_col in columns.items():
            config_lines.append(f"      {csv_col}: {db_col}")

    if date_mapping:
        config_lines.append("    date_mapping:")
        for key, value in date_mapping.items():
            config_lines.append(f"      {key}: '{value}'")

    file_path.write_text("\n".join(config_lines) + "\n")
    return file_path


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
