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


@pytest.fixture(scope="session")
def postgres_container_session():
    """Start a PostgreSQL container once per test session.

    This fixture starts a single PostgreSQL container that is shared across
    all tests in the session, making tests much faster.
    """
    skip, reason = should_skip_postgres_tests()
    if skip:
        pytest.skip(reason)

    from testcontainers.postgres import PostgresContainer

    # Create and start container once for entire test session
    container = PostgresContainer("postgres:16-alpine")
    container.start()

    yield container

    # Stop container at end of session
    container.stop()


@pytest.fixture
def postgres_db_clean(postgres_container_session):
    """Provide a clean PostgreSQL database for each test.

    Uses the session-scoped container but cleans up all tables between tests
    to ensure test isolation.
    """
    import psycopg

    db_url = postgres_container_session.get_connection_url(driver=None)

    # Drop all tables before test runs
    conn = psycopg.connect(db_url)
    try:
        cursor = conn.cursor()

        # Get all table names in public schema
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
        """)
        tables = cursor.fetchall()

        # Drop each table
        for (table_name,) in tables:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

        conn.commit()
    finally:
        conn.close()

    yield db_url


@pytest.fixture(params=["sqlite", "postgres"])
def db_url(request, tmp_path):
    """Provide database connection URL for both SQLite and PostgreSQL.

    This is a parametrized fixture that runs tests with both databases.
    For postgres, it uses the session-scoped container with per-test cleanup.
    """
    if request.param == "sqlite":
        # SQLite: use file-based database
        db_file = tmp_path / "test.db"
        return f"sqlite:///{db_file}"
    else:
        # PostgreSQL: get the session container and clean tables
        skip, reason = should_skip_postgres_tests()
        if skip:
            pytest.skip(reason)

        # Try to get the session-scoped container
        # If it doesn't exist yet, pytest will create it
        try:
            container = request.getfixturevalue("postgres_container_session")
        except Exception:
            # If container setup fails, skip this test
            pytest.skip("PostgreSQL container not available")

        import psycopg

        db_url = container.get_connection_url(driver=None)

        # Drop all tables before test runs
        conn = psycopg.connect(db_url)
        try:
            cursor = conn.cursor()

            # Get all table names in public schema
            cursor.execute("""
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public'
            """)
            tables = cursor.fetchall()

            # Drop each table
            for (table_name,) in tables:
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')

            conn.commit()
        finally:
            conn.close()

        return db_url


@pytest.fixture
def sqlite_db(tmp_path):
    """Provide SQLite database connection URL."""
    db_file = tmp_path / "test.db"
    return f"sqlite:///{db_file}"


@pytest.fixture
def postgres_db(postgres_db_clean):
    """Provide PostgreSQL database connection URL.

    Uses the session-scoped container with per-test table cleanup.
    """
    return postgres_db_clean
