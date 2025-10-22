"""Integration tests for database synchronization with real Postgres."""

import csv
import platform
from pathlib import Path

import psycopg
import pytest
from testcontainers.postgres import PostgresContainer

from data_sync.config import SyncConfig
from data_sync.database import DatabaseConnection, sync_csv_to_postgres


def _should_skip_integration_tests():
    """Check if integration tests should be skipped.

    Testcontainers has issues on Windows/macOS with Docker socket mounting.
    Only run integration tests on Linux (locally or in CI).
    """
    system = platform.system()

    # Skip on Windows and macOS - testcontainers doesn't work reliably
    if system in ("Windows", "Darwin"):
        return True, f"Integration tests not supported on {system} (testcontainers limitation)"

    # On Linux, check if Docker is available
    try:
        import docker

        client = docker.from_env()
        client.ping()
        return False, None
    except Exception as e:
        return True, f"Docker is not available: {e}"


@pytest.fixture(scope="module")
def postgres_container():
    """Provide a PostgreSQL test container."""
    should_skip, reason = _should_skip_integration_tests()

    if should_skip:
        pytest.skip(reason)

    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def db_url(postgres_container):
    """Provide database connection URL."""
    # Get URL with driver=None to get standard postgresql:// format
    # psycopg3 expects postgresql:// not psycopg2://
    return postgres_container.get_connection_url(driver=None)


class TestDatabaseIntegration:
    """Integration tests with real PostgreSQL database."""

    def test_sync_csv_with_column_mapping_and_exclusion(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing CSV with renamed column and excluded column.

        This test verifies:
        1. CSV with 3 columns: id, name (renamed to full_name), email (not synced)
        2. Idempotency: running twice doesn't duplicate or change data
        """
        # Create CSV file with 3 columns
        csv_file = tmp_path / "users.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["user_id", "name", "email"])
            writer.writeheader()
            writer.writerow({"user_id": "1", "name": "Alice", "email": "alice@example.com"})
            writer.writerow({"user_id": "2", "name": "Bob", "email": "bob@example.com"})
            writer.writerow({"user_id": "3", "name": "Charlie", "email": "charlie@example.com"})

        # Create config file
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  sync_users:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name: full_name
""")

        # Load config and get job
        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("sync_users")
        assert job is not None

        # First sync
        rows_synced = sync_csv_to_postgres(csv_file, job, db_url)
        assert rows_synced == 3

        # Verify data in database
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            # Check table exists and has correct columns
            cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'users'
                    ORDER BY column_name
                """)
            columns = [row[0] for row in cur.fetchall()]
            assert "id" in columns
            assert "full_name" in columns
            assert "email" not in columns  # This column should NOT be synced

            # Check data
            cur.execute("SELECT id, full_name FROM users ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 3
            assert rows[0] == ("1", "Alice")
            assert rows[1] == ("2", "Bob")
            assert rows[2] == ("3", "Charlie")

        # Second sync (idempotency test)
        rows_synced_2 = sync_csv_to_postgres(csv_file, job, db_url)
        assert rows_synced_2 == 3

        # Verify data hasn't changed
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT id, full_name FROM users ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 3  # Still 3 rows, no duplicates
            assert rows[0] == ("1", "Alice")
            assert rows[1] == ("2", "Bob")
            assert rows[2] == ("3", "Charlie")

    def test_sync_all_columns(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing all columns when no specific columns are listed."""
        csv_file = tmp_path / "products.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["product_id", "name", "price", "category"])
            writer.writeheader()
            writer.writerow(
                {"product_id": "P1", "name": "Widget", "price": "9.99", "category": "Tools"}
            )
            writer.writerow(
                {"product_id": "P2", "name": "Gadget", "price": "19.99", "category": "Electronics"}
            )

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  sync_products:
    target_table: products
    id_mapping:
      product_id: id
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("sync_products")
        assert job is not None

        rows_synced = sync_csv_to_postgres(csv_file, job, db_url)
        assert rows_synced == 2

        # Verify all columns were synced
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'products'
                    ORDER BY column_name
                """)
            columns = [row[0] for row in cur.fetchall()]
            assert "id" in columns
            assert "name" in columns
            assert "price" in columns
            assert "category" in columns

            cur.execute("SELECT id, name, price, category FROM products ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("P1", "Widget", "9.99", "Tools")
            assert rows[1] == ("P2", "Gadget", "19.99", "Electronics")

    def test_upsert_updates_existing_rows(self, tmp_path: Path, db_url: str) -> None:
        """Test that upserting updates existing rows instead of creating duplicates."""
        csv_file = tmp_path / "data.csv"

        # First version of data
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "original"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_upsert:
    target_table: test_data
    id_mapping:
      id: id
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_upsert")

        # First sync
        sync_csv_to_postgres(csv_file, job, db_url)

        # Update the CSV with new value for same ID
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "updated"})

        # Second sync should update the row
        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify only one row exists with updated value
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM test_data")
            count = cur.fetchone()[0]
            assert count == 1  # Still only one row

            cur.execute("SELECT id, value FROM test_data")
            row = cur.fetchone()
            assert row == ("1", "updated")  # Value was updated

    def test_missing_csv_column_error(self, tmp_path: Path, db_url: str) -> None:
        """Test error when CSV is missing a required column."""
        csv_file = tmp_path / "incomplete.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id"])
            writer.writeheader()
            writer.writerow({"id": "1"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  bad_job:
    target_table: test
    id_mapping:
      id: id
    columns:
      missing_column: value
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("bad_job")

        with DatabaseConnection(db_url) as db, pytest.raises(ValueError, match="not found in CSV"):
            db.sync_csv_file(csv_file, job)
