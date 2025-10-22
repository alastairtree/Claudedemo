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

    def test_sync_with_date_mapping(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing with date_mapping stores date in column."""
        # Create CSV file
        csv_file = tmp_path / "sales_2024-01-15.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["sale_id", "amount"])
            writer.writeheader()
            writer.writerow({"sale_id": "1", "amount": "100"})
            writer.writerow({"sale_id": "2", "amount": "200"})

        # Create config with date_mapping
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_sales:
    target_table: sales
    id_mapping:
      sale_id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'
      db_column: sync_date
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_sales")
        assert job is not None
        assert job.date_mapping is not None

        # Extract date and sync
        sync_date = job.date_mapping.extract_date_from_filename(csv_file)
        assert sync_date == "2024-01-15"

        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, sync_date)
        assert rows_synced == 2

        # Verify date column was created and populated
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'sales'
                ORDER BY column_name
            """)
            columns = [row[0] for row in cur.fetchall()]
            assert "sync_date" in columns

            cur.execute("SELECT id, amount, sync_date FROM sales ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("1", "100", "2024-01-15")
            assert rows[1] == ("2", "200", "2024-01-15")

    def test_delete_stale_records(self, tmp_path: Path, db_url: str) -> None:
        """Test that stale records are deleted after sync."""
        # First sync with 3 records for date 2024-01-15
        csv_file = tmp_path / "data_2024-01-15.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "A"})
            writer.writerow({"id": "2", "value": "B"})
            writer.writerow({"id": "3", "value": "C"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_data:
    target_table: data
    id_mapping:
      id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'
      db_column: sync_date
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_data")

        sync_date = job.date_mapping.extract_date_from_filename(csv_file)
        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, sync_date)
        assert rows_synced == 3

        # Verify 3 records exist
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM data WHERE sync_date = '2024-01-15'")
            count = cur.fetchone()[0]
            assert count == 3

        # Second sync with only 2 records (ID 3 removed)
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "A_updated"})
            writer.writerow({"id": "2", "value": "B_updated"})

        rows_synced_2 = sync_csv_to_postgres(csv_file, job, db_url, sync_date)
        assert rows_synced_2 == 2

        # Verify only 2 records remain for this date
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT id, value FROM data WHERE sync_date = '2024-01-15' ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("1", "A_updated")
            assert rows[1] == ("2", "B_updated")

    def test_delete_stale_records_preserves_other_dates(self, tmp_path: Path, db_url: str) -> None:
        """Test that deleting stale records only affects matching date."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_data:
    target_table: multi_date_data
    id_mapping:
      id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'
      db_column: sync_date
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_data")

        # Sync data for 2024-01-15
        csv_file_1 = tmp_path / "data_2024-01-15.csv"
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day1"})
            writer.writerow({"id": "2", "value": "Day1"})

        sync_date_1 = job.date_mapping.extract_date_from_filename(csv_file_1)
        sync_csv_to_postgres(csv_file_1, job, db_url, sync_date_1)

        # Sync data for 2024-01-16
        csv_file_2 = tmp_path / "data_2024-01-16.csv"
        with open(csv_file_2, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day2"})
            writer.writerow({"id": "2", "value": "Day2"})
            writer.writerow({"id": "3", "value": "Day2"})

        sync_date_2 = job.date_mapping.extract_date_from_filename(csv_file_2)
        sync_csv_to_postgres(csv_file_2, job, db_url, sync_date_2)

        # Verify total records
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM multi_date_data")
            total_count = cur.fetchone()[0]
            assert total_count == 5  # 2 from day 1 + 3 from day 2

        # Re-sync day 1 with only ID 1 (removing ID 2)
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day1_updated"})

        sync_csv_to_postgres(csv_file_1, job, db_url, sync_date_1)

        # Verify: Day 1 should have 1 record, Day 2 should still have 3
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM multi_date_data WHERE sync_date = '2024-01-15'")
            day1_count = cur.fetchone()[0]
            assert day1_count == 1

            cur.execute("SELECT COUNT(*) FROM multi_date_data WHERE sync_date = '2024-01-16'")
            day2_count = cur.fetchone()[0]
            assert day2_count == 3  # Day 2 data unchanged
