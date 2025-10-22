"""Integration tests for database synchronization with SQLite and PostgreSQL."""

import csv
import platform
import sqlite3
from pathlib import Path

import pytest

from data_sync.config import SyncConfig
from data_sync.database import DatabaseConnection, sync_csv_to_postgres


def _should_skip_postgres_tests():
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
        should_skip, reason = _should_skip_postgres_tests()
        if should_skip:
            pytest.skip(reason)

        from testcontainers.postgres import PostgresContainer

        # Create container for this test
        container = PostgresContainer("postgres:16-alpine")
        container.start()

        # Store container in request so we can clean it up
        request.addfinalizer(container.stop)

        return container.get_connection_url(driver=None)


def execute_query(db_url: str, query: str, params: tuple = ()) -> list[tuple]:
    """Execute a query and return results for any database type."""
    if db_url.startswith("sqlite"):
        # Extract path from sqlite:///path
        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # Replace %s with ? for SQLite
        sqlite_query = query.replace("%s", "?")
        cursor.execute(sqlite_query, params)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    else:
        import psycopg

        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def get_table_columns(db_url: str, table_name: str) -> list[str]:
    """Get column names from a table for any database type."""
    if db_url.startswith("sqlite"):
        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f'PRAGMA table_info("{table_name}")')
        columns = [row[1] for row in cursor.fetchall()]  # Column name is at index 1
        cursor.close()
        conn.close()
        return sorted(columns)
    else:
        import psycopg

        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY column_name
            """,
                (table_name,),
            )
            return [row[0] for row in cur.fetchall()]


class TestDatabaseIntegration:
    """Integration tests with SQLite and PostgreSQL databases."""

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
        columns = get_table_columns(db_url, "users")
        assert "id" in columns
        assert "full_name" in columns
        assert "email" not in columns  # This column should NOT be synced

        # Check data
        rows = execute_query(db_url, "SELECT id, full_name FROM users ORDER BY id")
        assert len(rows) == 3
        assert rows[0] == ("1", "Alice")
        assert rows[1] == ("2", "Bob")
        assert rows[2] == ("3", "Charlie")

        # Second sync (idempotency test)
        rows_synced_2 = sync_csv_to_postgres(csv_file, job, db_url)
        assert rows_synced_2 == 3

        # Verify data hasn't changed
        rows = execute_query(db_url, "SELECT id, full_name FROM users ORDER BY id")
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
        columns = get_table_columns(db_url, "products")
        assert "id" in columns
        assert "name" in columns
        assert "price" in columns
        assert "category" in columns

        rows = execute_query(db_url, "SELECT id, name, price, category FROM products ORDER BY id")
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
        count_result = execute_query(db_url, "SELECT COUNT(*) FROM test_data")
        assert count_result[0][0] == 1  # Still only one row

        row_result = execute_query(db_url, "SELECT id, value FROM test_data")
        assert row_result[0] == ("1", "updated")  # Value was updated

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
        columns = get_table_columns(db_url, "sales")
        assert "sync_date" in columns

        rows = execute_query(db_url, "SELECT id, amount, sync_date FROM sales ORDER BY id")
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
        count_result = execute_query(
            db_url, "SELECT COUNT(*) FROM data WHERE sync_date = %s", ("2024-01-15",)
        )
        assert count_result[0][0] == 3

        # Second sync with only 2 records (ID 3 removed)
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "A_updated"})
            writer.writerow({"id": "2", "value": "B_updated"})

        rows_synced_2 = sync_csv_to_postgres(csv_file, job, db_url, sync_date)
        assert rows_synced_2 == 2

        # Verify only 2 records remain for this date
        rows = execute_query(
            db_url, "SELECT id, value FROM data WHERE sync_date = %s ORDER BY id", ("2024-01-15",)
        )
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

        # Verify total records (IDs 1,2 were updated to day 2, ID 3 was inserted)
        total_count_result = execute_query(db_url, "SELECT COUNT(*) FROM multi_date_data")
        assert total_count_result[0][0] == 3

        # Re-sync day 1 with only ID 1 (updating it back to day 1)
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day1_updated"})

        sync_csv_to_postgres(csv_file_1, job, db_url, sync_date_1)

        # Verify: ID 1 updated back to day 1, IDs 2 and 3 still on day 2
        day1_count_result = execute_query(
            db_url,
            "SELECT COUNT(*) FROM multi_date_data WHERE sync_date = %s",
            ("2024-01-15",),
        )
        assert day1_count_result[0][0] == 1  # Only ID 1

        day2_count_result = execute_query(
            db_url,
            "SELECT COUNT(*) FROM multi_date_data WHERE sync_date = %s",
            ("2024-01-16",),
        )
        assert day2_count_result[0][0] == 2  # IDs 2 and 3

    def test_sync_with_typed_columns(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing with explicit data types for columns."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  typed_data:
    target_table: products
    id_mapping:
      product_id: id
    columns:
      name: product_name
      price:
        db_column: unit_price
        type: float
      stock:
        db_column: quantity
        type: integer
      description:
        db_column: desc
        type: text
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("typed_data")

        # Create CSV with sample data
        csv_file = tmp_path / "products.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["product_id", "name", "price", "stock", "description"]
            )
            writer.writeheader()
            writer.writerow(
                {
                    "product_id": "1",
                    "name": "Widget",
                    "price": "19.99",
                    "stock": "100",
                    "description": "A useful widget",
                }
            )

        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify data was synced
        rows = execute_query(
            db_url, "SELECT id, product_name, unit_price, quantity FROM products ORDER BY id"
        )
        assert len(rows) == 1
        assert rows[0][0] == "1"
        assert rows[0][1] == "Widget"
        # Note: Values are stored as strings in CSV, databases may convert them

    def test_schema_evolution_add_columns(self, tmp_path: Path, db_url: str) -> None:
        """Test that new columns are automatically added to existing tables."""
        config_file = tmp_path / "config.yaml"

        # Initial sync with 2 columns
        config_file.write_text(r"""
jobs:
  evolving_data:
    target_table: customers
    id_mapping:
      customer_id: id
    columns:
      name: customer_name
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("evolving_data")

        csv_file = tmp_path / "customers.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["customer_id", "name"])
            writer.writeheader()
            writer.writerow({"customer_id": "1", "name": "Alice"})

        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify initial schema
        columns = get_table_columns(db_url, "customers")
        assert "id" in columns
        assert "customer_name" in columns
        assert "email" not in columns  # Not yet added

        # Update config to add new columns
        config_file.write_text(r"""
jobs:
  evolving_data:
    target_table: customers
    id_mapping:
      customer_id: id
    columns:
      name: customer_name
      email:
        db_column: email_address
        type: text
      age:
        db_column: customer_age
        type: integer
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("evolving_data")

        # Sync with new columns
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["customer_id", "name", "email", "age"])
            writer.writeheader()
            writer.writerow(
                {"customer_id": "2", "name": "Bob", "email": "bob@example.com", "age": "30"}
            )

        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify schema now includes new columns
        columns = get_table_columns(db_url, "customers")
        assert "id" in columns
        assert "customer_name" in columns
        assert "email_address" in columns  # New column added
        assert "customer_age" in columns  # New column added

        # Verify both rows exist (old one has NULL for new columns)
        rows = execute_query(db_url, "SELECT id, customer_name FROM customers ORDER BY id")
        assert len(rows) == 2
        assert rows[0] == ("1", "Alice")
        assert rows[1] == ("2", "Bob")
