"""Integration tests for database synchronization with SQLite and PostgreSQL."""

import csv
import sqlite3
from pathlib import Path

import pytest

from data_sync.config import SyncConfig
from data_sync.database import DatabaseConnection, sync_csv_to_postgres

from .db_test_utils import execute_query, get_table_columns


def get_table_indexes(db_url: str, table_name: str) -> set[str]:
    """Get index names from a table for any database type."""
    if db_url.startswith("sqlite"):
        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?", (table_name,)
        )
        indexes = {row[0].lower() for row in cursor.fetchall()}
        cursor.close()
        conn.close()
        return indexes
    else:
        import psycopg

        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname
                FROM pg_indexes
                WHERE tablename = %s
            """,
                (table_name,),
            )
            return {row[0].lower() for row in cur.fetchall()}


class TestDatabaseIntegration:
    """Integration tests with SQLite and PostgreSQL databases."""

    def test_sync_csv_with_column_mapping_and_exclusion(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing CSV with renamed column and excluded column.

        This test verifies:
        1. CSV with 3 columns: id, name (renamed to full_name), email (not synced)
        2. Idempotency: running twice doesn't duplicate or change data
        """
        from conftest import create_config_file, create_csv_file

        # Create CSV file with 3 columns
        csv_file = tmp_path / "users.csv"
        create_csv_file(
            csv_file,
            ["user_id", "name", "email"],
            [
                {"user_id": "1", "name": "Alice", "email": "alice@example.com"},
                {"user_id": "2", "name": "Bob", "email": "bob@example.com"},
                {"user_id": "3", "name": "Charlie", "email": "charlie@example.com"},
            ],
        )

        # Create config file
        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "sync_users", "users", {"user_id": "id"}, {"name": "full_name"})

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

    @pytest.mark.parametrize("db_url", ["sqlite", "postgres"], indirect=True)
    def test_compound_primary_key(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing with compound primary key."""
        from conftest import create_csv_file
        from data_sync.config import ColumnMapping, SyncJob

        # Create CSV with data
        csv_file = tmp_path / "sales.csv"
        create_csv_file(
            csv_file,
            ["store_id", "product_id", "quantity", "price"],
            [
                {"store_id": "1", "product_id": "A", "quantity": "10", "price": "9.99"},
                {"store_id": "1", "product_id": "B", "quantity": "5", "price": "19.99"},
                {"store_id": "2", "product_id": "A", "quantity": "8", "price": "9.99"},
            ],
        )

        # Create job with compound primary key
        job = SyncJob(
            name="sales",
            target_table="sales",
            id_mapping=[
                ColumnMapping("store_id", "store_id"),
                ColumnMapping("product_id", "product_id"),
            ],
            columns=[
                ColumnMapping("quantity", "qty"),
                ColumnMapping("price", "price"),
            ],
        )

        # Sync data
        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify data
        rows = execute_query(
            db_url, "SELECT store_id, product_id, qty FROM sales ORDER BY store_id, product_id"
        )
        assert len(rows) == 3
        assert rows[0] == ("1", "A", "10")
        assert rows[1] == ("1", "B", "5")
        assert rows[2] == ("2", "A", "8")

        # Update existing row and add new row
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["store_id", "product_id", "quantity", "price"])
            writer.writeheader()
            writer.writerow(
                {"store_id": "1", "product_id": "A", "quantity": "15", "price": "9.99"}
            )  # Updated
            writer.writerow({"store_id": "1", "product_id": "B", "quantity": "5", "price": "19.99"})
            writer.writerow({"store_id": "2", "product_id": "A", "quantity": "8", "price": "9.99"})
            writer.writerow(
                {"store_id": "2", "product_id": "B", "quantity": "3", "price": "19.99"}
            )  # New

        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify update and insert
        rows = execute_query(
            db_url, "SELECT store_id, product_id, qty FROM sales ORDER BY store_id, product_id"
        )
        assert len(rows) == 4
        assert rows[0] == ("1", "A", "15")  # Updated quantity
        assert rows[3] == ("2", "B", "3")  # New row

    @pytest.mark.parametrize("db_url", ["sqlite", "postgres"], indirect=True)
    def test_single_column_index(self, tmp_path: Path, db_url: str) -> None:
        """Test creating single-column index."""
        from conftest import create_csv_file
        from data_sync.config import ColumnMapping, Index, IndexColumn, SyncJob

        # Create CSV
        csv_file = tmp_path / "users.csv"
        create_csv_file(
            csv_file,
            ["user_id", "email", "name"],
            [{"user_id": "1", "email": "alice@example.com", "name": "Alice"}],
        )

        # Create job with index
        job = SyncJob(
            name="users",
            target_table="users",
            id_mapping=[ColumnMapping("user_id", "id")],
            columns=[
                ColumnMapping("email", "email"),
                ColumnMapping("name", "name"),
            ],
            indexes=[Index(name="idx_email", columns=[IndexColumn("email", "ASC")])],
        )

        # Sync data
        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify index exists
        indexes = get_table_indexes(db_url, "users")
        assert "idx_email" in indexes

        # Sync again - index should not be recreated (no error)
        sync_csv_to_postgres(csv_file, job, db_url)

    @pytest.mark.parametrize("db_url", ["sqlite", "postgres"], indirect=True)
    def test_multi_column_index(self, tmp_path: Path, db_url: str) -> None:
        """Test creating multi-column index with different sort orders."""
        from data_sync.config import ColumnMapping, Index, IndexColumn, SyncJob

        # Create CSV
        csv_file = tmp_path / "orders.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["order_id", "customer_id", "order_date", "total"]
            )
            writer.writeheader()
            writer.writerow(
                {
                    "order_id": "1",
                    "customer_id": "100",
                    "order_date": "2024-01-01",
                    "total": "50.00",
                }
            )

        # Create job with multi-column index
        job = SyncJob(
            name="orders",
            target_table="orders",
            id_mapping=[ColumnMapping("order_id", "id")],
            columns=[
                ColumnMapping("customer_id", "customer_id"),
                ColumnMapping("order_date", "order_date"),
                ColumnMapping("total", "total"),
            ],
            indexes=[
                Index(
                    name="idx_customer_date",
                    columns=[
                        IndexColumn("customer_id", "ASC"),
                        IndexColumn("order_date", "DESC"),
                    ],
                )
            ],
        )

        # Sync data
        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify index exists
        indexes = get_table_indexes(db_url, "orders")
        assert "idx_customer_date" in indexes

    @pytest.mark.parametrize("db_url", ["sqlite", "postgres"], indirect=True)
    def test_multiple_indexes(self, tmp_path: Path, db_url: str) -> None:
        """Test creating multiple indexes on a table."""
        from data_sync.config import ColumnMapping, Index, IndexColumn, SyncJob

        # Create CSV
        csv_file = tmp_path / "products.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["product_id", "name", "category", "price"])
            writer.writeheader()
            writer.writerow(
                {"product_id": "1", "name": "Widget", "category": "Tools", "price": "9.99"}
            )

        # Create job with multiple indexes
        job = SyncJob(
            name="products",
            target_table="products",
            id_mapping=[ColumnMapping("product_id", "id")],
            columns=[
                ColumnMapping("name", "name"),
                ColumnMapping("category", "category"),
                ColumnMapping("price", "price"),
            ],
            indexes=[
                Index(name="idx_name", columns=[IndexColumn("name", "ASC")]),
                Index(name="idx_category", columns=[IndexColumn("category", "ASC")]),
                Index(
                    name="idx_category_price",
                    columns=[IndexColumn("category", "ASC"), IndexColumn("price", "DESC")],
                ),
            ],
        )

        # Sync data
        sync_csv_to_postgres(csv_file, job, db_url)

        # Verify all indexes exist
        indexes = get_table_indexes(db_url, "products")
        assert "idx_name" in indexes
        assert "idx_category" in indexes
        assert "idx_category_price" in indexes
