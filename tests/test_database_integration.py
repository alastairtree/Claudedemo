"""Integration tests for database synchronization with SQLite and PostgreSQL."""

import csv
from pathlib import Path

import pytest

from data_sync.config import SyncConfig
from data_sync.database import DatabaseConnection, sync_csv_to_postgres
from tests.db_test_utils import execute_query, get_table_columns, get_table_indexes


class TestDatabaseIntegration:
    """Integration tests with SQLite and PostgreSQL databases."""

    def test_sync_csv_with_column_mapping_and_exclusion(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing CSV with renamed column and excluded column.

        This test verifies:
        1. CSV with 3 columns: id, name (renamed to full_name), email (not synced)
        2. Idempotency: running twice doesn't duplicate or change data
        """
        from tests.test_helpers import create_config_file, create_csv_file

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
        create_config_file(
            config_file, "sync_users", "users", {"user_id": "id"}, {"name": "full_name"}
        )

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
        from tests.test_helpers import create_config_file, create_csv_file

        csv_file = tmp_path / "products.csv"
        create_csv_file(
            csv_file,
            ["product_id", "name", "price", "category"],
            [
                {"product_id": "P1", "name": "Widget", "price": "9.99", "category": "Tools"},
                {"product_id": "P2", "name": "Gadget", "price": "19.99", "category": "Electronics"},
            ],
        )

        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "sync_products", "products", {"product_id": "id"})

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
        from tests.test_helpers import create_config_file, create_csv_file

        csv_file = tmp_path / "data.csv"

        # First version of data
        create_csv_file(csv_file, ["id", "value"], [{"id": "1", "value": "original"}])

        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "test_upsert", "test_data", {"id": "id"})

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_upsert")

        # First sync
        sync_csv_to_postgres(csv_file, job, db_url)

        # Update the CSV with new value for same ID
        create_csv_file(csv_file, ["id", "value"], [{"id": "1", "value": "updated"}])

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

    def test_sync_with_filename_to_column(self, tmp_path: Path, db_url: str) -> None:
        """Test syncing with filename_to_column extracts and stores values."""
        # Create CSV file
        csv_file = tmp_path / "sales_2024-01-15.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["sale_id", "amount"])
            writer.writeheader()
            writer.writerow({"sale_id": "1", "amount": "100"})
            writer.writerow({"sale_id": "2", "amount": "200"})

        # Create config with filename_to_column
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_sales:
    target_table: sales
    id_mapping:
      sale_id: id
    filename_to_column:
      template: "sales_[date].csv"
      columns:
        date:
          db_column: sync_date
          type: date
          use_to_delete_old_rows: true
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_sales")
        assert job is not None
        assert job.filename_to_column is not None

        # Extract values and sync
        filename_values = job.filename_to_column.extract_values_from_filename(csv_file)
        assert filename_values == {"date": "2024-01-15"}

        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, filename_values)
        assert rows_synced == 2

        # Verify date column was created and populated
        columns = get_table_columns(db_url, "sales")
        assert "sync_date" in columns

        rows = execute_query(db_url, "SELECT id, amount, sync_date FROM sales ORDER BY id")
        assert len(rows) == 2
        # For PostgreSQL: date becomes date object, for SQLite: stays as string
        if db_url.startswith("sqlite"):
            assert rows[0] == ("1", "100", "2024-01-15")
            assert rows[1] == ("2", "200", "2024-01-15")
        else:
            from datetime import date

            assert rows[0] == ("1", "100", date(2024, 1, 15))
            assert rows[1] == ("2", "200", date(2024, 1, 15))

    def test_delete_stale_records(self, tmp_path: Path, db_url: str) -> None:
        """Test that stale records are deleted after sync with filename_to_column."""
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
    filename_to_column:
      template: "data_[date].csv"
      columns:
        date:
          db_column: sync_date
          type: date
          use_to_delete_old_rows: true
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_data")

        filename_values = job.filename_to_column.extract_values_from_filename(csv_file)
        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, filename_values)
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

        rows_synced_2 = sync_csv_to_postgres(csv_file, job, db_url, filename_values)
        assert rows_synced_2 == 2

        # Verify only 2 records remain for this date
        rows = execute_query(
            db_url, "SELECT id, value FROM data WHERE sync_date = %s ORDER BY id", ("2024-01-15",)
        )
        assert len(rows) == 2
        assert rows[0] == ("1", "A_updated")
        assert rows[1] == ("2", "B_updated")

    def test_delete_stale_records_preserves_other_dates(self, tmp_path: Path, db_url: str) -> None:
        """Test that deleting stale records only affects matching date with filename_to_column."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_data:
    target_table: multi_date_data
    id_mapping:
      id: id
    filename_to_column:
      template: "data_[date].csv"
      columns:
        date:
          db_column: sync_date
          type: date
          use_to_delete_old_rows: true
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

        filename_values_1 = job.filename_to_column.extract_values_from_filename(csv_file_1)
        sync_csv_to_postgres(csv_file_1, job, db_url, filename_values_1)

        # Sync data for 2024-01-16
        csv_file_2 = tmp_path / "data_2024-01-16.csv"
        with open(csv_file_2, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day2"})
            writer.writerow({"id": "2", "value": "Day2"})
            writer.writerow({"id": "3", "value": "Day2"})

        filename_values_2 = job.filename_to_column.extract_values_from_filename(csv_file_2)
        sync_csv_to_postgres(csv_file_2, job, db_url, filename_values_2)

        # Verify total records (IDs 1,2 were updated to day 2, ID 3 was inserted)
        total_count_result = execute_query(db_url, "SELECT COUNT(*) FROM multi_date_data")
        assert total_count_result[0][0] == 3

        # Re-sync day 1 with only ID 1 (updating it back to day 1)
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "Day1_updated"})

        sync_csv_to_postgres(csv_file_1, job, db_url, filename_values_1)

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
        from data_sync.config import ColumnMapping, SyncJob
        from tests.test_helpers import create_csv_file

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
        from data_sync.config import ColumnMapping, Index, IndexColumn, SyncJob
        from tests.test_helpers import create_csv_file

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

    def test_filename_to_column_multiple_values(self, tmp_path: Path, db_url: str) -> None:
        """Test extracting multiple values from filename with template syntax."""
        # Create CSV file with filename containing multiple values
        csv_file = tmp_path / "imap_level2_primary_20240115_v002.cdf"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "measurement"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "measurement": "42.5"})
            writer.writerow({"obs_id": "2", "measurement": "38.2"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  obs_data:
    target_table: observations
    id_mapping:
      obs_id: id
    filename_to_column:
      template: "[mission]_level2_[sensor]_[date]_v[version].cdf"
      columns:
        mission:
          db_column: mission_name
          type: varchar(10)
        sensor:
          db_column: sensor_type
          type: varchar(20)
        date:
          db_column: observation_date
          type: date
          use_to_delete_old_rows: true
        version:
          db_column: file_version
          type: varchar(10)
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("obs_data")
        assert job is not None
        assert job.filename_to_column is not None

        # Extract values
        filename_values = job.filename_to_column.extract_values_from_filename(csv_file)
        assert filename_values == {
            "mission": "imap",
            "sensor": "primary",
            "date": "20240115",
            "version": "002",
        }

        # Sync data
        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, filename_values)
        assert rows_synced == 2

        # Verify all columns were created
        columns = get_table_columns(db_url, "observations")
        assert "mission_name" in columns
        assert "sensor_type" in columns
        assert "observation_date" in columns
        assert "file_version" in columns

        # Verify data was inserted with extracted values
        rows = execute_query(
            db_url,
            "SELECT id, measurement, mission_name, sensor_type, observation_date, file_version FROM observations ORDER BY id",
        )
        assert len(rows) == 2
        # For PostgreSQL: date becomes date object, for SQLite: stays as string
        if db_url.startswith("sqlite"):
            assert rows[0] == ("1", "42.5", "imap", "primary", "20240115", "002")
            assert rows[1] == ("2", "38.2", "imap", "primary", "20240115", "002")
        else:
            from datetime import date

            assert rows[0] == ("1", "42.5", "imap", "primary", date(2024, 1, 15), "002")
            assert rows[1] == ("2", "38.2", "imap", "primary", date(2024, 1, 15), "002")

    def test_filename_to_column_regex_syntax(self, tmp_path: Path, db_url: str) -> None:
        """Test extracting values from filename with regex syntax."""
        csv_file = tmp_path / "data_20240315_v1.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["record_id", "value"])
            writer.writeheader()
            writer.writerow({"record_id": "1", "value": "test"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  versioned_data:
    target_table: versioned_records
    id_mapping:
      record_id: id
    filename_to_column:
      regex: "data_(?P<date>\\d{8})_v(?P<version>\\d+)\\.csv"
      columns:
        date:
          db_column: record_date
          type: date
          use_to_delete_old_rows: true
        version:
          db_column: data_version
          type: integer
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("versioned_data")

        # Extract values using regex
        filename_values = job.filename_to_column.extract_values_from_filename(csv_file)
        assert filename_values == {"date": "20240315", "version": "1"}

        # Sync data
        rows_synced = sync_csv_to_postgres(csv_file, job, db_url, filename_values)
        assert rows_synced == 1

        # Verify data
        rows = execute_query(
            db_url, "SELECT id, value, record_date, data_version FROM versioned_records"
        )
        assert len(rows) == 1
        # For SQLite: date stays as string, version becomes integer
        # For PostgreSQL: date becomes date object, version becomes integer
        if db_url.startswith("sqlite"):
            assert rows[0] == ("1", "test", "20240315", 1)
        else:
            from datetime import date

            assert rows[0] == ("1", "test", date(2024, 3, 15), 1)

    def test_filename_to_column_compound_key_can_update_filename_columns_in_a_later_file_meaning_stale_detection_does_not_locate_them(
        self, tmp_path: Path, db_url: str
    ) -> None:
        """Test stale record deletion using compound key from filename values."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  mission_data:
    target_table: mission_records
    id_mapping:
      obs_id: id
    filename_to_column:
      template: "[mission]_[date]_v[version].csv"
      columns:
        mission:
          db_column: mission_name
          type: varchar(20)
          use_to_delete_old_rows: true
        date:
          db_column: observation_date
          type: date
          use_to_delete_old_rows: true
        version:
          db_column: file_version
          type: varchar(10)
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("mission_data")

        # First sync: mission A, date 2024-01-15, version v1
        csv_file_1 = tmp_path / "missionA_2024-01-15_v1.csv"
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1"})
            writer.writerow({"obs_id": "2", "value": "A2"})
            writer.writerow({"obs_id": "3", "value": "A3"})

        filename_values_1 = job.filename_to_column.extract_values_from_filename(csv_file_1)
        sync_csv_to_postgres(csv_file_1, job, db_url, filename_values_1)

        # Second sync: mission A, different date, version v1
        csv_file_2 = tmp_path / "missionA_2024-01-16_v1.csv"
        with open(csv_file_2, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1_day2"})
            writer.writerow({"obs_id": "4", "value": "A2_day2"})

        filename_values_2 = job.filename_to_column.extract_values_from_filename(csv_file_2)
        sync_csv_to_postgres(csv_file_2, job, db_url, filename_values_2)

        # Verify we have 4 records total:
        # - 2 for missionA + 2024-01-15 (IDs 2,3 plus the now updated ID 1)
        # - 2 for missionA + 2024-01-16 (IDs 4 and ID 1 again but with updated value)
        total_count = execute_query(db_url, "SELECT COUNT(*) FROM mission_records")
        assert total_count[0][0] == 4

        # Re-sync first file with only 2 records (ID 3 removed)
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1_updated"})
            writer.writerow({"obs_id": "2", "value": "A2_updated"})

        sync_csv_to_postgres(csv_file_1, job, db_url, filename_values_1)

        # Verify: Records for mission A + date 2024-01-15 only have IDs 1,2
        # Mission A + date 2024-01-16 should only have ID 4
        day1_count = execute_query(
            db_url,
            "SELECT COUNT(*) FROM mission_records WHERE mission_name = %s AND observation_date = %s",
            ("missionA", "2024-01-15"),
        )
        assert (
            day1_count[0][0] == 2
        )  # ID 3 removed, id 1 updated to the second file and then back to the first

        day2_count = execute_query(
            db_url,
            "SELECT COUNT(*) FROM mission_records WHERE mission_name = %s AND observation_date = %s",
            ("missionA", "2024-01-16"),
        )
        assert day2_count[0][0] == 1  # just id 4, id 1 was updated back to day 1

        total_after = execute_query(db_url, "SELECT COUNT(*) FROM mission_records")
        assert total_after[0][0] == 3  # 2 from day 1, 1 from day 2

    def test_filename_to_column_compound_key_will_remove_updated_stale_records(
        self, tmp_path: Path, db_url: str
    ) -> None:
        """Test stale record deletion using compound key from filename values."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  mission_data:
    target_table: mission_records
    id_mapping:
      obs_id: id
    filename_to_column:
      template: "[mission]_[date]_v[version].csv"
      columns:
        mission:
          db_column: mission_name
          type: varchar(20)
          use_to_delete_old_rows: true
        date:
          db_column: observation_date
          type: date
          use_to_delete_old_rows: true
        version:
          db_column: file_version
          type: varchar(10)
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("mission_data")

        # First sync: mission A, date 2024-01-15, version v1
        csv_file_1 = tmp_path / "missionA_2024-01-15_v1.csv"
        with open(csv_file_1, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1"})
            writer.writerow({"obs_id": "2", "value": "A2"})
            writer.writerow({"obs_id": "3", "value": "A3"})

        filename_values_1 = job.filename_to_column.extract_values_from_filename(csv_file_1)
        sync_csv_to_postgres(csv_file_1, job, db_url, filename_values_1)

        # Second sync: mission A, different date, version v1
        csv_file_2 = tmp_path / "missionA_2024-01-16_v1.csv"
        with open(csv_file_2, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1"})
            writer.writerow({"obs_id": "2", "value": "A2"})
            writer.writerow({"obs_id": "3", "value": "A3_day3"})
            writer.writerow({"obs_id": "4", "value": "A2_day2"})

        filename_values_2 = job.filename_to_column.extract_values_from_filename(csv_file_2)
        sync_csv_to_postgres(csv_file_2, job, db_url, filename_values_2)

        # Verify we have 4 records total:
        total_count = execute_query(db_url, "SELECT COUNT(*) FROM mission_records")
        assert total_count[0][0] == 4

        # 3rd sync - Re-sync second file with only 3 records (ID 2 removed) and an update to ID 4
        with open(csv_file_2, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["obs_id", "value"])
            writer.writeheader()
            writer.writerow({"obs_id": "1", "value": "A1"})
            writer.writerow({"obs_id": "3", "value": "A3_day3"})
            writer.writerow({"obs_id": "4", "value": "A2_day2_updated"})
        sync_csv_to_postgres(csv_file_2, job, db_url, filename_values_2)

        # Verify: Records for mission A + date 2024-01-15 not there any more as were updated to date 2024-01-16
        day1_count = execute_query(
            db_url,
            "SELECT COUNT(*) FROM mission_records WHERE mission_name = %s AND observation_date = %s",
            ("missionA", "2024-01-15"),
        )
        assert day1_count[0][0] == 0  # all updated in day2

        day2_count = execute_query(
            db_url,
            "SELECT COUNT(*) FROM mission_records WHERE mission_name = %s AND observation_date = %s",
            ("missionA", "2024-01-16"),
        )
        assert day2_count[0][0] == 3  # IDs 1,3,4

        total_after = execute_query(db_url, "SELECT COUNT(*) FROM mission_records")
        assert total_after[0][0] == 3  # IDs 1,3,4
