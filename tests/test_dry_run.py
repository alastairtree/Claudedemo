"""Tests for dry-run functionality."""

import csv
import platform
from pathlib import Path

import pytest

from data_sync.config import ColumnMapping, DateMapping, Index, IndexColumn, SyncJob
from data_sync.database import DatabaseConnection, sync_csv_to_postgres_dry_run


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


def table_exists(db_url: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
    if db_url.startswith("sqlite"):
        import sqlite3

        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    else:
        import psycopg

        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table_name,),
            )
            return cur.fetchone()[0]


def test_dry_run_summary_new_table(db_url: str, tmp_path: Path) -> None:
    """Test dry-run summary when table doesn't exist."""
    # Create a CSV file
    csv_file = tmp_path / "test.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "name", "email"])
        writer.writeheader()
        writer.writerow({"user_id": "1", "name": "Alice", "email": "alice@example.com"})
        writer.writerow({"user_id": "2", "name": "Bob", "email": "bob@example.com"})
        writer.writerow({"user_id": "3", "name": "Charlie", "email": "charlie@example.com"})

    # Create a sync job configuration
    job = SyncJob(
        name="test_job",
        target_table="users",
        id_mapping=[ColumnMapping("user_id", "id")],
        columns=[
            ColumnMapping("name", "full_name"),
            ColumnMapping("email", "email_address"),
        ],
    )

    # Run dry-run
    summary = sync_csv_to_postgres_dry_run(csv_file, job, db_url)

    # Verify summary
    assert summary.table_name == "users"
    assert not summary.table_exists
    assert summary.rows_to_sync == 3
    assert summary.rows_to_delete == 0
    assert len(summary.new_columns) == 0  # No columns to add since table doesn't exist
    assert len(summary.new_indexes) == 0

    # Verify no tables were created during dry-run
    assert not table_exists(db_url, "users"), "No tables should have been created during dry-run"


def test_dry_run_summary_existing_table_no_changes(db_url: str, tmp_path: Path) -> None:
    """Test dry-run when table exists and no schema changes needed."""
    # Create a CSV file
    csv_file = tmp_path / "test.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "name", "price"])
        writer.writeheader()
        writer.writerow({"product_id": "1", "name": "Widget", "price": "19.99"})
        writer.writerow({"product_id": "2", "name": "Gadget", "price": "29.99"})

    # First, create the table with actual data
    job = SyncJob(
        name="test_job",
        target_table="products",
        id_mapping=[ColumnMapping("product_id", "id")],
        columns=[
            ColumnMapping("name", "name"),
            ColumnMapping("price", "price"),
        ],
    )

    with DatabaseConnection(db_url) as db:
        db.sync_csv_file(csv_file, job)

    # Now run dry-run with same schema
    summary = sync_csv_to_postgres_dry_run(csv_file, job, db_url)

    # Verify summary
    assert summary.table_name == "products"
    assert summary.table_exists
    assert summary.rows_to_sync == 2
    assert summary.rows_to_delete == 0
    assert len(summary.new_columns) == 0
    assert len(summary.new_indexes) == 0


def test_dry_run_summary_new_columns(db_url: str, tmp_path: Path) -> None:
    """Test dry-run when new columns need to be added."""
    # Create initial CSV file with fewer columns
    csv_file1 = tmp_path / "test1.csv"
    with open(csv_file1, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "customer_name"])
        writer.writeheader()
        writer.writerow({"order_id": "1", "customer_name": "Alice"})

    job1 = SyncJob(
        name="test_job",
        target_table="orders",
        id_mapping=[ColumnMapping("order_id", "id")],
        columns=[ColumnMapping("customer_name", "customer_name")],
    )

    with DatabaseConnection(db_url) as db:
        db.sync_csv_file(csv_file1, job1)

    # Create new CSV with additional columns
    csv_file2 = tmp_path / "test2.csv"
    with open(csv_file2, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "customer_name", "total", "status"])
        writer.writeheader()
        writer.writerow(
            {"order_id": "2", "customer_name": "Bob", "total": "100", "status": "shipped"}
        )

    # New job with additional columns
    job2 = SyncJob(
        name="test_job",
        target_table="orders",
        id_mapping=[ColumnMapping("order_id", "id")],
        columns=[
            ColumnMapping("customer_name", "customer_name"),
            ColumnMapping("total", "total_amount", "float"),
            ColumnMapping("status", "order_status"),
        ],
    )

    # Run dry-run
    summary = sync_csv_to_postgres_dry_run(csv_file2, job2, db_url)

    # Verify summary
    assert summary.table_name == "orders"
    assert summary.table_exists
    assert summary.rows_to_sync == 1
    assert len(summary.new_columns) == 2

    # Check that new columns are identified
    new_column_names = [col[0] for col in summary.new_columns]
    assert "total_amount" in new_column_names
    assert "order_status" in new_column_names


def test_dry_run_summary_new_indexes(db_url: str, tmp_path: Path) -> None:
    """Test dry-run when new indexes need to be created."""
    # Create a CSV file
    csv_file = tmp_path / "test.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "email"])
        writer.writeheader()
        writer.writerow({"user_id": "1", "email": "alice@example.com"})

    job_no_indexes = SyncJob(
        name="test_job",
        target_table="users",
        id_mapping=[ColumnMapping("user_id", "id")],
        columns=[ColumnMapping("email", "email")],
    )

    with DatabaseConnection(db_url) as db:
        db.sync_csv_file(csv_file, job_no_indexes)

    # Create job with indexes
    job_with_indexes = SyncJob(
        name="test_job",
        target_table="users",
        id_mapping=[ColumnMapping("user_id", "id")],
        columns=[ColumnMapping("email", "email")],
        indexes=[
            Index("idx_email", [IndexColumn("email", "ASC")]),
        ],
    )

    # Run dry-run
    summary = sync_csv_to_postgres_dry_run(csv_file, job_with_indexes, db_url)

    # Verify summary
    assert summary.table_name == "users"
    assert summary.table_exists
    assert len(summary.new_indexes) == 1
    assert "idx_email" in summary.new_indexes


def test_dry_run_with_date_mapping_and_stale_records(db_url: str, tmp_path: Path) -> None:
    """Test dry-run with date mapping and stale record detection."""
    # Create initial CSV with date in filename
    csv_file1 = tmp_path / "sales_2024-01-15.csv"
    with open(csv_file1, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["sale_id", "amount"])
        writer.writeheader()
        writer.writerow({"sale_id": "1", "amount": "100"})
        writer.writerow({"sale_id": "2", "amount": "200"})
        writer.writerow({"sale_id": "3", "amount": "300"})

    job = SyncJob(
        name="sales_job",
        target_table="sales",
        id_mapping=[ColumnMapping("sale_id", "id")],
        columns=[ColumnMapping("amount", "amount")],
        date_mapping=DateMapping(r"(\d{4}-\d{2}-\d{2})", "sync_date"),
    )

    with DatabaseConnection(db_url) as db:
        sync_date = job.date_mapping.extract_date_from_filename(csv_file1)
        db.sync_csv_file(csv_file1, job, sync_date)

    # Create new CSV with fewer records (simulating deletions)
    csv_file2 = tmp_path / "sales_2024-01-15_updated.csv"
    with open(csv_file2, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["sale_id", "amount"])
        writer.writeheader()
        writer.writerow({"sale_id": "1", "amount": "150"})  # Updated
        writer.writerow({"sale_id": "2", "amount": "200"})  # Same
        # sale_id 3 is missing - should be detected as stale

    # Run dry-run
    sync_date = job.date_mapping.extract_date_from_filename(csv_file2)
    summary = sync_csv_to_postgres_dry_run(csv_file2, job, db_url, sync_date)

    # Verify summary
    assert summary.table_name == "sales"
    assert summary.table_exists
    assert summary.rows_to_sync == 2
    assert summary.rows_to_delete == 1  # sale_id 3 would be deleted


def test_dry_run_with_compound_primary_key(db_url: str, tmp_path: Path) -> None:
    """Test dry-run with compound primary key."""
    # Create a CSV file
    csv_file = tmp_path / "test.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["store_id", "product_id", "quantity"])
        writer.writeheader()
        writer.writerow({"store_id": "1", "product_id": "A", "quantity": "10"})
        writer.writerow({"store_id": "1", "product_id": "B", "quantity": "20"})
        writer.writerow({"store_id": "2", "product_id": "A", "quantity": "15"})

    # Create a sync job with compound primary key
    job = SyncJob(
        name="inventory_job",
        target_table="inventory",
        id_mapping=[
            ColumnMapping("store_id", "store_id"),
            ColumnMapping("product_id", "product_id"),
        ],
        columns=[ColumnMapping("quantity", "qty")],
    )

    # Run dry-run
    summary = sync_csv_to_postgres_dry_run(csv_file, job, db_url)

    # Verify summary
    assert summary.table_name == "inventory"
    assert not summary.table_exists
    assert summary.rows_to_sync == 3
    assert summary.rows_to_delete == 0
