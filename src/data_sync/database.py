"""Database operations for data_sync."""

import csv
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from data_sync.config import SyncJob


class DatabaseConnection:
    """PostgreSQL database connection handler."""

    def __init__(self, connection_string: str) -> None:
        """Initialize database connection.

        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.conn: psycopg.Connection[Any] | None = None

    def __enter__(self) -> "DatabaseConnection":
        """Enter context manager."""
        self.conn = psycopg.connect(self.connection_string)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        if self.conn:
            self.conn.close()

    def create_table_if_not_exists(self, table_name: str, columns: dict[str, str]) -> None:
        """Create table if it doesn't exist.

        Args:
            table_name: Name of the table
            columns: Dictionary of column_name -> column_type
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type)))

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(table_name), sql.SQL(", ").join(column_defs)
        )

        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

    def upsert_row(self, table_name: str, id_column: str, row_data: dict[str, Any]) -> None:
        """Upsert a row into the database.

        Args:
            table_name: Name of the table
            id_column: Name of the ID column for conflict resolution
            row_data: Dictionary of column_name -> value
        """
        if not self.conn:
            raise RuntimeError("Database connection not established")

        columns = list(row_data.keys())
        values = list(row_data.values())

        # Build INSERT ... ON CONFLICT DO UPDATE query
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(col) for col in columns),
            sql.SQL(", ").join(sql.Placeholder() * len(values)),
            sql.Identifier(id_column),
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                for col in columns
                if col != id_column
            ),
        )

        with self.conn.cursor() as cur:
            cur.execute(insert_query, values)
        self.conn.commit()

    def sync_csv_file(self, csv_path: Path, job: SyncJob) -> int:
        """Sync a CSV file to the database using job configuration.

        Args:
            csv_path: Path to CSV file
            job: SyncJob configuration

        Returns:
            Number of rows synced

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV is invalid or columns don't match
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        rows_synced = 0

        with open(csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError("CSV file has no columns")

            # Validate that required columns exist in CSV
            csv_columns = set(reader.fieldnames)
            if job.id_mapping.csv_column not in csv_columns:
                raise ValueError(f"ID column '{job.id_mapping.csv_column}' not found in CSV")

            # Determine which columns to sync
            if job.columns:
                # Specific columns defined
                sync_columns = [job.id_mapping] + job.columns
                for col_mapping in job.columns:
                    if col_mapping.csv_column not in csv_columns:
                        raise ValueError(f"Column '{col_mapping.csv_column}' not found in CSV")
            else:
                # Sync all columns
                sync_columns = [job.id_mapping]
                for csv_col in csv_columns:
                    if csv_col != job.id_mapping.csv_column:
                        sync_columns.append(type(job.id_mapping)(csv_col, csv_col))

            # Create table with all needed columns (TEXT type for simplicity)
            columns_def = {job.id_mapping.db_column: "TEXT PRIMARY KEY"}
            for col_mapping in sync_columns:
                if col_mapping.db_column != job.id_mapping.db_column:
                    columns_def[col_mapping.db_column] = "TEXT"

            self.create_table_if_not_exists(job.target_table, columns_def)

            # Process each row
            for row in reader:
                row_data = {}
                for col_mapping in sync_columns:
                    if col_mapping.csv_column in row:
                        row_data[col_mapping.db_column] = row[col_mapping.csv_column]

                self.upsert_row(job.target_table, job.id_mapping.db_column, row_data)
                rows_synced += 1

        return rows_synced


def sync_csv_to_postgres(csv_path: Path, job: SyncJob, db_connection_string: str) -> int:
    """Sync a CSV file to PostgreSQL database.

    Args:
        csv_path: Path to the CSV file
        job: SyncJob configuration
        db_connection_string: PostgreSQL connection string

    Returns:
        Number of rows synced
    """
    with DatabaseConnection(db_connection_string) as db:
        return db.sync_csv_file(csv_path, job)
