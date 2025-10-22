"""Database operations for data_sync."""

import csv
import sqlite3
from pathlib import Path
from typing import Any, Protocol

import psycopg
from psycopg import sql

from data_sync.config import SyncJob


class DatabaseBackend(Protocol):
    """Protocol for database backend operations."""

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        """Execute a query."""
        ...

    def fetchall(self, query: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        """Fetch all results from a query."""
        ...

    def commit(self) -> None:
        """Commit the current transaction."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...


class PostgreSQLBackend:
    """PostgreSQL database backend."""

    def __init__(self, connection_string: str) -> None:
        """Initialize PostgreSQL connection."""
        self.conn = psycopg.connect(connection_string)

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        """Execute a query."""
        with self.conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

    def fetchall(self, query: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        """Fetch all results from a query."""
        with self.conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur.fetchall()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.conn.commit()

    def close(self) -> None:
        """Close the connection."""
        self.conn.close()


class SQLiteBackend:
    """SQLite database backend."""

    def __init__(self, connection_string: str) -> None:
        """Initialize SQLite connection."""
        # Extract database path from connection string
        # Supports: sqlite:///path/to/db.db or sqlite:///:memory:
        if connection_string.startswith("sqlite:///"):
            db_path = connection_string[10:]  # Remove 'sqlite:///'
        elif connection_string.startswith("sqlite://"):
            db_path = connection_string[9:]  # Remove 'sqlite://'
        else:
            db_path = connection_string

        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def execute(self, query: str, params: tuple[Any, ...] | None = None) -> None:
        """Execute a query."""
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def fetchall(self, query: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        """Fetch all results from a query."""
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)
        return self.cursor.fetchall()

    def commit(self) -> None:
        """Commit the current transaction."""
        self.conn.commit()

    def close(self) -> None:
        """Close the connection."""
        self.cursor.close()
        self.conn.close()


def map_data_type_to_sql(data_type: str | None, db_type: str) -> str:
    """Map config data type to SQL database type.

    Args:
        data_type: Data type from config (integer, float, date, datetime, text, varchar(N))
        db_type: Database type ('postgresql' or 'sqlite')

    Returns:
        SQL type string for the database

    Supported types:
        - integer: PostgreSQL INTEGER, SQLite INTEGER
        - float: PostgreSQL DOUBLE PRECISION, SQLite REAL
        - date: PostgreSQL DATE, SQLite TEXT
        - datetime: PostgreSQL TIMESTAMP, SQLite TEXT
        - text: PostgreSQL TEXT, SQLite TEXT
        - varchar(N): PostgreSQL VARCHAR(N), SQLite TEXT
        - None (default): TEXT for both
    """
    if data_type is None:
        return "TEXT"

    data_type_lower = data_type.lower().strip()

    # Check for varchar(N) pattern
    if data_type_lower.startswith("varchar"):
        if db_type == "postgresql":
            return data_type.upper()  # VARCHAR(N)
        else:
            return "TEXT"  # SQLite doesn't have VARCHAR, use TEXT

    # Map other types
    type_mapping = {
        "integer": {"postgresql": "INTEGER", "sqlite": "INTEGER"},
        "int": {"postgresql": "INTEGER", "sqlite": "INTEGER"},
        "float": {"postgresql": "DOUBLE PRECISION", "sqlite": "REAL"},
        "double": {"postgresql": "DOUBLE PRECISION", "sqlite": "REAL"},
        "date": {"postgresql": "DATE", "sqlite": "TEXT"},
        "datetime": {"postgresql": "TIMESTAMP", "sqlite": "TEXT"},
        "timestamp": {"postgresql": "TIMESTAMP", "sqlite": "TEXT"},
        "text": {"postgresql": "TEXT", "sqlite": "TEXT"},
        "string": {"postgresql": "TEXT", "sqlite": "TEXT"},
    }

    if data_type_lower in type_mapping:
        return type_mapping[data_type_lower][db_type]

    # Default to TEXT if type is not recognized
    return "TEXT"


class DatabaseConnection:
    """Database connection handler supporting PostgreSQL and SQLite."""

    def __init__(self, connection_string: str) -> None:
        """Initialize database connection.

        Args:
            connection_string: Database connection string
                - PostgreSQL: postgresql://user:pass@host:port/db
                - SQLite: sqlite:///path/to/db.db or sqlite:///:memory:
        """
        self.connection_string = connection_string
        self.backend: DatabaseBackend | None = None
        self.db_type = self._detect_db_type(connection_string)

    def _detect_db_type(self, connection_string: str) -> str:
        """Detect database type from connection string."""
        if connection_string.startswith("sqlite"):
            return "sqlite"
        elif connection_string.startswith("postgres"):
            return "postgresql"
        else:
            raise ValueError(f"Unsupported database type in connection string: {connection_string}")

    def __enter__(self) -> "DatabaseConnection":
        """Enter context manager."""
        if self.db_type == "sqlite":
            self.backend = SQLiteBackend(self.connection_string)
        else:
            self.backend = PostgreSQLBackend(self.connection_string)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        if self.backend:
            self.backend.close()

    def create_table_if_not_exists(
        self, table_name: str, columns: dict[str, str], primary_keys: list[str] | None = None
    ) -> None:
        """Create table if it doesn't exist.

        Args:
            table_name: Name of the table
            columns: Dictionary of column_name -> column_type (without PRIMARY KEY constraint)
            primary_keys: Optional list of column names that form the primary key
        """
        if not self.backend:
            raise RuntimeError("Database connection not established")

        if self.db_type == "postgresql":
            # Use psycopg's SQL composition for PostgreSQL
            column_defs = []
            for col_name, col_type in columns.items():
                column_defs.append(
                    sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(col_type))
                )

            # Add primary key constraint if specified
            if primary_keys:
                pk_constraint = sql.SQL("PRIMARY KEY ({})").format(
                    sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys)
                )
                column_defs.append(pk_constraint)

            query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
                sql.Identifier(table_name), sql.SQL(", ").join(column_defs)
            )
            self.backend.execute(query.as_string(self.backend.conn))  # type: ignore
        else:
            # SQLite: use string formatting (safe since we control table/column names)
            column_defs_str = ", ".join(
                f'"{col_name}" {col_type}' for col_name, col_type in columns.items()
            )

            # Add primary key constraint if specified
            if primary_keys:
                pk_columns = ", ".join(f'"{pk}"' for pk in primary_keys)
                column_defs_str += f", PRIMARY KEY ({pk_columns})"

            query = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_defs_str})'
            self.backend.execute(query)

        self.backend.commit()

    def get_existing_columns(self, table_name: str) -> set[str]:
        """Get set of existing column names in a table.

        Args:
            table_name: Name of the table

        Returns:
            Set of column names (lowercase)
        """
        if not self.backend:
            raise RuntimeError("Database connection not established")

        if self.db_type == "postgresql":
            query = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
            """
            results = self.backend.fetchall(query, (table_name,))
            return {row[0].lower() for row in results}
        else:
            # SQLite: use PRAGMA table_info
            query = f'PRAGMA table_info("{table_name}")'
            results = self.backend.fetchall(query)
            # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
            return {row[1].lower() for row in results}

    def add_column(self, table_name: str, column_name: str, column_type: str) -> None:
        """Add a new column to an existing table.

        Args:
            table_name: Name of the table
            column_name: Name of the column to add
            column_type: SQL type for the column
        """
        if not self.backend:
            raise RuntimeError("Database connection not established")

        if self.db_type == "postgresql":
            query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.SQL(column_type),
            )
            self.backend.execute(query.as_string(self.backend.conn))  # type: ignore
        else:
            # SQLite
            query = f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'
            self.backend.execute(query)

        self.backend.commit()

    def upsert_row(
        self, table_name: str, conflict_columns: list[str], row_data: dict[str, Any]
    ) -> None:
        """Upsert a row into the database.

        Args:
            table_name: Name of the table
            conflict_columns: List of column names for conflict resolution (primary key columns)
            row_data: Dictionary of column_name -> value
        """
        if not self.backend:
            raise RuntimeError("Database connection not established")

        columns = list(row_data.keys())
        values = tuple(row_data.values())

        if self.db_type == "postgresql":
            # PostgreSQL: INSERT ... ON CONFLICT DO UPDATE
            insert_query = sql.SQL(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(col) for col in columns),
                sql.SQL(", ").join(sql.Placeholder() * len(values)),
                sql.SQL(", ").join(sql.Identifier(col) for col in conflict_columns),
                sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col))
                    for col in columns
                    if col not in conflict_columns
                ),
            )
            self.backend.execute(insert_query.as_string(self.backend.conn), values)  # type: ignore
        else:
            # SQLite: INSERT ... ON CONFLICT DO UPDATE
            columns_str = ", ".join(f'"{col}"' for col in columns)
            placeholders = ", ".join("?" * len(values))
            update_str = ", ".join(
                f'"{col}" = excluded."{col}"' for col in columns if col not in conflict_columns
            )

            # SQLite ON CONFLICT clause with multiple columns
            conflict_cols_str = ", ".join(f'"{col}"' for col in conflict_columns)

            query = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders}) '
            query += f"ON CONFLICT ({conflict_cols_str}) DO UPDATE SET {update_str}"

            self.backend.execute(query, values)

        self.backend.commit()

    def delete_stale_records(
        self,
        table_name: str,
        id_column: str,
        date_column: str,
        sync_date: str,
        current_ids: set[str],
    ) -> int:
        """Delete records with matching date but IDs not in current set.

        This allows safe removal of records that were previously synced
        but are no longer in the current CSV file for this date.

        Args:
            table_name: Name of the table
            id_column: Name of the ID column
            date_column: Name of the date column
            sync_date: The date value to match
            current_ids: Set of IDs that are in the current CSV

        Returns:
            Number of records deleted
        """
        if not self.backend:
            raise RuntimeError("Database connection not established")

        if not current_ids:
            # If no current IDs, don't delete anything
            return 0

        current_ids_list = list(current_ids)

        if self.db_type == "postgresql":
            # Build DELETE query for PostgreSQL
            delete_query = sql.SQL("DELETE FROM {} WHERE {} = %s AND {} NOT IN ({})").format(
                sql.Identifier(table_name),
                sql.Identifier(date_column),
                sql.Identifier(id_column),
                sql.SQL(", ").join(sql.Placeholder() * len(current_ids)),
            )
            params = tuple([sync_date] + current_ids_list)

            # Get count before delete
            count_query = f'SELECT COUNT(*) FROM "{table_name}" WHERE "{date_column}" = %s AND "{id_column}" NOT IN ({", ".join(["%s"] * len(current_ids))})'
            count_result = self.backend.fetchall(count_query, params)
            deleted_count = count_result[0][0] if count_result else 0

            # Execute delete
            self.backend.execute(delete_query.as_string(self.backend.conn), params)  # type: ignore
        else:
            # SQLite: use ? placeholders
            placeholders = ", ".join("?" * len(current_ids))
            query = f'DELETE FROM "{table_name}" WHERE "{date_column}" = ? AND "{id_column}" NOT IN ({placeholders})'
            params = tuple([sync_date] + current_ids_list)

            # Get count before delete
            count_query = f'SELECT COUNT(*) FROM "{table_name}" WHERE "{date_column}" = ? AND "{id_column}" NOT IN ({placeholders})'
            count_result = self.backend.fetchall(count_query, params)
            deleted_count = count_result[0][0] if count_result else 0

            # Execute delete
            self.backend.execute(query, params)

        self.backend.commit()
        return deleted_count

    def sync_csv_file(self, csv_path: Path, job: SyncJob, sync_date: str | None = None) -> int:
        """Sync a CSV file to the database using job configuration.

        Args:
            csv_path: Path to CSV file
            job: SyncJob configuration
            sync_date: Optional date value to store in date column for all rows

        Returns:
            Number of rows synced

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV is invalid or columns don't match
        """
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        rows_synced = 0
        synced_ids = set()

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

            # Build column definitions with types from config
            columns_def = {}
            for col_mapping in sync_columns:
                sql_type = map_data_type_to_sql(col_mapping.data_type, self.db_type)
                columns_def[col_mapping.db_column] = sql_type

            # Add date column if date_mapping is configured
            if job.date_mapping:
                columns_def[job.date_mapping.db_column] = "TEXT"

            # Primary key is always based on id_mapping only (date is NOT part of primary key)
            primary_keys = [job.id_mapping.db_column]

            self.create_table_if_not_exists(job.target_table, columns_def, primary_keys)

            # Check for schema evolution: add missing columns from config
            existing_columns = self.get_existing_columns(job.target_table)
            for col_name, col_type in columns_def.items():
                if col_name.lower() not in existing_columns:
                    self.add_column(job.target_table, col_name, col_type)

            # Process each row
            for row in reader:
                row_data = {}
                for col_mapping in sync_columns:
                    if col_mapping.csv_column in row:
                        row_data[col_mapping.db_column] = row[col_mapping.csv_column]

                # Add sync date if configured
                if job.date_mapping and sync_date:
                    row_data[job.date_mapping.db_column] = sync_date

                self.upsert_row(job.target_table, primary_keys, row_data)

                # Track synced IDs for cleanup
                synced_ids.add(row_data[job.id_mapping.db_column])
                rows_synced += 1

        # Clean up stale records if date_mapping is configured
        if job.date_mapping and sync_date:
            self.delete_stale_records(
                job.target_table,
                job.id_mapping.db_column,
                job.date_mapping.db_column,
                sync_date,
                synced_ids,
            )

        return rows_synced


def sync_csv_to_postgres(
    csv_path: Path, job: SyncJob, db_connection_string: str, sync_date: str | None = None
) -> int:
    """Sync a CSV file to PostgreSQL database.

    Args:
        csv_path: Path to the CSV file
        job: SyncJob configuration
        db_connection_string: PostgreSQL connection string
        sync_date: Optional date value to store in date column for all rows

    Returns:
        Number of rows synced
    """
    with DatabaseConnection(db_connection_string) as db:
        return db.sync_csv_file(csv_path, job, sync_date)
