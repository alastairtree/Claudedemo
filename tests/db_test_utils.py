"""Shared database test utilities."""

import sqlite3
from typing import Any


def _get_db_connection(db_url: str) -> tuple[Any, Any, bool]:
    """Get database connection and cursor.

    Args:
        db_url: Database connection URL

    Returns:
        Tuple of (connection, cursor, is_sqlite)
    """
    if db_url.startswith("sqlite"):
        db_path = db_url.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        return conn, cursor, True
    else:
        import psycopg

        conn = psycopg.connect(db_url)
        cursor = conn.cursor()
        return conn, cursor, False


def execute_query(db_url: str, query: str, params: tuple = ()) -> list[tuple]:
    """Execute a query and return results for any database type."""
    conn, cursor, is_sqlite = _get_db_connection(db_url)

    try:
        # Replace %s with ? for SQLite
        if is_sqlite:
            query = query.replace("%s", "?")

        cursor.execute(query, params)
        results = cursor.fetchall()
        return results
    finally:
        cursor.close()
        conn.close()


def get_table_columns(db_url: str, table_name: str) -> list[str]:
    """Get column names from a table for any database type."""
    conn, cursor, is_sqlite = _get_db_connection(db_url)

    try:
        if is_sqlite:
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = [row[1] for row in cursor.fetchall()]  # Column name is at index 1
            return sorted(columns)
        else:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def table_exists(db_url: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
    conn, cursor, is_sqlite = _get_db_connection(db_url)

    try:
        if is_sqlite:
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
            )
            result = cursor.fetchone()
            return result is not None
        else:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table_name,),
            )
            return cursor.fetchone()[0]
    finally:
        cursor.close()
        conn.close()


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
