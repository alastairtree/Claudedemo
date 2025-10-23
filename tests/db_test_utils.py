"""Shared database test utilities."""

import sqlite3


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
                ORDER BY ordinal_position
                """,
                (table_name,),
            )
            return [row[0] for row in cur.fetchall()]


def table_exists(db_url: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
    if db_url.startswith("sqlite"):
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
