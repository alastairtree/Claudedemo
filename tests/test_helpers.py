"""Helper functions for tests."""

import csv
from pathlib import Path


def create_csv_file(file_path: Path, fieldnames: list[str], rows: list[dict]) -> Path:
    """Create a CSV file with the given fieldnames and rows.

    Args:
        file_path: Path where the CSV file should be created
        fieldnames: List of column names
        rows: List of dictionaries representing rows

    Returns:
        Path to the created CSV file
    """
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return file_path


def create_config_file(
    file_path: Path,
    job_name: str,
    target_table: str,
    id_mapping: dict[str, str],
    columns: dict[str, str] | None = None,
    date_mapping: dict[str, str] | None = None,
) -> Path:
    """Create a YAML config file with the specified job configuration.

    Args:
        file_path: Path where the config file should be created
        job_name: Name of the job
        target_table: Target database table name
        id_mapping: Dictionary mapping CSV columns to database ID columns
        columns: Optional dictionary mapping CSV columns to database columns
        date_mapping: Optional date mapping configuration

    Returns:
        Path to the created config file
    """
    config_lines = [
        "jobs:",
        f"  {job_name}:",
        f"    target_table: {target_table}",
        "    id_mapping:",
    ]

    for csv_col, db_col in id_mapping.items():
        config_lines.append(f"      {csv_col}: {db_col}")

    if columns:
        config_lines.append("    columns:")
        for csv_col, db_col in columns.items():
            config_lines.append(f"      {csv_col}: {db_col}")

    if date_mapping:
        config_lines.append("    date_mapping:")
        for key, value in date_mapping.items():
            config_lines.append(f"      {key}: '{value}'")

    file_path.write_text("\n".join(config_lines) + "\n")
    return file_path
