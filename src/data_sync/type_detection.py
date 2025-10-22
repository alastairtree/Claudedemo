"""Type detection for CSV columns."""

import csv
import re
from pathlib import Path


def detect_column_type(values: list[str]) -> str:
    """Detect the most appropriate data type for a column based on sample values.

    Args:
        values: List of string values from the column (excluding empty strings)

    Returns:
        Detected type: 'integer', 'float', 'date', 'datetime', 'text', or 'varchar(N)'
    """
    if not values:
        return "text"

    # Sample up to 1000 values for performance
    sample = values[:1000]
    non_empty = [v for v in sample if v.strip()]

    if not non_empty:
        return "text"

    # Check if all values are integers
    if all(_is_integer(v) for v in non_empty):
        return "integer"

    # Check if all values are floats
    if all(_is_float(v) for v in non_empty):
        return "float"

    # Check if all values are dates
    if all(_is_date(v) for v in non_empty):
        return "date"

    # Check if all values are datetimes
    if all(_is_datetime(v) for v in non_empty):
        return "datetime"

    # Check if it's a short text field (could use varchar)
    max_length = max(len(v) for v in non_empty)
    if max_length <= 255:
        return f"varchar({max_length})"

    return "text"


def _is_integer(value: str) -> bool:
    """Check if a string represents an integer."""
    try:
        int(value)
        return True
    except ValueError:
        return False


def _is_float(value: str) -> bool:
    """Check if a string represents a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def _is_date(value: str) -> bool:
    """Check if a string represents a date (YYYY-MM-DD format)."""
    # Common date patterns
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
        r"^\d{4}/\d{2}/\d{2}$",  # YYYY/MM/DD
        r"^\d{2}-\d{2}-\d{4}$",  # DD-MM-YYYY
        r"^\d{2}/\d{2}/\d{4}$",  # DD/MM/YYYY or MM/DD/YYYY
    ]

    return any(re.match(pattern, value.strip()) for pattern in date_patterns)


def _is_datetime(value: str) -> bool:
    """Check if a string represents a datetime."""
    # Common datetime patterns
    datetime_patterns = [
        r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}",  # YYYY-MM-DD HH:MM:SS
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",  # ISO format
        r"^\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2}",  # MM/DD/YYYY HH:MM:SS
    ]

    return any(re.match(pattern, value.strip()) for pattern in datetime_patterns)


def analyze_csv_types(csv_path: Path) -> dict[str, str]:
    """Analyze a CSV file and detect data types for each column.

    Args:
        csv_path: Path to the CSV file

    Returns:
        Dictionary mapping column names to detected types
    """
    column_values: dict[str, list[str]] = {}

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            return {}

        # Initialize empty lists for each column
        for col in reader.fieldnames:
            column_values[col] = []

        # Collect values for each column
        for row in reader:
            for col in reader.fieldnames:
                if col in row and row[col]:
                    column_values[col].append(row[col])

    # Detect type for each column
    return {col: detect_column_type(values) for col, values in column_values.items()}


def suggest_id_column(columns: list[str]) -> str:
    """Suggest which column should be the ID column.

    Args:
        columns: List of column names

    Returns:
        Name of suggested ID column
    """
    # Common ID column names (in priority order)
    id_candidates = ["id", "uuid", "key", "code"]

    # Check for exact matches
    lower_columns = {col.lower(): col for col in columns}
    for candidate in id_candidates:
        if candidate in lower_columns:
            return lower_columns[candidate]

    # Check for columns ending with _id
    for col in columns:
        if col.lower().endswith("_id"):
            return col

    # Default to first column
    return columns[0] if columns else "id"
