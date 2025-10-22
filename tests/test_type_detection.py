"""Tests for type detection module."""

import csv
from pathlib import Path

from data_sync.type_detection import (
    analyze_csv_types,
    detect_column_type,
    suggest_id_column,
)


class TestDetectColumnType:
    """Test suite for detect_column_type function."""

    def test_detect_integer(self) -> None:
        """Test detection of integer values."""
        values = ["1", "2", "3", "100", "-5"]
        assert detect_column_type(values) == "integer"

    def test_detect_float(self) -> None:
        """Test detection of float values."""
        values = ["1.5", "2.3", "3.14", "100.0", "-5.5"]
        assert detect_column_type(values) == "float"

    def test_detect_date(self) -> None:
        """Test detection of date values."""
        values = ["2024-01-15", "2024-02-20", "2023-12-31"]
        assert detect_column_type(values) == "date"

    def test_detect_datetime(self) -> None:
        """Test detection of datetime values."""
        values = ["2024-01-15 10:30:00", "2024-02-20 14:45:30"]
        assert detect_column_type(values) == "datetime"

    def test_detect_varchar(self) -> None:
        """Test detection of short text (varchar)."""
        values = ["Alice", "Bob", "Charlie"]
        result = detect_column_type(values)
        assert result.startswith("varchar(")

    def test_detect_text(self) -> None:
        """Test detection of long text."""
        values = ["This is a very long text" * 20]
        assert detect_column_type(values) == "text"

    def test_empty_values(self) -> None:
        """Test with empty values."""
        assert detect_column_type([]) == "text"
        assert detect_column_type([""]) == "text"
        assert detect_column_type(["", "", ""]) == "text"

    def test_mixed_integers_and_floats(self) -> None:
        """Test that mixed integers and floats are detected as float."""
        values = ["1", "2.5", "3", "4.7"]
        assert detect_column_type(values) == "float"


class TestSuggestIdColumn:
    """Test suite for suggest_id_column function."""

    def test_suggest_id(self) -> None:
        """Test suggesting column named 'id'."""
        columns = ["name", "id", "value"]
        assert suggest_id_column(columns) == "id"

    def test_suggest_uuid(self) -> None:
        """Test suggesting column named 'uuid'."""
        columns = ["name", "uuid", "value"]
        assert suggest_id_column(columns) == "uuid"

    def test_suggest_with_id_suffix(self) -> None:
        """Test suggesting column ending with '_id'."""
        columns = ["name", "user_id", "value"]
        assert suggest_id_column(columns) == "user_id"

    def test_suggest_first_column(self) -> None:
        """Test default to first column if no ID-like column found."""
        columns = ["name", "value", "description"]
        assert suggest_id_column(columns) == "name"

    def test_case_insensitive(self) -> None:
        """Test that ID detection is case insensitive."""
        columns = ["Name", "ID", "Value"]
        assert suggest_id_column(columns) == "ID"


class TestAnalyzeCsvTypes:
    """Test suite for analyze_csv_types function."""

    def test_analyze_simple_csv(self, tmp_path: Path) -> None:
        """Test analyzing a simple CSV file."""
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "age", "price"])
            writer.writeheader()
            writer.writerow({"id": "1", "name": "Alice", "age": "25", "price": "19.99"})
            writer.writerow({"id": "2", "name": "Bob", "age": "30", "price": "29.99"})

        types = analyze_csv_types(csv_file)

        assert types["id"] == "integer"
        assert types["name"].startswith("varchar(")
        assert types["age"] == "integer"
        assert types["price"] == "float"

    def test_analyze_empty_csv(self, tmp_path: Path) -> None:
        """Test analyzing an empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name"])
            writer.writeheader()

        types = analyze_csv_types(csv_file)

        # Empty columns should default to text
        assert types["id"] == "text"
        assert types["name"] == "text"

    def test_analyze_with_nulls(self, tmp_path: Path) -> None:
        """Test analyzing CSV with NULL/empty values."""
        csv_file = tmp_path / "nulls.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "10"})
            writer.writerow({"id": "2", "value": ""})
            writer.writerow({"id": "3", "value": "20"})

        types = analyze_csv_types(csv_file)

        assert types["id"] == "integer"
        assert types["value"] == "integer"  # Should ignore empty values
