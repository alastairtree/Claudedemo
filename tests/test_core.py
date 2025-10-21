"""Tests for core business logic."""

from pathlib import Path

import pytest

from data_sync.core import get_file_type, sync_file


class TestGetFileType:
    """Test suite for get_file_type function."""

    def test_csv_file(self, tmp_path: Path) -> None:
        """Test CSV file type detection."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()
        assert get_file_type(csv_file) == "csv"

    def test_cdf_file(self, tmp_path: Path) -> None:
        """Test CDF file type detection."""
        cdf_file = tmp_path / "science.cdf"
        cdf_file.touch()
        assert get_file_type(cdf_file) == "cdf"

    def test_uppercase_extension(self, tmp_path: Path) -> None:
        """Test file type detection with uppercase extension."""
        csv_file = tmp_path / "data.CSV"
        csv_file.touch()
        assert get_file_type(csv_file) == "csv"

    def test_mixed_case_extension(self, tmp_path: Path) -> None:
        """Test file type detection with mixed case extension."""
        cdf_file = tmp_path / "data.CdF"
        cdf_file.touch()
        assert get_file_type(cdf_file) == "cdf"

    def test_unsupported_extension(self, tmp_path: Path) -> None:
        """Test that unsupported file extensions raise ValueError."""
        txt_file = tmp_path / "data.txt"
        txt_file.touch()

        with pytest.raises(ValueError, match="Unsupported file extension"):
            get_file_type(txt_file)

    def test_no_extension(self, tmp_path: Path) -> None:
        """Test file with no extension raises ValueError."""
        no_ext_file = tmp_path / "data"
        no_ext_file.touch()

        with pytest.raises(ValueError, match="Unsupported file extension"):
            get_file_type(no_ext_file)


class TestSyncFile:
    """Test suite for sync_file function."""

    def test_sync_csv_file(self, tmp_path: Path) -> None:
        """Test syncing a CSV file."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        result = sync_file(csv_file)

        assert result["file_path"] == str(csv_file)
        assert result["file_type"] == "csv"
        assert result["records_processed"] == 0  # Placeholder returns 0

    def test_sync_cdf_file(self, tmp_path: Path) -> None:
        """Test syncing a CDF file."""
        cdf_file = tmp_path / "science.cdf"
        cdf_file.touch()

        result = sync_file(cdf_file)

        assert result["file_path"] == str(cdf_file)
        assert result["file_type"] == "cdf"
        assert result["records_processed"] == 0  # Placeholder returns 0

    def test_sync_nonexistent_file(self, tmp_path: Path) -> None:
        """Test syncing a file that doesn't exist raises FileNotFoundError."""
        nonexistent = tmp_path / "doesnotexist.csv"

        with pytest.raises(FileNotFoundError, match="File not found"):
            sync_file(nonexistent)

    def test_sync_unsupported_file(self, tmp_path: Path) -> None:
        """Test syncing an unsupported file type raises ValueError."""
        txt_file = tmp_path / "data.txt"
        txt_file.touch()

        with pytest.raises(ValueError, match="Unsupported file extension"):
            sync_file(txt_file)

    def test_sync_file_with_uppercase_extension(self, tmp_path: Path) -> None:
        """Test syncing file with uppercase extension."""
        csv_file = tmp_path / "DATA.CSV"
        csv_file.touch()

        result = sync_file(csv_file)

        assert result["file_type"] == "csv"

    def test_sync_result_structure(self, tmp_path: Path) -> None:
        """Test that sync result has correct structure."""
        csv_file = tmp_path / "test.csv"
        csv_file.touch()

        result = sync_file(csv_file)

        # Check all required keys are present
        assert "file_path" in result
        assert "file_type" in result
        assert "records_processed" in result

        # Check types
        assert isinstance(result["file_path"], str)
        assert isinstance(result["file_type"], str)
        assert isinstance(result["records_processed"], int)
