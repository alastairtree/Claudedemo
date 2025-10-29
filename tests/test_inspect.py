"""Tests for the inspect command."""

from pathlib import Path

import pytest
from click.testing import CliRunner

from data_sync.cli import main


class TestInspectCommand:
    """Test suite for inspect command."""

    def test_inspect_help(self, cli_runner: CliRunner) -> None:
        """Test that inspect command has help text."""
        result = cli_runner.invoke(main, ["inspect", "--help"])
        assert result.exit_code == 0
        assert "Inspect CSV or CDF files" in result.output
        assert "--max-records" in result.output
        assert "-n" in result.output

    def test_inspect_missing_file(self, cli_runner: CliRunner) -> None:
        """Test inspect with non-existent file."""
        result = cli_runner.invoke(main, ["inspect", "nonexistent.csv"])
        assert result.exit_code != 0

    def test_inspect_no_arguments(self, cli_runner: CliRunner) -> None:
        """Test inspect without arguments."""
        result = cli_runner.invoke(main, ["inspect"])
        assert result.exit_code != 0

    def test_inspect_csv_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect with a CSV file."""
        # Create a test CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n3,Carol,35\n")

        result = cli_runner.invoke(main, ["inspect", str(csv_file)])
        assert result.exit_code == 0
        assert "CSV File: test.csv" in result.output
        assert "Columns (3):" in result.output
        assert "id, name, age" in result.output
        assert "Alice" in result.output
        assert "3 rows total, 3 columns" in result.output

    def test_inspect_csv_with_custom_records(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect CSV with custom record count."""
        csv_file = tmp_path / "test.csv"
        csv_content = "id,value\n" + "\n".join(f"{i},{i * 10}" for i in range(1, 11))
        csv_file.write_text(csv_content)

        result = cli_runner.invoke(main, ["inspect", str(csv_file), "--max-records", "3"])
        assert result.exit_code == 0
        assert "first 3" in result.output
        assert "10 rows total" in result.output

    def test_inspect_empty_csv(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect with empty CSV file."""
        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("id,name,age\n")

        result = cli_runner.invoke(main, ["inspect", str(csv_file)])
        assert result.exit_code == 0
        assert "0 rows total" in result.output

    def test_inspect_csv_no_header(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect with CSV file with no content."""
        csv_file = tmp_path / "no_header.csv"
        csv_file.write_text("")

        result = cli_runner.invoke(main, ["inspect", str(csv_file)])
        # Should handle gracefully
        assert "Error" in result.output or result.exit_code != 0

    def test_inspect_unsupported_file_type(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect with unsupported file type."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Hello, world!")

        result = cli_runner.invoke(main, ["inspect", str(txt_file)])
        assert result.exit_code == 0
        assert "Unsupported file type" in result.output

    def test_inspect_multiple_files(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test inspect with multiple files."""
        csv1 = tmp_path / "file1.csv"
        csv1.write_text("id,name\n1,Alice\n")

        csv2 = tmp_path / "file2.csv"
        csv2.write_text("id,value\n1,100\n")

        result = cli_runner.invoke(main, ["inspect", str(csv1), str(csv2)])
        assert result.exit_code == 0
        assert "file1.csv" in result.output
        assert "file2.csv" in result.output
        assert result.output.count("====") >= 2  # Separator between files


class TestInspectCDFFiles:
    """Test suite for inspecting real CDF files."""

    def test_inspect_solar_orbiter_cdf(self, cli_runner: CliRunner) -> None:
        """Test inspect with Solar Orbiter CDF file."""
        cdf_file = Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf")
        if not cdf_file.exists():
            pytest.skip("CDF test file not found")

        result = cli_runner.invoke(main, ["inspect", str(cdf_file), "--max-records", "3"])
        assert result.exit_code == 0

        # Check basic CDF info
        assert "CDF File:" in result.output
        assert "CDF Version:" in result.output
        assert "Encoding:" in result.output
        assert "Majority:" in result.output

        # Check global attributes
        assert "Global Attributes:" in result.output
        assert "Project" in result.output or "PROJECT" in result.output.upper()

        # Check variables
        assert "Variables (" in result.output
        assert "EPOCH" in result.output
        assert "B_RTN" in result.output

        # Check variable details
        assert "Variable Details" in result.output
        assert "Records:" in result.output
        assert "Shape:" in result.output

        # Check that sample data is shown
        assert "more records" in result.output

    def test_inspect_imap_cdf(self, cli_runner: CliRunner) -> None:
        """Test inspect with IMAP CDF file."""
        cdf_file = Path("tests/data/imap_mag_l1c_norm-magi_20251010_v001.cdf")
        if not cdf_file.exists():
            pytest.skip("CDF test file not found")

        result = cli_runner.invoke(main, ["inspect", str(cdf_file), "-n", "5"])
        assert result.exit_code == 0

        # Check basic info
        assert "CDF File:" in result.output
        assert "imap_mag_l1c_norm-magi_20251010_v001.cdf" in result.output

        # Check for expected variables
        assert "vectors" in result.output
        assert "epoch" in result.output

        # Check variable details
        assert "54,366" in result.output or "54366" in result.output  # Record count

    def test_inspect_cdf_with_different_record_counts(self, cli_runner: CliRunner) -> None:
        """Test inspect CDF with different record count options."""
        cdf_file = Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf")
        if not cdf_file.exists():
            pytest.skip("CDF test file not found")

        # Test with 1 record
        result1 = cli_runner.invoke(main, ["inspect", str(cdf_file), "-n", "1"])
        assert result1.exit_code == 0
        assert "1,439 more records" in result1.output or "... 1,439" in result1.output

        # Test with 10 records (default)
        result10 = cli_runner.invoke(main, ["inspect", str(cdf_file)])
        assert result10.exit_code == 0

        # Test with large number
        result100 = cli_runner.invoke(main, ["inspect", str(cdf_file), "-n", "100"])
        assert result100.exit_code == 0

    def test_inspect_cdf_variable_sorting(self, cli_runner: CliRunner) -> None:
        """Test that CDF variables are sorted by record count (descending)."""
        cdf_file = Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf")
        if not cdf_file.exists():
            pytest.skip("CDF test file not found")

        result = cli_runner.invoke(main, ["inspect", str(cdf_file)])
        assert result.exit_code == 0

        # Check that variables with more records appear first in detailed view
        output_lines = result.output.split("\n")
        epoch_index = next(
            i
            for i, line in enumerate(output_lines)
            if line.strip().startswith("EPOCH")
            and "Variable Details" in "\n".join(output_lines[:i])
        )
        lbl_index = next(
            (i for i, line in enumerate(output_lines) if line.strip().startswith("LBL1_B_RTN")),
            None,
        )

        if lbl_index is not None:
            # EPOCH (1440 records) should appear before LBL1_B_RTN (3 records)
            assert epoch_index < lbl_index
