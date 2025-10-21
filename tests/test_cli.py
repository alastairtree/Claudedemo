"""Tests for CLI commands."""

from pathlib import Path

from click.testing import CliRunner

from data_sync import __version__
from data_sync.cli import main


class TestCLIBasics:
    """Test suite for basic CLI functionality."""

    def test_main_group_help(self, cli_runner: CliRunner) -> None:
        """Test main command group shows help."""
        result = cli_runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Sync CSV and CDF" in result.output

    def test_version_option(self, cli_runner: CliRunner) -> None:
        """Test --version flag displays version."""
        result = cli_runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestSyncCommand:
    """Test suite for sync command."""

    def test_sync_help(self, cli_runner: CliRunner) -> None:
        """Test sync command help."""
        result = cli_runner.invoke(main, ["sync", "--help"])
        assert result.exit_code == 0
        assert "Sync a CSV or CDF file" in result.output
        assert "FILE_PATH" in result.output

    def test_sync_csv_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test syncing a CSV file."""
        csv_file = tmp_path / "test.csv"
        csv_file.touch()

        result = cli_runner.invoke(main, ["sync", str(csv_file)])
        assert result.exit_code == 0
        assert "Successfully synced" in result.output
        assert str(csv_file) in result.output

    def test_sync_cdf_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test syncing a CDF file."""
        cdf_file = tmp_path / "science.cdf"
        cdf_file.touch()

        result = cli_runner.invoke(main, ["sync", str(cdf_file)])
        assert result.exit_code == 0
        assert "Successfully synced" in result.output
        assert str(cdf_file) in result.output

    def test_sync_missing_argument(self, cli_runner: CliRunner) -> None:
        """Test sync without required argument fails."""
        result = cli_runner.invoke(main, ["sync"])
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_sync_nonexistent_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with nonexistent file fails."""
        nonexistent = tmp_path / "doesnotexist.csv"

        result = cli_runner.invoke(main, ["sync", str(nonexistent)])
        assert result.exit_code != 0
        # Click validates path existence before our code runs

    def test_sync_unsupported_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with unsupported file type fails."""
        txt_file = tmp_path / "data.txt"
        txt_file.touch()

        result = cli_runner.invoke(main, ["sync", str(txt_file)])
        assert result.exit_code == 1
        assert "Error" in result.output

    def test_sync_shows_records_processed(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync shows number of records processed."""
        csv_file = tmp_path / "data.csv"
        csv_file.touch()

        result = cli_runner.invoke(main, ["sync", str(csv_file)])
        assert result.exit_code == 0
        assert "Records processed" in result.output

    def test_sync_with_uppercase_extension(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync handles uppercase file extensions."""
        csv_file = tmp_path / "data.CSV"
        csv_file.touch()

        result = cli_runner.invoke(main, ["sync", str(csv_file)])
        assert result.exit_code == 0
        assert "Successfully synced" in result.output
