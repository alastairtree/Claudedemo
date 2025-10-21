"""Tests for CLI commands."""

import csv
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
        assert "Sync a CSV file" in result.output
        assert "FILE_PATH" in result.output
        assert "CONFIG" in result.output
        assert "JOB" in result.output

    def test_sync_missing_arguments(self, cli_runner: CliRunner) -> None:
        """Test sync without required arguments fails."""
        result = cli_runner.invoke(main, ["sync"])
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_sync_nonexistent_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with nonexistent CSV file fails."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test:
    target_table: test
    id_mapping:
      id: id
""")

        nonexistent = tmp_path / "doesnotexist.csv"

        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(nonexistent),
                str(config_file),
                "test",
                "--db-url",
                "postgresql://localhost/test",
            ],
        )
        assert result.exit_code != 0

    def test_sync_nonexistent_config(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with nonexistent config file fails."""
        csv_file = tmp_path / "test.csv"
        csv_file.touch()

        nonexistent_config = tmp_path / "nonexistent.yaml"

        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(csv_file),
                str(nonexistent_config),
                "test",
                "--db-url",
                "postgresql://localhost/test",
            ],
        )
        assert result.exit_code != 0

    def test_sync_invalid_job_name(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with invalid job name fails gracefully."""
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerow({"id": "1", "value": "test"})

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  real_job:
    target_table: test
    id_mapping:
      id: id
""")

        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(csv_file),
                str(config_file),
                "nonexistent_job",
                "--db-url",
                "postgresql://localhost/test",
            ],
        )
        assert result.exit_code != 0
        assert "Job 'nonexistent_job' not found" in result.output
        assert "Available jobs: real_job" in result.output

    def test_sync_missing_database_url(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync without database URL fails."""
        csv_file = tmp_path / "test.csv"
        csv_file.touch()

        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test:
    target_table: test
    id_mapping:
      id: id
""")

        result = cli_runner.invoke(main, ["sync", str(csv_file), str(config_file), "test"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()
