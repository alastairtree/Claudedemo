"""Tests for the prepare command."""

from pathlib import Path

import pytest

from data_sync.cli_prepare import generate_job_name_from_filename


class TestGenerateJobNameFromFilename:
    """Tests for the generate_job_name_from_filename function."""

    def test_removes_extension(self) -> None:
        """Test that file extension is removed."""
        assert generate_job_name_from_filename("data.csv") == "data"
        assert generate_job_name_from_filename("report.xlsx") == "report"

    def test_removes_numbers(self) -> None:
        """Test that numbers are removed."""
        assert generate_job_name_from_filename("sales_2024.csv") == "sales"
        assert generate_job_name_from_filename("data123.csv") == "data"
        assert generate_job_name_from_filename("report_456_final.csv") == "report_final"

    def test_collapses_underscores(self) -> None:
        """Test that multiple underscores become single."""
        assert generate_job_name_from_filename("user__info.csv") == "user_info"
        assert generate_job_name_from_filename("test___data.csv") == "test_data"

    def test_collapses_hyphens(self) -> None:
        """Test that multiple hyphens become single."""
        assert generate_job_name_from_filename("test--file.csv") == "test-file"
        assert generate_job_name_from_filename("data---report.csv") == "data-report"

    def test_strips_trailing_separators(self) -> None:
        """Test that trailing underscores and hyphens are stripped."""
        assert generate_job_name_from_filename("data_123.csv") == "data"
        assert generate_job_name_from_filename("test-456.csv") == "test"
        assert generate_job_name_from_filename("_data_.csv") == "data"
        assert generate_job_name_from_filename("-test-.csv") == "test"

    def test_converts_to_lowercase(self) -> None:
        """Test that names are converted to lowercase."""
        assert generate_job_name_from_filename("SalesData.csv") == "salesdata"
        assert generate_job_name_from_filename("USER_INFO.csv") == "user_info"

    def test_combined_transformations(self) -> None:
        """Test multiple transformations together."""
        assert generate_job_name_from_filename("Sales_Data_2024.csv") == "sales_data"
        assert generate_job_name_from_filename("user__info__123.csv") == "user_info"
        assert generate_job_name_from_filename("Test--File--456.csv") == "test-file"
        assert generate_job_name_from_filename("REPORT__2024__Q1.csv") == "report_q"

    def test_empty_after_cleaning(self) -> None:
        """Test that empty strings after cleaning default to 'job'."""
        assert generate_job_name_from_filename("123.csv") == "job"
        assert generate_job_name_from_filename("___456___.csv") == "job"

    def test_with_path(self) -> None:
        """Test that paths are handled correctly."""
        assert generate_job_name_from_filename("/path/to/data.csv") == "data"
        assert generate_job_name_from_filename("../files/report_2024.csv") == "report"


class TestPrepareCommandIntegration:
    """Integration tests for the prepare command."""

    @pytest.fixture
    def sample_csv(self, tmp_path: Path) -> Path:
        """Create a sample CSV file."""
        csv_file = tmp_path / "test_data_2024.csv"
        csv_file.write_text(
            "id,name,age,created_date\n1,Alice,30,2024-01-01\n2,Bob,25,2024-01-02\n"
        )
        return csv_file

    @pytest.fixture
    def second_csv(self, tmp_path: Path) -> Path:
        """Create a second sample CSV file."""
        csv_file = tmp_path / "user__info__123.csv"
        csv_file.write_text(
            "user_id,email,status\n1,alice@test.com,active\n2,bob@test.com,inactive\n"
        )
        return csv_file

    def test_prepare_single_file_auto_generated_name(
        self, sample_csv: Path, tmp_path: Path
    ) -> None:
        """Test prepare with single file and auto-generated job name."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(prepare, [str(sample_csv), "--config", str(config_file)])

        assert result.exit_code == 0
        assert config_file.exists()

        # Load config and check job was created with auto-generated name
        from data_sync.config import SyncConfig

        config = SyncConfig.from_yaml(config_file)
        # Expected name: "test_data_2024.csv" -> "test_data"
        assert "test_data" in config.jobs
        job = config.jobs["test_data"]
        assert job.target_table == "test_data"

    def test_prepare_single_file_custom_name(self, sample_csv: Path, tmp_path: Path) -> None:
        """Test prepare with single file and custom job name."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(
            prepare, [str(sample_csv), "--config", str(config_file), "--job", "my_custom_job"]
        )

        assert result.exit_code == 0
        assert config_file.exists()

        # Load config and check job was created with custom name
        from data_sync.config import SyncConfig

        config = SyncConfig.from_yaml(config_file)
        assert "my_custom_job" in config.jobs
        assert "test_data" not in config.jobs

    def test_prepare_multiple_files_auto_generated_names(
        self, sample_csv: Path, second_csv: Path, tmp_path: Path
    ) -> None:
        """Test prepare with multiple files and auto-generated job names."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(
            prepare, [str(sample_csv), str(second_csv), "--config", str(config_file)]
        )

        assert result.exit_code == 0
        assert config_file.exists()

        # Load config and check both jobs were created
        from data_sync.config import SyncConfig

        config = SyncConfig.from_yaml(config_file)
        # Expected names: "test_data_2024.csv" -> "test_data", "user__info__123.csv" -> "user_info"
        assert "test_data" in config.jobs
        assert "user_info" in config.jobs

    def test_prepare_multiple_files_with_custom_name_fails(
        self, sample_csv: Path, second_csv: Path, tmp_path: Path
    ) -> None:
        """Test that specifying job name with multiple files fails."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(
            prepare,
            [str(sample_csv), str(second_csv), "--config", str(config_file), "--job", "custom_job"],
        )

        assert result.exit_code != 0
        assert "Cannot specify job name when processing multiple files" in result.output

    def test_prepare_updates_existing_with_force(self, sample_csv: Path, tmp_path: Path) -> None:
        """Test that prepare can update existing job with --force."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        # First run
        result1 = runner.invoke(prepare, [str(sample_csv), "--config", str(config_file)])
        assert result1.exit_code == 0

        # Second run without force should fail
        result2 = runner.invoke(prepare, [str(sample_csv), "--config", str(config_file)])
        assert result2.exit_code != 0
        assert "Use --force to overwrite" in result2.output

        # Third run with force should succeed
        result3 = runner.invoke(prepare, [str(sample_csv), "--config", str(config_file), "--force"])
        assert result3.exit_code == 0


class TestDetectFilenamePatterns:
    """Tests for the detect_filename_patterns function."""

    def test_detect_yyyymmdd_pattern(self) -> None:
        """Test detecting YYYYMMDD date pattern."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("data_20240115.csv")
        assert result is not None
        assert result.template == "data_[date].csv"
        assert "date" in result.columns
        assert result.columns["date"].db_column == "file_date"
        assert result.columns["date"].data_type == "date"
        assert result.columns["date"].use_to_delete_old_rows is True

    def test_detect_yyyy_mm_dd_pattern(self) -> None:
        """Test detecting YYYY-MM-DD date pattern."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("sales_2024-01-15.csv")
        assert result is not None
        assert result.template == "sales_[date].csv"
        assert "date" in result.columns

    def test_detect_yyyy_mm_dd_underscore_pattern(self) -> None:
        """Test detecting YYYY_MM_DD date pattern."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("report_2024_12_31.csv")
        assert result is not None
        assert result.template == "report_[date].csv"
        assert "date" in result.columns

    def test_no_pattern_detected(self) -> None:
        """Test when no date pattern is found."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("simple_data.csv")
        assert result is None

    def test_date_in_middle_of_filename(self) -> None:
        """Test date pattern in middle of filename."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("prefix_20241225_suffix.csv")
        assert result is not None
        assert result.template == "prefix_[date]_suffix.csv"

    def test_multiple_date_formats_prefer_first(self) -> None:
        """Test that first matching pattern is used when multiple exist."""
        from data_sync.cli_prepare import detect_filename_patterns

        # YYYYMMDD appears first in search order
        result = detect_filename_patterns("data_20240115_2024-01-15.csv")
        assert result is not None
        # Should match YYYYMMDD (first pattern)
        assert "[date]_2024-01-15.csv" in result.template

    def test_extraction_works_with_detected_pattern(self) -> None:
        """Test that the detected pattern can actually extract values."""
        from data_sync.cli_prepare import detect_filename_patterns

        result = detect_filename_patterns("sales_20240315.csv")
        assert result is not None

        # Test that extraction works
        values = result.extract_values_from_filename("sales_20240315.csv")
        assert values is not None
        assert values["date"] == "20240315"

        # Test with different date
        values2 = result.extract_values_from_filename("sales_20241201.csv")
        assert values2 is not None
        assert values2["date"] == "20241201"


class TestPrepareWithFilenameDetection:
    """Integration tests for prepare command with filename pattern detection."""

    @pytest.fixture
    def dated_csv(self, tmp_path: Path) -> Path:
        """Create a CSV file with date in filename."""
        csv_file = tmp_path / "data_20240115.csv"
        csv_file.write_text("id,value\n1,100\n2,200\n")
        return csv_file

    def test_prepare_detects_and_adds_filename_to_column(
        self, dated_csv: Path, tmp_path: Path
    ) -> None:
        """Test that prepare command detects date pattern and adds filename_to_column."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare
        from data_sync.config import SyncConfig

        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(prepare, [str(dated_csv), "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Detected date pattern in filename" in result.output

        # Load config and verify filename_to_column was added
        config = SyncConfig.from_yaml(config_file)
        job = config.jobs["data"]

        assert job.filename_to_column is not None
        assert job.filename_to_column.template == "data_[date].csv"
        assert "date" in job.filename_to_column.columns
        assert job.filename_to_column.columns["date"].use_to_delete_old_rows is True

    def test_prepare_no_detection_for_simple_filename(
        self, sample_csv: Path, tmp_path: Path
    ) -> None:
        """Test that no filename_to_column is added for files without date patterns."""
        from click.testing import CliRunner

        from data_sync.cli_prepare import prepare
        from data_sync.config import SyncConfig

        # sample_csv is from parent class fixture: "test_data_2024.csv"
        # This should match YYYYMMDD pattern (2024)
        config_file = tmp_path / "config.yaml"
        runner = CliRunner()

        result = runner.invoke(prepare, [str(sample_csv), "--config", str(config_file)])

        assert result.exit_code == 0

        # Actually this file HAS a date pattern (2024), so it should detect it
        config = SyncConfig.from_yaml(config_file)
        job = list(config.jobs.values())[0]
        # The fixture creates "test_data_2024.csv" which contains "2024" - 4 digits
        # But our pattern looks for 8 digits (YYYYMMDD), so this should NOT match
        # Let me check... actually "2024" is only 4 digits, not 8, so YYYYMMDD won't match

        # But wait, let me re-read the fixture. It's "test_data_2024.csv"
        # Our patterns are:
        # - YYYYMMDD: r"(\d{8})" - requires 8 digits
        # - YYYY-MM-DD: r"(\d{4}-\d{2}-\d{2})" - requires dashes
        # - YYYY_MM_DD: r"(\d{4}_\d{2}_\d{2})" - requires underscores
        # So "2024" alone won't match any pattern
        assert job.filename_to_column is None

    @pytest.fixture
    def sample_csv(self, tmp_path: Path) -> Path:
        """Create a simple CSV file without date pattern."""
        csv_file = tmp_path / "simple_data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
        return csv_file
