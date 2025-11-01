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
        assert "Sync a CSV" in result.output
        assert "FILE_PATH" in result.output
        assert "CONFIG" in result.output
        assert "--job" in result.output

    def test_sync_missing_arguments(self, cli_runner: CliRunner) -> None:
        """Test sync without required arguments fails."""
        result = cli_runner.invoke(main, ["sync"])
        assert result.exit_code != 0
        assert "Missing argument" in result.output

    def test_sync_nonexistent_file(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with nonexistent CSV file fails."""
        from tests.test_helpers import create_config_file

        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "test", "test", {"id": "id"})

        nonexistent = tmp_path / "doesnotexist.csv"

        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(nonexistent),
                str(config_file),
                "--job",
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
                "--job",
                "test",
                "--db-url",
                "postgresql://localhost/test",
            ],
        )
        assert result.exit_code != 0

    def test_sync_invalid_job_name(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with invalid job name fails gracefully."""
        from tests.test_helpers import create_config_file, create_csv_file

        csv_file = tmp_path / "test.csv"
        create_csv_file(csv_file, ["id", "value"], [{"id": "1", "value": "test"}])

        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "real_job", "test", {"id": "id"})

        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(csv_file),
                str(config_file),
                "--job",
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
        from tests.test_helpers import create_config_file

        csv_file = tmp_path / "test.csv"
        csv_file.touch()

        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "test", "test", {"id": "id"})

        result = cli_runner.invoke(main, ["sync", str(csv_file), str(config_file), "test"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestSuggestIndexes:
    """Test suite for index suggestion functionality."""

    def test_suggest_indexes_for_date_columns(self) -> None:
        """Test that date columns get descending indexes."""
        from data_sync.cli_prepare import suggest_indexes

        columns = {
            "id": ("integer", False),
            "created_date": ("date", False),
            "updated_at": ("datetime", True),
            "name": ("text", True),
        }

        indexes = suggest_indexes(columns, "id")

        # Should have 2 indexes (for date and datetime columns)
        assert len(indexes) == 2

        # Find the indexes by name
        created_idx = next(idx for idx in indexes if idx.name == "idx_created_date")
        updated_idx = next(idx for idx in indexes if idx.name == "idx_updated_at")

        # Both should be descending
        assert created_idx.columns[0].column == "created_date"
        assert created_idx.columns[0].order == "DESC"
        assert updated_idx.columns[0].column == "updated_at"
        assert updated_idx.columns[0].order == "DESC"

    def test_suggest_indexes_for_id_key_columns(self) -> None:
        """Test that columns ending in _id or _key get ascending indexes."""
        from data_sync.cli_prepare import suggest_indexes

        columns = {
            "id": ("integer", False),
            "user_id": ("integer", False),
            "account_key": ("text", True),
            "name": ("text", False),
        }

        indexes = suggest_indexes(columns, "id")

        # Should have 2 indexes (for user_id and account_key)
        assert len(indexes) == 2

        # Find the indexes by name
        user_idx = next(idx for idx in indexes if idx.name == "idx_user_id")
        account_idx = next(idx for idx in indexes if idx.name == "idx_account_key")

        # Both should be ascending
        assert user_idx.columns[0].column == "user_id"
        assert user_idx.columns[0].order == "ASC"
        assert account_idx.columns[0].column == "account_key"
        assert account_idx.columns[0].order == "ASC"

    def test_suggest_indexes_excludes_id_column(self) -> None:
        """Test that the ID column doesn't get an index."""
        from data_sync.cli_prepare import suggest_indexes

        columns = {
            "user_id": ("integer", False),
            "created_at": ("datetime", False),
        }

        indexes = suggest_indexes(columns, "user_id")

        # Should only have index for created_at, not user_id
        assert len(indexes) == 1
        assert indexes[0].name == "idx_created_at"

    def test_suggest_indexes_mixed_columns(self) -> None:
        """Test index suggestion with mixed column types."""
        from data_sync.cli_prepare import suggest_indexes

        columns = {
            "order_id": ("integer", False),
            "customer_id": ("integer", False),
            "product_key": ("text", True),
            "order_date": ("date", False),
            "delivery_date": ("datetime", True),
            "total_amount": ("float", False),
            "notes": ("text", True),
        }

        indexes = suggest_indexes(columns, "order_id")

        # Should have 4 indexes: customer_id, product_key, order_date, delivery_date
        assert len(indexes) == 4

        index_names = {idx.name for idx in indexes}
        assert "idx_customer_id" in index_names
        assert "idx_product_key" in index_names
        assert "idx_order_date" in index_names
        assert "idx_delivery_date" in index_names

        # Check orders
        for idx in indexes:
            if idx.name in ["idx_order_date", "idx_delivery_date"]:
                assert idx.columns[0].order == "DESC"
            else:
                assert idx.columns[0].order == "ASC"

    def test_suggest_indexes_no_indexable_columns(self) -> None:
        """Test with no columns that should be indexed."""
        from data_sync.cli_prepare import suggest_indexes

        columns = {
            "id": ("integer", False),
            "name": ("text", True),
            "description": ("text", True),
        }

        indexes = suggest_indexes(columns, "id")

        assert len(indexes) == 0


class TestDryRunCommand:
    """Test suite for dry-run functionality."""

    def test_sync_help_includes_dry_run(self, cli_runner: CliRunner) -> None:
        """Test that sync command help includes --dry-run option."""
        result = cli_runner.invoke(main, ["sync", "--help"])
        assert result.exit_code == 0
        assert "--dry-run" in result.output
        assert "Simulate the sync without making" in result.output

    def test_sync_dry_run_flag_with_sqlite(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test sync with --dry-run flag using SQLite."""
        from tests.test_helpers import create_config_file, create_csv_file

        # Create a simple CSV file
        csv_file = tmp_path / "test.csv"
        create_csv_file(
            csv_file,
            ["id", "name", "value"],
            [
                {"id": "1", "name": "Alice", "value": "100"},
                {"id": "2", "name": "Bob", "value": "200"},
            ],
        )

        # Create a config file
        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "test_job", "test_table", {"id": "id"})

        # Create an SQLite database URL
        db_file = tmp_path / "test.db"
        db_url = f"sqlite:///{db_file}"

        # Run sync with dry-run flag
        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(csv_file),
                str(config_file),
                "--job",
                "test_job",
                "--db-url",
                db_url,
                "--dry-run",
            ],
        )

        # Check that dry-run executed successfully
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "Dry-run Summary" in result.output
        assert "would be inserted/updated" in result.output
        assert "no changes made to database" in result.output

        # Verify no tables were created (connection may exist for SQLite)
        if db_file.exists():
            from tests.db_test_utils import execute_query

            tables = execute_query(db_url, "SELECT name FROM sqlite_master WHERE type='table'")
            assert len(tables) == 0, "No tables should have been created during dry-run"

    def test_sync_without_dry_run_creates_data(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        """Test that regular sync (without --dry-run) creates data."""
        from tests.test_helpers import create_config_file, create_csv_file

        # Create a simple CSV file
        csv_file = tmp_path / "test.csv"
        create_csv_file(csv_file, ["id", "name"], [{"id": "1", "name": "Alice"}])

        # Create a config file
        config_file = tmp_path / "config.yaml"
        create_config_file(config_file, "test_job", "test_table", {"id": "id"})

        # Create an SQLite database URL
        db_file = tmp_path / "test.db"
        db_url = f"sqlite:///{db_file}"

        # Run sync WITHOUT dry-run flag
        result = cli_runner.invoke(
            main,
            [
                "sync",
                str(csv_file),
                str(config_file),
                "--job",
                "test_job",
                "--db-url",
                db_url,
            ],
        )

        # Check that sync executed successfully
        assert result.exit_code == 0
        assert "Successfully synced" in result.output

        # Verify database was created and contains data
        assert db_file.exists()

        from tests.db_test_utils import execute_query

        count = execute_query(db_url, "SELECT COUNT(*) FROM test_table")
        assert count[0][0] == 1
