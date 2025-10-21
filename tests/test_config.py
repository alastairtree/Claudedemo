"""Tests for config module."""

from pathlib import Path

import pytest

from data_sync.config import SyncConfig


class TestConfigParsing:
    """Test suite for config file parsing."""

    def test_load_valid_config(self, tmp_path: Path) -> None:
        """Test loading a valid configuration file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      csv_column: user_id
      db_column: id
    columns:
      - csv_column: name
        db_column: full_name
      - csv_column: email
        db_column: email
""")

        config = SyncConfig.from_yaml(config_file)
        assert len(config.jobs) == 1
        assert "test_job" in config.jobs

        job = config.get_job("test_job")
        assert job is not None
        assert job.name == "test_job"
        assert job.target_table == "users"
        assert job.id_mapping.csv_column == "user_id"
        assert job.id_mapping.db_column == "id"
        assert len(job.columns) == 2

    def test_config_with_no_columns(self, tmp_path: Path) -> None:
        """Test config where no specific columns are listed (sync all)."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  sync_all:
    target_table: products
    id_mapping:
      csv_column: product_id
      db_column: id
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("sync_all")
        assert job is not None
        assert job.columns == []

    def test_config_file_not_found(self, tmp_path: Path) -> None:
        """Test error when config file doesn't exist."""
        config_file = tmp_path / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError, match="Config file not found"):
            SyncConfig.from_yaml(config_file)

    def test_config_missing_jobs(self, tmp_path: Path) -> None:
        """Test error when config file doesn't have jobs section."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("other_section: value")

        with pytest.raises(ValueError, match="must contain 'jobs' section"):
            SyncConfig.from_yaml(config_file)

    def test_config_missing_target_table(self, tmp_path: Path) -> None:
        """Test error when job is missing target_table."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  bad_job:
    id_mapping:
      csv_column: id
      db_column: id
""")

        with pytest.raises(ValueError, match="missing 'target_table'"):
            SyncConfig.from_yaml(config_file)

    def test_config_missing_id_mapping(self, tmp_path: Path) -> None:
        """Test error when job is missing id_mapping."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  bad_job:
    target_table: users
""")

        with pytest.raises(ValueError, match="missing 'id_mapping'"):
            SyncConfig.from_yaml(config_file)

    def test_get_nonexistent_job(self, tmp_path: Path) -> None:
        """Test getting a job that doesn't exist."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  job1:
    target_table: table1
    id_mapping:
      csv_column: id
      db_column: id
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("nonexistent")
        assert job is None
