"""Test auto-detection of job when config has only one job."""

from pathlib import Path

import pytest

from data_sync.config import SyncConfig


def test_get_job_or_auto_detect_single_job(tmp_path: Path) -> None:
    """Test auto-detection when config has exactly one job."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
jobs:
  my_job:
    target_table: test_table
    id_mapping:
      id: db_id
    columns:
      name: db_name
""")

    config = SyncConfig.from_yaml(config_file)

    # Auto-detect (no job name provided)
    result = config.get_job_or_auto_detect(None)
    assert result is not None
    job, job_name = result
    assert job_name == "my_job"
    assert job.target_table == "test_table"


def test_get_job_or_auto_detect_explicit_job_name(tmp_path: Path) -> None:
    """Test explicit job name works even with single job."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
jobs:
  my_job:
    target_table: test_table
    id_mapping:
      id: db_id
    columns:
      name: db_name
""")

    config = SyncConfig.from_yaml(config_file)

    # Explicit job name
    result = config.get_job_or_auto_detect("my_job")
    assert result is not None
    job, job_name = result
    assert job_name == "my_job"
    assert job.target_table == "test_table"


def test_get_job_or_auto_detect_multiple_jobs_requires_name(tmp_path: Path) -> None:
    """Test that multiple jobs require explicit job name."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
jobs:
  job1:
    target_table: table1
    id_mapping:
      id: db_id
    columns:
      name: db_name
  job2:
    target_table: table2
    id_mapping:
      id: db_id
    columns:
      email: db_email
""")

    config = SyncConfig.from_yaml(config_file)

    # Auto-detect should fail with multiple jobs
    with pytest.raises(ValueError, match="Config contains 2 jobs"):
        config.get_job_or_auto_detect(None)

    # Explicit job name should work
    result = config.get_job_or_auto_detect("job1")
    assert result is not None
    job, job_name = result
    assert job_name == "job1"
    assert job.target_table == "table1"


def test_get_job_or_auto_detect_nonexistent_job(tmp_path: Path) -> None:
    """Test that nonexistent job returns None."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
jobs:
  my_job:
    target_table: test_table
    id_mapping:
      id: db_id
    columns:
      name: db_name
""")

    config = SyncConfig.from_yaml(config_file)

    # Nonexistent job
    result = config.get_job_or_auto_detect("nonexistent")
    assert result is None


def test_get_job_or_auto_detect_empty_config(tmp_path: Path) -> None:
    """Test that empty config returns None."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("jobs: {}")

    config = SyncConfig.from_yaml(config_file)

    # Empty config
    result = config.get_job_or_auto_detect(None)
    assert result is None
