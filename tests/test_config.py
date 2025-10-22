"""Tests for config module."""

from pathlib import Path

import pytest

from data_sync.config import DateMapping, SyncConfig


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
      user_id: id
    columns:
      name: full_name
      email: email
""")

        config = SyncConfig.from_yaml(config_file)
        assert len(config.jobs) == 1
        assert "test_job" in config.jobs

        job = config.get_job("test_job")
        assert job is not None
        assert job.name == "test_job"
        assert job.target_table == "users"
        assert len(job.id_mapping) == 1
        assert job.id_mapping[0].csv_column == "user_id"
        assert job.id_mapping[0].db_column == "id"
        assert len(job.columns) == 2

    def test_config_with_no_columns(self, tmp_path: Path) -> None:
        """Test config where no specific columns are listed (sync all)."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  sync_all:
    target_table: products
    id_mapping:
      product_id: id
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
      id: id
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
      id: id
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("nonexistent")
        assert job is None

    def test_config_with_date_mapping(self, tmp_path: Path) -> None:
        """Test config with date_mapping configured."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  daily_sync:
    target_table: sales
    id_mapping:
      sale_id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'
      db_column: sync_date
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("daily_sync")
        assert job is not None
        assert job.date_mapping is not None
        assert job.date_mapping.filename_regex == r"(\d{4}-\d{2}-\d{2})"
        assert job.date_mapping.db_column == "sync_date"

    def test_config_missing_date_mapping_regex(self, tmp_path: Path) -> None:
        """Test error when date_mapping is missing filename_regex."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  bad_job:
    target_table: sales
    id_mapping:
      id: id
    date_mapping:
      db_column: sync_date
""")

        with pytest.raises(ValueError, match="date_mapping missing 'filename_regex'"):
            SyncConfig.from_yaml(config_file)

    def test_config_missing_date_mapping_column(self, tmp_path: Path) -> None:
        """Test error when date_mapping is missing db_column."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(r"""
jobs:
  bad_job:
    target_table: sales
    id_mapping:
      id: id
    date_mapping:
      filename_regex: '(\d{4}-\d{2}-\d{2})'
""")

        with pytest.raises(ValueError, match="date_mapping missing 'db_column'"):
            SyncConfig.from_yaml(config_file)


class TestDateMapping:
    """Test suite for DateMapping functionality."""

    def test_extract_date_from_filename_basic(self) -> None:
        """Test extracting date from filename with basic pattern."""
        mapping = DateMapping(r"(\d{4}-\d{2}-\d{2})", "sync_date")

        date = mapping.extract_date_from_filename("sales_2024-01-15.csv")
        assert date == "2024-01-15"

    def test_extract_date_from_path(self) -> None:
        """Test extracting date from Path object."""
        mapping = DateMapping(r"(\d{4}-\d{2}-\d{2})", "sync_date")

        path = Path("/data/files/report_2024-03-20.csv")
        date = mapping.extract_date_from_filename(path)
        assert date == "2024-03-20"

    def test_extract_date_different_format(self) -> None:
        """Test extracting date with different format."""
        mapping = DateMapping(r"(\d{8})", "sync_date")

        date = mapping.extract_date_from_filename("data_20240115.csv")
        assert date == "20240115"

    def test_extract_date_not_found(self) -> None:
        """Test when date pattern is not found in filename."""
        mapping = DateMapping(r"(\d{4}-\d{2}-\d{2})", "sync_date")

        date = mapping.extract_date_from_filename("data.csv")
        assert date is None

    def test_extract_date_complex_pattern(self) -> None:
        """Test extracting date with complex pattern."""
        mapping = DateMapping(r"report_(\d{4}_\d{2}_\d{2})_final", "sync_date")

        date = mapping.extract_date_from_filename("report_2024_01_15_final.csv")
        assert date == "2024_01_15"


class TestColumnDataTypes:
    """Test suite for column data types in config."""

    def test_config_with_data_types(self, tmp_path: Path) -> None:
        """Test config with explicit data types."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  typed_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name: full_name
      age:
        db_column: user_age
        type: integer
      salary:
        db_column: monthly_salary
        type: float
      birth_date:
        db_column: dob
        type: date
      bio:
        db_column: biography
        type: text
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("typed_job")

        assert job is not None
        assert len(job.id_mapping) == 1
        assert job.id_mapping[0].data_type is None  # ID has no explicit type
        assert len(job.columns) == 5

        # Check that data types are preserved
        name_col = next(c for c in job.columns if c.csv_column == "name")
        assert name_col.db_column == "full_name"
        assert name_col.data_type is None  # Simple format, no type

        age_col = next(c for c in job.columns if c.csv_column == "age")
        assert age_col.db_column == "user_age"
        assert age_col.data_type == "integer"

        salary_col = next(c for c in job.columns if c.csv_column == "salary")
        assert salary_col.db_column == "monthly_salary"
        assert salary_col.data_type == "float"

        date_col = next(c for c in job.columns if c.csv_column == "birth_date")
        assert date_col.db_column == "dob"
        assert date_col.data_type == "date"

        bio_col = next(c for c in job.columns if c.csv_column == "bio")
        assert bio_col.db_column == "biography"
        assert bio_col.data_type == "text"

    def test_extended_format_without_type(self, tmp_path: Path) -> None:
        """Test extended format with db_column but no type."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name:
        db_column: full_name
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_job")

        assert job is not None
        assert len(job.columns) == 1

        name_col = job.columns[0]
        assert name_col.csv_column == "name"
        assert name_col.db_column == "full_name"
        assert name_col.data_type is None

    def test_invalid_extended_format(self, tmp_path: Path) -> None:
        """Test that extended format without db_column raises error."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name:
        type: text
""")

        with pytest.raises(ValueError, match="must have 'db_column'"):
            SyncConfig.from_yaml(config_file)


class TestIdColumnMatchers:
    """Test suite for id_column_matchers configuration."""

    def test_config_with_id_column_matchers(self, tmp_path: Path) -> None:
        """Test loading config with id_column_matchers."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
id_column_matchers:
  - customer_id
  - account_id
  - user_id
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name: full_name
""")

        config = SyncConfig.from_yaml(config_file)

        assert config.id_column_matchers is not None
        assert config.id_column_matchers == ["customer_id", "account_id", "user_id"]

    def test_config_without_id_column_matchers(self, tmp_path: Path) -> None:
        """Test loading config without id_column_matchers (uses defaults)."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      name: full_name
""")

        config = SyncConfig.from_yaml(config_file)

        assert config.id_column_matchers is None

    def test_config_invalid_id_column_matchers(self, tmp_path: Path) -> None:
        """Test that non-list id_column_matchers raises error."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
id_column_matchers: "not a list"
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
""")

        with pytest.raises(ValueError, match="id_column_matchers must be a list"):
            SyncConfig.from_yaml(config_file)

    def test_save_config_with_id_column_matchers(self, tmp_path: Path) -> None:
        """Test saving config with id_column_matchers."""
        from data_sync.config import ColumnMapping, SyncJob

        job = SyncJob(
            name="test_job",
            target_table="users",
            id_mapping=[ColumnMapping("user_id", "id")],
            columns=[ColumnMapping("name", "full_name")],
        )

        config = SyncConfig(jobs={"test_job": job}, id_column_matchers=["custom_id", "record_id"])

        config_file = tmp_path / "config.yaml"
        config.save_to_yaml(config_file)

        # Reload and verify
        loaded_config = SyncConfig.from_yaml(config_file)
        assert loaded_config.id_column_matchers == ["custom_id", "record_id"]


class TestCompoundPrimaryKeys:
    """Test suite for compound primary keys."""

    def test_config_with_compound_primary_key(self, tmp_path: Path) -> None:
        """Test loading config with compound primary key."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: user_logins
    id_mapping:
      user_id: user_id
      login_date: login_date
    columns:
      ip_address: ip
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_job")
        assert job is not None
        assert len(job.id_mapping) == 2
        assert job.id_mapping[0].csv_column == "user_id"
        assert job.id_mapping[0].db_column == "user_id"
        assert job.id_mapping[1].csv_column == "login_date"
        assert job.id_mapping[1].db_column == "login_date"

    def test_save_config_with_compound_primary_key(self, tmp_path: Path) -> None:
        """Test saving config with compound primary key."""
        from data_sync.config import ColumnMapping, SyncJob

        job = SyncJob(
            name="test_job",
            target_table="sales",
            id_mapping=[
                ColumnMapping("store_id", "store_id"),
                ColumnMapping("product_id", "product_id"),
            ],
            columns=[ColumnMapping("quantity", "qty")],
        )

        config = SyncConfig(jobs={"test_job": job})
        config_file = tmp_path / "config.yaml"
        config.save_to_yaml(config_file)

        # Reload and verify
        loaded_config = SyncConfig.from_yaml(config_file)
        loaded_job = loaded_config.get_job("test_job")
        assert loaded_job is not None
        assert len(loaded_job.id_mapping) == 2
        assert loaded_job.id_mapping[0].db_column == "store_id"
        assert loaded_job.id_mapping[1].db_column == "product_id"


class TestIndexes:
    """Test suite for database indexes."""

    def test_config_with_single_column_index(self, tmp_path: Path) -> None:
        """Test loading config with single-column index."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      email: email
    indexes:
      - name: idx_email
        columns:
          - column: email
            order: ASC
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_job")
        assert job is not None
        assert len(job.indexes) == 1
        assert job.indexes[0].name == "idx_email"
        assert len(job.indexes[0].columns) == 1
        assert job.indexes[0].columns[0].column == "email"
        assert job.indexes[0].columns[0].order == "ASC"

    def test_config_with_multi_column_index(self, tmp_path: Path) -> None:
        """Test loading config with multi-column index."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: orders
    id_mapping:
      order_id: id
    columns:
      customer_id: customer_id
      order_date: order_date
    indexes:
      - name: idx_customer_date
        columns:
          - column: customer_id
            order: ASC
          - column: order_date
            order: DESC
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_job")
        assert job is not None
        assert len(job.indexes) == 1
        index = job.indexes[0]
        assert index.name == "idx_customer_date"
        assert len(index.columns) == 2
        assert index.columns[0].column == "customer_id"
        assert index.columns[0].order == "ASC"
        assert index.columns[1].column == "order_date"
        assert index.columns[1].order == "DESC"

    def test_config_with_multiple_indexes(self, tmp_path: Path) -> None:
        """Test loading config with multiple indexes."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
jobs:
  test_job:
    target_table: users
    id_mapping:
      user_id: id
    columns:
      email: email
      name: name
    indexes:
      - name: idx_email
        columns:
          - column: email
            order: ASC
      - name: idx_name
        columns:
          - column: name
            order: ASC
""")

        config = SyncConfig.from_yaml(config_file)
        job = config.get_job("test_job")
        assert job is not None
        assert len(job.indexes) == 2
        assert job.indexes[0].name == "idx_email"
        assert job.indexes[1].name == "idx_name"

    def test_save_config_with_indexes(self, tmp_path: Path) -> None:
        """Test saving config with indexes."""
        from data_sync.config import ColumnMapping, Index, IndexColumn, SyncJob

        job = SyncJob(
            name="test_job",
            target_table="products",
            id_mapping=[ColumnMapping("product_id", "id")],
            columns=[ColumnMapping("category", "cat"), ColumnMapping("price", "price")],
            indexes=[
                Index(
                    name="idx_category_price",
                    columns=[IndexColumn("cat", "ASC"), IndexColumn("price", "DESC")],
                )
            ],
        )

        config = SyncConfig(jobs={"test_job": job})
        config_file = tmp_path / "config.yaml"
        config.save_to_yaml(config_file)

        # Reload and verify
        loaded_config = SyncConfig.from_yaml(config_file)
        loaded_job = loaded_config.get_job("test_job")
        assert loaded_job is not None
        assert len(loaded_job.indexes) == 1
        index = loaded_job.indexes[0]
        assert index.name == "idx_category_price"
        assert len(index.columns) == 2
        assert index.columns[0].column == "cat"
        assert index.columns[0].order == "ASC"
        assert index.columns[1].column == "price"
        assert index.columns[1].order == "DESC"

    def test_index_invalid_order(self) -> None:
        """Test that invalid index order raises error."""
        from data_sync.config import IndexColumn

        with pytest.raises(ValueError, match="Index order must be 'ASC' or 'DESC'"):
            IndexColumn("email", "INVALID")

    def test_index_no_columns(self) -> None:
        """Test that index with no columns raises error."""
        from data_sync.config import Index

        with pytest.raises(ValueError, match="Index must have at least one column"):
            Index("idx_test", [])
