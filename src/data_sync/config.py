"""Configuration file handling for data_sync."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


class ColumnMapping:
    """Mapping between CSV and database columns."""

    def __init__(
        self,
        csv_column: str,
        db_column: str,
        data_type: str | None = None,
        nullable: bool | None = None,
    ) -> None:
        """Initialize column mapping.

        Args:
            csv_column: Name of the column in the CSV file
            db_column: Name of the column in the database
            data_type: Optional data type (integer, float, date, datetime, text, varchar(N))
            nullable: Optional flag indicating if NULL values are allowed (True=NULL, False=NOT NULL)
        """
        self.csv_column = csv_column
        self.db_column = db_column
        self.data_type = data_type
        self.nullable = nullable


class FilenameColumnMapping:
    """Mapping for a single column extracted from filename."""

    def __init__(
        self,
        name: str,
        db_column: str | None = None,
        data_type: str | None = None,
        use_to_delete_old_rows: bool = False,
    ) -> None:
        """Initialize filename column mapping.

        Args:
            name: Name of the extracted value (from template/regex)
            db_column: Database column name (defaults to name if not specified)
            data_type: Data type (varchar(N), integer, float, date, datetime, text)
            use_to_delete_old_rows: If True, this column is used to identify stale rows
        """
        self.name = name
        self.db_column = db_column or name
        self.data_type = data_type
        self.use_to_delete_old_rows = use_to_delete_old_rows


class FilenameToColumn:
    """Configuration for extracting multiple values from filename."""

    def __init__(
        self,
        columns: dict[str, FilenameColumnMapping],
        template: str | None = None,
        regex: str | None = None,
    ) -> None:
        """Initialize filename to column mapping.

        Args:
            columns: Dictionary of column name to FilenameColumnMapping
            template: Filename template with [column_name] syntax (mutually exclusive with regex)
            regex: Regex pattern with named groups (mutually exclusive with template)

        Raises:
            ValueError: If both template and regex are specified, or neither is specified
        """
        if (template is None) == (regex is None):
            raise ValueError("Must specify exactly one of 'template' or 'regex'")

        self.columns = columns
        self.template = template
        self.regex = regex

        # Pre-compile regex
        if template:
            self._compiled_regex = self._template_to_regex(template)
        else:
            self._compiled_regex = re.compile(regex)

    def _template_to_regex(self, template: str) -> re.Pattern:
        r"""Convert template string to regex pattern.

        Args:
            template: Template string with [column_name] placeholders

        Returns:
            Compiled regex pattern with named groups

        Example:
            >>> mapping = FilenameToColumn(...)
            >>> # Template: "[mission]level2[sensor]_[date].cdf"
            >>> # becomes: "(?P<mission>.+?)level2(?P<sensor>.+?)_(?P<date>.+?)\.cdf"
        """
        # Escape special regex characters
        escaped = re.escape(template)
        # Replace \[column_name\] with named groups using non-greedy matching
        pattern = re.sub(r"\\\[(\w+)\\\]", r"(?P<\1>.+?)", escaped)
        return re.compile(pattern)

    def extract_values_from_filename(self, filename: str | Path) -> dict[str, str] | None:
        """Extract values from filename using template or regex.

        Args:
            filename: The filename (or path) to extract values from

        Returns:
            Dictionary of column name to extracted value, or None if no match

        Example:
            >>> mapping = FilenameToColumn(
            ...     columns={...},
            ...     template="[mission]level2[sensor]_[date]_v[version].cdf"
            ... )
            >>> mapping.extract_values_from_filename("imap_level2_primary_20000102_v002.cdf")
            {'mission': 'imap', 'sensor': 'primary', 'date': '20000102', 'version': '002'}
        """
        if isinstance(filename, Path):
            filename = filename.name

        match = self._compiled_regex.search(filename)
        if not match:
            return None

        return match.groupdict()

    def get_delete_key_columns(self) -> list[str]:
        """Get list of database column names used for stale row deletion.

        Returns:
            List of db_column names where use_to_delete_old_rows is True
        """
        return [col.db_column for col in self.columns.values() if col.use_to_delete_old_rows]


class IndexColumn:
    """Column definition for a database index."""

    def __init__(self, column: str, order: str = "ASC") -> None:
        """Initialize index column.

        Args:
            column: Column name
            order: Sort order - 'ASC' or 'DESC' (default: 'ASC')

        Raises:
            ValueError: If order is not 'ASC' or 'DESC'
        """
        if order.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Index order must be 'ASC' or 'DESC', got '{order}'")
        self.column = column
        self.order = order.upper()


class Index:
    """Database index configuration."""

    def __init__(self, name: str, columns: list[IndexColumn]) -> None:
        """Initialize index.

        Args:
            name: Index name
            columns: List of columns with sort order

        Raises:
            ValueError: If columns list is empty
        """
        if not columns:
            raise ValueError("Index must have at least one column")
        self.name = name
        self.columns = columns


class SyncJob:
    """Configuration for a single sync job."""

    def __init__(
        self,
        name: str,
        target_table: str,
        id_mapping: list[ColumnMapping],
        columns: list[ColumnMapping] | None = None,
        filename_to_column: FilenameToColumn | None = None,
        indexes: list[Index] | None = None,
    ) -> None:
        """Initialize a sync job.

        Args:
            name: Name of the job
            target_table: Target database table name
            id_mapping: List of mappings for ID columns (supports compound primary keys)
            columns: List of column mappings to sync (all columns if None)
            filename_to_column: Optional filename-to-column extraction configuration
            indexes: Optional list of database indexes to create
        """
        self.name = name
        self.target_table = target_table
        self.id_mapping = id_mapping
        self.columns = columns or []
        self.filename_to_column = filename_to_column
        self.indexes = indexes or []


class SyncConfig:
    """Configuration for data synchronization."""

    def __init__(
        self, jobs: dict[str, SyncJob], id_column_matchers: list[str] | None = None
    ) -> None:
        """Initialize sync configuration.

        Args:
            jobs: Dictionary of job name to SyncJob
            id_column_matchers: Optional list of column name patterns to match as ID columns
                               (e.g., ['id', 'uuid', 'key']). If None, uses default patterns.
        """
        self.jobs = jobs
        self.id_column_matchers = id_column_matchers

    def get_job(self, name: str) -> SyncJob | None:
        """Get a job by name.

        Args:
            name: Name of the job

        Returns:
            SyncJob if found, None otherwise
        """
        return self.jobs.get(name)

    @classmethod
    def from_yaml(cls, config_path: Path) -> SyncConfig:
        r"""Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            SyncConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid

        Example YAML structure:
            id_column_matchers:  # Optional, root-level config
              - id
              - uuid
              - key
            jobs:
              my_job:
                target_table: users
                id_mapping:
                  user_id: id  # Single column primary key
                  # Or for compound primary key:
                  # user_id: id
                  # tenant_id: tenant
                columns:
                  name: full_name  # Simple format
                  email: email_address
                  age:
                    db_column: user_age
                    type: integer
                  salary:
                    db_column: monthly_salary
                    type: float
                filename_to_column:  # Optional: extract values from filename
                  template: "[mission]level2[sensor]_[date]_v[version].cdf"
                  # OR use regex with named groups:
                  # regex: "(?P<mission>[a-z]+)_level2_(?P<sensor>[a-z]+)_(?P<date>\\d{8})_v(?P<version>\\d+)\\.cdf"
                  columns:
                    mission:
                      db_column: mission_name
                      type: varchar(10)
                    sensor:
                      db_column: sensor_type
                      type: varchar(20)
                    date:
                      db_column: observation_date
                      type: date
                      use_to_delete_old_rows: true  # Use this column to identify stale rows
                    version:
                      db_column: file_version
                      type: varchar(10)
                indexes:  # Optional
                  - name: idx_email
                    columns:
                      - column: email
                        order: ASC
                  - name: idx_observation_date
                    columns:
                      - column: observation_date
                        order: DESC
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data or "jobs" not in data:
            raise ValueError("Config file must contain 'jobs' section")

        # Parse optional id_column_matchers
        id_column_matchers = data.get("id_column_matchers")
        if id_column_matchers is not None and not isinstance(id_column_matchers, list):
            raise ValueError("id_column_matchers must be a list of strings")

        jobs = {}
        for job_name, job_data in data["jobs"].items():
            jobs[job_name] = cls._parse_job(job_name, job_data)

        return cls(jobs=jobs, id_column_matchers=id_column_matchers)

    @staticmethod
    def _parse_column_mapping(csv_col: str, value: Any, job_name: str) -> ColumnMapping:
        """Parse a column mapping from config value.

        Supports two formats:
        1. Simple: csv_column: db_column
        2. Extended: csv_column: {db_column: name, type: data_type, nullable: true/false}

        Args:
            csv_col: CSV column name
            value: Either a string (db_column) or dict with db_column and optional type/nullable
            job_name: Job name (for error messages)

        Returns:
            ColumnMapping instance

        Raises:
            ValueError: If mapping format is invalid
        """
        if isinstance(value, str):
            # Simple format: csv_column: db_column
            return ColumnMapping(csv_column=csv_col, db_column=value)
        elif isinstance(value, dict):
            # Extended format: csv_column: {db_column: name, type: data_type, nullable: true/false}
            if "db_column" not in value:
                raise ValueError(
                    f"Job '{job_name}' column '{csv_col}' extended mapping must have 'db_column'"
                )
            db_column = value["db_column"]
            data_type = value.get("type")  # Optional
            nullable = value.get("nullable")  # Optional
            return ColumnMapping(
                csv_column=csv_col, db_column=db_column, data_type=data_type, nullable=nullable
            )
        else:
            raise ValueError(
                f"Job '{job_name}' column '{csv_col}' must be string or dict, got {type(value)}"
            )

    @staticmethod
    def _parse_job(name: str, job_data: dict[str, Any]) -> SyncJob:
        """Parse a job from configuration data.

        Args:
            name: Name of the job
            job_data: Job configuration dictionary

        Returns:
            SyncJob instance

        Raises:
            ValueError: If job configuration is invalid
        """
        if "target_table" not in job_data:
            raise ValueError(f"Job '{name}' missing 'target_table'")

        if "id_mapping" not in job_data:
            raise ValueError(f"Job '{name}' missing 'id_mapping'")

        # Parse id_mapping as a dict: {csv_column: db_column} or {csv_column: {db_column: x, type: y}}
        # Supports multiple columns for compound primary keys
        id_data = job_data["id_mapping"]
        if not isinstance(id_data, dict):
            raise ValueError(f"Job '{name}' id_mapping must be a dictionary")

        if len(id_data) < 1:
            raise ValueError(f"Job '{name}' id_mapping must have at least one mapping")

        id_mapping = []
        for csv_col, value in id_data.items():
            id_mapping.append(SyncConfig._parse_column_mapping(csv_col, value, name))

        # Parse columns as a dict: {csv_column: db_column} or {csv_column: {db_column: x, type: y}}
        columns = []
        if "columns" in job_data and job_data["columns"]:
            col_data = job_data["columns"]
            if not isinstance(col_data, dict):
                raise ValueError(f"Job '{name}' columns must be a dictionary")

            for csv_col, value in col_data.items():
                columns.append(SyncConfig._parse_column_mapping(csv_col, value, name))

        # Parse optional filename_to_column
        filename_to_column = None
        if "filename_to_column" in job_data and job_data["filename_to_column"]:
            ftc_data = job_data["filename_to_column"]
            if not isinstance(ftc_data, dict):
                raise ValueError(f"Job '{name}' filename_to_column must be a dictionary")

            # Check that exactly one of template or regex is specified
            has_template = "template" in ftc_data and ftc_data["template"]
            has_regex = "regex" in ftc_data and ftc_data["regex"]

            if not has_template and not has_regex:
                raise ValueError(
                    f"Job '{name}' filename_to_column must have either 'template' or 'regex'"
                )

            if has_template and has_regex:
                raise ValueError(
                    f"Job '{name}' filename_to_column cannot have both 'template' and 'regex'"
                )

            # Parse columns
            if "columns" not in ftc_data or not ftc_data["columns"]:
                raise ValueError(f"Job '{name}' filename_to_column must have 'columns'")

            if not isinstance(ftc_data["columns"], dict):
                raise ValueError(f"Job '{name}' filename_to_column columns must be a dictionary")

            ftc_columns = {}
            for col_name, col_data in ftc_data["columns"].items():
                if isinstance(col_data, dict):
                    db_column = col_data.get("db_column")
                    data_type = col_data.get("type")
                    use_to_delete_old_rows = col_data.get("use_to_delete_old_rows", False)
                elif col_data is None:
                    # Simple format: column_name: null (use defaults)
                    db_column = None
                    data_type = None
                    use_to_delete_old_rows = False
                else:
                    raise ValueError(
                        f"Job '{name}' filename_to_column column '{col_name}' must be a dictionary or null"
                    )

                ftc_columns[col_name] = FilenameColumnMapping(
                    name=col_name,
                    db_column=db_column,
                    data_type=data_type,
                    use_to_delete_old_rows=use_to_delete_old_rows,
                )

            filename_to_column = FilenameToColumn(
                columns=ftc_columns,
                template=ftc_data.get("template"),
                regex=ftc_data.get("regex"),
            )

        # Parse optional indexes
        indexes = []
        if "indexes" in job_data and job_data["indexes"]:
            indexes_data = job_data["indexes"]
            if not isinstance(indexes_data, list):
                raise ValueError(f"Job '{name}' indexes must be a list")

            for idx_data in indexes_data:
                if not isinstance(idx_data, dict):
                    raise ValueError(f"Job '{name}' index entry must be a dictionary")

                if "name" not in idx_data:
                    raise ValueError(f"Job '{name}' index missing 'name'")

                if "columns" not in idx_data:
                    raise ValueError(f"Job '{name}' index missing 'columns'")

                idx_columns = []
                for col_data in idx_data["columns"]:
                    if not isinstance(col_data, dict):
                        raise ValueError(f"Job '{name}' index column must be a dictionary")

                    if "column" not in col_data:
                        raise ValueError(f"Job '{name}' index column missing 'column' field")

                    order = col_data.get("order", "ASC")
                    idx_columns.append(IndexColumn(column=col_data["column"], order=order))

                indexes.append(Index(name=idx_data["name"], columns=idx_columns))

        return SyncJob(
            name=name,
            target_table=job_data["target_table"],
            id_mapping=id_mapping,
            columns=columns if columns else None,
            filename_to_column=filename_to_column,
            indexes=indexes if indexes else None,
        )

    def add_or_update_job(self, job: SyncJob, force: bool = False) -> bool:
        """Add a new job or update an existing one.

        Args:
            job: SyncJob to add or update
            force: If True, overwrite existing job. If False, raise error if job exists.

        Returns:
            True if job was added/updated

        Raises:
            ValueError: If job already exists and force=False
        """
        if job.name in self.jobs and not force:
            raise ValueError(f"Job '{job.name}' already exists. Use force=True to overwrite.")

        self.jobs[job.name] = job
        return True

    def to_yaml_dict(self) -> dict[str, Any]:
        """Convert config to dictionary suitable for YAML serialization.

        Returns:
            Dictionary representation of config
        """
        jobs_dict = {}

        for job_name, job in self.jobs.items():
            # Build id_mapping dict (supports compound primary keys)
            id_mapping_dict = {}
            for id_col in job.id_mapping:
                if id_col.data_type or id_col.nullable is not None:
                    mapping_dict = {"db_column": id_col.db_column}
                    if id_col.data_type:
                        mapping_dict["type"] = id_col.data_type
                    if id_col.nullable is not None:
                        mapping_dict["nullable"] = id_col.nullable
                    id_mapping_dict[id_col.csv_column] = mapping_dict
                else:
                    id_mapping_dict[id_col.csv_column] = id_col.db_column

            job_dict: dict[str, Any] = {
                "target_table": job.target_table,
                "id_mapping": id_mapping_dict,
            }

            # Add columns if present
            if job.columns:
                columns_dict = {}
                for col in job.columns:
                    if col.data_type or col.nullable is not None:
                        mapping_dict = {"db_column": col.db_column}
                        if col.data_type:
                            mapping_dict["type"] = col.data_type
                        if col.nullable is not None:
                            mapping_dict["nullable"] = col.nullable
                        columns_dict[col.csv_column] = mapping_dict
                    else:
                        columns_dict[col.csv_column] = col.db_column
                job_dict["columns"] = columns_dict

            # Add filename_to_column if present
            if job.filename_to_column:
                ftc_dict: dict[str, Any] = {}
                if job.filename_to_column.template:
                    ftc_dict["template"] = job.filename_to_column.template
                else:
                    ftc_dict["regex"] = job.filename_to_column.regex

                columns_dict = {}
                for col_name, col in job.filename_to_column.columns.items():
                    col_dict: dict[str, Any] = {}
                    if col.db_column != col.name:
                        col_dict["db_column"] = col.db_column
                    if col.data_type:
                        col_dict["type"] = col.data_type
                    if col.use_to_delete_old_rows:
                        col_dict["use_to_delete_old_rows"] = True

                    # If col_dict is empty, use None to keep it minimal
                    columns_dict[col_name] = col_dict if col_dict else None

                ftc_dict["columns"] = columns_dict
                job_dict["filename_to_column"] = ftc_dict

            # Add indexes if present
            if job.indexes:
                indexes_list = []
                for index in job.indexes:
                    index_dict = {
                        "name": index.name,
                        "columns": [
                            {"column": col.column, "order": col.order} for col in index.columns
                        ],
                    }
                    indexes_list.append(index_dict)
                job_dict["indexes"] = indexes_list

            jobs_dict[job_name] = job_dict

        result: dict[str, Any] = {"jobs": jobs_dict}

        # Add id_column_matchers if present
        if self.id_column_matchers is not None:
            result["id_column_matchers"] = self.id_column_matchers

        return result

    def save_to_yaml(self, config_path: Path) -> None:
        """Save configuration to a YAML file.

        Args:
            config_path: Path to save the YAML file
        """
        config_dict = self.to_yaml_dict()

        with open(config_path, "w", encoding="utf-8") as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)
