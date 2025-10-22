"""Configuration file handling for data_sync."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


class ColumnMapping:
    """Mapping between CSV and database columns."""

    def __init__(self, csv_column: str, db_column: str, data_type: str | None = None) -> None:
        """Initialize column mapping.

        Args:
            csv_column: Name of the column in the CSV file
            db_column: Name of the column in the database
            data_type: Optional data type (integer, float, date, datetime, text, varchar(N))
        """
        self.csv_column = csv_column
        self.db_column = db_column
        self.data_type = data_type


class DateMapping:
    """Configuration for extracting date from filename."""

    def __init__(self, filename_regex: str, db_column: str) -> None:
        """Initialize date mapping.

        Args:
            filename_regex: Regex pattern to extract date from filename
            db_column: Database column to store the extracted date
        """
        self.filename_regex = filename_regex
        self.db_column = db_column

    def extract_date_from_filename(self, filename: str | Path) -> str | None:
        r"""Extract date from filename using the configured regex.

        Args:
            filename: The filename (or path) to extract date from

        Returns:
            Extracted date string if found, None otherwise

        Example:
            >>> mapping = DateMapping(r'(\d{4}-\d{2}-\d{2})', 'sync_date')
            >>> mapping.extract_date_from_filename('data_2024-01-15.csv')
            '2024-01-15'
        """
        if isinstance(filename, Path):
            filename = filename.name

        match = re.search(self.filename_regex, filename)
        if match:
            return match.group(1)
        return None


class SyncJob:
    """Configuration for a single sync job."""

    def __init__(
        self,
        name: str,
        target_table: str,
        id_mapping: ColumnMapping,
        columns: list[ColumnMapping] | None = None,
        date_mapping: DateMapping | None = None,
    ) -> None:
        """Initialize a sync job.

        Args:
            name: Name of the job
            target_table: Target database table name
            id_mapping: Mapping for the ID column
            columns: List of column mappings to sync (all columns if None)
            date_mapping: Optional date extraction and storage configuration
        """
        self.name = name
        self.target_table = target_table
        self.id_mapping = id_mapping
        self.columns = columns or []
        self.date_mapping = date_mapping


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
                  user_id: id
                columns:
                  name: full_name  # Simple format
                  email: email_address
                  age:
                    db_column: user_age
                    type: integer
                  salary:
                    db_column: monthly_salary
                    type: float
                date_mapping:
                  filename_regex: '(\d{4}-\d{2}-\d{2})'
                  db_column: sync_date
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
        2. Extended: csv_column: {db_column: name, type: data_type}

        Args:
            csv_col: CSV column name
            value: Either a string (db_column) or dict with db_column and optional type
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
            # Extended format: csv_column: {db_column: name, type: data_type}
            if "db_column" not in value:
                raise ValueError(
                    f"Job '{job_name}' column '{csv_col}' extended mapping must have 'db_column'"
                )
            db_column = value["db_column"]
            data_type = value.get("type")  # Optional
            return ColumnMapping(csv_column=csv_col, db_column=db_column, data_type=data_type)
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
        id_data = job_data["id_mapping"]
        if not isinstance(id_data, dict):
            raise ValueError(f"Job '{name}' id_mapping must be a dictionary")

        if len(id_data) != 1:
            raise ValueError(
                f"Job '{name}' id_mapping must have exactly one mapping (source: destination)"
            )

        csv_col, value = next(iter(id_data.items()))
        id_mapping = SyncConfig._parse_column_mapping(csv_col, value, name)

        # Parse columns as a dict: {csv_column: db_column} or {csv_column: {db_column: x, type: y}}
        columns = []
        if "columns" in job_data and job_data["columns"]:
            col_data = job_data["columns"]
            if not isinstance(col_data, dict):
                raise ValueError(f"Job '{name}' columns must be a dictionary")

            for csv_col, value in col_data.items():
                columns.append(SyncConfig._parse_column_mapping(csv_col, value, name))

        # Parse optional date_mapping
        date_mapping = None
        if "date_mapping" in job_data and job_data["date_mapping"]:
            date_data = job_data["date_mapping"]
            if not isinstance(date_data, dict):
                raise ValueError(f"Job '{name}' date_mapping must be a dictionary")

            if "filename_regex" not in date_data:
                raise ValueError(f"Job '{name}' date_mapping missing 'filename_regex'")

            if "db_column" not in date_data:
                raise ValueError(f"Job '{name}' date_mapping missing 'db_column'")

            date_mapping = DateMapping(
                filename_regex=date_data["filename_regex"],
                db_column=date_data["db_column"],
            )

        return SyncJob(
            name=name,
            target_table=job_data["target_table"],
            id_mapping=id_mapping,
            columns=columns if columns else None,
            date_mapping=date_mapping,
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
            job_dict: dict[str, Any] = {
                "target_table": job.target_table,
                "id_mapping": {job.id_mapping.csv_column: job.id_mapping.db_column},
            }

            # Add columns if present
            if job.columns:
                columns_dict = {}
                for col in job.columns:
                    if col.data_type:
                        columns_dict[col.csv_column] = {
                            "db_column": col.db_column,
                            "type": col.data_type,
                        }
                    else:
                        columns_dict[col.csv_column] = col.db_column
                job_dict["columns"] = columns_dict

            # Add date_mapping if present
            if job.date_mapping:
                job_dict["date_mapping"] = {
                    "filename_regex": job.date_mapping.filename_regex,
                    "db_column": job.date_mapping.db_column,
                }

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
