"""Configuration file handling for data_sync."""

import re
from pathlib import Path
from typing import Any

import yaml


class ColumnMapping:
    """Mapping between CSV and database columns."""

    def __init__(self, csv_column: str, db_column: str) -> None:
        """Initialize column mapping.

        Args:
            csv_column: Name of the column in the CSV file
            db_column: Name of the column in the database
        """
        self.csv_column = csv_column
        self.db_column = db_column


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

    def __init__(self, jobs: dict[str, SyncJob]) -> None:
        """Initialize sync configuration.

        Args:
            jobs: Dictionary of job name to SyncJob
        """
        self.jobs = jobs

    def get_job(self, name: str) -> SyncJob | None:
        """Get a job by name.

        Args:
            name: Name of the job

        Returns:
            SyncJob if found, None otherwise
        """
        return self.jobs.get(name)

    @classmethod
    def from_yaml(cls, config_path: Path) -> "SyncConfig":
        r"""Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            SyncConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid

        Example YAML structure:
            jobs:
              my_job:
                target_table: users
                id_mapping:
                  user_id: id
                columns:
                  name: full_name
                  email: email_address
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

        jobs = {}
        for job_name, job_data in data["jobs"].items():
            jobs[job_name] = cls._parse_job(job_name, job_data)

        return cls(jobs=jobs)

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

        # Parse id_mapping as a dict: {csv_column: db_column}
        id_data = job_data["id_mapping"]
        if not isinstance(id_data, dict):
            raise ValueError(f"Job '{name}' id_mapping must be a dictionary")

        if len(id_data) != 1:
            raise ValueError(
                f"Job '{name}' id_mapping must have exactly one mapping (source: destination)"
            )

        csv_col, db_col = next(iter(id_data.items()))
        id_mapping = ColumnMapping(csv_column=csv_col, db_column=db_col)

        # Parse columns as a dict: {csv_column: db_column}
        columns = []
        if "columns" in job_data and job_data["columns"]:
            col_data = job_data["columns"]
            if not isinstance(col_data, dict):
                raise ValueError(f"Job '{name}' columns must be a dictionary")

            for csv_col, db_col in col_data.items():
                columns.append(ColumnMapping(csv_column=csv_col, db_column=db_col))

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
