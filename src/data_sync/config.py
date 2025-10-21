"""Configuration file handling for data_sync."""

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


class SyncJob:
    """Configuration for a single sync job."""

    def __init__(
        self,
        name: str,
        target_table: str,
        id_mapping: ColumnMapping,
        columns: list[ColumnMapping] | None = None,
    ) -> None:
        """Initialize a sync job.

        Args:
            name: Name of the job
            target_table: Target database table name
            id_mapping: Mapping for the ID column
            columns: List of column mappings to sync (all columns if None)
        """
        self.name = name
        self.target_table = target_table
        self.id_mapping = id_mapping
        self.columns = columns or []


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
        """Load configuration from a YAML file.

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
                  csv_column: user_id
                  db_column: id
                columns:
                  - csv_column: name
                    db_column: full_name
                  - csv_column: email
                    db_column: email
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

        id_data = job_data["id_mapping"]
        if "csv_column" not in id_data or "db_column" not in id_data:
            raise ValueError(f"Job '{name}' id_mapping must have 'csv_column' and 'db_column'")

        id_mapping = ColumnMapping(csv_column=id_data["csv_column"], db_column=id_data["db_column"])

        columns = []
        if "columns" in job_data and job_data["columns"]:
            for col_data in job_data["columns"]:
                if "csv_column" not in col_data or "db_column" not in col_data:
                    raise ValueError(
                        f"Job '{name}' column mapping must have 'csv_column' and 'db_column'"
                    )
                columns.append(
                    ColumnMapping(
                        csv_column=col_data["csv_column"],
                        db_column=col_data["db_column"],
                    )
                )

        return SyncJob(
            name=name,
            target_table=job_data["target_table"],
            id_mapping=id_mapping,
            columns=columns if columns else None,
        )
