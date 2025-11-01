"""Sync CSV and CDF science files into PostgreSQL database.

This package provides both a CLI tool and programmatic API for syncing
CSV and CDF files into a PostgreSQL database.

CLI Usage:
    crump sync <file> <config> <job> --db-url <url>
    crump prepare <file> <config> <job>
    crump inspect <file>

Programmatic Usage:
    from crump import sync_csv, prepare_config, analyze_csv
    from pathlib import Path

    # Sync a CSV file
    sync_csv(
        file_path=Path("data.csv"),
        config_path=Path("crump_config.yaml"),
        job_name="my_job",
        db_url="postgresql://localhost/mydb"
    )
"""

__version__ = "0.1.0"

# Export main API functions
from crump.config import (
    ColumnMapping,
    CrumpConfig,
    CrumpJob,
    Index,
    IndexColumn,
)
from crump.database import (
    DryRunSummary,
    sync_csv_to_postgres,
    sync_csv_to_postgres_dry_run,
)
from crump.type_detection import analyze_csv_types_and_nullable, suggest_id_column

__all__ = [
    "__version__",
    # Configuration
    "CrumpConfig",
    "CrumpJob",
    "ColumnMapping",
    "Index",
    "IndexColumn",
    # Database operations
    "sync_csv_to_postgres",
    "sync_csv_to_postgres_dry_run",
    "DryRunSummary",
    # Type detection
    "analyze_csv_types_and_nullable",
    "suggest_id_column",
]
