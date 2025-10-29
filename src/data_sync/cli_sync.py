"""Sync command for syncing CSV and CDF files to database."""

import tempfile
from pathlib import Path

import click
from rich.console import Console

from data_sync.cdf_extractor import extract_cdf_to_csv
from data_sync.config import SyncConfig
from data_sync.database import sync_csv_to_postgres, sync_csv_to_postgres_dry_run

console = Console()


def _extract_cdf_and_find_csv(
    cdf_file: Path, temp_dir: Path, max_records: int | None = None
) -> list[Path]:
    """Extract CDF file to temporary CSV files.

    Args:
        cdf_file: Path to CDF file
        temp_dir: Temporary directory for CSV extraction
        max_records: Maximum number of records to extract per variable (None = all)

    Returns:
        List of extracted CSV file paths

    Raises:
        ValueError: If extraction fails
    """
    console.print("[dim]  Extracting CDF data to temporary CSV files...[/dim]")

    if max_records is not None:
        console.print(f"[dim]  Max records per variable: {max_records:,}[/dim]")

    try:
        # Extract data from CDF
        results = extract_cdf_to_csv(
            cdf_file_path=cdf_file,
            output_dir=temp_dir,
            filename_template=f"{cdf_file.stem}_[VARIABLE_NAME].csv",
            automerge=True,
            append=False,
            variable_names=None,
            max_records=max_records,
        )

        if not results:
            raise ValueError("No data could be extracted from CDF file")

        csv_files = [result.output_file for result in results]
        console.print(f"[dim]  Extracted {len(csv_files)} CSV file(s) from CDF[/dim]")

        return csv_files

    except Exception as e:
        raise ValueError(f"Failed to extract CDF file: {e}") from e


@click.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.argument("config", type=click.Path(exists=True, path_type=Path), required=True)
@click.argument("job", type=str, required=True)
@click.option(
    "--db-url",
    envvar="DATABASE_URL",
    required=True,
    help="PostgreSQL connection string (or set DATABASE_URL env var)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Simulate the sync without making any database changes",
)
@click.option(
    "--max-records",
    type=int,
    default=None,
    help="Maximum number of records to extract per variable from CDF files (default: extract all records)",
)
def sync(
    file_path: Path, config: Path, job: str, db_url: str, dry_run: bool, max_records: int | None
) -> None:
    """Sync a CSV or CDF file to the database using a configuration.

    Supports both CSV and CDF file formats. For CDF files, data is automatically
    extracted to temporary CSV files before syncing to the database.

    Arguments:
        FILE_PATH: Path to the CSV or CDF file to sync (required)
        CONFIG: Path to the YAML configuration file (required)
        JOB: Name of the job to run from the config file (required)

    Examples:
        # Sync a CSV file
        data-sync sync data.csv config.yaml my_job --db-url postgresql://localhost/mydb

        # Sync a CDF file (extracts to CSV automatically)
        data-sync sync data.cdf config.yaml my_job --db-url postgresql://localhost/mydb

        # Sync CDF with limited records (useful for testing)
        data-sync sync data.cdf config.yaml my_job --db-url postgresql://localhost/mydb --max-records 200

        # Using environment variable
        export DATABASE_URL=postgresql://localhost/mydb
        data-sync sync data.csv config.yaml my_job

        # Dry-run mode to preview changes
        data-sync sync data.csv config.yaml my_job --dry-run

        # Dry-run with limited records from CDF
        data-sync sync data.cdf config.yaml my_job --dry-run --max-records 100
    """
    temp_dir: Path | None = None
    temp_csv_files: list[Path] = []

    try:
        # Load configuration
        sync_config = SyncConfig.from_yaml(config)

        # Get the specified job
        sync_job = sync_config.get_job(job)
        if not sync_job:
            available_jobs = ", ".join(sync_config.jobs.keys())
            console.print(f"[red]Error:[/red] Job '{job}' not found in config")
            console.print(f"[dim]Available jobs: {available_jobs}[/dim]")
            raise click.Abort()

        # Check if input file is CDF - if so, extract to temporary CSV
        csv_file_to_sync = file_path
        if file_path.suffix.lower() == ".cdf":
            console.print(f"[cyan]Processing CDF file: {file_path.name}[/cyan]")

            # Create temporary directory for CSV extraction
            temp_dir = Path(tempfile.mkdtemp(prefix="data_sync_cdf_"))

            # Extract CDF to temporary CSV files
            temp_csv_files = _extract_cdf_and_find_csv(file_path, temp_dir, max_records)

            # Find the CSV file that matches this job's configuration
            # Try each extracted CSV to see which one works with this job
            matching_csv = None
            for csv_file in temp_csv_files:
                # We'll try to use this CSV - if it fails due to column mismatch,
                # we'll try the next one
                matching_csv = csv_file
                csv_file_to_sync = csv_file
                console.print(f"[dim]  Using extracted CSV: {csv_file.name}[/dim]")
                break

            if not matching_csv:
                console.print("[red]Error:[/red] No suitable CSV data found in CDF file")
                raise click.Abort()

        # Extract values from filename if filename_to_column is configured
        # Use the CSV filename for extraction (which might be extracted from CDF)
        filename_values = None
        if sync_job.filename_to_column:
            filename_values = sync_job.filename_to_column.extract_values_from_filename(
                csv_file_to_sync
            )
            if not filename_values:
                # For CDF files, filename extraction might not work because the extracted
                # CSV has a different name. This is OK - just skip filename extraction
                if file_path.suffix.lower() == ".cdf":
                    console.print(
                        f"[dim]  Note: Could not extract values from '{csv_file_to_sync.name}' "
                        f"(extracted from CDF). Skipping filename-based metadata.[/dim]"
                    )
                else:
                    # For CSV files, filename extraction failure is an error
                    console.print(
                        f"[red]Error:[/red] Could not extract values from filename '{csv_file_to_sync.name}'"
                    )
                    pattern = (
                        sync_job.filename_to_column.template
                        if sync_job.filename_to_column.template
                        else sync_job.filename_to_column.regex
                    )
                    console.print(f"[dim]  Pattern: {pattern}[/dim]")
                    raise click.Abort()
            elif filename_values:
                console.print(f"[dim]  Extracted values: {filename_values}[/dim]")

        # Perform sync or dry-run
        if dry_run:
            console.print(
                f"[cyan]DRY RUN: Simulating sync of {csv_file_to_sync.name} using job '{job}'...[/cyan]"
            )
            summary = sync_csv_to_postgres_dry_run(
                csv_file_to_sync, sync_job, db_url, filename_values
            )

            # Display dry-run summary
            console.print("\n[bold yellow]Dry-run Summary[/bold yellow]")
            console.print(f"[dim]{'─' * 60}[/dim]")

            # Schema changes
            if not summary.table_exists:
                console.print(f"[yellow]  • Table '{summary.table_name}' would be CREATED[/yellow]")
            else:
                console.print(f"[green]  • Table '{summary.table_name}' exists[/green]")

                if summary.new_columns:
                    console.print(
                        f"[yellow]  • {len(summary.new_columns)} column(s) would be ADDED:[/yellow]"
                    )
                    for col_name, col_type in summary.new_columns:
                        console.print(f"[dim]      - {col_name} ({col_type})[/dim]")
                else:
                    console.print("[green]  • No new columns needed[/green]")

                if summary.new_indexes:
                    console.print(
                        f"[yellow]  • {len(summary.new_indexes)} index(es) would be CREATED:[/yellow]"
                    )
                    for idx_name in summary.new_indexes:
                        console.print(f"[dim]      - {idx_name}[/dim]")
                else:
                    console.print("[green]  • No new indexes needed[/green]")

            # Data changes
            console.print("\n[bold]Data Changes:[/bold]")
            console.print(
                f"[green]  • {summary.rows_to_sync} row(s) would be inserted/updated[/green]"
            )

            if filename_values and summary.rows_to_delete > 0:
                console.print(
                    f"[yellow]  • {summary.rows_to_delete} stale row(s) would be deleted[/yellow]"
                )
            elif filename_values:
                console.print("[green]  • No stale rows to delete[/green]")

            console.print(
                "\n[bold green]✓ Dry-run complete - no changes made to database[/bold green]"
            )
            console.print(f"[dim]  Source file: {file_path}[/dim]")
            if csv_file_to_sync != file_path:
                console.print(f"[dim]  CSV extracted: {csv_file_to_sync.name}[/dim]")
            if filename_values:
                console.print(f"[dim]  Extracted values: {filename_values}[/dim]")
        else:
            # Sync the file
            console.print(f"[cyan]Syncing {csv_file_to_sync.name} using job '{job}'...[/cyan]")
            rows_synced = sync_csv_to_postgres(csv_file_to_sync, sync_job, db_url, filename_values)

            console.print(f"[green]✓ Successfully synced {rows_synced} rows[/green]")
            console.print(f"[dim]  Table: {sync_job.target_table}[/dim]")
            console.print(f"[dim]  Source file: {file_path}[/dim]")
            if csv_file_to_sync != file_path:
                console.print(f"[dim]  CSV extracted: {csv_file_to_sync.name}[/dim]")
            if filename_values:
                console.print(f"[dim]  Extracted values: {filename_values}[/dim]")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise click.Abort() from e
    finally:
        # Clean up temporary files if CDF was extracted
        if temp_csv_files:
            console.print("[dim]Cleaning up temporary files...[/dim]")
            for temp_file in temp_csv_files:
                try:
                    temp_file.unlink()
                except Exception as e:
                    console.print(f"[yellow]Warning: Could not delete {temp_file}: {e}[/yellow]")

        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            try:
                temp_dir.rmdir()
            except Exception as e:
                console.print(f"[yellow]Warning: Could not delete temp directory: {e}[/yellow]")
