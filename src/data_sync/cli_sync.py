"""Sync command for syncing CSV files to database."""

from pathlib import Path

import click
from rich.console import Console

from data_sync.config import SyncConfig
from data_sync.database import sync_csv_to_postgres, sync_csv_to_postgres_dry_run

console = Console()


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
def sync(file_path: Path, config: Path, job: str, db_url: str, dry_run: bool) -> None:
    """Sync a CSV file to the database using a configuration.

    Arguments:
        FILE_PATH: Path to the CSV file to sync (required)
        CONFIG: Path to the YAML configuration file (required)
        JOB: Name of the job to run from the config file (required)

    Examples:
        # Using command line option
        data-sync sync data.csv config.yaml my_job --db-url postgresql://localhost/mydb

        # Using environment variable
        export DATABASE_URL=postgresql://localhost/mydb
        data-sync sync data.csv config.yaml my_job

        # Dry-run mode to preview changes
        data-sync sync data.csv config.yaml my_job --dry-run
    """
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

        # Extract date from filename if date_mapping is configured
        sync_date = None
        if sync_job.date_mapping:
            sync_date = sync_job.date_mapping.extract_date_from_filename(file_path)
            if not sync_date:
                console.print(
                    f"[red]Error:[/red] Could not extract date from filename '{file_path.name}'"
                )
                console.print(f"[dim]  Regex pattern: {sync_job.date_mapping.filename_regex}[/dim]")
                raise click.Abort()
            console.print(f"[dim]  Extracted date: {sync_date}[/dim]")

        # Perform sync or dry-run
        if dry_run:
            console.print(
                f"[cyan]DRY RUN: Simulating sync of {file_path.name} using job '{job}'...[/cyan]"
            )
            summary = sync_csv_to_postgres_dry_run(file_path, sync_job, db_url, sync_date)

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

            if sync_date and summary.rows_to_delete > 0:
                console.print(
                    f"[yellow]  • {summary.rows_to_delete} stale row(s) would be deleted[/yellow]"
                )
            elif sync_date:
                console.print("[green]  • No stale rows to delete[/green]")

            console.print(
                "\n[bold green]✓ Dry-run complete - no changes made to database[/bold green]"
            )
            console.print(f"[dim]  File: {file_path}[/dim]")
            if sync_date:
                console.print(f"[dim]  Date: {sync_date}[/dim]")
        else:
            # Sync the file
            console.print(f"[cyan]Syncing {file_path.name} using job '{job}'...[/cyan]")
            rows_synced = sync_csv_to_postgres(file_path, sync_job, db_url, sync_date)

            console.print(f"[green]✓ Successfully synced {rows_synced} rows[/green]")
            console.print(f"[dim]  Table: {sync_job.target_table}[/dim]")
            console.print(f"[dim]  File: {file_path}[/dim]")
            if sync_date:
                console.print(f"[dim]  Date: {sync_date}[/dim]")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise click.Abort() from e
