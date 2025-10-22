"""Command-line interface for data_sync."""

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from data_sync import __version__
from data_sync.config import ColumnMapping, Index, IndexColumn, SyncConfig, SyncJob
from data_sync.database import sync_csv_to_postgres
from data_sync.type_detection import analyze_csv_types, suggest_id_column

console = Console()


def suggest_indexes(columns: dict[str, str], id_column: str) -> list[Index]:
    """Suggest database indexes based on column types and names.

    Args:
        columns: Dictionary mapping column names to detected types
        id_column: Name of the ID column (to exclude from indexing)

    Returns:
        List of suggested Index objects

    Rules:
        - Date/datetime columns get descending indexes
        - Columns ending in '_id' or '_key' get ascending indexes
        - ID column is excluded (already a primary key)
    """
    indexes = []

    for col_name, col_type in columns.items():
        # Skip the ID column (it's already a primary key)
        if col_name == id_column:
            continue

        index_name = None
        order = None

        # Date/datetime columns get descending indexes
        if col_type in ("date", "datetime"):
            index_name = f"idx_{col_name}"
            order = "DESC"

        # Columns ending in _id or _key get ascending indexes
        elif col_name.lower().endswith("_id") or col_name.lower().endswith("_key"):
            index_name = f"idx_{col_name}"
            order = "ASC"

        # Create the index if we determined it should have one
        if index_name and order:
            indexes.append(
                Index(name=index_name, columns=[IndexColumn(column=col_name, order=order)])
            )

    return indexes


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def main(ctx: click.Context) -> None:
    """Sync CSV and CDF science files into PostgreSQL database.

    This application provides tools for syncing scientific data files
    into a PostgreSQL database for analysis and storage.
    """
    ctx.ensure_object(dict)


@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.argument("config", type=click.Path(exists=True, path_type=Path), required=True)
@click.argument("job", type=str, required=True)
@click.option(
    "--db-url",
    envvar="DATABASE_URL",
    required=True,
    help="PostgreSQL connection string (or set DATABASE_URL env var)",
)
def sync(file_path: Path, config: Path, job: str, db_url: str) -> None:
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


@main.command()
@click.argument("file_path", type=click.Path(exists=True, path_type=Path), required=True)
@click.argument("config", type=click.Path(path_type=Path), required=True)
@click.argument("job", type=str, required=True)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite job if it already exists in config",
)
def prepare(file_path: Path, config: Path, job: str, force: bool) -> None:
    """Prepare a config entry by analyzing a CSV file.

    Analyzes the CSV file to detect column names and data types, then generates
    a configuration entry for the specified job. The entry is added to the config
    file with suggested column mappings and types.

    Arguments:
        FILE_PATH: Path to the CSV file to analyze (required)
        CONFIG: Path to the YAML configuration file (required)
        JOB: Name of the job to create in the config file (required)

    Examples:
        # Create a new job config
        data-sync prepare data.csv config.yaml my_job

        # Overwrite existing job config
        data-sync prepare data.csv config.yaml my_job --force
    """
    try:
        console.print(f"[cyan]Analyzing {file_path.name}...[/cyan]")

        # Analyze CSV file to detect types
        column_types = analyze_csv_types(file_path)

        if not column_types:
            console.print("[red]Error:[/red] No columns found in CSV file")
            raise click.Abort()

        columns = list(column_types.keys())
        console.print(f"[dim]  Found {len(columns)} columns[/dim]")

        # Load or create config (need this early to get id_column_matchers)
        sync_config = SyncConfig.from_yaml(config) if config.exists() else SyncConfig(jobs={})

        # Suggest ID column using matchers from config if available
        id_column = suggest_id_column(columns, sync_config.id_column_matchers)
        console.print(f"[dim]  Suggested ID column: {id_column}[/dim]")

        # Create column mappings for non-ID columns
        column_mappings = []
        for col in columns:
            if col != id_column:
                col_type = column_types[col]
                column_mappings.append(
                    ColumnMapping(csv_column=col, db_column=col, data_type=col_type)
                )

        # Suggest indexes based on column types and names
        suggested_indexes = suggest_indexes(column_types, id_column)
        console.print(f"[dim]  Suggested {len(suggested_indexes)} index(es)[/dim]")

        # Create the job
        new_job = SyncJob(
            name=job,
            target_table=job,  # Use job name as table name
            id_mapping=[ColumnMapping(csv_column=id_column, db_column="id")],
            columns=column_mappings if column_mappings else None,
            indexes=suggested_indexes if suggested_indexes else None,
        )

        # Add or update job
        try:
            sync_config.add_or_update_job(new_job, force=force)
        except ValueError as e:
            console.print(f"[red]Error:[/red] {e}")
            console.print("[dim]Use --force to overwrite the existing job[/dim]")
            raise click.Abort() from e

        # Save config
        sync_config.save_to_yaml(config)

        # Display the generated config
        console.print("[green]✓ Job configuration created successfully![/green]")
        console.print(f"[dim]  Config file: {config}[/dim]")
        console.print(f"[dim]  Job name: {job}[/dim]")
        console.print(f"[dim]  Target table: {new_job.target_table}[/dim]")

        # Display column mappings in a table
        table = Table(title="Column Mappings")
        table.add_column("CSV Column", style="cyan")
        table.add_column("DB Column", style="green")
        table.add_column("Type", style="yellow")

        # Add ID mapping
        table.add_row(id_column, "id", column_types[id_column] + " (ID)")

        # Add other columns
        for col_mapping in column_mappings:
            table.add_row(
                col_mapping.csv_column, col_mapping.db_column, col_mapping.data_type or "text"
            )

        console.print(table)

        # Display suggested indexes if any
        if suggested_indexes:
            index_table = Table(title="Suggested Indexes")
            index_table.add_column("Index Name", style="cyan")
            index_table.add_column("Column", style="green")
            index_table.add_column("Order", style="yellow")

            for index in suggested_indexes:
                for idx_col in index.columns:
                    index_table.add_row(index.name, idx_col.column, idx_col.order)

            console.print(index_table)

        console.print("[dim]Review the configuration and adjust as needed before syncing.[/dim]")

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise click.Abort() from e


if __name__ == "__main__":
    main()
