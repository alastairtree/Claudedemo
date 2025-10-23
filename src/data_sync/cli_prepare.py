"""Prepare command for analyzing CSV files and generating config."""

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from data_sync.config import ColumnMapping, Index, IndexColumn, SyncConfig, SyncJob
from data_sync.type_detection import analyze_csv_types_and_nullable, suggest_id_column

console = Console()


def suggest_indexes(column_info: dict[str, tuple[str, bool]], id_column: str) -> list[Index]:
    """Suggest database indexes based on column types and names.

    Args:
        column_info: Dictionary mapping column names to (data_type, nullable) tuples
        id_column: Name of the ID column (to exclude from indexing)

    Returns:
        List of suggested Index objects

    Rules:
        - Date/datetime columns get descending indexes
        - Columns ending in '_id' or '_key' get ascending indexes
        - ID column is excluded (already a primary key)
    """
    indexes = []

    for col_name, (col_type, _nullable) in column_info.items():
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


def _create_column_mappings(
    columns: list[str],
    id_column: str,
    column_info: dict[str, tuple[str, bool]],
) -> list[ColumnMapping]:
    """Create column mappings for non-ID columns.

    Args:
        columns: List of all column names
        id_column: Name of the ID column to exclude
        column_info: Dictionary mapping column names to (data_type, nullable) tuples

    Returns:
        List of ColumnMapping objects for non-ID columns
    """
    column_mappings = []
    for col in columns:
        if col != id_column:
            col_type, nullable = column_info[col]
            column_mappings.append(
                ColumnMapping(csv_column=col, db_column=col, data_type=col_type, nullable=nullable)
            )
    return column_mappings


def _display_prepare_results(
    job: SyncJob,
    config: Path,
    id_column: str,
    column_info: dict[str, tuple[str, bool]],
    column_mappings: list[ColumnMapping],
    suggested_indexes: list[Index],
) -> None:
    """Display the results of the prepare command.

    Args:
        job: The created SyncJob
        config: Path to config file
        id_column: Name of the ID column
        column_info: Dictionary mapping column names to (data_type, nullable) tuples
        column_mappings: List of column mappings (excluding ID)
        suggested_indexes: List of suggested indexes
    """
    console.print("[green]✓ Job configuration created successfully![/green]")
    console.print(f"[dim]  Config file: {config}[/dim]")
    console.print(f"[dim]  Job name: {job.name}[/dim]")
    console.print(f"[dim]  Target table: {job.target_table}[/dim]")

    # Display column mappings in a table
    table = Table(title="Column Mappings")
    table.add_column("CSV Column", style="cyan")
    table.add_column("DB Column", style="green")
    table.add_column("Type", style="yellow")
    table.add_column("Nullable", style="magenta")

    # Add ID mapping
    id_type, id_nullable = column_info[id_column]
    nullable_str = "NULL" if id_nullable else "NOT NULL"
    table.add_row(id_column, "id", id_type + " (ID)", nullable_str)

    # Add other columns
    for col_mapping in column_mappings:
        nullable_str = "NULL" if col_mapping.nullable else "NOT NULL"
        table.add_row(
            col_mapping.csv_column,
            col_mapping.db_column,
            col_mapping.data_type or "text",
            nullable_str,
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


@click.command()
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

        # Analyze CSV file to detect types and nullable status
        column_info = analyze_csv_types_and_nullable(file_path)

        if not column_info:
            console.print("[red]Error:[/red] No columns found in CSV file")
            raise click.Abort()

        columns = list(column_info.keys())
        console.print(f"[dim]  Found {len(columns)} columns[/dim]")

        # Load or create config (need this early to get id_column_matchers)
        sync_config = SyncConfig.from_yaml(config) if config.exists() else SyncConfig(jobs={})

        # Suggest ID column using matchers from config if available
        id_column = suggest_id_column(columns, sync_config.id_column_matchers)
        console.print(f"[dim]  Suggested ID column: {id_column}[/dim]")

        # Create column mappings and suggest indexes
        column_mappings = _create_column_mappings(columns, id_column, column_info)
        suggested_indexes = suggest_indexes(column_info, id_column)
        console.print(f"[dim]  Suggested {len(suggested_indexes)} index(es)[/dim]")

        # Create the job with ID mapping that includes nullable info
        id_type, id_nullable = column_info[id_column]
        new_job = SyncJob(
            name=job,
            target_table=job,  # Use job name as table name
            id_mapping=[
                ColumnMapping(
                    csv_column=id_column,
                    db_column="id",
                    data_type=id_type,
                    nullable=id_nullable,
                )
            ],
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

        # Save config and display results
        sync_config.save_to_yaml(config)
        _display_prepare_results(
            new_job, config, id_column, column_info, column_mappings, suggested_indexes
        )

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise click.Abort() from e
