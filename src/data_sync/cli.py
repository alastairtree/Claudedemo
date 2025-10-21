"""Command-line interface for data_sync."""

from pathlib import Path

import click
from rich.console import Console

from data_sync import __version__
from data_sync.core import sync_file

console = Console()


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
def sync(file_path: Path) -> None:
    """Sync a CSV or CDF file to the database.

    FILE_PATH is the path to the file to sync. This argument is required.

    Examples:
        data-sync sync /path/to/data.csv
        data-sync sync /path/to/science.cdf
    """
    try:
        result = sync_file(file_path)
        console.print(f"[green]Successfully synced:[/green] {file_path}")
        console.print(f"[dim]Records processed: {result['records_processed']}[/dim]")
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise click.Abort() from e


if __name__ == "__main__":
    main()
