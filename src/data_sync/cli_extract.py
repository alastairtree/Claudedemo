"""Extract command for converting CDF files to CSV."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from data_sync.cdf_extractor import extract_cdf_to_csv

console = Console()


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format.

    Args:
        size_bytes: File size in bytes

    Returns:
        Formatted file size string
    """
    size: float = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


@click.command()
@click.argument("files", nargs=-1, type=click.Path(exists=True, path_type=Path), required=True)
@click.option(
    "--output-path",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for CSV files (default: current directory)",
)
@click.option(
    "--filename",
    type=str,
    default="[SOURCE_FILE]-[VARIABLE_NAME].csv",
    help="Filename template for output files. Use [SOURCE_FILE] and [VARIABLE_NAME] as placeholders.",
)
@click.option(
    "--automerge/--no-automerge",
    default=True,
    help="Merge variables with the same record count into a single CSV (default: enabled)",
)
@click.option(
    "--append",
    is_flag=True,
    default=False,
    help="Append to existing CSV files instead of overwriting (default: disabled)",
)
@click.option(
    "--variables",
    "-v",
    multiple=True,
    help="Specific variable names to extract (can be specified multiple times). Default: extract all variables.",
)
@click.option(
    "--max-records",
    type=int,
    default=None,
    help="Maximum number of records to extract per variable (default: extract all records)",
)
def extract(
    files: tuple[Path, ...],
    output_path: Path | None,
    filename: str,
    automerge: bool,
    append: bool,
    variables: tuple[str, ...],
    max_records: int | None,
) -> None:
    """Extract data from CDF files to CSV format.

    Reads CDF science data files and extracts variable data into CSV files.
    Variables with array data are expanded into multiple columns with sensible names.

    Arguments:
        FILES: One or more CDF files to extract data from

    Examples:
        # Extract all variables from a CDF file
        data-sync extract data.cdf

        # Extract to a specific directory with custom filename
        data-sync extract data.cdf -o output/ --filename "[SOURCE_FILE]_data.csv"

        # Extract specific variables without auto-merging
        data-sync extract data.cdf -v epoch -v vectors --no-automerge

        # Extract and append to existing CSV files
        data-sync extract data1.cdf data2.cdf --append

        # Extract first 100 records from each variable
        data-sync extract data.cdf --max-records 100

        # Extract multiple files with auto-merge enabled
        data-sync extract *.cdf -o csv_output/
    """
    try:
        # Determine output directory
        output_dir = output_path if output_path else Path.cwd()

        # Convert variables tuple to list (None if empty)
        variable_list = list(variables) if variables else None

        console.print(f"[cyan]Extracting data from {len(files)} CDF file(s)...[/cyan]")
        if variable_list:
            console.print(f"[dim]  Extracting variables: {', '.join(variable_list)}[/dim]")
        console.print(f"[dim]  Output directory: {output_dir}[/dim]")
        console.print(f"[dim]  Auto-merge: {automerge}[/dim]")
        console.print(f"[dim]  Append mode: {append}[/dim]")
        if max_records is not None:
            console.print(f"[dim]  Max records per variable: {max_records:,}[/dim]")
        console.print()

        total_files_created = 0
        total_rows = 0

        for cdf_file in files:
            console.print(f"[bold]Processing:[/bold] {cdf_file.name}")

            try:
                results = extract_cdf_to_csv(
                    cdf_file_path=cdf_file,
                    output_dir=output_dir,
                    filename_template=filename,
                    automerge=automerge,
                    append=append,
                    variable_names=variable_list,
                    max_records=max_records,
                )

                if not results:
                    console.print(
                        "[yellow]  No data extracted (no suitable variables found)[/yellow]\n"
                    )
                    continue

                # Display results table
                table = Table(show_header=True, box=None, padding=(0, 1))
                table.add_column("Output File", style="cyan")
                table.add_column("Variables", style="yellow")
                table.add_column("Columns", justify="right", style="green")
                table.add_column("Rows", justify="right", style="magenta")
                table.add_column("Size", justify="right", style="dim")

                for result in results:
                    var_display = ", ".join(result.variable_names)
                    if len(var_display) > 40:
                        var_display = var_display[:37] + "..."

                    table.add_row(
                        result.output_file.name,
                        var_display,
                        str(result.num_columns),
                        f"{result.num_rows:,}",
                        format_file_size(result.file_size),
                    )

                    total_files_created += 1
                    total_rows += result.num_rows

                console.print(table)
                console.print()

            except ValueError as e:
                console.print(f"[red]Error processing {cdf_file.name}:[/red] {e}\n")
                continue
            except Exception as e:
                console.print(f"[red]Unexpected error processing {cdf_file.name}:[/red] {e}\n")
                continue

        # Final summary
        console.print("[bold green]✓ Extraction complete[/bold green]")
        console.print(f"[dim]  Created/updated {total_files_created} CSV file(s)[/dim]")
        console.print(f"[dim]  Total rows extracted: {total_rows:,}[/dim]")
        console.print(f"[dim]  Output directory: {output_dir.absolute()}[/dim]")

    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise click.Abort() from e
