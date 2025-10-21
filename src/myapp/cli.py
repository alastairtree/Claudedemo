"""Command-line interface for myapp."""

from typing import Optional

import click
from rich.console import Console

from myapp import __version__
from myapp.core import process_data

console = Console()


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def main(ctx: click.Context) -> None:
    """A robust CLI application built with Python 3.13.

    This application demonstrates best practices for building
    maintainable Python CLI tools.
    """
    ctx.ensure_object(dict)


@main.command()
@click.argument("input_text", type=str)
@click.option(
    "--uppercase",
    "-u",
    is_flag=True,
    help="Convert output to uppercase",
)
@click.option(
    "--repeat",
    "-r",
    type=int,
    default=1,
    help="Number of times to repeat the output",
)
def greet(input_text: str, uppercase: bool, repeat: int) -> None:
    """Greet with the provided INPUT_TEXT.

    Example:
        myapp greet "Hello World" --uppercase --repeat 2
    """
    result = process_data(input_text, uppercase=uppercase, repeat=repeat)
    console.print(f"[green]{result}[/green]")


@main.command()
@click.option(
    "--format",
    "-f",
    type=click.Choice(["json", "yaml", "text"], case_sensitive=False),
    default="text",
    help="Output format",
)
def info(format: str) -> None:
    """Display application information.

    Example:
        myapp info --format json
    """
    info_data = {
        "name": "myapp",
        "version": __version__,
        "description": "A robust CLI application",
    }

    if format == "json":
        import json
        console.print(json.dumps(info_data, indent=2))
    elif format == "yaml":
        console.print("[yellow]YAML format not yet implemented[/yellow]")
    else:
        console.print(f"[bold]Name:[/bold] {info_data['name']}")
        console.print(f"[bold]Version:[/bold] {info_data['version']}")
        console.print(f"[bold]Description:[/bold] {info_data['description']}")


if __name__ == "__main__":
    main()
