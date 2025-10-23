"""Command-line interface for data_sync."""

import click

from data_sync import __version__
from data_sync.cli_inspect import inspect
from data_sync.cli_prepare import prepare
from data_sync.cli_sync import sync


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def main(ctx: click.Context) -> None:
    """Sync CSV and CDF science files into PostgreSQL database.

    This application provides tools for syncing scientific data files
    into a PostgreSQL database for analysis and storage.
    """
    ctx.ensure_object(dict)


# Register commands
main.add_command(sync)
main.add_command(prepare)
main.add_command(inspect)
