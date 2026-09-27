"""`mnp` command-line entrypoint."""

import typer
from sqlalchemy import text

from mnp import __version__
from mnp.config import get_settings, load_sources
from mnp.db import get_engine

app = typer.Typer(no_args_is_help=True, help="Market news pipeline.")


@app.command()
def version() -> None:
    """Print the mnp version."""
    typer.echo(__version__)


@app.command()
def check() -> None:
    """Validate config files and check the database connection."""
    settings = get_settings()
    sources = load_sources()
    typer.echo(f"config: {len(sources)} source(s) loaded from {settings.config_dir}")
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        typer.echo(f"database: unreachable ({exc.__class__.__name__}: {exc})", err=True)
        raise typer.Exit(1) from exc
    typer.echo("database: ok")


if __name__ == "__main__":
    app()
