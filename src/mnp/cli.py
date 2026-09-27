"""`mnp` command-line entrypoint."""

import asyncio
import logging
import signal
from typing import Annotated

import typer
from sqlalchemy import text

from mnp import __version__
from mnp.collectors.base import make_http_client
from mnp.collectors.service import (
    collect_once,
    make_collector,
    run_source_loop,
    sync_sources,
)
from mnp.config import get_settings, load_sources
from mnp.db import make_engine

app = typer.Typer(no_args_is_help=True, help="Market news pipeline.")


def _setup_logging() -> None:
    logging.basicConfig(
        level=get_settings().log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Per-request lines are noise; collectors log one summary per poll.
    logging.getLogger("httpx").setLevel(logging.WARNING)


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

    async def ping() -> None:
        engine = make_engine()
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        finally:
            await engine.dispose()

    try:
        asyncio.run(ping())
    except Exception as exc:
        typer.echo(f"database: unreachable ({exc.__class__.__name__}: {exc})", err=True)
        raise typer.Exit(1) from exc
    typer.echo("database: ok")


@app.command()
def collect(
    source: Annotated[
        list[str] | None,
        typer.Option("--source", "-s", help="Source name (repeatable). Default: all enabled."),
    ] = None,
    once: Annotated[bool, typer.Option(help="Poll each source once, then exit.")] = False,
) -> None:
    """Fetch sources into raw_items; polls continuously unless --once."""
    _setup_logging()
    configs = load_sources()
    if source:
        by_name = {c.name: c for c in configs}
        if unknown := [n for n in source if n not in by_name]:
            typer.echo(f"unknown source(s): {', '.join(unknown)}", err=True)
            typer.echo(f"available: {', '.join(by_name)}", err=True)
            raise typer.Exit(2)
        selected = [by_name[n] for n in source]
    else:
        selected = [c for c in configs if c.enabled]
    if not selected:
        typer.echo("no enabled sources configured", err=True)
        raise typer.Exit(2)

    ok = asyncio.run(_collect(configs, selected, once))
    if not ok:
        raise typer.Exit(1)


async def _collect(configs, selected, once: bool) -> bool:
    settings = get_settings()
    engine = make_engine()
    try:
        ids = await sync_sources(engine, configs)
        async with make_http_client(settings) as client:
            jobs = []
            for cfg in selected:
                collector = make_collector(cfg, client)
                if collector is None:
                    typer.echo(f"{cfg.name}: skipped ({cfg.kind} collector not implemented)")
                    continue
                jobs.append((ids[cfg.name], collector))

            if once:
                results = await asyncio.gather(*(collect_once(engine, i, c) for i, c in jobs))
                for r in results:
                    status = "ok" if r.ok else f"FAILED ({r.error})"
                    typer.echo(f"{r.source}: {r.received} received, {r.inserted} new, {status}")
                return all(r.ok for r in results)

            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop.set)
            await asyncio.gather(*(run_source_loop(engine, i, c, stop) for i, c in jobs))
            return True
    finally:
        await engine.dispose()


if __name__ == "__main__":
    app()
