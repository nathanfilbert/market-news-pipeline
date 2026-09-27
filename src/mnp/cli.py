"""`mnp` command-line entrypoint."""

import asyncio
import logging
import signal
from datetime import UTC, datetime
from typing import Annotated

import typer
from sqlalchemy import func, select, text

from mnp import __version__
from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.jev import JevClassifier
from mnp.classify.questions import QuestionSetError, load_question_set
from mnp.classify.service import ClassifyHandler, enqueue_classify
from mnp.collectors.base import CollectorUnavailable, make_http_client
from mnp.collectors.service import (
    collect_once,
    make_collector,
    run_source_loop,
    sync_sources,
)
from mnp.config import get_settings, load_sources
from mnp.db import make_engine
from mnp.jobs import CLASSIFY, NORMALIZE, run_pending
from mnp.models import (
    Article,
    ArticleAsset,
    ArticleVersion,
    Asset,
    Classification,
    Cluster,
    RawItem,
    Source,
)
from mnp.normalize.service import handle_normalize_job

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
                try:
                    jobs.append((ids[cfg.name], make_collector(cfg, client, settings)))
                except CollectorUnavailable as exc:
                    typer.echo(f"{cfg.name}: skipped ({exc})")

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


@app.command()
def normalize(
    limit: Annotated[int | None, typer.Option(help="Stop after this many jobs.")] = None,
) -> None:
    """Process pending normalize jobs: raw items -> articles, versions, clusters."""
    _setup_logging()
    ok = asyncio.run(_normalize(limit))
    if not ok:
        raise typer.Exit(1)


async def _normalize(limit: int | None) -> bool:
    engine = make_engine()

    async def counts() -> tuple[int, int, int]:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    select(
                        select(func.count()).select_from(Article).scalar_subquery(),
                        select(func.count()).select_from(ArticleVersion).scalar_subquery(),
                        select(func.count()).select_from(Cluster).scalar_subquery(),
                    )
                )
            ).one()
            return tuple(row)

    try:
        before = await counts()
        stats = await run_pending(engine, NORMALIZE, handle_normalize_job, limit=limit)
        after = await counts()
    finally:
        await engine.dispose()
    articles, versions, clusters = (a - b for a, b in zip(after, before, strict=True))
    typer.echo(
        f"normalize: {stats.done} done, {stats.retrying} retrying, {stats.failed} failed; "
        f"+{articles} articles, +{versions} versions, +{clusters} clusters"
    )
    return stats.retrying == 0 and stats.failed == 0


LimitOption = Annotated[int | None, typer.Option(help="Stop after this many jobs.")]
ShowOption = Annotated[bool, typer.Option(help="Print each classification made, for review.")]


@app.command()
def classify(limit: LimitOption = None, show: ShowOption = False) -> None:
    """Process pending classify jobs with Jev."""
    _setup_logging()
    ok = asyncio.run(_classify(limit=limit, show=show))
    if not ok:
        raise typer.Exit(1)


@app.command()
def reclassify(
    question_set: Annotated[str, typer.Option(help="Question set version, e.g. v1.1.")],
    since: Annotated[
        datetime | None,
        typer.Option(
            formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"],
            help="Only article versions received at or after this time (UTC).",
        ),
    ] = None,
    limit: LimitOption = None,
    show: ShowOption = False,
) -> None:
    """Classify stored article versions with a question set, next to existing labels."""
    _setup_logging()
    try:
        load_question_set(question_set)
    except QuestionSetError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    since = since.replace(tzinfo=UTC) if since and since.tzinfo is None else since
    ok = asyncio.run(_classify(limit=limit, show=show, question_set=question_set, since=since))
    if not ok:
        raise typer.Exit(1)


async def _classify(
    *,
    limit: int | None,
    show: bool,
    question_set: str | None = None,
    since: datetime | None = None,
) -> bool:
    settings = get_settings()
    if settings.jev_api_key is None:
        typer.echo("JEV_API_KEY is not set in .env", err=True)
        raise typer.Exit(2)
    engine = make_engine()
    try:
        await sync_assets(engine, load_assets())
        if question_set:
            async with engine.begin() as conn:
                stmt = select(ArticleVersion.id).order_by(ArticleVersion.id)
                if since:
                    stmt = stmt.where(ArticleVersion.received_at >= since)
                ids = list((await conn.execute(stmt)).scalars())
                queued = await enqueue_classify(conn, ids, question_set)
            typer.echo(f"reclassify: {queued} of {len(ids)} article versions queued")

        async with make_http_client(settings) as client:
            classifier = JevClassifier(
                client,
                settings.jev_api_key.get_secret_value(),
                model=settings.jev_model,
                url=settings.jev_url,
            )
            handler = ClassifyHandler(classifier)
            stats = await run_pending(engine, CLASSIFY, handler, limit=limit)
        typer.echo(
            f"classify: {stats.done} done, {stats.retrying} retrying, {stats.failed} failed; "
            f"{len(handler.classified)} new classifications"
        )
        if show and handler.classified:
            await _show_classifications(engine, handler.classified)
    finally:
        await engine.dispose()
    return stats.retrying == 0 and stats.failed == 0


async def _show_classifications(engine, classification_ids: list[int]) -> None:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(Classification, ArticleVersion.headline, Source.name.label("source"))
                .join(ArticleVersion, ArticleVersion.id == Classification.article_version_id)
                .join(RawItem, RawItem.id == ArticleVersion.raw_item_id)
                .join(Source, Source.id == RawItem.source_id)
                .where(Classification.id.in_(classification_ids))
                .order_by(Classification.id)
            )
        ).all()
        assets = (
            await conn.execute(
                select(
                    ArticleAsset.classification_id,
                    Asset.symbol,
                    ArticleAsset.relevance_prob,
                    ArticleAsset.candidate_via,
                )
                .join(Asset, Asset.id == ArticleAsset.asset_id)
                .where(ArticleAsset.classification_id.in_(classification_ids))
                .order_by(ArticleAsset.relevance_prob.desc())
            )
        ).all()
    by_classification: dict[int, list[str]] = {}
    for a in assets:
        via = "" if a.candidate_via == "alias_match" else " tag"
        by_classification.setdefault(a.classification_id, []).append(
            f"{a.symbol} {a.relevance_prob:.2f}{via}"
        )
    for r in rows:
        typer.echo(f"\n[{r.source}] {r.headline}")
        typer.echo(
            f"  {r.event_type} ({r.event_type_prob:.2f}) {r.domain}"
            f" | relevant {r.is_market_relevant_prob:.2f} new {r.is_new_information_prob:.2f}"
            f" promo {r.is_promotional_prob:.2f}"
            f" | sentiment {r.sentiment:+.2f} impact {r.impact:.2f} urgency {r.urgency:.2f}"
            f" | {r.model_version} {r.latency_ms}ms"
        )
        if tagged := by_classification.get(r.id):
            typer.echo(f"  assets: {', '.join(tagged)}")


if __name__ == "__main__":
    app()
