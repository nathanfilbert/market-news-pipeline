"""`mnp` command-line entrypoint."""

import asyncio
import dataclasses
import json
import signal
from datetime import UTC, datetime, timedelta
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
from mnp.logs import setup_logging
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
from mnp.outputs.queries import (
    ArticleFilter,
    ArticleRow,
    health,
    parse_time,
    search_articles,
)

app = typer.Typer(no_args_is_help=True, help="Market news pipeline.")


def _setup_logging(fmt: str = "text") -> None:
    setup_logging(get_settings().log_level, fmt)  # type: ignore[arg-type]


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


def _time_option(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_time(value)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command()
def news(
    since: Annotated[
        str | None, typer.Option(help="e.g. 1h, 2d or 2026-09-27T12:00 (UTC).")
    ] = "24h",
    until: Annotated[str | None, typer.Option(help="Same formats as --since.")] = None,
    asset: Annotated[
        list[str] | None, typer.Option("--asset", "-a", help="Symbol, repeatable (any).")
    ] = None,
    event_type: Annotated[
        list[str] | None, typer.Option("--event-type", "-e", help="Repeatable (any).")
    ] = None,
    domain: Annotated[str | None, typer.Option(help="crypto, equities, macro or other.")] = None,
    source: Annotated[list[str] | None, typer.Option("--source", "-s")] = None,
    min_impact: Annotated[float | None, typer.Option(min=0, max=1)] = None,
    min_relevance: Annotated[
        float | None, typer.Option(min=0, max=1, help="Minimum market relevance.")
    ] = None,
    include_backfill: Annotated[
        bool, typer.Option(help="Include old news we only just saw (hidden by default).")
    ] = False,
    limit: Annotated[int, typer.Option(min=1, max=500)] = 20,
    as_json: Annotated[bool, typer.Option("--json", help="One JSON object per line.")] = False,
) -> None:
    """Query classified news, newest first."""
    f = ArticleFilter(
        since=_time_option(since),
        until=_time_option(until),
        assets=tuple(asset or ()),
        event_types=tuple(event_type or ()),
        domain=domain,
        sources=tuple(source or ()),
        min_impact=min_impact,
        min_relevance=min_relevance,
        include_backfill=include_backfill,
        limit=limit,
    )

    async def run() -> list[ArticleRow]:
        engine = make_engine()
        try:
            async with engine.connect() as conn:
                return await search_articles(conn, f)
        finally:
            await engine.dispose()

    rows = asyncio.run(run())
    if as_json:
        for r in rows:
            typer.echo(json.dumps(dataclasses.asdict(r), default=str))
        return
    if not rows:
        typer.echo("no matching articles")
    for r in rows:
        typer.echo(_format_article(r))


def _format_article(r: ArticleRow) -> str:
    when = r.first_seen_at.astimezone(UTC).strftime("%Y-%m-%d %H:%M")
    if r.published_at and r.first_seen_at - r.published_at > timedelta(hours=6):
        # Old news we only just saw (e.g. a new source's first fetch): say so.
        when += f" (published {r.published_at.astimezone(UTC):%Y-%m-%d %H:%M})"
    c = r.classification
    if c:
        label = (
            f"{c['event_type']} {c['event_type_prob']:.2f} | impact {c['impact']:.2f}"
            f" | sentiment {c['sentiment']:+.2f} | relevant {c['is_market_relevant_prob']:.2f}"
        )
    elif r.is_backfill:
        label = "(backfill: old news, not classified)"
    else:
        label = "(not classified yet)"
    tagged = [f"{a.symbol} {a.relevance_prob:.2f}" for a in r.assets if a.relevance_prob >= 0.5]
    lines = [f"{when}  [{r.source}]  {label}" + (f" | {', '.join(tagged)}" if tagged else "")]
    lines.append(f"  {r.headline}" + (f"  (v{r.version_no})" if r.version_no > 1 else ""))
    cluster = f"  cluster {r.cluster_id}: {r.cluster_size} articles" if r.cluster_size > 1 else ""
    lines.append(f"  {r.canonical_url}{cluster}")
    return "\n".join(lines)


@app.command()
def api(
    host: Annotated[str, typer.Option(help="Bind address; the API has no authentication.")] = (
        "127.0.0.1"
    ),
    port: int = 8000,
) -> None:
    """Serve the read-only HTTP API (docs at /docs)."""
    import uvicorn

    from mnp.outputs.api import create_app

    _setup_logging()
    uvicorn.run(create_app(make_engine()), host=host, port=port, log_level="info")


@app.command()
def run(
    api: Annotated[bool, typer.Option(help="Serve the read-only API in the same process.")] = True,
    host: Annotated[str, typer.Option(help="API bind address (no authentication).")] = (
        "127.0.0.1"
    ),
    port: int = 8000,
    log_format: Annotated[str, typer.Option(help="json or text.")] = "json",
) -> None:
    """Run everything: collectors, normalize and classify workers, and the API. Ctrl-C stops."""
    from mnp.runner import run as run_pipeline

    if log_format not in ("json", "text"):
        raise typer.BadParameter("--log-format must be json or text")
    _setup_logging(log_format)

    async def main() -> None:
        engine = make_engine()
        try:
            await run_pipeline(get_settings(), engine, api=api, host=host, port=port)
        finally:
            await engine.dispose()

    asyncio.run(main())


@app.command("health")
def health_command(
    as_json: Annotated[bool, typer.Option("--json", help="Print the full report as JSON.")] = False,
) -> None:
    """Source freshness and job backlog (same as GET /health). Exits 1 when degraded."""

    async def fetch() -> dict:
        engine = make_engine()
        try:
            async with engine.connect() as conn:
                return await health(conn)
        finally:
            await engine.dispose()

    report = asyncio.run(fetch())
    if as_json:
        typer.echo(json.dumps(report, default=str, indent=2))
    else:
        typer.echo(f"status: {report['status']}")
        for src in report["sources"]:
            last = src["last_success_at"]
            last = last.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S") if last else "never"
            error = f"  last error: {src['last_error']}" if src["status"] == "failing" else ""
            typer.echo(f"  {src['name']:18} {src['status']:9} last success {last}{error}")
        for kind, jobs in report["jobs"].items():
            age = jobs["oldest_pending_age_seconds"]
            oldest = f", oldest {age}s" if age is not None else ""
            typer.echo(
                f"  jobs {kind:13} {jobs['pending']} pending{oldest}, {jobs['failed']} failed"
            )
    if report["status"] != "ok":
        raise typer.Exit(1)


@app.command("catch-up")
def catch_up_command(
    since: Annotated[
        str, typer.Option(help="How far back you want history: e.g. 7d, 30d or 2026-09-01.")
    ] = "7d",
    source: Annotated[
        list[str] | None,
        typer.Option("--source", "-s", help="Source name (repeatable). Default: all enabled."),
    ] = None,
    classify: Annotated[bool, typer.Option(help="Classify new articles with Jev.")] = True,
) -> None:
    """Fetch everything the sources currently offer, process it, and report coverage.

    One-shot counterpart to `mnp run`: builds an initial dataset or fills gaps after downtime.
    Feeds only hold their latest items, so how far back it reaches depends on the source.
    """
    from mnp.catchup import catch_up

    _setup_logging()
    window_start = _time_option(since)
    if source:
        known = {c.name for c in load_sources()}
        if unknown := [n for n in source if n not in known]:
            typer.echo(f"unknown source(s): {', '.join(unknown)}", err=True)
            raise typer.Exit(2)

    async def main():
        engine = make_engine()
        try:
            return await catch_up(
                get_settings(), engine, since=window_start, source_names=source, classify=classify
            )
        finally:
            await engine.dispose()

    report = asyncio.run(main())
    typer.echo(f"\ncatch-up since {report.since:%Y-%m-%d %H:%M} UTC")
    for r in report.fetched:
        status = "ok" if r.ok else f"FAILED ({r.error})"
        typer.echo(f"  {r.source:18} {r.received:4} received, {r.inserted:4} new, {status}")
    for name, reason in report.skipped.items():
        typer.echo(f"  {name:18} skipped ({reason})")
    n = report.normalize
    typer.echo(f"normalize: {n.done} done, {n.retrying} retrying, {n.failed} failed")
    typer.echo(f"history adopted (old-news flag cleared inside the window): {report.adopted}")
    if report.classify is None:
        reason = "--no-classify" if not classify else "JEV_API_KEY not set"
        typer.echo(f"classify: skipped ({reason}); jobs stay queued")
    else:
        c = report.classify
        typer.echo(f"classify: {c.done} done, {c.retrying} retrying, {c.failed} failed")

    total_days = (datetime.now(UTC).date() - report.since.date()).days + 1
    typer.echo(f"\ncoverage ({total_days} days, by publish date):")
    for cov in report.coverage:
        oldest = f"back to {cov.oldest:%Y-%m-%d}" if cov.oldest else "nothing in window"
        empty = f"{len(cov.empty_days)} of {total_days} days without articles"
        if cov.empty_days and len(cov.empty_days) <= 10:
            empty += ": " + ", ".join(f"{d:%m-%d}" for d in cov.empty_days)
        typer.echo(f"  {cov.source:18} {cov.articles:5} articles, {oldest}; {empty}")

    failed = any(not r.ok for r in report.fetched) or report.normalize.failed
    if failed or (report.classify and report.classify.failed):
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
