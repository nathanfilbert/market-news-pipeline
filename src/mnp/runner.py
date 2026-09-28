"""`mnp run`: every collector loop, the job workers and the API in one process.

Each loop is supervised: an unexpected exception is logged and the loop restarts after a
backoff, so one broken part never takes the process down. SIGINT/SIGTERM set a stop event;
loops finish their current step (a job's transaction commits or rolls back as a whole, so an
interrupted job simply stays pending) and the process exits. A second signal cancels
immediately.
"""

import asyncio
import contextlib
import logging
import random
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress

import uvicorn
from sqlalchemy.ext.asyncio import AsyncEngine

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.jev import JevClassifier
from mnp.classify.service import ClassifyHandler
from mnp.collectors.base import CollectorUnavailable, make_http_client
from mnp.collectors.service import make_collector, run_source_loop, sync_sources
from mnp.config import Settings, load_sources
from mnp.feed.build import handle_feed_job
from mnp.jobs import CLASSIFY, FEED, NORMALIZE, Handler, run_pending
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from mnp.outputs.queries import health

log = logging.getLogger(__name__)

WORKER_IDLE_SECONDS = 2.0  # how often an idle worker checks for new jobs
STATUS_LOG_SECONDS = 600.0  # periodic health summary in the log
SHUTDOWN_GRACE_SECONDS = 30.0
RESTART_BACKOFF_MAX = 300.0
# A repeat signal within this window is the same stop request (process group + forwarding),
# not a second Ctrl-C asking to cancel immediately.
DUPLICATE_SIGNAL_SECONDS = 2.0


async def _sleep(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, waking early if `stop` is set."""
    with suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def supervise(name: str, loop: Callable[[], Awaitable[None]], stop: asyncio.Event) -> None:
    """Run `loop` until stop; restart it with backoff if it raises."""
    failures = 0
    while not stop.is_set():
        try:
            await loop()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            delay = min(RESTART_BACKOFF_MAX, 5 * 2 ** min(failures, 10)) * random.uniform(0.5, 1)
            log.exception(
                "%s crashed; restarting in %.0fs",
                name,
                delay,
                extra={"task": name, "failures": failures, "retry_in_s": round(delay)},
            )
            await _sleep(stop, delay)


async def run_worker(engine: AsyncEngine, kind: str, handler: Handler, stop: asyncio.Event) -> None:
    """Drain `kind` jobs, then poll for new ones every few seconds."""
    while not stop.is_set():
        stats = await run_pending(engine, kind, handler, limit=100, stop=stop)
        if stats.processed:
            log.info(
                "%s: %d done, %d retrying, %d failed",
                kind,
                stats.done,
                stats.retrying,
                stats.failed,
                extra={
                    "worker": kind,
                    "done": stats.done,
                    "retrying": stats.retrying,
                    "failed": stats.failed,
                },
            )
        if stats.processed < 100:
            await _sleep(stop, WORKER_IDLE_SECONDS)


async def log_status(engine: AsyncEngine, stop: asyncio.Event) -> None:
    """Every few minutes, log the same summary /health returns."""
    while not stop.is_set():
        await _sleep(stop, STATUS_LOG_SECONDS)
        if stop.is_set():
            return
        async with engine.connect() as conn:
            h = await health(conn)
        problems = {
            s["name"]: s["status"] for s in h["sources"] if s["status"] not in ("ok", "disabled")
        }
        backlog = {k: v["pending"] for k, v in h["jobs"].items()}
        log.log(
            logging.WARNING if problems else logging.INFO,
            "status %s; sources not ok: %s; pending jobs: %s",
            h["status"],
            problems or "none",
            backlog,
            extra={"health": h["status"], "sources_not_ok": problems, "pending": backlog},
        )


class _EmbeddedServer(uvicorn.Server):
    """uvicorn without its own signal handling: the runner owns shutdown."""

    @contextlib.contextmanager
    def capture_signals(self):
        yield


async def serve_api(engine: AsyncEngine, host: str, port: int, stop: asyncio.Event) -> None:
    config = uvicorn.Config(
        create_app(engine), host=host, port=port, log_config=None, access_log=False
    )
    server = _EmbeddedServer(config)
    serving = asyncio.create_task(server.serve())
    await stop.wait()
    server.should_exit = True
    await serving


async def run(
    settings: Settings,
    engine: AsyncEngine,
    *,
    api: bool = True,
    host: str = "127.0.0.1",
    port: int = 8000,
    stop: asyncio.Event | None = None,
    install_signal_handlers: bool = True,
) -> None:
    stop = stop or asyncio.Event()
    configs = load_sources()
    ids = await sync_sources(engine, configs)
    await sync_assets(engine, load_assets())

    loop = asyncio.get_running_loop()
    if install_signal_handlers:
        first_signal_at: float | None = None

        def on_signal(sig: signal.Signals) -> None:
            nonlocal first_signal_at
            now = loop.time()
            if first_signal_at is None:
                first_signal_at = now
                log.info("%s received: shutting down gracefully", sig.name)
                stop.set()
            elif now - first_signal_at < DUPLICATE_SIGNAL_SECONDS:
                # The same stop request delivered twice, e.g. to the process group and
                # forwarded by `uv run`, or by systemd: not a request to hurry.
                return
            else:
                log.warning("%s received again: cancelling now", sig.name)
                for task in tasks:
                    task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, on_signal, sig)

    tasks: list[asyncio.Task[None]] = []
    async with make_http_client(settings) as client:

        def start(name: str, factory: Callable[[], Awaitable[None]]) -> None:
            tasks.append(asyncio.create_task(supervise(name, factory, stop), name=name))

        collectors = []
        for cfg in configs:
            if not cfg.enabled:
                continue
            try:
                collector = make_collector(cfg, client, settings)
            except CollectorUnavailable as exc:
                log.warning("source %s skipped: %s", cfg.name, exc, extra={"source": cfg.name})
                continue
            collectors.append(cfg.name)
            start(
                f"collect:{cfg.name}",
                lambda i=ids[cfg.name], c=collector: run_source_loop(engine, i, c, stop),
            )

        start("worker:normalize", lambda: run_worker(engine, NORMALIZE, handle_normalize_job, stop))
        start("worker:feed", lambda: run_worker(engine, FEED, handle_feed_job, stop))

        if settings.jev_api_key is not None:
            classifier = JevClassifier(
                client,
                settings.jev_api_key.get_secret_value(),
                model=settings.jev_model,
                url=settings.jev_url,
            )
            handler = ClassifyHandler(classifier)
            start("worker:classify", lambda: run_worker(engine, CLASSIFY, handler, stop))
        else:
            log.warning("JEV_API_KEY not set: classify jobs will queue up until it is")

        start("status", lambda: log_status(engine, stop))
        if api:
            start("api", lambda: serve_api(engine, host, port, stop))

        log.info(
            "running %d collectors (%s), workers: normalize%s, feed%s",
            len(collectors),
            ", ".join(collectors),
            ", classify" if settings.jev_api_key else "",
            f"; API on http://{host}:{port}" if api else "",
            extra={"collectors": collectors, "api": f"{host}:{port}" if api else None},
        )

        await stop.wait()
        _, pending = await asyncio.wait(tasks, timeout=SHUTDOWN_GRACE_SECONDS)
        for task in pending:
            log.warning("%s did not stop in time; cancelling", task.get_name())
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    if install_signal_handlers:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    log.info("stopped")
