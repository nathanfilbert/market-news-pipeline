"""Dashboard pages. Server-rendered (Jinja2), with htmx for in-place filtering.

Everything is read-only: pages use the API's READ ONLY transaction. Feed content is untrusted,
so templates rely on autoescaping and never mark data as safe.
"""

import difflib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from mnp.classify.questions import QuestionSetError, load_question_set
from mnp.config import get_settings
from mnp.dashboard import queries as dq
from mnp.dashboard import tz
from mnp.outputs.api import Conn
from mnp.outputs.queries import (
    ArticleFilter,
    get_article,
    get_cluster,
    get_raw_item,
    health,
    parse_time,
    search_articles,
)

HERE = Path(__file__).parent
STATIC_DIR = HERE / "static"
templates = Jinja2Templates(directory=HERE / "templates")
router = APIRouter(prefix="/ui", include_in_schema=False)


# --- template helpers -----------------------------------------------------------------------


def fmt_dt(value: datetime | None) -> str:
    return value.astimezone(tz.zone()).strftime("%Y-%m-%d %H:%M") if value else "—"


def ago(value: datetime | None) -> str:
    if not value:
        return "never"
    seconds = (datetime.now(UTC) - value).total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{int(seconds // size)}{unit} ago"
    return "just now"


def pct(value: float | None, digits: int = 0) -> str:
    return "—" if value is None else f"{value * 100:.{digits}f}%"


def num(value: float | None, fmt: str = "{:.2f}") -> str:
    return "—" if value is None else fmt.format(value)


def merge(params: dict[str, Any], **changes: Any) -> dict[str, Any]:
    """Query parameters with `changes` applied, dropping blanks (for pagination links)."""
    return {k: v for k, v in {**params, **changes}.items() if v not in ("", None)}


templates.env.filters.update(dt=fmt_dt, ago=ago, pct=pct, num=num, merge=merge)
templates.env.globals["tojson_pretty"] = lambda v: json.dumps(v, indent=2, default=str)
templates.env.globals["timezone_label"] = tz.label


def render(request: Request, name: str, **context: Any) -> HTMLResponse:
    return templates.TemplateResponse(request, name, context)


def word_diff(old: str, new: str) -> list[tuple[str, str]]:
    """(op, text) segments turning `old` into `new`; op is equal, insert or delete."""
    a, b = old.split(), new.split()
    segments: list[tuple[str, str]] = []
    for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=a, b=b).get_opcodes():
        if op == "equal":
            segments.append(("equal", " ".join(a[i1:i2])))
        else:
            if i2 > i1:
                segments.append(("delete", " ".join(a[i1:i2])))
            if j2 > j1:
                segments.append(("insert", " ".join(b[j1:j2])))
    return segments


# --- chart data (Chart.js) -------------------------------------------------------------------


def label(value: str | None) -> str:
    return (value or "—").replace("_", " ")


def stacked_chart(days: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Articles per day stacked by source."""
    index = {d: i for i, d in enumerate(days)}
    series: dict[str, list[int]] = {}
    for r in rows:
        day = r["day"].isoformat()
        if day in index:
            series.setdefault(r["source"], [0] * len(days))[index[day]] += r["n"]
    return {
        "labels": [d[5:] for d in days],
        "datasets": [{"label": s, "data": series[s]} for s in sorted(series)],
    }


def bar_chart(pairs: list[tuple[str, int]], horizontal: bool = True) -> dict[str, Any]:
    return {
        "labels": [label(k) for k, _ in pairs],
        "datasets": [{"label": "articles", "data": [n for _, n in pairs]}],
        "horizontal": horizontal,
    }


def histogram_chart(counts: list[int], low: float, high: float) -> dict[str, Any]:
    step = (high - low) / len(counts)
    return {
        "labels": [f"{low + i * step:.1f}" for i in range(len(counts))],
        "datasets": [{"label": "articles", "data": counts}],
    }


def question_set_versions() -> list[str]:
    folder = get_settings().config_dir / "questions"
    return sorted(p.stem for p in folder.glob("*.yaml"))


def _question_set_raw(version: str) -> dict[str, Any]:
    return yaml.safe_load((get_settings().config_dir / "questions" / f"{version}.yaml").read_text())


# --- pages ----------------------------------------------------------------------------------


@router.get("", response_class=HTMLResponse)
async def overview(request: Request, conn: Conn) -> HTMLResponse:
    now = datetime.now(UTC)
    since = now - timedelta(days=14)
    qs = get_settings().question_set
    per_day = await dq.articles_per_day(conn, since, by="published", zone=tz.zone_name())
    summary = await dq.classification_summary(conn, qs, since=now - timedelta(days=7))
    return render(
        request,
        "overview.html",
        health=await health(conn),
        per_day_chart=stacked_chart(dq.days_between(since, now, tz.zone()), per_day),
        event_chart=bar_chart(summary["event_types"]),
        summary=summary,
        question_set=qs,
        high_impact=await search_articles(
            conn, ArticleFilter(since=now - timedelta(hours=48), min_impact=0.6, limit=10)
        ),
    )


@router.get("/sources", response_class=HTMLResponse)
async def sources(request: Request, conn: Conn) -> HTMLResponse:
    report = await health(conn)
    stats = await dq.source_stats(conn)
    return render(request, "sources.html", sources=report["sources"], stats=stats)


@router.get("/sources/{name}", response_class=HTMLResponse)
async def source_detail(request: Request, conn: Conn, name: str) -> HTMLResponse:
    row = await dq.source_row(conn, name)
    if row is None:
        raise HTTPException(404, "source not found")
    now = datetime.now(UTC)
    since = now - timedelta(days=30)
    status = next(s for s in (await health(conn))["sources"] if s["name"] == name)
    return render(
        request,
        "source.html",
        source=row,
        status=status,
        stats=(await dq.source_stats(conn)).get(name, {}),
        coverage_chart=stacked_chart(
            dq.days_between(since, now, tz.zone()),
            await dq.articles_per_day(
                conn, since, source=name, by="published", zone=tz.zone_name()
            ),
        ),
        raw_items=await dq.source_raw_items(conn, name),
        recent=await search_articles(conn, ArticleFilter(sources=(name,), limit=15)),
    )


PAGE_SIZE = 50


def ui_filter(params: dict[str, str]) -> tuple[ArticleFilter, str | None]:
    """ArticleFilter from form fields: blanks are ignored, bad values reported, not fatal."""
    kwargs: dict[str, Any] = {"limit": PAGE_SIZE}
    try:
        if since := params.get("since", "").strip():
            kwargs["since"] = parse_time(since)
        for key, target in (("source", "sources"), ("event_type", "event_types")):
            if value := params.get(key, "").strip():
                kwargs[target] = (value,)
        if assets := params.get("asset", "").strip():
            kwargs["assets"] = tuple(a.strip().upper() for a in assets.split(",") if a.strip())
        if domain := params.get("domain", "").strip():
            kwargs["domain"] = domain
        for key in ("min_impact", "min_relevance"):
            if value := params.get(key, "").strip():
                kwargs[key] = min(max(float(value), 0.0), 1.0)
        kwargs["include_backfill"] = params.get("include_backfill") == "true"
        kwargs["offset"] = max(int(params.get("offset") or 0), 0)
    except ValueError as exc:
        return ArticleFilter(limit=PAGE_SIZE), f"Ignored filters: {exc}"
    return ArticleFilter(**kwargs), None


@router.get("/articles", response_class=HTMLResponse)
async def articles(request: Request, conn: Conn) -> HTMLResponse:
    params = dict(request.query_params)
    f, error = ui_filter(params)
    rows = await search_articles(conn, f)
    context = {"articles": rows, "filter": f, "params": params, "error": error}
    if request.headers.get("HX-Request") and request.headers.get("HX-Target") == "article-rows":
        return render(request, "_article_rows.html", **context)
    event_types = list(load_question_set(f.question_set).questions["event_type"]["criteria"])
    sources = [s["name"] for s in (await health(conn))["sources"]]
    return render(request, "articles.html", event_types=event_types, sources=sources, **context)


@router.get("/articles/{article_id}", response_class=HTMLResponse)
async def article_detail(request: Request, conn: Conn, article_id: int) -> HTMLResponse:
    article = await get_article(conn, article_id)
    if article is None:
        raise HTTPException(404, "article not found")
    versions = article["versions"]
    for prev, cur in zip([None, *versions], versions, strict=False):
        cur["headline_diff"] = word_diff(prev["headline"], cur["headline"]) if prev else None
    cluster = None
    if article["cluster_id"]:
        cluster = await get_cluster(
            conn, article["cluster_id"], ArticleFilter(include_backfill=True, limit=100)
        )
    return render(request, "article.html", article=article, cluster=cluster)


@router.get("/classifications", response_class=HTMLResponse)
async def classifications(
    request: Request,
    conn: Conn,
    question_set: str | None = None,
    compare: str | None = None,
    days: int = 30,
) -> HTMLResponse:
    in_use = await dq.question_sets_in_use(conn)
    qs = question_set or get_settings().question_set
    since = datetime.now(UTC) - timedelta(days=days) if days > 0 else None
    comparison = None
    if compare and compare != qs:
        comparison = await dq.compare_question_sets(conn, compare, qs)
    summary = await dq.classification_summary(conn, qs, since=since)
    ranges = {"sentiment": (-1, 1)}
    return render(
        request,
        "classifications.html",
        question_set=qs,
        compare=compare,
        days=days,
        in_use=in_use,
        summary=summary,
        event_chart=bar_chart(summary["event_types"]),
        domain_chart=bar_chart(summary["domains"]),
        scatter_chart={
            "datasets": [{"label": "articles", "data": summary["scatter"]}],
            "xLabel": "sentiment (bearish → bullish)",
            "yLabel": "impact",
        },
        histogram_charts={
            field: histogram_chart(counts, *ranges.get(field, (0, 1)))
            for field, counts in summary["histograms"].items()
        },
        comparison=comparison,
    )


@router.get("/classifications/{classification_id}", response_class=HTMLResponse)
async def classification_detail(
    request: Request, conn: Conn, classification_id: int
) -> HTMLResponse:
    row = await dq.classification_detail(conn, classification_id)
    if row is None:
        raise HTTPException(404, "classification not found")
    c = row  # classification columns plus article_id, version_no and headline
    try:
        qs = load_question_set(c.question_set_version)
        prefix = qs.asset_prefix
        fixed = qs.questions
    except QuestionSetError:
        qs, prefix, fixed = None, "about_", {}
    answers = c.results.get("response", {}).get("answers", {})
    # Question-set order first (event type, domain, ...), then per-asset questions.
    order = [q for q in fixed if q in answers] + sorted(q for q in answers if q not in fixed)
    items = []
    for qid in order:
        answer = answers[qid]
        question = fixed.get(qid)
        if question is None and qid.startswith(prefix):
            question = {
                "type": "noul",
                "instructions": f"Is this article materially about {qid.removeprefix(prefix)}?",
            }
        items.append({"id": qid, "question": question or {}, "answer": answer})
    return render(
        request,
        "classification.html",
        c=c,
        row=row,
        items=items,
        assets=await dq.classification_assets(conn, classification_id),
        state=c.results.get("state"),
        response=c.results.get("response"),
        candidates=c.results.get("asset_candidates", []),
    )


@router.get("/questions", response_class=HTMLResponse)
async def questions(request: Request, version: str | None = None) -> HTMLResponse:
    versions = question_set_versions()
    if not versions:
        raise HTTPException(404, "no question sets found")
    current = version or get_settings().question_set
    if current not in versions:
        raise HTTPException(404, "question set not found")
    data = _question_set_raw(current)
    index = versions.index(current)
    previous = _question_set_raw(versions[index - 1]) if index > 0 else None
    changed = set()
    if previous:
        old, new = previous.get("questions", {}), data.get("questions", {})
        changed = {q for q in new if old.get(q) != new[q]}
        if previous.get("asset_question") != data.get("asset_question"):
            changed.add("asset_question")
    return render(
        request,
        "questions.html",
        versions=versions,
        version=current,
        active=get_settings().question_set,
        data=data,
        previous=previous,
        previous_version=versions[index - 1] if index > 0 else None,
        changed=changed,
    )


@router.get("/raw/{raw_item_id}", response_class=HTMLResponse)
async def raw_item(request: Request, conn: Conn, raw_item_id: int) -> HTMLResponse:
    item = await get_raw_item(conn, raw_item_id)
    if item is None:
        raise HTTPException(404, "raw item not found")
    return render(
        request,
        "raw.html",
        item=item,
        outputs=await dq.raw_item_outputs(conn, raw_item_id),
    )


@router.get("/clusters", response_class=HTMLResponse)
async def clusters(request: Request, conn: Conn, days: int = 7) -> HTMLResponse:
    since = datetime.now(UTC) - timedelta(days=days)
    return render(request, "clusters.html", clusters=await dq.clusters(conn, since), days=days)


@router.get("/clusters/{cluster_id}", response_class=HTMLResponse)
async def cluster_detail(request: Request, conn: Conn, cluster_id: int) -> HTMLResponse:
    cluster = await get_cluster(conn, cluster_id, ArticleFilter(include_backfill=True, limit=500))
    if cluster is None:
        raise HTTPException(404, "cluster not found")
    return render(request, "cluster.html", cluster=cluster)
