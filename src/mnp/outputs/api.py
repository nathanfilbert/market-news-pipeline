"""Read-only HTTP API (FastAPI).

Every request runs in a READ ONLY transaction. There is no authentication: bind to localhost
(the default in `mnp api`). Finnhub's terms also forbid redistributing its data. Articles from
sources whose terms require a citation (GDELT) carry it in `attribution`.
"""

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from mnp import __version__
from mnp.outputs.queries import (
    DEFAULT_MIN_ASSET_RELEVANCE,
    MAX_LIMIT,
    ArticleFilter,
    ArticleRow,
    get_article,
    get_cluster,
    get_raw_item,
    health,
    parse_time,
    search_articles,
)


class AssetTagOut(BaseModel):
    symbol: str
    relevance_prob: float
    via: str


class AttributionOut(BaseModel):
    """A citation the source's terms require wherever its data is used or redistributed."""

    text: str
    url: str


class ClassificationOut(BaseModel):
    id: int
    event_type: str | None
    event_type_prob: float | None
    domain: str | None
    is_market_relevant_prob: float | None
    is_new_information_prob: float | None
    is_promotional_prob: float | None
    sentiment: float | None
    impact: float | None
    urgency: float | None
    classifier: str
    model_version: str
    question_set_version: str
    classified_at: datetime


class ArticleOut(BaseModel):
    id: int
    url: str
    first_seen_at: datetime
    cluster_id: int | None
    cluster_size: int
    clustered_at: datetime | None
    cluster_method: str | None
    is_backfill: bool
    source: str
    version_no: int
    headline: str
    summary: str | None
    published_at: datetime | None
    received_at: datetime
    classification: ClassificationOut | None
    assets: list[AssetTagOut]
    attribution: AttributionOut | None = None

    @classmethod
    def from_row(cls, row: ArticleRow) -> "ArticleOut":
        return cls(
            id=row.id,
            url=row.canonical_url,
            first_seen_at=row.first_seen_at,
            cluster_id=row.cluster_id,
            cluster_size=row.cluster_size,
            clustered_at=row.clustered_at,
            cluster_method=row.cluster_method,
            is_backfill=row.is_backfill,
            source=row.source,
            version_no=row.version_no,
            headline=row.headline,
            summary=row.summary,
            published_at=row.published_at,
            received_at=row.received_at,
            classification=row.classification,
            assets=[AssetTagOut(**vars(a)) for a in row.assets],
            attribution=row.attribution,
        )


class ArticleList(BaseModel):
    count: int
    limit: int
    offset: int
    articles: list[ArticleOut]


class ClassificationDetail(ClassificationOut):
    latency_ms: int
    content_hash: str
    results: dict[str, Any]
    assets: list[AssetTagOut]


class VersionOut(BaseModel):
    id: int
    version_no: int
    source: str
    headline: str
    summary: str | None
    body: str | None
    author: str | None
    language: str
    published_at: datetime | None
    received_at: datetime
    content_hash: str
    raw_item_id: int
    raw_url: str
    attribution: AttributionOut | None = None
    classifications: list[ClassificationDetail]


class ArticleDetail(BaseModel):
    id: int
    url: str
    first_seen_at: datetime
    cluster_id: int | None
    cluster_url: str | None
    clustered_at: datetime | None
    cluster_method: str | None
    is_backfill: bool
    versions: list[VersionOut]


class ClusterOut(BaseModel):
    id: int
    first_seen_at: datetime
    representative_article_id: int
    articles: list[ArticleOut]


class RawItemOut(BaseModel):
    id: int
    source: str
    fetched_at: datetime
    external_id: str | None
    url: str | None
    payload_sha256: str
    payload: dict[str, Any]
    attribution: AttributionOut | None = None


async def _connection(request: Request) -> AsyncIterator[AsyncConnection]:
    engine: AsyncEngine = request.app.state.engine
    async with engine.connect() as conn, conn.begin():
        await conn.execute(text("SET TRANSACTION READ ONLY"))
        yield conn


Conn = Annotated[AsyncConnection, Depends(_connection)]


def _time(value: str | None, name: str) -> datetime | None:
    if value is None:
        return None
    try:
        return parse_time(value)
    except ValueError as exc:
        raise HTTPException(422, f"{name}: {exc}") from exc


def _filter(
    since: Annotated[str | None, Query(description="e.g. 1h, 2d or an ISO datetime")] = None,
    until: Annotated[str | None, Query(description="e.g. 30m or an ISO datetime")] = None,
    asset: Annotated[list[str] | None, Query(description="Asset symbol; repeatable (any)")] = None,
    event_type: Annotated[list[str] | None, Query(description="Repeatable (any)")] = None,
    domain: str | None = None,
    source: Annotated[list[str] | None, Query(description="Source name; repeatable")] = None,
    min_impact: Annotated[float | None, Query(ge=0, le=1)] = None,
    min_relevance: Annotated[
        float | None, Query(ge=0, le=1, description="Minimum is_market_relevant_prob")
    ] = None,
    min_asset_relevance: Annotated[float, Query(ge=0, le=1)] = DEFAULT_MIN_ASSET_RELEVANCE,
    include_backfill: Annotated[
        bool, Query(description="Include old news we only just saw (hidden by default)")
    ] = False,
    question_set: Annotated[str | None, Query(description="Default: QUESTION_SET")] = None,
    classifier: str = "jev",
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ArticleFilter:
    return ArticleFilter(
        since=_time(since, "since"),
        until=_time(until, "until"),
        assets=tuple(asset or ()),
        event_types=tuple(event_type or ()),
        domain=domain,
        sources=tuple(source or ()),
        min_impact=min_impact,
        min_relevance=min_relevance,
        min_asset_relevance=min_asset_relevance,
        include_backfill=include_backfill,
        classifier=classifier,
        limit=limit,
        offset=offset,
        **({"question_set": question_set} if question_set else {}),
    )


Filter = Annotated[ArticleFilter, Depends(_filter)]


def create_app(engine: AsyncEngine) -> FastAPI:
    app = FastAPI(
        title="Market News Pipeline",
        version=__version__,
        description="Read-only access to collected, classified market news.",
    )
    app.state.engine = engine

    # The read-only dashboard (v1.1) lives under /ui; "/" sends people there.
    from fastapi.responses import RedirectResponse
    from fastapi.staticfiles import StaticFiles

    from mnp.dashboard.routes import STATIC_DIR, router

    app.include_router(router)

    from mnp.feed.api import router as feed_router

    app.include_router(feed_router)
    app.mount("/ui/static", StaticFiles(directory=STATIC_DIR), name="dashboard-static")

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/ui")

    @app.get("/articles", response_model=ArticleList)
    async def list_articles(conn: Conn, f: Filter) -> ArticleList:
        """Newest first. Each article is shown as its latest version and classification."""
        rows = await search_articles(conn, f)
        return ArticleList(
            count=len(rows),
            limit=f.limit,
            offset=f.offset,
            articles=[ArticleOut.from_row(r) for r in rows],
        )

    @app.get("/articles/{article_id}", response_model=ArticleDetail)
    async def article_detail(conn: Conn, article_id: int) -> ArticleDetail:
        """Every version of an article, with all classifications and a link to the raw item."""
        article = await get_article(conn, article_id)
        if article is None:
            raise HTTPException(404, "article not found")
        cluster_id = article["cluster_id"]
        return ArticleDetail(
            id=article["id"],
            url=article["canonical_url"],
            first_seen_at=article["first_seen_at"],
            cluster_id=cluster_id,
            cluster_url=f"/clusters/{cluster_id}" if cluster_id else None,
            clustered_at=article["clustered_at"],
            cluster_method=article["cluster_method"],
            is_backfill=article["is_backfill"],
            versions=[
                VersionOut(
                    **{k: v for k, v in version.items() if k != "classifications"},
                    raw_url=f"/raw/{version['raw_item_id']}",
                    classifications=[
                        ClassificationDetail(
                            **{k: v for k, v in c.items() if k != "assets"},
                            assets=[AssetTagOut(**vars(a)) for a in c["assets"]],
                        )
                        for c in version["classifications"]
                    ],
                )
                for version in article["versions"]
            ],
        )

    @app.get("/clusters/{cluster_id}", response_model=ClusterOut)
    async def cluster_detail(
        conn: Conn, cluster_id: int, question_set: str | None = None, classifier: str = "jev"
    ) -> ClusterOut:
        """A near-duplicate story group: every article covering the same story."""
        f = ArticleFilter(
            limit=MAX_LIMIT,
            include_backfill=True,  # a cluster shows all of its articles
            classifier=classifier,
            **({"question_set": question_set} if question_set else {}),
        )
        cluster = await get_cluster(conn, cluster_id, f)
        if cluster is None:
            raise HTTPException(404, "cluster not found")
        return ClusterOut(
            id=cluster["id"],
            first_seen_at=cluster["first_seen_at"],
            representative_article_id=cluster["representative_article_id"],
            articles=[ArticleOut.from_row(r) for r in cluster["articles"]],
        )

    @app.get("/raw/{raw_item_id}", response_model=RawItemOut)
    async def raw_item(conn: Conn, raw_item_id: int) -> RawItemOut:
        """A raw item exactly as collected."""
        item = await get_raw_item(conn, raw_item_id)
        if item is None:
            raise HTTPException(404, "raw item not found")
        return RawItemOut(**item)

    @app.get("/health")
    async def health_check(conn: Conn) -> dict[str, Any]:
        """Per-source freshness and last error, plus job backlog. status: ok | degraded."""
        return await health(conn)

    return app
