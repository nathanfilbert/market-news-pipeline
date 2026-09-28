"""v1.1 dashboard: every page renders from fixture data; filters match `mnp news`."""

import re

import httpx
import pytest
from sqlalchemy import update

from mnp.classify.assets import load_assets, sync_assets
from mnp.classify.fake import FakeClassifier
from mnp.classify.service import ClassifyHandler
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import PROJECT_ROOT
from mnp.dashboard.routes import merge, ui_filter, word_diff
from mnp.jobs import CLASSIFY, NORMALIZE, run_pending
from mnp.models import ArticleVersion
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from mnp.outputs.queries import search_articles
from tests.conftest import fixture_bytes, source

pytestmark = pytest.mark.db

XSS = "<img src=x onerror=alert(1)>"


@pytest.fixture
async def ui(engine, server, client):
    """Two sources collected, normalized and classified (FakeClassifier named 'jev')."""
    await sync_assets(engine, load_assets(PROJECT_ROOT / "config" / "assets.yaml"))
    ids = await sync_sources(engine, [source("a"), source("b")])
    feed = fixture_bytes("rss/wordpress.xml").replace(
        b"Protocol X patches",
        b"Bitcoin lender Protocol X patches",  # so BTC gets tagged
    )
    server.serve("a", content=feed)
    server.serve("b", fixture="rss/second_source.xml")
    for name in ("a", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    fake = FakeClassifier()
    fake.name = "jev"
    await run_pending(engine, CLASSIFY, ClassifyHandler(fake, PROJECT_ROOT / "config"))
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def page(ui, path, **kwargs) -> str:
    response = await ui.get(path, **kwargs)
    assert response.status_code == 200, (path, response.text[:500])
    return response.text


def links(html: str, prefix: str) -> list[str]:
    return re.findall(rf'href="({re.escape(prefix)}\d+)"', html)


async def test_root_redirects_to_dashboard(ui):
    response = await ui.get("/")
    assert response.status_code == 307 and response.headers["location"] == "/ui"


async def test_overview(ui):
    html = await page(ui, "/ui")
    assert "Overview" in html
    assert 'data-chart="stacked"' in html and 'id="per-day"' in html
    assert "hack exploit" in html  # event-type mix and the high-impact table


async def test_sources_and_source_detail(ui):
    html = await page(ui, "/ui/sources")
    assert 'href="/ui/sources/a"' in html and 'href="/ui/sources/b"' in html
    detail = await page(ui, "/ui/sources/a")
    assert "https://a.example.com/rss.xml" in detail
    assert links(detail, "/ui/raw/")  # recent raw items
    assert (await ui.get("/ui/sources/nope")).status_code == 404


async def test_articles_list_matches_the_query_layer(ui, engine):
    html = await page(ui, "/ui/articles", params={"event_type": "hack_exploit"})
    async with engine.connect() as conn:
        f, _ = ui_filter({"event_type": "hack_exploit"})
        expected = [r.id for r in await search_articles(conn, f)]
    shown = [int(x.rsplit("/", 1)[1]) for x in links(html, "/ui/articles/")]
    assert shown == expected and expected


async def test_articles_blank_and_bad_filters(ui):
    blank = await page(ui, "/ui/articles", params={"min_impact": "", "since": "", "asset": ""})
    assert len(links(blank, "/ui/articles/")) == 5
    bad = await page(ui, "/ui/articles", params={"since": "whenever"})
    assert "Ignored filters" in bad


async def test_articles_htmx_partial(ui):
    html = await page(
        ui,
        "/ui/articles",
        params={"domain": "crypto"},
        headers={"HX-Request": "true", "HX-Target": "article-rows"},
    )
    assert "<nav>" not in html and "<table" in html


async def test_trace_article_to_classification_and_raw(ui):
    listing = await page(ui, "/ui/articles", params={"event_type": "hack_exploit"})
    [article_url] = links(listing, "/ui/articles/")
    article = await page(ui, article_url)
    [classification_url] = links(article, "/ui/classifications/")
    [raw_url] = links(article, "/ui/raw/")

    classification = await page(ui, classification_url)
    for qid in ("event_type", "domain", "is_market_relevant", "sentiment", "impact", "about_BTC"):
        assert f"<code>{qid}</code>" in classification
    assert "State sent to the classifier" in classification
    assert "hack exploit" in classification  # choice probabilities table

    raw = await page(ui, raw_url)
    assert "Item XML, exactly as received" in raw
    assert "&lt;item&gt;" in raw  # payload shown as text


async def test_stored_markup_is_shown_as_text(ui, engine):
    # Feed markup is already stripped at several layers (XML parser, feedparser, clean_text);
    # this checks the last line of defence: templates escape whatever is stored.
    async with engine.begin() as conn:
        await conn.execute(
            update(ArticleVersion)
            .where(ArticleVersion.headline.like("%Protocol X%"))
            .values(headline=XSS + " " + ArticleVersion.headline)
        )
    listing = await page(ui, "/ui/articles")
    assert "&lt;img src=x onerror=alert(1)&gt; Bitcoin lender Protocol X" in listing
    [article_url] = re.findall(r'href="(/ui/articles/\d+)">&lt;img', listing)
    for path in ("/ui", "/ui/articles", article_url):
        assert XSS not in await page(ui, path)


async def test_classifications_overview_and_comparison(ui):
    html = await page(ui, "/ui/classifications", params={"days": 0})
    assert 'id="scatter"' in html and 'id="events"' in html
    compared = await page(
        ui, "/ui/classifications", params={"question_set": "v1.1", "compare": "v1.0"}
    )
    assert "No article version has been classified under both sets" in compared


async def test_questions_show_changes_between_versions(ui):
    html = await page(ui, "/ui/questions", params={"version": "v1.1"})
    changed = re.findall(r'<article class="changed">\s*<header><strong><code>(\w+)</code>', html)
    assert changed == ["is_promotional", "sentiment"]
    assert (await ui.get("/ui/questions", params={"version": "v9"})).status_code == 404


async def test_clusters(ui):
    html = await page(ui, "/ui/clusters")
    [cluster_url] = links(html, "/ui/clusters/")
    detail = await page(ui, cluster_url)
    assert "<small>a</small>" in detail and "<small>b</small>" in detail  # both sources


async def test_static_assets_are_served_locally(ui):
    for asset in ("app.css", "app.js", "vendor/htmx-2.0.11.min.js", "vendor/pico-2.1.1.min.css"):
        assert (await ui.get(f"/ui/static/{asset}")).status_code == 200
    assert "https://" not in re.sub(r'href="https?://[^"]+"', "", await page(ui, "/ui/sources"))


@pytest.mark.parametrize(
    "path", ["/ui/articles/999999", "/ui/classifications/999999", "/ui/raw/999999"]
)
async def test_missing_objects_404(ui, path):
    assert (await ui.get(path)).status_code == 404


def test_word_diff():
    assert word_diff(
        "patches bug after $12M exploit", "patches bug after $14M exploit, team says"
    ) == [
        ("equal", "patches bug after"),
        ("delete", "$12M exploit"),
        ("insert", "$14M exploit, team says"),
    ]


def test_merge_drops_blanks():
    assert merge({"a": "1", "b": "", "offset": "0"}, offset=50) == {"a": "1", "offset": 50}
