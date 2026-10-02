"""Citations required by some sources' terms, shown wherever their articles are used."""

import httpx
import pytest

from mnp.attribution import attribution_for
from mnp.collectors.rss import RssCollector
from mnp.collectors.service import collect_once, sync_sources
from mnp.config import load_sources
from mnp.feed.build import handle_feed_job
from mnp.jobs import FEED, NORMALIZE, run_pending
from mnp.normalize.service import handle_normalize_job
from mnp.outputs.api import create_app
from tests.conftest import source

PANEWS_CITATION = {"text": "PANews", "url": "https://www.panewslab.com/en"}
GDELT_CITATION = {"text": "The GDELT Project", "url": "https://www.gdeltproject.org/"}


def test_attribution_by_source_name_or_kind():
    assert attribution_for("rss", "panews") == PANEWS_CITATION
    assert attribution_for("gdelt", "gdelt_conflict_oil") == GDELT_CITATION
    assert attribution_for("rss", "coindesk") is None
    assert attribution_for("rss") is None


def test_configured_panews_source_is_cited():
    [panews] = [s for s in load_sources() if s.name == "panews"]
    assert attribution_for(panews.kind, panews.name) == PANEWS_CITATION


@pytest.mark.db
async def test_panews_articles_carry_attribution(engine, server, client):
    ids = await sync_sources(engine, [source("panews"), source("b")])
    server.serve("panews", fixture="rss/wordpress.xml")
    server.serve("b", fixture="rss/second_source.xml")
    for name in ("panews", "b"):
        await collect_once(engine, ids[name], RssCollector(source(name), client))
    await run_pending(engine, NORMALIZE, handle_normalize_job)
    await run_pending(engine, FEED, handle_feed_job)

    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as api:
        articles = (await api.get("/articles", params={"since": "30d"})).json()["articles"]
        by_source: dict[str, list] = {}
        for a in articles:
            by_source.setdefault(a["source"], []).append(a)
        assert all(a["attribution"] == PANEWS_CITATION for a in by_source["panews"])
        assert all(a["attribution"] is None for a in by_source["b"])

        panews_id = by_source["panews"][0]["id"]
        [version] = (await api.get(f"/articles/{panews_id}")).json()["versions"]
        assert version["attribution"] == PANEWS_CITATION
        page = (await api.get(f"/ui/articles/{panews_id}")).text
        assert 'href="https://www.panewslab.com/en"' in page

        events = (await api.get("/v1/feed/snapshot", params={"since": "30d"})).json()["events"]
    cited = [e for e in events if any(a["source"] == "panews" for a in e["articles"])]
    assert cited and all(PANEWS_CITATION in e["attributions"] for e in cited)
    assert all(not e.get("attributions") for e in events if e not in cited)
