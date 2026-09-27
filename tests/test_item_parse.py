from datetime import UTC, datetime

import pytest

from mnp.collectors.rss import item_payload, split_feed
from mnp.normalize.item import UnparseableItem, parse_payload
from tests.conftest import fixture_bytes


def parsed(fixture: str, index: int = 0, feed_url: str = "https://feed.example.com/rss"):
    item = split_feed(fixture_bytes(fixture))[index]
    return parse_payload(item_payload(item, feed_url=feed_url, http={}).payload)


def test_rss2_wordpress_item():
    p = parsed("rss/wordpress.xml")
    assert p.headline == "Example Exchange lists ‘TOKEN’ perpetuals — trading opens today"  # noqa: RUF001
    assert p.url == "https://news.example.com/markets/2026/09/27/example-exchange-lists-token"
    assert p.summary == "Example Exchange said it will list TOKEN perpetual futures & spot pairs."
    assert p.body == "Full body text for the listing story."
    assert p.author == "Jane Doe"
    assert p.published_at == datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def test_item_without_body_or_author():
    p = parsed("rss/wordpress.xml", index=2)
    assert p.body is None
    assert p.author is None
    assert p.summary == "A look at fee markets."


def test_empty_dc_description_does_not_hide_description():
    # CoinDesk pattern: <description> followed by an empty <dc:description/>.
    data = fixture_bytes("rss/wordpress.xml").replace(
        b"</content:encoded>", b"</content:encoded><dc:description/><content:encoded/>", 1
    )
    item = split_feed(data)[0]
    p = parse_payload(item_payload(item, feed_url="https://f", http={}).payload)
    assert p.summary.startswith("Example Exchange said")
    assert p.body == "Full body text for the listing story."


def test_cdata_links_and_bom():
    p = parsed("rss/bom_cdata.xml")
    assert p.url == "https://bank.example.gov/press/monetary20260927a.htm"
    assert p.published_at == datetime(2026, 9, 27, 18, 0, tzinfo=UTC)


def test_relative_link_resolved_against_xml_base_and_html_escaped_description():
    p = parsed("rss/relative_links.xml")
    assert p.url.startswith("https://regulator.example.gov/newsroom/press-releases/2026-101")
    assert p.summary == "The regulator adopted amendments & guidance."
    # published_at kept as given by the source, converted to UTC
    assert p.published_at == datetime(2026, 9, 25, 18, 5, tzinfo=UTC)


def test_atom_entry():
    p = parsed("rss/atom.xml")
    assert p.headline == "Stablecoin issuer mints 1B new tokens"
    assert p.url == "https://atom.example.com/posts/1"
    assert p.summary == "Minting details"
    assert p.author == "A. Writer"
    assert p.published_at == datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def test_rss1_item():
    p = parsed("rss/rdf.xml")
    assert p.headline == "Consumer Price Index, August 2026"
    assert p.url == "https://rdf.example.org/release/cpi-2026-08"
    assert p.published_at == datetime(2026, 9, 11, 12, 30, tzinfo=UTC)


def test_latin1_item():
    assert parsed("rss/latin1.xml").headline == "Marchés: l'euro recule"


def test_missing_date_is_none():
    assert parsed("rss/template_headlines.xml").published_at is None


def test_unknown_format_is_unparseable():
    with pytest.raises(UnparseableItem, match="unknown payload format"):
        parse_payload({"format": "something_else"})


def test_non_item_xml_is_unparseable():
    with pytest.raises(UnparseableItem, match="unsupported item element"):
        parse_payload({"format": "xml_item", "xml": "<channel><title>x</title></channel>"})
