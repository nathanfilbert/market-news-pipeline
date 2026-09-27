import hashlib

import pytest

from mnp.collectors.rss import FeedParseError, detect_encoding, split_feed
from tests.conftest import fixture_bytes


def test_wordpress_feed_items_are_exact_byte_slices():
    data = fixture_bytes("rss/wordpress.xml")
    items = split_feed(data)
    assert [i.external_id for i in items] == [
        "https://news.example.com/?p=1001",
        "https://news.example.com/?p=1002",
        "https://news.example.com/?p=1003",
    ]
    assert (
        items[0].link == "https://news.example.com/markets/2026/09/27/example-exchange-lists-token"
    )
    for item in items:
        assert item.raw in data
        assert item.raw.startswith(b"<item>") and item.raw.endswith(b"</item>")
        assert item.text.encode(item.encoding) == item.raw
    assert "\u2018TOKEN\u2019" in items[0].text  # curly quotes survive
    assert "<![CDATA[" in items[0].text  # stored as received, not unwrapped
    assert items[0].namespaces["dc"] == "http://purl.org/dc/elements/1.1/"
    assert items[0].namespaces["media"] == "http://search.yahoo.com/mrss/"


def test_edit_changes_only_that_items_hash():
    def hashes(name):
        return [hashlib.sha256(i.raw).hexdigest() for i in split_feed(fixture_bytes(name))]

    before, after = hashes("rss/wordpress.xml"), hashes("rss/wordpress_edited.xml")
    assert [b == a for b, a in zip(before, after, strict=True)] == [True, False, True]


def test_bom_and_cdata_links():
    data = fixture_bytes("rss/bom_cdata.xml")
    assert data.startswith(b"\xef\xbb\xbf")
    items = split_feed(data)
    assert [i.link for i in items] == [
        "https://bank.example.gov/press/monetary20260927a.htm",
        "https://bank.example.gov/press/orders20260926a.htm",
    ]
    assert items[0].external_id == items[0].link
    assert all(i.raw.startswith(b"<item>") for i in items)
    assert items[0].namespaces == {}


def test_xml_base_is_captured_from_ancestors():
    [item] = split_feed(fixture_bytes("rss/xml_base.xml"))
    assert item.xml_base == "https://regulator.example.gov/"
    assert item.external_id == "2026-100"
    assert item.namespaces == {"dc": "http://purl.org/dc/elements/1.1/"}


def test_atom_entries():
    items = split_feed(fixture_bytes("rss/atom.xml"))
    assert [(i.external_id, i.link) for i in items] == [
        ("urn:example:entry:1", "https://atom.example.com/posts/1"),
        ("urn:example:entry:2", "https://atom.example.com/posts/2"),
    ]
    assert items[0].raw.startswith(b"<entry>") and items[0].raw.endswith(b"</entry>")
    assert items[0].namespaces == {"": "http://www.w3.org/2005/Atom"}


def test_rss1_rdf_items():
    [item] = split_feed(fixture_bytes("rss/rdf.xml"))
    assert item.external_id == "https://rdf.example.org/release/cpi-2026-08"
    assert item.link == "https://rdf.example.org/release/cpi-2026-08"
    assert item.namespaces[""] == "http://purl.org/rss/1.0/"
    assert item.namespaces["rdf"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


def test_latin1_feed_decodes_and_round_trips():
    data = fixture_bytes("rss/latin1.xml")
    assert detect_encoding(data) == "iso8859-1"
    [item] = split_feed(data)
    assert "Marchés" in item.text
    assert item.text.encode(item.encoding) == item.raw


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("rss/not_a_feed.html", "not a feed"),
        ("rss/malformed.xml", "invalid XML"),
    ],
)
def test_rejects_non_feeds(name, message):
    with pytest.raises(FeedParseError, match=message):
        split_feed(fixture_bytes(name))


def test_empty_feed_has_no_items():
    assert split_feed(b'<rss version="2.0"><channel><title>t</title></channel></rss>') == []
