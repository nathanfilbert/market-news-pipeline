"""Parse a stored raw payload into normalized article fields."""

import calendar
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from xml.sax.saxutils import quoteattr

import feedparser

from mnp.collectors.rss import ATOM_NS, RDF_NS, RSS1_NS
from mnp.normalize.text import clean_text

_FIRST_TAG = re.compile(r"\s*<([A-Za-z_][\w.-]*)(?::([A-Za-z_][\w.-]*))?")
# Empty text elements that feedparser lets overwrite a filled one, e.g. CoinDesk's
# <description>…</description> followed by <dc:description/>.
_EMPTY_TEXT_ELEMENTS = re.compile(
    r"<((?:dc:)?description|content:encoded|(?:atom:)?summary|(?:atom:)?content)"
    r"(?:\s[^>]*)?(?:/>|>\s*</\1>)"
)


class UnparseableItem(ValueError):
    """The payload can't be turned into an article; retrying won't help."""


@dataclass(frozen=True, slots=True)
class ParsedItem:
    url: str | None
    headline: str | None
    summary: str | None
    body: str | None
    author: str | None
    language: str | None
    published_at: datetime | None


def _wrap_xml_item(payload: dict[str, Any]) -> str:
    """Rebuild a minimal feed document around one stored item so feedparser can read it."""
    xml: str = _EMPTY_TEXT_ELEMENTS.sub("", payload["xml"])
    namespaces: dict[str, str] = dict(payload.get("namespaces") or {})
    m = _FIRST_TAG.match(xml)
    if not m:
        raise UnparseableItem("stored item does not start with an element")
    prefix, local = (m.group(1), m.group(2)) if m.group(2) else ("", m.group(1))
    item_ns = namespaces.get(prefix, "")

    def qname(name: str) -> str:
        return f"{prefix}:{name}" if prefix else name

    if local == "entry" and item_ns == ATOM_NS:
        open_tags, close_tags = [qname("feed")], [qname("feed")]
    elif local == "item" and item_ns == RSS1_NS:
        rdf_prefix = next((p for p, uri in namespaces.items() if uri == RDF_NS and p), None)
        if rdf_prefix is None:
            rdf_prefix = "rdf"
            namespaces[rdf_prefix] = RDF_NS
        open_tags, close_tags = [f"{rdf_prefix}:RDF"], [f"{rdf_prefix}:RDF"]
    elif local == "item" and not item_ns:
        open_tags, close_tags = ['rss version="2.0"', "channel"], ["channel", "rss"]
    else:
        raise UnparseableItem(f"unsupported item element <{m.group(0).strip()[1:]}>")

    attrs = "".join(
        f" xmlns:{p}={quoteattr(uri)}" if p else f" xmlns={quoteattr(uri)}"
        for p, uri in namespaces.items()
    )
    if base := payload.get("xml_base"):
        attrs += f" xml:base={quoteattr(base)}"
    root, *inner = open_tags
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        + f"<{root}{attrs}>"
        + "".join(f"<{t}>" for t in inner)
        + xml
        + "".join(f"</{t}>" for t in close_tags)
    )


def _timestamp(parsed) -> datetime | None:
    if not parsed:
        return None
    try:
        return datetime.fromtimestamp(calendar.timegm(parsed), UTC)
    except (OverflowError, ValueError, TypeError):
        return None


def parse_payload(payload: dict[str, Any]) -> ParsedItem:
    fmt = payload.get("format")
    if fmt != "xml_item":
        raise UnparseableItem(f"unknown payload format {fmt!r}")

    document = _wrap_xml_item(payload)
    # response_headers pins the base URL used for relative links when there is no xml:base.
    feed = feedparser.parse(
        document.encode("utf-8"),
        response_headers={"content-location": payload.get("feed_url") or ""},
    )
    if not feed.entries:
        raise UnparseableItem(f"no entry found ({feed.get('bozo_exception')!r})")
    entry = feed.entries[0]

    content = entry.get("content") or []
    body_html = max((c.get("value") or "" for c in content), key=len, default=None)
    summary = clean_text(entry.get("summary"))
    body = clean_text(body_html)
    if body is not None and body == summary:
        body = None  # feedparser mirrors a lone description into both fields

    return ParsedItem(
        url=(entry.get("link") or "").strip() or None,
        headline=clean_text(entry.get("title")),
        summary=summary,
        body=body,
        author=clean_text(entry.get("author")),
        language=entry.get("language") or None,
        published_at=_timestamp(entry.get("published_parsed") or entry.get("updated_parsed")),
    )
