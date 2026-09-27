"""RSS 2.0 / RSS 1.0 / Atom collector.

Each item is stored as the exact bytes it occupied in the feed document (decoded to text for
jsonb, with the encoding recorded so the bytes can be reproduced). Namespace declarations and
xml:base in scope from ancestor elements are stored alongside, so the item can be parsed on
its own later. Nothing is cleaned or interpreted here.
"""

import codecs
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from xml.parsers import expat

from mnp.collectors.base import Collector, CollectorError, RawPayload, raise_for_status

RSS1_NS = "http://purl.org/rss/1.0/"
ATOM_NS = "http://www.w3.org/2005/Atom"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XML_NS = "http://www.w3.org/XML/1998/namespace"

# Element names as reported by expat with namespace_separator=" ".
FEED_ROOTS = {"rss", f"{RDF_NS} RDF", f"{ATOM_NS} feed"}
ITEM_ELEMENTS = {"item", f"{RSS1_NS} item", f"{ATOM_NS} entry"}
ID_ELEMENTS = {"guid", f"{ATOM_NS} id"}
LINK_ELEMENTS = {"link", f"{RSS1_NS} link"}

ACCEPT = (
    "application/rss+xml, application/atom+xml, application/rdf+xml, "
    "application/xml;q=0.9, text/xml;q=0.9, */*;q=0.1"
)

_XML_DECL_ENCODING = re.compile(rb"""^<\?xml[^>]*?encoding\s*=\s*["']([A-Za-z0-9._-]+)["']""")


class FeedParseError(CollectorError):
    pass


@dataclass(slots=True)
class FeedItem:
    raw: bytes
    text: str
    encoding: str
    namespaces: dict[str, str] = field(default_factory=dict)
    xml_base: str | None = None
    external_id: str | None = None
    link: str | None = None


def detect_encoding(data: bytes) -> str:
    """Encoding of an XML document: BOM, then the XML declaration, then UTF-8."""
    for bom, name in (
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
    ):
        if data.startswith(bom):
            return name
    if m := _XML_DECL_ENCODING.match(data.lstrip()):
        name = m.group(1).decode("ascii")
        try:
            return codecs.lookup(name).name
        except LookupError:
            pass
    return "utf-8"


def split_feed(data: bytes) -> list[FeedItem]:
    """Split a feed document into its items, keeping each item's exact bytes."""
    encoding = detect_encoding(data)
    parser = expat.ParserCreate(namespace_separator=" ")
    items: list[FeedItem] = []
    ns_decls: list[tuple[str, str]] = []  # (prefix, uri) in scope, innermost last
    bases: list[str | None] = []  # xml:base per open element
    root: str | None = None
    item: FeedItem | None = None
    item_start = item_depth = 0
    capture: str | None = None
    buf: list[str] = []

    def start_ns(prefix: str | None, uri: str) -> None:
        ns_decls.append((prefix or "", uri))

    def end_ns(prefix: str | None) -> None:
        prefix = prefix or ""
        for i in range(len(ns_decls) - 1, -1, -1):
            if ns_decls[i][0] == prefix:
                del ns_decls[i]
                break

    def start(name: str, attrs: dict[str, str]) -> None:
        nonlocal root, item, item_start, item_depth, capture
        if root is None:
            root = name
            if name not in FEED_ROOTS:
                raise FeedParseError(f"not a feed: root element is <{name.split(' ')[-1]}>")
        bases.append(attrs.get(f"{XML_NS} base"))
        depth = len(bases)
        if item is None:
            if name in ITEM_ELEMENTS:
                item_start, item_depth = parser.CurrentByteIndex, depth
                item = FeedItem(
                    raw=b"",
                    text="",
                    encoding=encoding,
                    namespaces=dict(ns_decls),
                    xml_base=next((b for b in reversed(bases[:-1]) if b), None),
                    external_id=attrs.get(f"{RDF_NS} about"),
                )
        elif depth == item_depth + 1:
            if name in ID_ELEMENTS or name in LINK_ELEMENTS:
                capture = name
                buf.clear()
            elif name == f"{ATOM_NS} link" and item.link is None:
                if attrs.get("rel", "alternate") == "alternate" and attrs.get("href"):
                    item.link = attrs["href"]

    def chars(data: str) -> None:
        if capture is not None:
            buf.append(data)

    def end(name: str) -> None:
        nonlocal item, capture
        depth = len(bases)
        if item is not None:
            if capture is not None and depth == item_depth + 1:
                value = "".join(buf).strip() or None
                if capture in ID_ELEMENTS:
                    item.external_id = value or item.external_id
                elif item.link is None:
                    item.link = value
                capture = None
            elif depth == item_depth:
                end = data.index(b">", parser.CurrentByteIndex) + 1
                item.raw = data[item_start:end]
                item.text = item.raw.decode(encoding, errors="replace")
                items.append(item)
                item = None
        bases.pop()

    parser.StartNamespaceDeclHandler = start_ns
    parser.EndNamespaceDeclHandler = end_ns
    parser.StartElementHandler = start
    parser.EndElementHandler = end
    parser.CharacterDataHandler = chars
    try:
        parser.Parse(data, True)
    except expat.ExpatError as exc:
        raise FeedParseError(f"invalid XML: {exc}") from exc
    if root is None:
        raise FeedParseError("empty document")
    return items


class RssCollector(Collector):
    async def fetch(self, checkpoint: Mapping[str, Any]) -> tuple[list[RawPayload], dict[str, Any]]:
        headers = {"Accept": ACCEPT}
        if etag := checkpoint.get("etag"):
            headers["If-None-Match"] = etag
        if last_modified := checkpoint.get("last_modified"):
            headers["If-Modified-Since"] = last_modified

        response = await self.client.get(str(self.source.url), headers=headers)
        if response.status_code == 304:
            return [], dict(checkpoint)
        raise_for_status(response)

        http = {
            "status": response.status_code,
            "content_type": response.headers.get("content-type"),
            "etag": response.headers.get("etag"),
            "last_modified": response.headers.get("last-modified"),
        }
        payloads = [
            RawPayload(
                payload={
                    "format": "xml_item",
                    "xml": item.text,
                    "encoding": item.encoding,
                    "namespaces": item.namespaces,
                    "xml_base": item.xml_base,
                    "feed_url": str(response.url),
                    "http": http,
                },
                payload_sha256=hashlib.sha256(item.raw).hexdigest(),
                external_id=item.external_id,
                url=item.link,
            )
            for item in split_feed(response.content)
        ]
        new_checkpoint = {
            key: value
            for key, value in (("etag", http["etag"]), ("last_modified", http["last_modified"]))
            if value
        }
        return payloads, new_checkpoint
