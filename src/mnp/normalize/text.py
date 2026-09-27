"""Text normalization: strip HTML, unescape entities, collapse whitespace, NFC."""

import re
import unicodedata
from html.parser import HTMLParser

# Tags whose boundaries separate words ("<p>a</p><p>b</p>" -> "a b").
_BLOCK_TAGS = {
    "address", "article", "aside", "blockquote", "br", "dd", "div", "dl", "dt", "figcaption",
    "figure", "footer", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "img", "li",
    "main", "nav", "ol", "p", "pre", "section", "table", "td", "th", "tr", "ul",
}  # fmt: skip
_SKIP_TAGS = {"script", "style", "template", "noscript"}
_WHITESPACE = re.compile(r"\s+")


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag in _SKIP_TAGS:
            self.skip_depth += 1
        elif tag in _BLOCK_TAGS:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP_TAGS:
            self.skip_depth = max(0, self.skip_depth - 1)
        elif tag in _BLOCK_TAGS:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        if not self.skip_depth:
            self.parts.append(data)


def clean_text(value: str | None) -> str | None:
    """Plain, single-spaced, NFC text from an HTML fragment; None if nothing is left."""
    if value is None:
        return None
    extractor = _TextExtractor()
    extractor.feed(value)
    extractor.close()
    text = unicodedata.normalize("NFC", "".join(extractor.parts))
    text = _WHITESPACE.sub(" ", text).strip()  # \s covers NBSP and other Unicode spaces
    return text or None
