"""Content hash: identifies an article version by its normalized content."""

import hashlib

# Unit separator: can't appear in normalized text, so field boundaries are unambiguous.
_SEP = "\x1f"


def content_hash(
    *, headline: str | None, summary: str | None, body: str | None, canonical_url: str
) -> str:
    """sha256 over already-normalized fields; None and "" hash the same."""
    fields = (headline or "", summary or "", body or "", canonical_url)
    return hashlib.sha256(_SEP.join(fields).encode("utf-8")).hexdigest()
