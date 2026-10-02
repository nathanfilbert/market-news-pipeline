"""Citations some sources require wherever their data is used or redistributed."""

from typing import Any

# By source kind. GDELT's terms: "any use or redistribution of the data must include a citation
# to the GDELT Project and a link to this website" (gdeltproject.org/about.html#termsofuse).
ATTRIBUTIONS: dict[str, dict[str, str]] = {
    "gdelt": {"text": "The GDELT Project", "url": "https://www.gdeltproject.org/"},
}

# By source name, for sources of a shared kind (RSS). PANews's User Agreement §2.4 allows
# non-commercial reproduction that names the author, links the original and states
# "Source: PANews" (panewslab.com/en/user-agreement); the article's own author and link are kept.
SOURCE_ATTRIBUTIONS: dict[str, dict[str, str]] = {
    "panews": {"text": "PANews", "url": "https://www.panewslab.com/en"},
}


def attribution_for(source_kind: str, source_name: str | None = None) -> dict[str, Any] | None:
    """The citation to show with an article from this source, if one is required."""
    found = SOURCE_ATTRIBUTIONS.get(source_name or "") or ATTRIBUTIONS.get(source_kind)
    return dict(found) if found else None
