"""Citations some sources require wherever their data is used or redistributed."""

from typing import Any

# By source kind. GDELT's terms: "any use or redistribution of the data must include a citation
# to the GDELT Project and a link to this website" (gdeltproject.org/about.html#termsofuse).
ATTRIBUTIONS: dict[str, dict[str, str]] = {
    "gdelt": {"text": "The GDELT Project", "url": "https://www.gdeltproject.org/"},
}


def attribution_for(source_kind: str) -> dict[str, Any] | None:
    """The citation to show with an article from this kind of source, if one is required."""
    found = ATTRIBUTIONS.get(source_kind)
    return dict(found) if found else None
