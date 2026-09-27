"""Asset candidates for an article: alias matching and source tags (docs/v1-plan.md §6).

Step 1 (here): find candidate assets cheaply. Step 2 (the classifier): ask one yes/no question
per candidate, "is this article materially about <asset>?", and store the probability.

Matching rules, per asset in config/assets.yaml:
- `aliases` match case-insensitively as whole words ("bitcoin", "Bitcoin").
- `exact` aliases and the symbol match case-sensitively as whole words ("Fed", "BTC").
- "$SYMBOL" cashtags always count.
- An `ambiguous` symbol (a common word: LINK, NEAR, SPX) never counts on its own, so such an
  asset needs an alias, exact alias, cashtag or source tag.
- A source-supplied tag (RSS <category>, Finnhub `related`) equal to the symbol, name or an
  alias makes the asset a candidate even if its symbol is ambiguous.
"""

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from mnp.config import get_settings, load_yaml
from mnp.models import Asset

MAX_CANDIDATES = 10


class AssetConfig(BaseModel):
    symbol: str = Field(pattern=r"^[A-Z0-9]+$")
    name: str
    kind: Literal["crypto", "equity", "index", "macro"]
    aliases: list[str] = Field(default_factory=list)
    exact: list[str] = Field(default_factory=list)
    ambiguous: bool = False


def load_assets(path: Path | None = None) -> list[AssetConfig]:
    path = path or get_settings().config_dir / "assets.yaml"
    assets = [AssetConfig.model_validate(a) for a in load_yaml(path) or []]
    symbols = [a.symbol for a in assets]
    if dupes := sorted({s for s in symbols if symbols.count(s) > 1}):
        raise ValueError(f"duplicate asset symbols in {path}: {', '.join(dupes)}")
    return assets


async def sync_assets(engine: AsyncEngine, assets: Iterable[AssetConfig]) -> int:
    """Upsert assets from config; ones no longer configured are disabled, not deleted."""
    assets = list(assets)
    async with engine.begin() as conn:
        for a in assets:
            values = {
                "name": a.name,
                "kind": a.kind,
                "aliases": a.aliases,
                "exact_aliases": a.exact,
                "ambiguous": a.ambiguous,
                "enabled": True,
            }
            await conn.execute(
                insert(Asset)
                .values(symbol=a.symbol, **values)
                .on_conflict_do_update(index_elements=[Asset.symbol], set_=values)
            )
        symbols = [a.symbol for a in assets] or [""]
        await conn.execute(update(Asset).where(Asset.symbol.not_in(symbols)).values(enabled=False))
    return len(assets)


@dataclass(frozen=True, slots=True)
class AssetRef:
    id: int
    symbol: str
    name: str
    kind: str
    aliases: tuple[str, ...]
    exact_aliases: tuple[str, ...]
    ambiguous: bool


@dataclass(frozen=True, slots=True)
class AssetCandidate:
    asset: AssetRef
    via: Literal["alias_match", "source_tag"]


def _words(terms: Iterable[str], flags: int = 0) -> re.Pattern[str] | None:
    terms = sorted({t for t in terms if t}, key=len, reverse=True)
    if not terms:
        return None
    alternation = "|".join(re.escape(t) for t in terms)
    return re.compile(rf"(?<![\w$])(?:{alternation})(?!\w)", flags)


class AssetMatcher:
    def __init__(self, assets: Sequence[AssetRef]) -> None:
        self._entries = []
        for a in assets:
            exact = [*a.exact_aliases, *([] if a.ambiguous else [a.symbol])]
            patterns = [
                p
                for p in (
                    _words(a.aliases, re.IGNORECASE),
                    _words(exact),
                    re.compile(rf"(?<!\w)\${re.escape(a.symbol)}(?!\w)"),
                )
                if p is not None
            ]
            tag_terms = {t.casefold() for t in (a.symbol, a.name, *a.aliases, *a.exact_aliases)}
            self._entries.append((a, patterns, tag_terms))

    def candidates(
        self, text: str, tags: Iterable[str] = (), limit: int = MAX_CANDIDATES
    ) -> list[AssetCandidate]:
        """Candidates in order: source tags first, then by first mention in `text`."""
        tag_set = {t.strip().casefold() for t in tags if t.strip()}
        found: list[tuple[int, AssetCandidate]] = []
        for asset, patterns, tag_terms in self._entries:
            if tag_set & tag_terms:
                found.append((-1, AssetCandidate(asset, "source_tag")))
                continue
            positions = [m.start() for p in patterns if (m := p.search(text))]
            if positions:
                found.append((min(positions), AssetCandidate(asset, "alias_match")))
        found.sort(key=lambda f: f[0])
        return [c for _, c in found[:limit]]


async def load_matcher(conn: AsyncConnection) -> AssetMatcher:
    rows = (await conn.execute(select(Asset).where(Asset.enabled))).all()
    return AssetMatcher(
        [
            AssetRef(
                id=r.id,
                symbol=r.symbol,
                name=r.name,
                kind=r.kind,
                aliases=tuple(r.aliases),
                exact_aliases=tuple(r.exact_aliases),
                ambiguous=r.ambiguous,
            )
            for r in rows
        ]
    )
