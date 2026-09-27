import pytest
from sqlalchemy import select

from mnp.classify.assets import AssetMatcher, AssetRef, load_assets, sync_assets
from mnp.config import PROJECT_ROOT
from mnp.models import Asset


def ref(i, symbol, name, aliases=(), exact=(), ambiguous=False, kind="crypto"):
    return AssetRef(i, symbol, name, kind, tuple(aliases), tuple(exact), ambiguous)


ASSETS = [
    ref(1, "BTC", "Bitcoin", aliases=["Bitcoin"], exact=["XBT"]),
    ref(2, "ETH", "Ethereum", aliases=["Ethereum", "Ether"]),
    ref(3, "ETHA", "iShares Ethereum Trust", aliases=["iShares Ethereum Trust"], kind="equity"),
    ref(4, "LINK", "Chainlink", aliases=["Chainlink"], ambiguous=True),
    ref(5, "FED", "Federal Reserve", aliases=["Federal Reserve"], exact=["Fed"], kind="macro"),
    ref(6, "AVAX", "Avalanche", exact=["Avalanche"]),
    ref(7, "SEC", "Securities and Exchange Commission", kind="macro"),
]
MATCHER = AssetMatcher(ASSETS)


def symbols(text, tags=(), limit=10):
    return [(c.asset.symbol, c.via) for c in MATCHER.candidates(text, tags, limit)]


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Bitcoin rallies past resistance", ["BTC"]),
        ("bitcoin miners sell", ["BTC"]),
        ("BTC/USD and XBT futures", ["BTC"]),
        ("btc is lowercase here", []),  # symbols are case-sensitive
        ("ETHA sees record inflows", ["ETHA"]),  # ETH must not match inside ETHA
        ("ETH-USD perps", ["ETH"]),
        ("whether ether rises", ["ETH"]),  # "whether" is not "ether"
        ("LINK holders celebrate", []),  # ambiguous symbol alone
        ("click the link below", []),
        ("Chainlink (LINK) jumps", ["LINK"]),
        ("$LINK breaks out", ["LINK"]),  # cashtag
        ("Fed holds rates", ["FED"]),
        ("fed up with fees", []),  # exact alias is case-sensitive
        ("Federal Reserve minutes", ["FED"]),
        ("an avalanche of liquidations", []),
        ("Avalanche upgrade goes live", ["AVAX"]),
        ("SEC sues exchange", ["SEC"]),
    ],
)
def test_alias_matching(text, expected):
    assert [s for s, _ in symbols(text)] == expected


def test_source_tags_override_ambiguity_and_come_first():
    assert symbols("Ethereum news; LINK integration", tags=["link"]) == [
        ("LINK", "source_tag"),
        ("ETH", "alias_match"),
    ]


def test_candidates_ordered_by_first_mention_and_capped():
    text = "SEC and Fed weigh in as Bitcoin and Ethereum slide"
    assert [s for s, _ in symbols(text)] == ["SEC", "FED", "BTC", "ETH"]
    assert [s for s, _ in symbols(text, limit=2)] == ["SEC", "FED"]


def test_repo_assets_file():
    assets = load_assets(PROJECT_ROOT / "config" / "assets.yaml")
    kinds = {a.kind for a in assets}
    by_symbol = {a.symbol: a for a in assets}
    assert sum(a.kind == "crypto" for a in assets) >= 100
    assert kinds == {"crypto", "equity", "macro"}
    assert {"USDT", "USDC", "IBIT", "ETHA", "FED", "FOMC", "CPI", "SEC", "CFTC"} <= set(by_symbol)
    assert by_symbol["LINK"].ambiguous and not by_symbol["BTC"].ambiguous
    # every ambiguous asset can still be found some other way
    assert all(a.aliases or a.exact for a in assets if a.ambiguous)


@pytest.mark.db
async def test_sync_assets_upserts_and_disables(engine):
    assets = load_assets(PROJECT_ROOT / "config" / "assets.yaml")
    await sync_assets(engine, assets)
    await sync_assets(engine, [a for a in assets if a.symbol != "DOGE"])
    async with engine.connect() as conn:
        rows = dict((await conn.execute(select(Asset.symbol, Asset.enabled))).all())
    assert len(rows) == len(assets)
    assert rows["DOGE"] is False and rows["BTC"] is True
