from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import pytest

from mnp.config import get_settings
from mnp.dashboard import tz
from mnp.dashboard.queries import days_between
from mnp.dashboard.routes import fmt_dt

EVENING_CHICAGO = datetime(2026, 9, 28, 1, 30, tzinfo=UTC)  # 2026-09-27 20:30 CDT


@pytest.fixture
def display(monkeypatch):
    def set_zone(value: str) -> None:
        monkeypatch.setenv("DISPLAY_TIMEZONE", value)
        get_settings.cache_clear()

    return set_zone


def test_times_are_shown_in_the_display_zone(display):
    display("America/Chicago")
    assert fmt_dt(EVENING_CHICAGO) == "2026-09-27 20:30"
    assert tz.label() == "America/Chicago (CDT)" or tz.label().startswith("America/Chicago")
    display("UTC")
    assert fmt_dt(EVENING_CHICAGO) == "2026-09-28 01:30"
    assert fmt_dt(None) == "—"


def test_local_uses_the_system_zone(display, monkeypatch):
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    tz.system_zone_name.cache_clear()
    display("local")
    try:
        assert tz.zone_name() == "Asia/Tokyo"
    finally:
        tz.system_zone_name.cache_clear()


def test_invalid_zone_falls_back_to_utc(display):
    display("Mars/Olympus_Mons")
    assert tz.zone_name() == "UTC"


def test_days_between_uses_the_zone():
    since = datetime(2026, 9, 26, 3, 0, tzinfo=UTC)  # 2026-09-25 22:00 in Chicago
    until = datetime(2026, 9, 28, 1, 30, tzinfo=UTC)  # 2026-09-27 20:30 in Chicago
    assert days_between(since, until) == ["2026-09-26", "2026-09-27", "2026-09-28"]
    assert days_between(since, until, ZoneInfo("America/Chicago")) == [
        "2026-09-25",
        "2026-09-26",
        "2026-09-27",
    ]


@pytest.mark.db
async def test_footer_names_the_zone(engine, display):
    import httpx

    from mnp.outputs.api import create_app

    display("America/Chicago")
    transport = httpx.ASGITransport(app=create_app(engine))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        html = (await client.get("/ui/sources")).text
    assert "times in America/Chicago" in html
