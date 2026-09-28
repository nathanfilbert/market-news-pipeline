"""Time zone for displaying times in the dashboard (the API and database stay in UTC)."""

import os
from datetime import datetime
from functools import cache
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mnp.config import get_settings


def _valid(name: str | None) -> str | None:
    if not name:
        return None
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return name


@cache
def system_zone_name() -> str:
    """The machine's IANA time zone (e.g. America/Chicago), or UTC if it can't be found."""
    candidates = [os.environ.get("TZ", "").lstrip(":")]
    timezone_file = Path("/etc/timezone")
    if timezone_file.exists():
        candidates.append(timezone_file.read_text().strip())
    localtime = Path("/etc/localtime")
    if localtime.is_symlink():
        target = str(localtime.resolve())
        if "zoneinfo/" in target:
            candidates.append(target.split("zoneinfo/", 1)[1])
    return next((name for c in candidates if (name := _valid(c))), "UTC")


def zone_name() -> str:
    """DISPLAY_TIMEZONE: "local" (default) for the system zone, or an IANA name like UTC."""
    setting = get_settings().display_timezone
    return system_zone_name() if setting == "local" else (_valid(setting) or "UTC")


def zone() -> ZoneInfo:
    return ZoneInfo(zone_name())


def label() -> str:
    """e.g. 'America/Chicago (CDT)'."""
    name = zone_name()
    abbreviation = datetime.now(zone()).tzname()
    return name if abbreviation in (None, name) else f"{name} ({abbreviation})"
