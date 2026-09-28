"""Logging setup: human-readable text, or one JSON object per line for `mnp run`.

Structured fields go in `extra=` (e.g. `log.info("collected", extra={"source": "coindesk"})`);
the JSON formatter emits them as top-level keys.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Literal

# Attributes every LogRecord has; anything else came from `extra=`.
_RECORD_FIELDS = set(vars(logging.makeLogRecord({}))) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RECORD_FIELDS and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str, ensure_ascii=False)


def setup_logging(level: str = "INFO", fmt: Literal["text", "json"] = "text") -> None:
    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
    # Per-request lines are noise; collectors log one summary per poll.
    logging.getLogger("httpx").setLevel(logging.WARNING)
