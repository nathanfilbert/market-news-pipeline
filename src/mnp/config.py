"""Settings from the environment / .env, and YAML config loading."""

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://mnp:mnp@localhost:5432/mnp"
    jev_api_key: SecretStr | None = None
    aggregator_api_key: SecretStr | None = None
    # Included in the User-Agent; SEC and BLS ask automated clients to identify themselves.
    contact_email: str | None = None
    log_level: str = "INFO"
    config_dir: Path = PROJECT_ROOT / "config"


@lru_cache
def get_settings() -> Settings:
    return Settings()


class SourceConfig(BaseModel):
    name: str
    kind: Literal["rss", "aggregator"]
    url: HttpUrl
    poll_seconds: int = Field(default=60, gt=0)
    category: str
    reputation: float = Field(ge=0, le=1)
    enabled: bool = True


def load_yaml(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sources(path: Path | None = None) -> list[SourceConfig]:
    path = path or get_settings().config_dir / "sources.yaml"
    sources = [SourceConfig.model_validate(item) for item in load_yaml(path) or []]
    names = [s.name for s in sources]
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise ValueError(f"duplicate source names in {path}: {', '.join(dupes)}")
    return sources
