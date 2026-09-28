"""Settings from the environment / .env, and YAML config loading."""

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, HttpUrl, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+psycopg://mnp:mnp@localhost:5432/mnp"
    jev_api_key: SecretStr | None = None
    # Jev model: "jev-latest" follows new releases; pin e.g. "jev-1.13.0" to freeze behaviour.
    jev_model: str = "jev-latest"
    jev_url: str = "https://api.typesafe.ai/v1/systemone"
    # Question set new article versions are classified with (config/questions/<version>.yaml).
    question_set: str = "v1.1"
    finnhub_api_key: SecretStr | None = None  # Finnhub sources are skipped when unset
    # Included in the User-Agent; SEC and BLS ask automated clients to identify themselves.
    contact_email: str | None = None
    log_level: str = "INFO"
    # Dashboard times: "local" (the system's zone), or an IANA name such as UTC.
    display_timezone: str = "local"
    # Near-duplicate clustering (docs/v1-plan.md §5, v1.2 in §12).
    cluster_method: Literal["embedding", "trigram"] = "embedding"
    cluster_window_hours: float = Field(default=48, gt=0)
    cluster_similarity_threshold: float = Field(default=0.6, gt=0, le=1)  # trigram method
    # Embedding method: join at >= join; between confirm and join ask Jev "same event?" and
    # join if its yes-probability >= confirm_min_prob; if Jev is unavailable, join at >= fallback.
    # Chosen on labelled pairs (2026-09-28): 89% recall, 99% precision; see `mnp cluster-eval`.
    cluster_join_similarity: float = Field(default=0.88, gt=0, le=1)
    cluster_confirm_similarity: float = Field(default=0.78, gt=0, le=1)
    cluster_confirm_min_prob: float = Field(default=0.7, ge=0, le=1)
    cluster_fallback_similarity: float = Field(default=0.84, gt=0, le=1)
    cluster_confirm_with_jev: bool = True
    # "hashing" is an offline, dependency-free embedder for tests and development.
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_cache_dir: Path = Path.home() / ".cache" / "mnp" / "fastembed"
    # Items published this long before we first see them are old news (a new source's first
    # fetch, feeds that resurface old posts): not classified, not clustered, hidden by default.
    backfill_after_days: float = Field(default=7, gt=0)
    config_dir: Path = PROJECT_ROOT / "config"

    @field_validator("jev_api_key", "finnhub_api_key", "contact_email", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: Any) -> Any:
        # `FINNHUB_API_KEY=` (as copied from .env.example) means "not configured".
        return None if isinstance(value, str) and not value.strip() else value


@lru_cache
def get_settings() -> Settings:
    return Settings()


class SourceConfig(BaseModel):
    name: str
    kind: Literal["rss", "finnhub"]
    url: HttpUrl
    poll_seconds: int = Field(default=60, gt=0)
    category: str
    reputation: float = Field(ge=0, le=1)
    enabled: bool = True
    language: str = "en"  # default for items that don't declare one
    options: dict[str, str | int | float | bool] = Field(default_factory=dict)  # per-collector


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
