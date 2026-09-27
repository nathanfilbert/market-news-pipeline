from pathlib import Path

import pytest
from pydantic import ValidationError

from mnp.config import PROJECT_ROOT, Settings, load_sources


def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "sources.yaml"
    path.write_text(body)
    return path


def test_load_sources(tmp_path):
    path = write(
        tmp_path,
        """
- name: coindesk
  kind: rss
  url: https://example.com/feed.xml
  category: crypto
  reputation: 0.9
""",
    )
    [src] = load_sources(path)
    assert src.name == "coindesk"
    assert src.poll_seconds == 60
    assert src.enabled


def test_empty_sources_file(tmp_path):
    assert load_sources(write(tmp_path, "[]\n")) == []


def test_repo_sources_file_is_valid():
    load_sources(PROJECT_ROOT / "config" / "sources.yaml")


def test_rejects_bad_reputation(tmp_path):
    path = write(
        tmp_path,
        "- {name: a, kind: rss, url: 'https://x.test/f', category: crypto, reputation: 1.5}\n",
    )
    with pytest.raises(ValidationError):
        load_sources(path)


def test_rejects_duplicate_names(tmp_path):
    item = "- {name: a, kind: rss, url: 'https://x.test/f', category: crypto, reputation: 0.5}\n"
    with pytest.raises(ValueError, match="duplicate"):
        load_sources(write(tmp_path, item * 2))


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@h:1/d")
    monkeypatch.setenv("JEV_API_KEY", "secret")
    settings = Settings(_env_file=None)
    assert settings.database_url == "postgresql+psycopg://u:p@h:1/d"
    assert settings.jev_api_key.get_secret_value() == "secret"
    assert "secret" not in repr(settings)


def test_blank_optional_settings_are_unset(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "")
    monkeypatch.setenv("CONTACT_EMAIL", "  ")
    settings = Settings(_env_file=None)
    assert settings.finnhub_api_key is None
    assert settings.contact_email is None
