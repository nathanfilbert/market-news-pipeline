# Market News Pipeline

Collects, classifies, stores, and outputs news event data.

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker. See `docs/v1-plan.md` for the design.

```bash
cp .env.example .env
docker compose up -d
uv run alembic upgrade head
uv run pytest
uv run mnp check
```
