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

Collect news into `raw_items` (sources are configured in `config/sources.yaml`):

```bash
uv run mnp collect --once                   # poll every enabled source once
uv run mnp collect --source coindesk --once # one source
uv run mnp collect                          # poll continuously until Ctrl-C
```

The Finnhub news sources (`finnhub_crypto`, `finnhub_general`) run only when `FINNHUB_API_KEY`
is set in `.env`; get a free key at https://finnhub.io. The free plan is for personal use only
and doesn't allow redistributing the data.

Each new raw item queues a normalize job. Process the queue into articles, versions and
clusters:

```bash
uv run mnp normalize
```

Tests use a separate `mnp_test` database, created and migrated automatically.
