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

Each new article version queues a classify job. Classify with Jev (needs `JEV_API_KEY`):

```bash
uv run mnp classify --show                  # process the queue, print results for review
uv run mnp reclassify --question-set v1.1 --since 2026-10-01
```

Query the results:

```bash
uv run mnp news --since 1h --asset BTC --event-type hack_exploit --min-impact 0.6
uv run mnp news --domain macro --min-relevance 0.8 --json   # one JSON object per line
uv run mnp api                                             # read-only API on 127.0.0.1:8000
```

The API serves `GET /articles` (same filters as `mnp news`), `/articles/{id}` (every version,
classification and a link to the raw item), `/clusters/{id}`, `/raw/{id}` and `/health`
(per-source freshness, last error and job backlog). Interactive docs are at `/docs`. It has no
authentication, so keep it on localhost; Finnhub's terms also forbid redistributing its data.

Questions live in `config/questions/<version>.yaml`; change wording only in a new version, which
`reclassify` runs side by side with the old one. Taggable assets live in `config/assets.yaml`.

Tests use a separate `mnp_test` database, created and migrated automatically.
