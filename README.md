# Market News Pipeline

Collects crypto and general market news in near real time, stores every raw item untouched,
classifies each article with [Jev](https://docs.typesafe.ai) (TypeSafe AI's decision model),
and serves the results through a CLI and a small read-only HTTP API.

For research, not a trading signal: latency is about a minute. The design, milestones and
decisions are in [docs/v1-plan.md](docs/v1-plan.md). Alerting is planned for the v1.1 release.

## How it works

```
config/sources.yaml ─► collectors (RSS, Finnhub) ─► raw_items         append-only, exact payloads
                                                      │ normalize job
                                                      ▼
                        articles / article_versions / clusters         canonical URL, edits, near-dups
                                                      │ classify job
                                                      ▼
                        classifications + article_assets (Jev)         event type, relevance, sentiment…
                                                      │
                                   mnp news · read-only API · /health
```

Stages talk through Postgres: each new raw item queues a normalize job, each new article
version queues a classify job. Every stage is idempotent, enforced by unique constraints.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Docker (with the Compose plugin).

```bash
cp .env.example .env         # then fill in the keys below
docker compose up -d         # Postgres 16 on 127.0.0.1:5432
uv run alembic upgrade head
uv run mnp check             # config and database reachable?
```

Keys in `.env`:

| Setting | Needed for | Notes |
|---|---|---|
| `JEV_API_KEY` | classification | From [TypeSafe](https://docs.typesafe.ai). Without it, classify jobs queue up until it's set. |
| `FINNHUB_API_KEY` | `finnhub_general` source | Free key at [finnhub.io](https://finnhub.io). Personal use only; no redistribution. Skipped when unset. |
| `CONTACT_EMAIL` | polite fetching | Sent in the User-Agent. BLS requires it (the `bls_latest` source is disabled until then). |
| `JEV_MODEL` | optional | `jev-latest` (default) follows new releases; pin e.g. `jev-1.13.0` to freeze behaviour. |
| `QUESTION_SET` | optional | Question set for new articles (default `v1.1`). |

## Run it

```bash
uv run mnp run               # collectors + normalize/classify workers + API on 127.0.0.1:8000
```

`mnp run` polls every enabled source, processes the job queues within seconds, and serves the
API from the same process. It logs one JSON object per line to stderr (`--log-format text` for
humans), including a health summary every 10 minutes. Ctrl-C or SIGTERM stops it gracefully: work
in progress either finishes or stays queued, never half-done. A second Ctrl-C forces an
immediate stop.

To keep it running and save logs:

```bash
nohup uv run mnp run >> mnp.jsonl 2>&1 &
```

Check on it:

```bash
uv run mnp health            # per-source status and job backlog; exits 1 when degraded
curl -s localhost:8000/health
```

A source is `stale` after missing 5 polls (at least 10 minutes) and `failing` while its latest
fetch errors; the overall status is `degraded` if any enabled source is stale, failing or has
never run.

## Query the news

```bash
uv run mnp news --since 1h
uv run mnp news --since 1d --asset BTC --event-type hack_exploit --min-impact 0.6
uv run mnp news --domain macro --min-relevance 0.8 --json    # one JSON object per line
```

Filters: `--since/--until` (`30m`, `2d`, or an ISO date/time), `--asset` (only articles Jev
confirmed are about it), `--event-type`, `--domain` (crypto, equities, macro, other),
`--source`, `--min-impact`, `--min-relevance`. Each article is shown as its latest version.

The API (`uv run mnp api` on its own, or part of `mnp run`) has interactive docs at
[localhost:8000/docs](http://localhost:8000/docs):

| Endpoint | Returns |
|---|---|
| `GET /articles` | Filtered list, newest first (same filters as `mnp news`, plus `limit`/`offset`) |
| `GET /articles/{id}` | Every version, all classifications with full Jev output, links to raw item and cluster |
| `GET /clusters/{id}` | All articles covering the same story |
| `GET /raw/{id}` | A raw item exactly as collected |
| `GET /health` | Source freshness, last errors, job backlog |

The API is read-only and has no authentication: keep it on localhost. Finnhub's terms also
forbid redistributing its data.

## Configuration

- **Sources:** `config/sources.yaml`. RSS feeds and Finnhub, each with a poll interval and a
  reputation. Verify a feed works before adding it.
- **Questions:** `config/questions/<version>.yaml`. Never edit a question set in place: copy it
  to a new version, then `uv run mnp reclassify --question-set v1.2 [--since 2026-10-01]` labels
  stored articles with it, side by side with the old labels. Set `QUESTION_SET` to make it the
  default for new articles.
- **Assets:** `config/assets.yaml`. Symbols, names and aliases used to find candidate assets in
  headlines; Jev then confirms each one. Mark symbols that are common words (`LINK`, `NEAR`)
  `ambiguous: true`.

Step-by-step commands (`mnp collect --once`, `mnp normalize`, `mnp classify --show`) run one
stage at a time, which is handy for debugging. `uv run mnp --help` lists everything.

## Development

```bash
uv run pytest                # uses a separate mnp_test database, created automatically
uv run ruff check . && uv run ruff format --check .
```

Tests never touch the network: feeds, Finnhub and Jev are mocked with synthetic fixtures in
`tests/fixtures/`. CI runs lint, migrations and tests against Postgres on every pull request.
