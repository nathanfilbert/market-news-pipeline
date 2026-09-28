# Market News Pipeline

Collects crypto and general market news in near real time, stores every raw item untouched,
classifies each article with [Jev](https://docs.typesafe.ai) (TypeSafe AI's decision model),
and serves the results through a CLI, a read-only HTTP API and a trading feed of events.

Latency is about a minute. The design, milestones and decisions are in
[docs/v1-plan.md](docs/v1-plan.md). Next: exchange announcement sources (v1.4) and GDELT (v1.5).

## How it works

```
config/sources.yaml ─► collectors (RSS, Finnhub) ─► raw_items         append-only, exact payloads
                                                      │ normalize job
                                                      ▼
                        articles / article_versions / clusters         canonical URL, edits, near-dups
                                                      │ classify job
                                                      ▼
                        classifications + article_assets (Jev)         event type, relevance, sentiment…
                                                      │ feed job
                                                      ▼
                        feed_revisions                                 events, append-only, cursor
                                                      │
                          mnp news · read-only API · /v1/feed · /health
```

Stages talk through Postgres: each new raw item queues a normalize job, each new article
version queues a classify job, and every change to a story queues a feed job. Every stage is
idempotent, enforced by unique constraints.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and Docker (with the Compose plugin).

```bash
cp .env.example .env         # then fill in the keys below
git config core.hooksPath .githooks   # block commits containing secrets
docker compose up -d         # Postgres 16 on 127.0.0.1:5432
uv run alembic upgrade head
uv run mnp check             # config and database reachable?
```

Keys belong only in `.env`, which git ignores. The pre-commit hook (plus
[gitleaks](https://github.com/gitleaks/gitleaks), if installed) and CI stop credentials from
being committed; see [docs/secrets.md](docs/secrets.md).

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

## Catch up: initial data and gaps

```bash
uv run mnp catch-up --since 14d   # fetch everything the sources offer, process it, report coverage
```

A one-shot counterpart to `mnp run`, for building an initial dataset or filling gaps after
downtime. It fetches every enabled source once (ignoring "not modified" shortcuts), pages back
where a source supports it, runs the normalize and classify queues to completion, and prints
per-source coverage: how far back each source reached and which days have no articles. Safe to
run while `mnp run` is running.

How far back it can reach depends on the source: feeds only list their latest items, which is
about 3 days for CoinDesk, Cointelegraph, Decrypt and Finnhub, weeks for the Fed, SEC and CNBC,
about 2 weeks for The Block (which supports paging), and up to 3 months for GDELT queries. So a
gap from downtime is fully recoverable for roughly 2-3 days.

Items published long before they're first seen (more than `BACKFILL_AFTER_DAYS`, default 7)
are normally treated as stale old news: not classified and hidden from queries
(`--include-backfill` shows them). `catch-up` treats anything inside `--since` as wanted
history instead, so it's classified and shown.

## Trading feed

`/v1/feed` serves **events** (one per story, however many outlets report it) for a trading
platform, under a versioned schema: [docs/feed-v1.md](docs/feed-v1.md).

```bash
curl -s 'localhost:8000/v1/feed/events?after=0&limit=100'     # every change, in order, with a cursor
curl -s 'localhost:8000/v1/feed/snapshot?since=24h&asset=BTC&min_impact=0.6'
curl -s 'localhost:8000/v1/feed/snapshot?as_of=2026-10-01T12:00:00Z'   # as it was then (backtests)
```

Every change to an event (another outlet, an edited headline, a classification) appends a new
revision; nothing is rewritten. `mnp run` keeps the feed current; `uv run mnp feed build`
processes pending updates by hand and `uv run mnp feed rebuild` re-checks every event, e.g. after
changing `QUESTION_SET`.

## Dashboard

Open [localhost:8000/ui](http://localhost:8000/ui) while `mnp run` (or `mnp api`) is running. It's
read-only and lets you browse and visualize everything the pipeline has stored:

- **Overview:** pipeline health, job backlog, articles per day by source, event-type mix, recent
  high-impact articles.
- **Sources:** settings, health, coverage by day and the latest raw items for each source.
- **Articles:** the cleaned-up output with the same filters as `mnp news`; each article shows every
  version (headline edits highlighted), its classifications, asset tags and same-story articles
  from other sources.
- **Classifications:** Jev's full answer to every question (probability distributions,
  confidence, the exact state sent), plus aggregate charts and a comparison between question-set
  versions.
- **Questions:** each question set, with changes from the previous version highlighted.
- **Raw items:** payloads exactly as received. **Clusters:** stories covered by several sources.

It uses no external services: its CSS and JavaScript (Pico.css, htmx, Chart.js) are served from
the app itself.
Times are shown in your system's time zone (set `DISPLAY_TIMEZONE`, e.g. `UTC`, to change it);
the API and database always use UTC.

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
| `GET /v1/feed/…` | The trading feed: see [docs/feed-v1.md](docs/feed-v1.md) |

The API is read-only and has no authentication: keep it on localhost. Finnhub's terms also
forbid redistributing its data.

## Configuration

- **Sources:** `config/sources.yaml`. RSS feeds, Finnhub and GDELT queries, each with a poll
  interval and a reputation. Verify a feed works before adding it. GDELT sources (conflict and
  sanctions paired with oil, stocks or currencies; financial regulation; central banks and
  interest rates) are disabled until switched on. They keep only articles whose headline has a
  market keyword, and share one request every 10 seconds, within GDELT's limit of one per 5.
- **GDELT attribution:** GDELT data is free for any use, but [its terms](https://www.gdeltproject.org/about.html#termsofuse)
  require any use or redistribution to cite the GDELT Project and link to
  https://www.gdeltproject.org/. Anything built on this pipeline's GDELT articles must carry that
  citation too: the API, the feed and the dashboard show it with each GDELT article
  (`attribution`) and each event that includes one (`attributions`).
- **Questions:** `config/questions/<version>.yaml`. Never edit a question set in place: copy it
  to a new version, then `uv run mnp reclassify --question-set v1.2 [--since 2026-10-01]` labels
  stored articles with it, side by side with the old labels. Set `QUESTION_SET` to make it the
  default for new articles.
- **Assets:** `config/assets.yaml`. Symbols, names and aliases used to find candidate assets in
  headlines; Jev then confirms each one. Mark symbols that are common words (`LINK`, `NEAR`)
  `ambiguous: true`.

- **Clustering (same story, different outlets):** articles are embedded with a small local model
  (`BAAI/bge-small-en-v1.5`, downloaded once to `~/.cache/mnp/fastembed`) and join a story when
  similar enough; borderline pairs are checked with Jev ("same event?"). Tune with the
  `CLUSTER_*` settings and check the effect with `uv run mnp cluster-eval [--jev]` against
  `data/cluster_eval/pairs.yaml`. `uv run mnp recluster [--since 30d]` rebuilds clusters after a
  change; each article records when and how it was clustered (`clustered_at`, `cluster_method`).

Step-by-step commands (`mnp collect --once`, `mnp normalize`, `mnp classify --show`) run one
stage at a time, which is handy for debugging. `uv run mnp --help` lists everything.

## Development

```bash
uv run pytest                # uses a separate mnp_test database, created automatically
uv run ruff check . && uv run ruff format --check .
```

Tests never touch the network: feeds, Finnhub, GDELT and Jev are mocked with synthetic fixtures in
`tests/fixtures/`. CI runs lint, migrations and tests against Postgres, plus a secrets scan, on
every pull request. Stage files by explicit path; `git add -A` is blocked for Claude Code in
this repo (`.claude/settings.json`).
