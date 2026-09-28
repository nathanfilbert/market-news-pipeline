# Market News Pipeline — v1 Plan

> Handoff document for implementation. Read this whole file before writing code.
> Build milestone by milestone; each milestone ends with its acceptance checks passing.

## 1. Goal

Collect crypto and general market news in near real time, store every raw item untouched,
classify each article with **Jev** (TypeSafe AI's decision/classification model), and store
the classifications linked back to the raw data by ID and content hash. Expose the results
through a CLI and a small read-only HTTP API. A read-only dashboard follows in the v1.1 release
and filtered alerts in v1.2 (§12).

**Primary use:** research for crypto trading (and alerting from the v1.2 release), with
general-market/macro news as context. v1 is **not** a low-latency trading signal; minute-level
latency is acceptable.

### Non-goals for v1
- GDELT ingestion (planned for v1.5 as another collector)
- Fetching or scraping full article text from publisher websites (use what feeds/APIs provide)
- Generative summaries or numeric/date extraction (amounts, unlock dates) — v2
- Sub-minute latency, streaming infrastructure (Kafka etc.)
- Web UI: a read-only dashboard is the v1.1 release, see §12
- Automated trading of any kind
- Alerting (rules and webhook delivery): deferred to the v1.2 release, see §12

## 2. Tech stack (defaults — keep unless there's a concrete reason)

| Concern | Choice |
|---|---|
| Language | Python 3.12, managed with `uv` |
| DB | PostgreSQL 16 via `docker compose`, with the `pg_trgm` extension |
| DB access / migrations | SQLAlchemy 2.x (core or ORM) + Alembic |
| HTTP | `httpx` (async) |
| RSS parsing | `feedparser` |
| API | FastAPI (read-only) |
| CLI | `typer` |
| Config | `.env` for secrets (`pydantic-settings`), YAML for sources, assets and Jev question sets |
| Tests | `pytest`, with recorded fixtures — **no network calls in tests** |
| Lint/format | `ruff` |

All timestamps are stored as `timestamptz` in UTC.

## 3. Architecture

```
 sources.yaml
     │
 Collectors (RSS, aggregator API)   ── poll on interval, checkpointed, rate-limited
     │  writes untouched payloads
     ▼
 raw_items (append-only)
     │  job: normalize
     ▼
 articles / article_versions        ── canonical URL, content hash, versioning, near-dup grouping
     │  job: classify
     ▼
 Classifier interface ── JevClassifier (prod) / FakeClassifier (tests)
     │
 classifications + article_assets
     │
 Outputs: CLI · FastAPI  (dashboard: v1.1 release · alert webhooks: v1.2 release)
```

- **Stages communicate through Postgres.** A `jobs` table is consumed with `SELECT … FOR UPDATE SKIP LOCKED`. No Redis or Kafka.
- **Every stage is idempotent.** Re-running a stage on the same input must not create duplicates. Enforce this with unique constraints, not only application logic.
- **One long-running process** (`mnp run`) hosts all collector loops and job workers as asyncio tasks. Each stage can also be run as a one-shot CLI command for debugging.
- The system runs 24/7 (crypto never closes). Every loop must survive exceptions, log them, back off, and continue.

## 4. Sources (v1)

Configured in `config/sources.yaml`:

```yaml
- name: coindesk
  kind: rss
  url: <verify current CoinDesk RSS URL>
  poll_seconds: 60
  category: crypto
  reputation: 0.9      # 0–1, used downstream; not decided by Jev
```

Initial set. **Verify each URL works before committing it.**
- Crypto RSS: CoinDesk, Cointelegraph, The Block, Decrypt
- Macro / official RSS: Federal Reserve press releases, SEC press releases, BLS release schedule/news
- General markets RSS: one or two general finance feeds (e.g. CNBC markets). Pick feeds whose terms allow this use.
- Aggregator API: **one** adapter behind a common interface. NewsAPI was the original idea, but its free tier has historically been dev-only with delayed articles. Check the current terms. If it's unsuitable, implement CryptoPanic or Finnhub instead. Keep the aggregator optional (disabled when no API key is set).

Collector contract:
- `fetch(checkpoint) -> (list[RawPayload], new_checkpoint)`
- Checkpoints are persisted in `source_state` (ETag / Last-Modified / last-seen IDs / cursor).
- Use conditional GETs for RSS, honor rate limits and `Retry-After`, apply exponential backoff with jitter, and set a descriptive User-Agent.
- A collector never parses or cleans content. It only stores exactly what it received.

## 5. Data model

```
sources(id, name UNIQUE, kind, url, category, reputation, enabled, poll_seconds)

source_state(source_id PK, checkpoint jsonb, last_success_at, last_error_at,
             last_error, consecutive_failures)

raw_items(id, source_id, fetched_at, external_id, url,
          payload jsonb,             -- exactly as received (per-entry for RSS)
          payload_sha256,            -- integrity hash of the raw bytes
          UNIQUE(source_id, payload_sha256))

articles(id, canonical_url UNIQUE, first_seen_at, cluster_id NULL)

article_versions(id, article_id, version_no,
                 content_hash,       -- sha256 of normalized headline+summary+body+canonical_url
                 headline, summary, body NULL, author NULL, language,
                 published_at,       -- from the source (may be null or wrong; keep as given)
                 received_at,        -- when we first fetched it
                 raw_item_id,        -- link back to the raw payload
                 UNIQUE(article_id, content_hash))

clusters(id, first_seen_at, representative_article_id)   -- near-duplicate story grouping

assets(id, symbol, name, kind,       -- kind: crypto | equity | index | macro
       aliases text[], ambiguous bool)  -- ambiguous = symbol is a common word (LINK, ONE, NEAR…)

classifications(id, article_version_id, content_hash,
                classifier,          -- 'jev'
                model_version,       -- as reported by the API
                question_set_version,-- e.g. 'v1.0'; bump whenever question wording changes
                results jsonb,       -- FULL output: every probability/score, not just argmax
                event_type, event_type_prob, is_market_relevant_prob, sentiment, impact,  -- denormalized for querying
                latency_ms, classified_at,
                UNIQUE(article_version_id, classifier, question_set_version))

article_assets(article_version_id, asset_id,
               candidate_via,        -- 'alias_match' | 'source_tag'
               relevance_prob,       -- Jev yes/no: "is this article about <asset>?"
               classification_id,
               PRIMARY KEY(article_version_id, asset_id, classification_id))

jobs(id, kind, payload jsonb, status, attempts, run_after, locked_at, last_error,
     created_at, UNIQUE(kind, dedupe_key))
```

### Normalization rules
- Canonical URL: lowercase the host, strip tracking params (`utm_*`, `ref`, `fbclid`, etc.), drop fragments, resolve known redirect wrappers where cheap.
- Text: strip HTML, unescape entities, collapse whitespace, NFC-normalize unicode.
- `content_hash` is computed from normalized fields only, so fetch-time noise never creates a new version.
- Same canonical URL + new content hash → new `article_version` (headline edits are kept, not overwritten).
- Same content from a different URL/source → separate article in the same cluster.

### Near-duplicate clustering (v1: simple)
Within a 48h window, assign an article to an existing cluster when normalized headline trigram similarity (`pg_trgm`) ≥ a configurable threshold (start at 0.6). Otherwise create a new cluster. Keep this in one module so it can be replaced by embeddings later.

## 6. Classification (Jev)

### Interface
```python
class Classifier(Protocol):
    name: str
    async def classify(self, state: ArticleState, questions: QuestionSet) -> ClassificationResult: ...
```
- `JevClassifier` calls TypeSafe AI's API. **Read the current Jev API docs before implementing.** It is a new model (Sept 2026), so don't guess request/response shapes. Put the API key in `.env` (`JEV_API_KEY`).
- `FakeClassifier` returns deterministic results for tests.
- The rest of the pipeline depends only on the interface, so another model can be swapped in later.

### State sent to Jev
`{headline, summary, body (if any), source name, source category, published_at}`. Don't rely on Jev for dates or numbers; its docs say it's weak there.

### Question set v1 (`config/questions/v1.0.yaml`)
All questions are sent in one call; Jev evaluates them in parallel.

**Choice**
- `event_type`: `exchange_listing`, `exchange_delisting`, `hack_exploit`, `regulatory_enforcement`, `regulatory_policy`, `legal_court`, `etf_institutional_flows`, `token_unlock_supply`, `stablecoin_depeg_or_issuance`, `protocol_upgrade_fork`, `partnership_adoption`, `macro_data_release`, `monetary_policy`, `corporate_earnings_guidance`, `market_move_commentary`, `opinion_analysis`, `other`
- `domain`: `crypto`, `equities`, `macro`, `other`

**Yes/No**
- `is_market_relevant`: could this plausibly move prices of a tradable asset?
- `is_new_information`: does this report new facts, as opposed to commentary or a recap?
- `is_promotional`: is this a press release, sponsored post, or promotional content?
- Per candidate asset: `about_<SYMBOL>`: is this article materially about <Name> (<SYMBOL>)?

**Score**
- `sentiment`: −1 (very bearish) … +1 (very bullish), for the primary asset/market
- `impact`: 0 (none) … 1 (likely major, immediate price impact)
- `urgency`: 0 (evergreen) … 1 (time-critical, act within minutes)

### Asset tagging (two steps)
1. **Candidates:** match `assets.aliases` against headline/summary with word boundaries. Ambiguous symbols (`ambiguous = true`) only become candidates if the name or a non-ambiguous alias also appears, or if the source supplied the tag.
2. **Confirmation:** add an `about_<SYMBOL>` yes/no question for each candidate (cap at ~10 per article). Store all probabilities in `article_assets`.

Seed `config/assets.yaml` with the top ~100 crypto assets by market cap, major stablecoins, BTC/ETH ETFs, and macro entities (Fed, CPI, FOMC, SEC, CFTC). Mark ambiguous symbols.

### Versioning and re-classification
- Changing the wording of any question requires a new `question_set_version`.
- `mnp reclassify --question-set v1.1 [--since 2026-10-01]` re-runs classification without touching raw or article data. Old and new labels sit side by side.

## 7. Outputs

- **CLI:** `mnp news --since 1h --asset BTC --event-type hack_exploit --min-impact 0.6`
- **API (FastAPI, read-only):** `GET /articles`, `GET /articles/{id}` (includes versions, classifications and a raw link), `GET /clusters/{id}`, `GET /health` (per-source freshness and last error). Support filters for asset, event_type, domain, time range, and minimum impact/relevance.
- **Dashboard:** read-only web UI, the v1.1 release (§12).
- **Alerts:** deferred to the v1.2 release (§12).

## 8. Repo layout

```
market-news-pipeline/
  pyproject.toml
  docker-compose.yml
  .env.example
  alembic/ …
  config/
    sources.yaml  assets.yaml  (alerts.yaml: v1.2 release)
    questions/v1.0.yaml
  src/mnp/
    config.py  db.py  models.py  jobs.py  cli.py  runner.py
    collectors/  base.py  rss.py  aggregator.py
    normalize/   canonical_url.py  text.py  hashing.py  cluster.py
    classify/    base.py  jev.py  fake.py  assets.py  questions.py
    outputs/     api.py  (dashboard/: v1.1 release · alerts.py: v1.2 release)
  tests/
    fixtures/ (recorded RSS/API payloads, sample Jev responses)
  docs/v1-plan.md
```

## 9. Milestones and acceptance checks

**M0 — Scaffold**
Set up `uv` project, ruff, pytest, docker-compose Postgres, Alembic baseline, config loading, `.env.example`, and a `mnp` CLI entrypoint.
✅ `docker compose up -d && uv run alembic upgrade head && uv run pytest` passes.

**M1 — RSS ingestion to raw store**
Build the collector base class, RSS collector, `source_state` checkpoints, backoff, and `mnp collect --source coindesk --once`.
✅ Running twice in a row inserts no duplicate `raw_items`. A failed source doesn't stop the others. Tests use fixture feeds.

**M2 — Normalize, version, cluster**
Build the normalize job, canonical URLs, content hash, article versioning, and trigram clustering.
✅ Unit tests cover URL canonicalization and hashing. An edited headline creates version 2. The same story from two sources lands in one cluster. Re-running normalize is a no-op.

**M3 — Aggregator adapter**
Implement one aggregator (see §4) behind the collector interface; it's skipped when no API key is set.
✅ Fixture-based tests pass. Overlap with RSS items dedupes to the same article or cluster.

**M4 — Classification**
Build the classifier interface, FakeClassifier, JevClassifier, question-set loader, asset candidates and confirmation, the classify job, and `mnp reclassify`.
✅ Every new article version gets exactly one classification per question-set version. The full Jev response is stored in `results`. A Jev API outage causes retries with backoff, and no data is lost. One manual smoke test runs against the real API on ~20 recent articles, and the results are printed for review.

**M5 — Outputs**
Build the CLI query and FastAPI endpoints. (Alert rules and webhook delivery moved to the v1.2 release, §12.)
✅ The API returns filtered results. `/health` shows stale sources.

**M6 — Run it**
Build `mnp run` (all loops plus workers), structured JSON logging, and graceful shutdown. Update the README with setup and usage.
✅ Runs unattended for 24h locally. `/health` is green, there are no unhandled crashes, and job backlog stays near zero.

## 10. Engineering rules
- Never mutate or delete `raw_items`.
- Keep the three timestamps separate: `published_at` (source), `received_at` (us), `classified_at` (classifier). Never overwrite one with another.
- Secrets only in `.env`; commit `.env.example` only.
- No network in tests; record fixtures.
- Small commits per milestone; don't start a milestone until the previous one's checks pass.

## 11. Open decisions (defaults in bold; confirm with the owner if unsure)
- Aggregator: NewsAPI vs **CryptoPanic** vs Finnhub. Choose after checking current terms and latency.
- Alert channel (v1.2 release): **Discord webhook** vs Telegram.
- Dashboard stack (v1.1 release): **server-rendered pages (FastAPI + Jinja2 + htmx) with vendored
  CSS and chart libraries** vs a JavaScript single-page app (React/Vite).
- Hosting: **local machine first**, then a small VPS.

## 12. After v1 (not now)

Release versions are numbered independently of *question set* versions
(`config/questions/v1.1.yaml`).

### v1.1 release: dashboard
A simple, modern, **read-only** web dashboard for browsing and visualizing the data. Decided by
the owner (2026-09-28); alerting moved to v1.2. No editing or configuration from the UI:
sources, questions and assets stay in `config/`.

- **Overview:** pipeline health (per-source status, last success/error), job backlog, articles
  per day by source, event-type mix, recent high-impact articles.
- **Sources:** each source's settings, health, fetch history and coverage (articles per day),
  and its recent raw items.
- **Articles:** the cleaned-up output. Filterable list (time, source, asset, event type,
  domain, min impact/relevance, include backfill), same semantics as `mnp news`. Article detail:
  every version (headline edits highlighted), the classification(s) side by side per question
  set, asset tags with relevance, cluster siblings, and a link to the raw item.
- **Classifications:** the full Jev response per question, with probability distributions
  (choice probabilities, score level distributions, noul values) and confidence, plus the state
  that was sent. Aggregate views: event-type distribution, sentiment vs impact, per-question
  answer distributions, comparison across question-set versions.
- **Questions:** each question set rendered readably (instructions, options/levels, ranges),
  with differences between versions highlighted.
- **Raw items:** the payload exactly as received (pretty-printed XML/JSON) with its hash,
  fetch time and the article/version it produced.
- **Clusters:** multi-source stories with their articles.

Constraints: served by the existing FastAPI app (so `mnp run` and `mnp api` include it) on
localhost, no authentication, no external CDNs at runtime (assets vendored), works without a
build step, reads through the same query layer as the API.

- ✅ Every page renders from the live database with no errors; filters match `mnp news` results;
  an article can be traced from list → versions → classification (every probability) → raw
  payload; tests cover each page with fixture data.

### v1.2 release: alerting
Deferred from M5 by the owner (2026-09-27), then from v1.1 to v1.2 (2026-09-28).

- **Rules** in `config/alerts.yaml`, e.g. `event_type in [hack_exploit, exchange_listing] and impact >= 0.7 and is_promotional < 0.5`. Rules read the classification columns (including `domain`, `urgency`, `is_new_information_prob`) and asset tags from `article_assets`.
- **Delivery** through a Discord or Telegram webhook (see §11).
- Alert **once per cluster**, not once per article.
- ✅ One alert fires per cluster (tested with FakeClassifier). Webhook failures are retried without sending duplicates.

### Later
- GDELT collector (filtered by themes: sanctions, conflict, regulation, central banks)
- Embedding-based clustering (`pgvector`)
- Generative model for summaries and amount/date extraction
- Lower-latency sources (exchange announcement feeds, on-chain alerts, X/Telegram)
- Price data join for measuring which labels actually predict moves
