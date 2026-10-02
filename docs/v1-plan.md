# Market News Pipeline — v1 Plan

> Handoff document for implementation. Read this whole file before writing code.
> Build milestone by milestone; each milestone ends with its acceptance checks passing.

## 1. Goal

Collect crypto and general market news in near real time, store every raw item untouched,
classify each article with **Jev** (TypeSafe AI's decision/classification model), and store
the classifications linked back to the raw data by ID and content hash. Expose the results
through a CLI, a small read-only HTTP API and (v1.1) a read-only dashboard.

**Primary use:** feeding the owner's crypto trading platform with classified, de-duplicated news
(updated 2026-09-28), with general-market/macro news as context; the dashboard supports research.
The platform has its own price data. v1 is **not** a low-latency trading signal; minute-level
latency is acceptable.

### Non-goals for v1
- GDELT ingestion (scheduled for the v1.5 release, §12)
- Fetching or scraping full article text from publisher websites (use what feeds/APIs provide)
- Generative summaries or numeric/date extraction (amounts, unlock dates) — v2
- Sub-minute latency, streaming infrastructure (Kafka etc.)
- Web UI: a read-only dashboard is the v1.1 release, see §12
- Automated trading of any kind
- Alerting (rules and webhook delivery): not scheduled, see "Ideas" in §12

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
 Outputs: CLI · FastAPI · dashboard (v1.1 release)
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
- **Alerts:** not scheduled (§12, Ideas).

## 8. Repo layout

```
market-news-pipeline/
  pyproject.toml
  docker-compose.yml
  .env.example
  alembic/ …
  config/
    sources.yaml  assets.yaml
    questions/v1.0.yaml
  src/mnp/
    config.py  db.py  models.py  jobs.py  cli.py  runner.py
    collectors/  base.py  rss.py  aggregator.py
    normalize/   canonical_url.py  text.py  hashing.py  cluster.py
    classify/    base.py  jev.py  fake.py  assets.py  questions.py
    outputs/     api.py
    dashboard/   routes.py  queries.py  templates/  static/   (v1.1 release)
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
Build the CLI query and FastAPI endpoints. (Alerting was moved out of M5; now unscheduled, §12 Ideas.)
✅ The API returns filtered results. `/health` shows stale sources.

**M6 — Run it**
Build `mnp run` (all loops plus workers), structured JSON logging, and graceful shutdown. Update the README with setup and usage.
✅ Runs unattended for 24h locally. `/health` is green, there are no unhandled crashes, and job backlog stays near zero.
*Status (2026-09-28): the 24-hour check was waived by the owner. The first attempt ran ~10 hours
across a system suspend (~8 h), a network drop and a feed error; it recovered from each on its
own with no crashes and an empty backlog, then stopped cleanly on SIGTERM. For continuous use,
stop the machine sleeping (`systemd-inhibit`) or run on an always-on host.*

## 10. Engineering rules
- Never mutate or delete `raw_items`.
- Keep the three timestamps separate: `published_at` (source), `received_at` (us), `classified_at` (classifier). Never overwrite one with another.
- Secrets only in `.env`; commit `.env.example` only.
- No network in tests; record fixtures.
- Small commits per milestone; don't start a milestone until the previous one's checks pass.

## 11. Open decisions (defaults in bold; confirm with the owner if unsure)
- Aggregator: NewsAPI vs **CryptoPanic** vs Finnhub. Choose after checking current terms and latency.
- Dashboard stack (v1.1 release): **server-rendered pages (FastAPI + Jinja2 + htmx) with vendored
  CSS and chart libraries** vs a JavaScript single-page app (React/Vite).
- Hosting: **local machine first**, then a small VPS.

## 12. After v1 (not now)

Release versions are numbered independently of *question set* versions
(`config/questions/v1.1.yaml`).

### v1.1 release: dashboard
A simple, modern, **read-only** web dashboard for browsing and visualizing the data. Decided by
the owner (2026-09-28); alerting was later taken off the roadmap. No editing or configuration from the UI:
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

### v1.2 release: embedding-based clustering
Owner decision (2026-09-28): the next focus, because the trading platform needs each real-world
event to appear once, whichever outlets report it. Trigram headline similarity missed
paraphrases (five outlets' Bitget-hack headlines became separate clusters).

As built (2026-09-28):
- **Embeddings:** each article version's headline + start of summary, embedded with
  `BAAI/bge-small-en-v1.5` via `fastembed` (ONNX on CPU, 384 dimensions, milliseconds per
  article; the model is cached locally). Stored in `article_embeddings` as a `real[]` with the
  model name. **pgvector was not needed:** the matching window holds at most ~1,000 vectors and
  NumPy compares them in well under a millisecond. Revisit if volume grows by ~100x.
- **Assignment** (`normalize/cluster.py`): nearest articles from *other* sources within ±48h of
  publish time (first seen when unknown). Cosine ≥ 0.88 joins; 0.78–0.88 asks Jev "Do these two
  articles report the same event?" and joins if yes ≥ 0.7 (at most two clusters asked); if Jev is
  unavailable, ≥ 0.84 joins. Backfill articles stay isolated. All thresholds are settings.
- **Evaluation:** `data/cluster_eval/pairs.yaml` holds 287 labelled cross-source pairs (80 same
  event), identified by URL. `mnp cluster-eval [--jev]` scores them:

  | Method | Recall | Precision |
  |---|---|---|
  | Trigram ≥ 0.6 (v1) | 2% | 100% |
  | Embedding ≥ 0.82 | 91% | 91% |
  | **Hybrid (defaults)** | **88%** | **99%** |

  Precision is favoured: merging two different events would hide one from the trading platform.
- **Provenance:** `articles.clustered_at` and `cluster_method` (embedding, trigram, isolated),
  exposed in the API. Consumers replaying history must not use a cluster assignment before its
  `clustered_at`.
- **Rebuild:** `mnp recluster [--since]` replays articles in publish-time order with the
  configured method. On the dev database: 433 articles, 17 multi-article stories (was 2), 37
  same-event checks, all 17 stories verified correct by hand.
- ✅ Met: embeddings find far more same-event pairs than trigrams (88% vs 2% recall) without
  merging distinct same-template events; new articles are embedded and clustered during
  normalize; re-clustering is repeatable (tested).

### Ideas (not scheduled)
- **Alerting.** Documented for reference; not planned while the pipeline's main consumer is the
  trading platform (owner, 2026-09-28). Rules in `config/alerts.yaml` over classification columns
  and asset tags (e.g. `event_type in [hack_exploit, exchange_listing] and impact >= 0.7 and
  is_promotional < 0.5`); delivery through a Discord or Telegram webhook; once per cluster, not per
  article; retried on webhook failure without duplicates.

### v1.3 release: trading feed API
Owner decision (2026-09-28). The pipeline's main consumer is the owner's trading platform, which
today would have to rebuild events from article-level endpoints. This release gives it events it
can act on directly, under a versioned contract.

- **Events, not articles:** one record per cluster with a stable `event_id`: first-reported time,
  headline of the representative article, sources and article count so far, the event's
  classification (event type with probability, impact, sentiment, urgency, relevance, promo),
  confirmed assets with relevance, and links to its articles. Rules for combining articles into
  one event summary (e.g. which classification represents the event, how asset relevance
  combines) are decided and documented in this release.
- **Revisions:** an event gets a new `revision` whenever it changes (another outlet reports it,
  a headline is edited, a classification lands); consumers see updates, never duplicates.
- **Incremental reading:** `GET /v1/feed/events?after=<cursor>` returns new and changed events in
  a stable order with a cursor, so a consumer polling on any schedule misses nothing and repeats
  nothing, including after downtime.
- **Point-in-time queries:** `?as_of=<time>` returns the feed exactly as it was at that moment,
  using the timestamps already kept separate (published, received, classified, clustered), so
  backtests can't see news before the pipeline had it. Each event carries its own latency
  breakdown (published → received → classified → available).
- **Versioned contract:** `/v1/…` with a documented schema (OpenAPI), clean numeric formatting,
  and a changelog; breaking changes need a new version.
- **Owner answers** (2026-09-28): the platform pulls over HTTP to start with; it runs on this
  machine (so localhost, no authentication); the schema is ours to design and the consumer builds
  around it. Push delivery can come later on top of the same revision log.
- **Built** (2026-09-28): an append-only `feed_revisions` table (id = cursor; writes serialized
  so commits land in cursor order), feed jobs queued in the same transaction as every change,
  content-hash dedupe, retraction with `superseded_by` after a re-cluster, `/v1/feed/events`,
  `/v1/feed/snapshot`, `/v1/feed/events/{id}[/revisions]`, `mnp feed build|rebuild`. Schema and
  combination rules: [feed-v1.md](feed-v1.md). History starts at deployment: `as_of` before then
  returns nothing rather than a reconstruction that could leak later knowledge.
- ✅ Met: an event appears once however many outlets report it, with a revision per change
  (join, edit, classification) and none when nothing changed; paging with a cursor yields every
  revision exactly once, and new changes appear after a saved cursor; `as_of` returns only
  revisions available by then (replayed in tests: a pre-classification moment shows the
  unclassified revision); retracted events leave the snapshot; the table rejects UPDATE and
  DELETE; schema documented in feed-v1.md and OpenAPI. On the dev database: 368 events built in
  5 s, 22 with several sources; snapshot queries take 5-40 ms.

### v1.4 release: exchange announcement sources
Owner decision (2026-09-28). Exchanges' own announcements (listings, delistings, maintenance,
trading suspensions, deposit/withdrawal halts) often move prices within minutes and appear
before any news site reports them.

- **Research first:** for each candidate exchange (at least Binance, Coinbase, OKX, Kraken;
  others by trading relevance), find the official announcement channel (RSS, API or status
  page), its latency, rate limits and terms of use. Only sources whose terms allow this use are
  added; unofficial scraping endpoints are avoided.
- **One collector per channel type** behind the existing collector interface, with the same
  guarantees (raw items stored exactly as received, checkpoints, backoff, dedupe).
- Announcements are classified like articles and cluster with news coverage of the same event
  (e.g. an exchange's listing notice and the news stories about it).
- ✅ Each added exchange's announcements arrive within its polling interval of publication;
  fixture-based tests per collector; listings and delistings are classified as such on a
  labelled sample; an announcement and its news coverage land in one event.

### v1.5 release: GDELT
GDELT as another collector (planned since v1; confirmed 2026-09-28), filtered to themes relevant
to markets: sanctions, conflict, regulation and central banks. It adds global coverage from
sources the RSS and aggregator feeds don't reach.

- Uses the GDELT DOC 2.0 API (free, no key; limited to one request per 5 seconds, and it
  throttled bursty clients during testing on 2026-09-27, so the collector paces itself
  conservatively and backs off). Articles come with URL, title, domain, language and time
  seen, but no summary; they are stored raw like every other source.
- Theme and keyword queries live in `config/sources.yaml`; results go through normalization,
  classification and clustering like other articles, so GDELT reports of an event merge with
  existing coverage.
- Terms of use and attribution requirements are checked before enabling.
- ✅ Fixture-based tests; the collector stays within GDELT's rate limits under continuous
  running; GDELT articles about an event already covered by other sources join that event.

As built (2026-09-28; conflict x oil and sanctions x oil enabled, the other four disabled):
- **Collector** (`collectors/gdelt.py`, kind `gdelt`): one source per query in
  `config/sources.yaml` (`gdelt_sanctions`, `gdelt_conflict`, `gdelt_regulation`,
  `gdelt_central_banks`, GKG themes, English-language coverage, polled every 5 minutes). Each poll
  reads `ArtList` results oldest first from the checkpoint (`seen_through`, minus 30 minutes of
  overlap for GDELT's 15-minute indexing), following full pages of 250 up to `max_pages`; a
  window still unfinished resumes at the next poll. Articles are stored as received; the
  normalizer takes the title, URL, language and GDELT's first-seen time (no summary).
- **Rate limit:** all GDELT sources share one pacer, one request every 10 s. A 429 or GDELT's
  "Please limit requests" text pauses every GDELT source for 5 minutes and backs the source off.
  Requests get a 60 s read timeout (GDELT took ~21 s to answer in live testing).
- **Live test** (2026-09-28, from the owner's machine): GDELT mostly answered 429 even after
  150 s of silence, so throttling is partly per IP and outside our control. `gdelt_conflict`
  returned 230 English articles from 144 domains for about half an hour of coverage (GDELT's
  newest articles lag 30-45 minutes; the first poll now looks back 120 minutes), with weak market
  relevance (travel lists, campus politics), so the owner asked for it to be narrowed. Every
  query now requires a topic theme plus a market theme (oil price, oil, stock market; for
  regulation stock market, bankruptcy, debt) and excludes a few spam domains. Measured per hour:
  conflict ~125 (was ~450), sanctions ~43, regulation ~17 (was ~200), central banks ~85 (still
  mixed). GDELT rejects long queries ("too short or too long"), so each keeps to ~3 OR'd themes.
  Each query needed 3-6 tries through the throttling.
- **Narrowed further** (owner, 2026-09-28): each source now pairs its topic with one specific
  market theme (conflict x oil price, conflict x stock market, sanctions x oil price, sanctions x
  currency, central banks x interest rates; regulation unchanged), and the collector drops
  articles whose title has no market keyword (`DEFAULT_TITLE_KEYWORDS`, overridable per source)
  before they are stored or classified. The checkpoint still advances past dropped articles.
  Live: conflict x oil price kept ~38/h of ~47/h (dropped mostly diplomatic updates); LPG and
  kerosene were added after two fuel-tax headlines were dropped. Sources poll every 15 minutes,
  GDELT's batch interval, since the owner's IP is throttled heavily (8 tries for one query).
- **Why some sources were always blocked** (2026-09-28): an interleaved test (21 requests, 60 s
  apart) let ~1 in 7 through regardless of query, window or result size; the two "always
  blocked" sources were unlucky, not rejected. GDELT throttles per IP, sends no Retry-After and
  holds refusals 10-20 s, so the throttle pause is now randomized (5-10 min) to avoid retrying in
  step. Durable options: fewer requests, asking GDELT for more capacity, or ingesting GDELT's
  15-minute bulk files instead of the search API.
- **Terms** (checked 2026-09-28, https://www.gdeltproject.org/about.html#termsofuse): free for
  any use, redistribution allowed, but any use or redistribution must cite the GDELT Project and
  link to https://www.gdeltproject.org/. Cited in the README, and every GDELT article carries the
  citation in the API (`attribution`), the feed (`attribution` per article, `attributions` per
  event) and the dashboard, so downstream users can pass it on.
- **Before enabling:** the theme queries were
  written from GDELT's documentation without a live test (the dev container can't reach GDELT),
  so run each once with `mnp collect --source <name> --once` and look at volume and relevance.

### Later
- Generative model for summaries and amount/date extraction
- Other lower-latency sources (on-chain alerts, X/Telegram)
- Price data join for measuring which labels actually predict moves (the platform has prices)
