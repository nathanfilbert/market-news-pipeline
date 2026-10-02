# Trading feed v1

`/v1/feed` gives a trading platform **events**, not articles: one record per real-world story,
however many outlets report it. Each change to an event is a new **revision**, appended to a
log that is never edited. Read it incrementally with a cursor, or ask what the feed looked like
at any past moment.

The API has no authentication. `mnp run` and `mnp api` bind to 127.0.0.1 by default; the
systemd service binds to 0.0.0.0 so it is reachable on the LAN (see the README). Interactive docs and the OpenAPI schema
are at `/docs` and `/openapi.json` while `mnp run` (or `mnp api`) is running.

## Endpoints

| Endpoint | Returns |
|---|---|
| `GET /v1/feed/events?after=<cursor>&limit=100` | Every revision after the cursor, oldest first: `{revisions, next_cursor, has_more}` |
| `GET /v1/feed/snapshot?as_of=&since=24h&event_type=&asset=&min_impact=&limit=200` | Every active event as of a moment (default now), newest first: `{as_of, count, events}` |
| `GET /v1/feed/events/{event_id}?as_of=` | One event's latest revision (as of a moment) |
| `GET /v1/feed/events/{event_id}/revisions` | Every revision of one event, oldest first |

`limit` is 1–1000. Times accept ISO 8601 (`2026-09-28T12:00:00Z`) or a relative form (`30m`,
`2h`, `7d`); all returned times are UTC.

### Polling (recommended)

```
cursor = load() or "0"
loop:
    page = GET /v1/feed/events?after={cursor}&limit=1000
    for revision in page.revisions:
        apply(revision)            # replace your copy of revision.event_id
    cursor = page.next_cursor; save(cursor)
    if not page.has_more: sleep(a few seconds)
```

Guarantees: pages contain every revision exactly once, in order, however irregularly you poll
or however long you were away. Cursors are opaque strings; don't compute with them. An empty
page returns the cursor you sent.

A revision is the complete event state, not a diff: keep the latest revision per `event_id`.
Revisions of one event have `revision` = 1, 2, 3, … in cursor order.

### Point in time (backtests)

`snapshot?as_of=T` and `events/{id}?as_of=T` return, for each event, the latest revision whose
`available_at` ≤ T. `available_at` is when the revision entered the feed, so nothing the
pipeline learned after T is visible. The feed only records revisions from when it was set up
(2026-09-28): asking about earlier times returns nothing, not a reconstruction.

## Revision

```jsonc
{
  "cursor": "1234",                 // position in the feed; pass as `after`
  "event_id": 501,                  // stable id of the story
  "revision": 2,                    // 1, 2, … per event
  "status": "active",               // or "retracted" (see below)
  "available_at": "2026-09-28T13:35:36.817425Z",
  "schema_version": "1",

  "headline": "…",                  // the event's representative article (latest version)
  "summary": "…",
  "url": "https://…",
  "first_published_at": "…Z",       // earliest publisher timestamp (can be null)
  "first_received_at": "…Z",        // when the pipeline first fetched any of its articles
  "first_classified_at": "…Z",      // null until an article is classified
  "sources": ["coindesk", "theblock"],
  "article_count": 2,
  "classified_article_count": 2,

  "classification": {               // null until an article is classified
    "question_set": "v1.1",
    "event_type": "regulatory_policy",
    "event_type_prob": 0.99,        // 0..1
    "domain": "crypto",             // crypto, equities, macro, other
    "market_relevance": 0.81,       // 0..1, probability
    "new_information": 0.83,        // 0..1, probability
    "promotional": 0.03,            // 0..1, probability
    "sentiment": -0.1,              // -1 bearish … +1 bullish
    "impact": 0.28,                 // 0..1
    "urgency": 0.4667               // 0..1
  },
  "assets": [                       // assets confirmed for the event, strongest first
    {"symbol": "BTC", "name": "Bitcoin", "kind": "crypto", "relevance": 0.93, "article_count": 2}
  ],
  "articles": [
    {"article_id": 436, "source": "theblock", "url": "…", "headline": "…",
     "published_at": "…Z", "received_at": "…Z", "classified": true}
  ],
  "latency": {                      // seconds
    "published_to_received": 95.2,
    "received_to_classified": 6.7,
    "received_to_available": 7.1
  },
  "attributions": [                 // citations the event's sources require (see below)
    {"text": "The GDELT Project", "url": "https://www.gdeltproject.org/"}
  ],
  "superseded_by": []               // retracted revisions only
}
```

Numbers are rounded to 4 decimal places. Fields may be added within v1; consumers should
ignore fields they don't know. Removing or changing a field's meaning needs `/v2`.

### Attribution

Some sources' terms require a citation wherever their data is used or redistributed. GDELT's
require citing the GDELT Project with a link to https://www.gdeltproject.org/. An article from
such a source carries `attribution: {text, url}` in `articles`, and the event lists every
citation its articles need in `attributions` (empty when none). Show them wherever you use or
pass on the event. Events with no such article are unchanged (no new revision).

### Retracted events

Re-clustering (`mnp recluster`, e.g. after a clustering improvement) can regroup articles into
different events. An event that ends up with no articles gets a final revision with
`status: "retracted"`, only `event_id`, `schema_version` and `superseded_by` (the events that now
hold its articles), and leaves the snapshot. Treat it as "replaced by these events". Events never
come back after retraction.

## How articles combine into an event

- **Members:** the articles in the cluster, excluding backfill (old news seen late). Each article
  counts as its latest version.
- **Headline, summary, url:** the cluster's representative article (the first one reported).
- **Classification:** only classifications under the configured question set (`QUESTION_SET`)
  count. Articles are weighted by their source's reputation (minimum 0.1).
  - `event_type` and `domain`: the weighted average of each article's probability distribution;
    the most likely option and its probability.
  - Scores (`market_relevance`, `new_information`, `promotional`, `sentiment`, `impact`,
    `urgency`): weighted means.
- **Assets:** an asset is included when Jev rated its relevance ≥ 0.5 for at least one article;
  `relevance` is the strongest rating and `article_count` the number of articles confirming it.

## What creates a revision

An event is re-checked whenever it may have changed: an article joins it, an article's headline
or summary is edited, an article is classified, old news inside a `catch-up` window is adopted,
or clusters are rebuilt. A revision is written only if the event's content differs from its last
revision. `mnp feed rebuild` re-checks every event (e.g. after changing `QUESTION_SET`).

Revisions commit in cursor order (one writer at a time), so a reader never sees a later cursor
before an earlier one.

## Changelog

- **v1, schema_version 1** (2026-09-28): first release.
- 2026-09-28: added `attributions` on events and `attribution` on articles, for GDELT.
