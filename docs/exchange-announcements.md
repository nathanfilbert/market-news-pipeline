# Exchange announcement channels (v1.4 research)

Research for the v1.4 release in [v1-plan.md](v1-plan.md) §12, done 2026-09-28. Read only:
no code or config changed. The use case being checked is the one the plan describes: a
collector on the owner's machine feeding the owner's own trading platform (localhost, not
redistributed).

**Caveat on latency and limits.** The research environment could not reach the exchanges
directly (outbound connections were refused), so nothing below was measured. Rate limits and
payloads are from each exchange's documentation; latencies are what the delivery mechanism
implies (push = seconds, polling = poll interval plus the exchange's own publishing lag).
Each collector should record `published → received` on real traffic before it is trusted.

## Summary

| Exchange | Recommended channel | Kind | Auth | Terms for this use | Recommendation |
|---|---|---|---|---|---|
| OKX | `GET /api/v5/support/announcements` | REST poll | none | Allowed for own internal use (API Agreement §9.2) | **Add first** |
| Kraken | Blog "Asset Listings" RSS + status.kraken.com RSS | RSS poll | none | RSS is a published feed; general ToS bans bots "on Our Content" without consent | **Add**, note the ToS ambiguity |
| Coinbase | status.coinbase.com RSS + Advanced Trade WS `status` channel | RSS poll + websocket | none | Market Data Terms allow personal/internal use, no redistribution | **Add the Statuspage feed; WS channel later** |
| Binance | CMS websocket, topic `com_announcement_en` | websocket push | API key + HMAC signature | Needs a Binance.com account; product ToU not checked (page wouldn't load) | **Hold** until eligibility is confirmed |

No exchange offers a first-party RSS feed of its full announcement stream. The unofficial
endpoints behind the exchanges' own announcement web pages (e.g. Binance's `bapi/.../cms/...`)
are left out, as the plan requires.

## OKX

- **Channel:** `GET https://www.okx.com/api/v5/support/announcements`, public REST, no key.
  Returns `title`, `url`, `annType`, `pTime` (publish time) and `businessPTime`. Companion
  endpoint `GET /api/v5/support/announcement-types` lists the categories (new listings,
  delistings, maintenance, etc.), so filtering by type is possible.
- **Rate limit:** 5 requests per 2 seconds (documented). Polling every 60 s is far inside it.
- **Latency:** poll interval (60 s) plus OKX's own publishing lag.
- **Other channels:** `GET /api/v5/system/status` and the public WS `status` channel cover
  scheduled system maintenance only, not listings. Useful later, not needed first.
- **Terms:** the [OKX API Agreement](https://www.okx.com/en-us/help/okx-api-agreement)
  licenses API use "solely for your own internal purposes" (§9.2) and forbids offering it as a
  signal service or redistributing to third parties (§9.3, §9.4). A local feed for the owner's
  own platform fits; exposing it to anyone else would not.
- **Watch out:** OKX restricts some regions (US users are served by a separate OKX US). Check the
  endpoint answers from the machine the pipeline runs on.
- **Recommendation:** add first. It is the only exchange with an official, keyless,
  machine-readable announcement list, and it fits the existing polling collector model.

## Kraken

- **Channels:**
  - Blog category feed `https://blog.kraken.com/category/product/asset-listings/feed`
    (WordPress RSS 2.0). Fetched 2026-09-28: valid, latest item "TREAD is available for trading!"
    dated 2026-09-16. It can reuse the existing RSS collector with no new code.
  - `https://status.kraken.com/history.rss` (Atlassian Statuspage; also `api/v2/*.json`):
    maintenance windows, funding gateway pauses, incidents.
  - WS v2 `status` channel (`wss://ws.kraken.com/v2`, public): engine state only (`online`,
    `maintenance`, `cancel_only`, `post_only`).
- **Rate limits:** none documented for the feeds; poll at 60–120 s like other RSS sources.
- **Latency:** poll interval plus publishing lag. Kraken's own
  [listing process post](https://blog.kraken.com/product/accelerating-new-listings-at-kraken)
  says a listing is official when announced on the @krakenpro X account; whether the blog post
  appears at the same moment is not documented (inferred to be close, unmeasured). The
  kraken.com/listings roadmap page lists assets earlier but has no feed.
- **Gap:** delistings are posted on support.kraken.com, which has no feed. Not covered.
- **Terms:** Kraken's [Global Terms](https://www.kraken.com/legal/global-terms) ban "bots,
  robots, parsers, spiders, scripts" used "on Our Content" without prior written consent, with
  no carve-out in the text. Reading a published RSS feed or a public status page at a polite
  rate is the use those feeds exist for, but the wording is broad, so this is a judgement call
  for the owner, not a clear yes.
- **Recommendation:** add both RSS feeds as config-only sources once the owner accepts the ToS
  reading above.

## Coinbase

- **Channels:**
  - `https://status.coinbase.com/history.rss` (Atlassian Statuspage; also Atom and
    `api/v2/*.json`): per-asset send/receive delays, deposit pauses, and trading-mode changes
    (e.g. a recent incident moved WMTX-USD to limit-only after a third-party security
    incident). Config-only with the existing RSS collector.
  - Advanced Trade WebSocket `status` channel (no auth): pushes each product's `status` and
    `status_message`, so a new product appearing or trading being disabled shows up within
    seconds. The channel closes after 60–90 s without updates unless `heartbeats` is also
    subscribed. This needs a new websocket collector type.
- **Listing announcements themselves** go out on X (@CoinbaseMarkets / listing roadmap posts).
  The X API is paid and its terms are separate, so it is out of scope here.
- **Terms:** Coinbase [Market Data Terms](https://www.coinbase.com/legal/market_data) allow use
  "exclusively for you or your entity's personal or research purposes" and forbid
  redistribution to third parties. They also forbid using the data to train or benchmark ML
  models; classifying with Jev is inference, but don't feed this data into model training.
  The [CDP Terms](https://www.coinbase.com/legal/developer-platform/terms-of-service) §7 let
  Coinbase throttle or revoke access over rate-limit abuse.
- **Recommendation:** add the Statuspage RSS now (no code). Add the WS `status` collector as the
  first websocket collector if v1.4 builds one for Binance anyway; otherwise defer it.

## Binance

- **Channel:** the official CMS websocket
  ([docs](https://developers.binance.com/docs/cms/announcement)):
  `wss://api.binance.com/sapi/wss?random=…&topic=com_announcement_en&recvWindow=…&timestamp=…&signature=…`.
  Pushes `catalogName` (e.g. "Delisting", "New Cryptocurrency Listing"), `title`, `body`,
  `publishDate` (ms) and `disclaimer`. This is the only official machine-readable announcement
  source; there is no RSS.
- **Auth:** an API key in `X-MBX-APIKEY` plus an HMAC-SHA256 signature
  ([general info](https://developers.binance.com/docs/cms/general-info)). A read-only key is
  enough, but it requires a Binance.com account.
- **Limits:** 5 messages per second per connection including pings (exceeding it disconnects,
  repeats can ban the IP); ping every 30 s; connections last at most 24 h, so the collector must
  reconnect.
- **Latency:** push, seconds after publication. The fastest of the four.
- **Gap:** a websocket can't backfill. Announcements published while disconnected are missed,
  which breaks the "misses nothing" guarantee the other collectors keep. There is no official
  REST list to catch up from.
- **Terms and eligibility:** Binance.com does not serve US persons, and `api.binance.com`
  answers HTTP 451 from restricted locations. The Binance
  [Terms of Use](https://www.binance.com/en/terms) could not be read (the page wouldn't render
  for the research tools), so the API-use clauses are unchecked.
- **Recommendation:** hold. Enable only if the owner has (or can have) a Binance.com account in
  an eligible country and the ToU check passes. If it goes ahead, it needs a websocket collector
  type with reconnect, signing and a documented gap on disconnect.

## Suggested v1.4 order

1. Config-only RSS sources: Kraken asset listings, Kraken status, Coinbase status.
2. OKX announcements collector (small REST poller behind the collector interface).
3. A websocket collector type, if Binance is cleared; Coinbase `status` rides on it.

## Owner decisions (2026-09-28)

- The pipeline runs from the **US**. That rules out Binance (Binance.com does not serve US
  persons) and OKX's global API (US users are served by OKX US, whose announcements API was not
  checked here). Both are dropped from v1.4 unless that changes.
- **Kraken** goes first: the asset-listings and status RSS feeds are added to
  `config/sources.yaml` as config-only sources (listings every 60 s, status every 120 s).
