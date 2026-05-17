# Intraday 5m Contract

Status: first usable implementation now exists for separate `5m` regular-session research and session-aware live storage.

This document defines the long-term contract for reusable `5m` intraday stores in `tradinglab-data` without conflating them with the existing extended-hours monitoring cache.
The current implementation intentionally covers the first narrow slice: separate research and live roots, Yahoo-backed stock/ETF universe flows, parquet persistence, UTC/session-date normalization, validation, inspection commands, and a shared sync command that fetches once and writes both stores.

## Purpose

`tradinglab-data` should own:

- vendor retrieval for `5m` equity and ETF bars
- canonical normalization and storage
- session and timezone normalization
- retention and backfill policy
- store-health validation

`tradinglab` should later consume this store for:

- intraday feature engineering
- intraday research and replay
- intraday reporting and signal generation

## Separation From Existing Extended-Hours Store

Today the repo already maintains an intraday parquet store under:

- `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`

That store exists to support the extended-hours monitoring workflow and may mix pre-market, regular-session, and post-market bars depending on provider behavior and `prepost=True` fetches.

The general intraday research store is a separate lane with stricter semantics:

- default objective: research-grade `5m` bars for repeatable strategy work
- default session policy: regular session only
- explicit optional extended-hours mode rather than implicit mixing
- explicit provider and adjustment policy metadata

The implemented research layout is:

- `<intraday.research_root>/5m/<SYMBOL>.parquet`

The implemented live layout is:

- `<intraday_live.live_root>/5m/<SYMBOL>.parquet`

The extended-hours monitoring root should not be reused for these stores; monitoring cache semantics must not leak into research or live semantics.

## Implemented Initial Scope

Start narrow.

- interval: `5m`
- instrument types: US stocks and ETFs
- session policy: regular session only
- research default universe: `intraday_pilot`
- live default universe: `intraday_live_core`
- history goal:
  - accumulate and retain at least `6-12` months locally

Do not start with the full equity universe until the retrieval, storage, and validation path is stable.

## Canonical Storage Layout

Research artifact contract:

- one symbol per parquet file
- interval-specific directory level
- ascending unique timestamps inside each file

Research path:

- `<intraday.research_root>/5m/<SYMBOL>.parquet`

Live path:

- `<intraday_live.live_root>/5m/<SYMBOL>.parquet`

Preferred file semantics:

- one file contains one symbol and one interval only
- rows are sorted ascending by `timestamp`
- `timestamp` is unique within a file
- no mixed-session duplicates for the same timestamp

A partitioned-by-year/month layout can be reconsidered later if file sizes or update costs become a real issue, but the first implementation should stay aligned with the repo's existing one-symbol-per-file convention.

## Canonical Columns

Minimum required columns:

| Column | Type | Notes |
|---|---|---|
| `timestamp` | `Datetime` | UTC-normalized canonical bar timestamp |
| `open` | `Float64` | raw vendor open |
| `high` | `Float64` | raw vendor high |
| `low` | `Float64` | raw vendor low |
| `close` | `Float64` | raw vendor close |
| `volume` | `Float64` | bar volume |
| `currency` | `String` | listing currency when known |

Recommended metadata columns:

| Column | Type | Notes |
|---|---|---|
| `symbol` | `String` | optional but useful in downstream concatenation |
| `interval` | `String` | canonical interval label, e.g. `5m` |
| `provider` | `String` | e.g. `yahoo` |
| `session` | `String` | `regular`, `pre`, `post`, or `mixed` |
| `session_date` | `Date` | exchange-local trading session date |
| `is_regular_session` | `Boolean` | explicit regular-session flag |
| `ingested_at` | `Datetime` | UTC ingest timestamp |

Notes:

- `adj_close` should not be assumed for intraday as a core required field.
- If a provider exposes only raw intraday OHLCV, the store should keep those raw values and record the adjustment policy in metadata rather than fabricate adjusted intraday bars.

## Timezone and Session Policy

Canonical storage should be UTC.

Rules:

- store `timestamp` in UTC-normalized form
- derive `session_date` from the exchange-local session calendar
- regular-session-only should be the default research mode
- if extended-hours bars are stored, they must be explicitly labeled via `session` and `is_regular_session`
- do not mix unlabeled regular and extended-hours bars in the same research contract

Recommended first implementation choice:

- US equities and ETFs
- canonical exchange timezone for session logic: `America/New_York`
- canonical storage timezone for persisted timestamps: UTC

## Corporate Actions and Adjustment Policy

Intraday adjustment policy must be explicit.

Recommended default:

- persist raw vendor intraday OHLCV bars
- do not back-adjust intraday OHLC fields unless the provider contract is explicit and tested
- document splits/dividends handling in the store metadata

If a provider restates historical intraday bars after splits, validation should allow legitimate provider restatements but still flag unexpected row drift.

## Retention and Backfill Policy

The first implementation should be append-first, not cache-first.

Rules:

- preserve older locally accumulated `5m` history whenever possible
- do not trim to the provider's rolling fetch window by default
- when the provider only exposes a limited lookback, repeatedly append fresh windows to grow the local archive over time
- keep retention configurable, but prefer full local retention for research use

Yahoo-specific practical caution:

- `5m` history is typically limited to roughly `60d` per fetch window
- `1m` history is much shorter

That means the local store must be treated as the durable archive if intraday research is a real goal.

## Validation Requirements

A general intraday `5m` workflow should validate at least:

- schema presence and type consistency
- timestamp ordering and uniqueness
- positive OHLC values
- `high >= max(open, close, low)`
- `low <= min(open, close, high)`
- session labeling consistency
- stale-last-bar detection for active symbols
- suspicious gaps during expected market sessions

Validation should distinguish between:

- acceptable overnight and weekend gaps
- provider outages or symbol-specific failures
- missing bars inside regular-session windows

## Implemented Commands

The first-class command surface is in `tradinglab-data`.

```bash
tradinglab-data --config /path/to/config.yaml intraday backfill --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday update --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday validate --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday inspect --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday-live backfill --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live update --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live validate --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-live inspect --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-sync backfill --universe intraday_live_core
tradinglab-data --config /path/to/config.yaml intraday-sync update --universe intraday_live_core
```

The command surface intentionally omits an interval flag for the first implementation; `5m` is configured through `intraday.interval` and `intraday_live.interval`.

## Consumer Contract For `tradinglab`

The consuming `tradinglab` repo should assume:

- a stable per-symbol parquet contract
- explicit session semantics
- UTC timestamps with exchange-local session date available
- no hidden on-demand data retrieval during research runs

That boundary keeps:

- retrieval and normalization in `tradinglab-data`
- strategy/research logic in `tradinglab`

## Rollout State

Completed first-slice implementation:

1. define config and storage roots for research and live intraday stores
2. ingest `5m` regular-session bars for a small research universe
3. ingest `5m` session-aware bars for a live universe
4. add validation and inspection commands for both stores
5. add shared sync to fetch once and write both stores

Next rollout steps:

1. verify retention and append behavior across multiple daily updates
2. expand universe size only after operational validation remains clean
3. add optional intervals or additional session policies only through explicit contract changes

## Non-Goals For The First Iteration

The first general intraday research store should not try to solve everything.

Avoid adding all of this at once:

- multiple intervals beyond `5m`
- options or futures support
- tick-level storage
- complex corporate-action restatement logic
- strategy or prediction logic inside `tradinglab-data`

The first success criterion is:

- a clean, durable, validated, regular-session `5m` archive that `tradinglab` can consume reproducibly.
