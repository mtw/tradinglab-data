# Intraday 5m Contract

Status: design target for future general intraday research support.

This document does not describe a fully implemented workflow yet.
It defines the preferred data contract for adding a reusable `5m` intraday store to `tradinglab-data` without conflating it with the existing extended-hours monitoring cache.

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

The future general intraday research store should be treated as a separate lane with stricter semantics:

- default objective: research-grade `5m` bars for repeatable strategy work
- default session policy: regular session only
- explicit optional extended-hours mode rather than implicit mixing
- explicit provider and adjustment policy metadata

The preferred long-term layout is a dedicated root, for example:

- `<intraday.research_root>/5m/<SYMBOL>.parquet`

If the project temporarily reuses the current extended-hours root, it should do so only with clearly separated interval or session namespaces so monitoring cache semantics do not leak into research semantics.

## Recommended Initial Scope

Start narrow.

- interval: `5m`
- instrument types: US stocks and ETFs
- session policy: regular session only
- pilot symbols:
  - `SPY`
  - `QQQ`
  - `IWM`
  - 10-20 additional liquid stocks
- history goal:
  - accumulate and retain at least `6-12` months locally

Do not start with the full equity universe until the retrieval, storage, and validation path is stable.

## Canonical Storage Layout

Preferred artifact contract:

- one symbol per parquet file
- interval-specific directory level
- ascending unique timestamps inside each file

Preferred path:

- `<intraday.research_root>/5m/<SYMBOL>.parquet`

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

## Recommended Future Commands

The first-class command surface should remain in `tradinglab-data`.

Suggested future commands:

```bash
tradinglab-data --config /path/to/config.yaml intraday backfill --interval 5m --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday update --interval 5m --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday validate --interval 5m --universe intraday_pilot
tradinglab-data --config /path/to/config.yaml intraday inspect --interval 5m --universe intraday_pilot
```

Supporting maintenance helpers could include:

- `bootstrap_intraday_history.py`
- `update_intraday_history.py`
- `validate_intraday_store.py`
- `summarize_intraday_coverage.py`

## Consumer Contract For `tradinglab`

The consuming `tradinglab` repo should assume:

- a stable per-symbol parquet contract
- explicit session semantics
- UTC timestamps with exchange-local session date available
- no hidden on-demand data retrieval during research runs

That boundary keeps:

- retrieval and normalization in `tradinglab-data`
- strategy/research logic in `tradinglab`

## Recommended Rollout Sequence

1. define config and storage root for the pilot intraday research store
2. ingest `5m` regular-session bars for a small liquid pilot universe
3. add validation and coverage reporting
4. verify retention and append behavior across multiple daily updates
5. only then expand universe size and add optional extended-hours support

## Non-Goals For The First Iteration

The first general intraday research store should not try to solve everything.

Avoid adding all of this at once:

- multiple intervals beyond `5m`
- options or futures support
- tick-level storage
- complex corporate-action restatement logic
- strategy or prediction logic inside `tradinglab-data`

The first success criterion is simple:

- a clean, durable, validated, regular-session `5m` archive that `tradinglab` can consume reproducibly.
