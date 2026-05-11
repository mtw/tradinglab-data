# Data Parquet Schema

Artifact schema version: `v0.2.0`

Machine-readable sources:

- `tradinglab_data.compatibility_manifest()["artifact_schema_version"]`
- `tradinglab_data.schema_manifest()["artifact_schema_version"]`

## Daily

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `adj_close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |

## Intraday

This section describes the current implemented intraday parquet store used by the extended-hours workflow.
The dedicated general-purpose `5m` research store is described separately below and in [INTRADAY_5M_CONTRACT.md](INTRADAY_5M_CONTRACT.md).


| Column | Type |
|---|---|
| `date` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `adj_close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |

## Crypto

| Column | Type |
|---|---|
| `timestamp` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `volume` | `Float64` |
| `provider` | `String` |
| `exchange` | `String` |
| `market_type` | `String` |
| `symbol` | `String` |
| `base_asset` | `String` |
| `quote_asset` | `String` |
| `interval` | `String` |
| `is_closed` | `Boolean` |
| `ingested_at` | `Datetime` |
| `source_symbol` | `String` |

## Storage Layout

- Daily parquet path: `<paths.parquet_root>/<SYMBOL>.parquet`
- Intraday parquet path: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- Intraday research parquet path: `<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet`
- Crypto parquet path: `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`
- One symbol per file
- Rows sorted ascending by `date`
- `date` unique within a file

## Semantics

- `open`, `high`, `low`, `close` are raw vendor OHLC bars
- `adj_close` is the provider-adjusted close when available
- `currency` is listing currency when known, otherwise `"UNKNOWN"`
- Daily bars represent regular-session daily history
- Intraday bars may include pre-market and after-hours data when `prepost=True`
- Intraday research bars are currently regular-session-only `5m` US stock/ETF bars with explicit UTC/session metadata
- Crypto bars are exchange-native OHLCV bars with explicit exchange, market type, interval, and canonical symbol metadata
- Canonical crypto parquet persists closed bars only

## Intraday Research

The first general intraday research store is implemented as a separate lane from the extended-hours cache.

| Column | Type |
|---|---|
| `timestamp` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `volume` | `Float64` |
| `currency` | `String` |
| `symbol` | `String` |
| `interval` | `String` |
| `provider` | `String` |
| `session` | `String` |
| `session_date` | `Date` |
| `is_regular_session` | `Boolean` |
| `ingested_at` | `Datetime` |

Current first-iteration constraints:

- Path: `<intraday.research_root>/5m/<SYMBOL>.parquet`
- One symbol per file
- Rows sorted ascending by `timestamp`
- `timestamp` unique within a file
- `session` must be `regular`
- `is_regular_session` must be `true`
- `timestamp` and `ingested_at` are UTC-normalized datetimes
- `session_date` is the exchange-local `America/New_York` trading date

## Validity Constraints

- `date` must be non-null
- `open`, `high`, `low`, `close` must be non-null and strictly positive
- `high >= open`
- `high >= close`
- `high >= low`
- `low <= open`
- `low <= close`

## Time Handling

- Internal processing should normalize intraday timestamps to UTC-compatible datetimes
- Daily bars should remain day-level market session timestamps
- Mixed timezone dtypes inside one parquet file are not allowed
- Crypto `timestamp` and `ingested_at` are UTC-normalized datetimes
