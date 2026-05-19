# Data Parquet Schema

Artifact schema version: `v0.3.0`

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

## Intraday Research

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

## Intraday Live

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
| `is_closed_bar` | `Boolean` |
| `ingested_at` | `Datetime` |

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

## FX Daily

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `open` | `Float64` |
| `high` | `Float64` |
| `low` | `Float64` |
| `close` | `Float64` |
| `provider` | `String` |
| `pair` | `String` |
| `base_currency` | `String` |
| `quote_currency` | `String` |
| `source_symbol` | `String` |
| `ingested_at` | `Datetime` |

## Symbol Master CSV

| Column | Type |
|---|---|
| `symbol` | `String` |
| `exchange` | `String` |
| `country` | `String` |
| `asset_currency` | `String` |
| `base_listing_currency` | `String` |
| `tax_country` | `String` |
| `asset_class` | `String` |
| `fx_pair_to_base` | `String` |
| `lot_size` | `Float64` |
| `price_multiplier` | `Float64` |
| `name` | `String` |
| `isin` | `String` |
| `instrument_type` | `String` |
| `active` | `String` |
| `source` | `String` |
| `metadata_source` | `String` |
| `metadata_quality` | `String` |
| `notes` | `String` |

## Notes

- One parquet file per symbol. Daily store: `<paths.parquet_root>/<SYMBOL>.parquet`. Intraday store: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`.
- Intraday research store: `<intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet`.
- Intraday live store: `<intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet`.
- Crypto store: `<paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet`.
- FX daily store: `<paths.fx_daily_root>/<PAIR>.parquet`.
- Authoritative symbol metadata lives under `<paths.meta_root>/symbol_master.csv`, with exchange defaults and symbol overrides as companion CSV artifacts.
- Daily OHLC `currency` remains diagnostic provider data. `symbol_master.csv` is the authoritative accounting metadata surface.
- `metadata_quality=non_authoritative_country` means `country` was derived from `exchange_defaults.csv` as fallback metadata.
- `metadata_quality=non_authoritative_tax_country` means `tax_country` was derived from `exchange_defaults.csv` as fallback metadata.
- FX daily pair direction is explicit. `USDEUR` means EUR value of `1` USD.
- Consumers must not silently invert pair direction.
- Identity pairs such as `EUREUR` are explicit in the symbol master and do not require parquet files by default.
