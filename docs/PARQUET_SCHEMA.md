# Data Parquet Schema

Artifact schema version: `v0.4.0`

Dataframe policy: `polars-first`

Schema dtypes are rendered from Polars definitions. Public tabular Python APIs return `polars.DataFrame`; pandas is not part of the public dataframe contract.

Machine-readable sources:

- `tradinglab_data.compatibility_manifest()["artifact_schema_version"]`
- `tradinglab_data.compatibility_manifest()["dataframe_policy"]`
- `tradinglab_data.schema_manifest()["artifact_schema_version"]`
- `tradinglab_data.schema_manifest()["dataframe_policy"]`

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

## Market Cap

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `symbol` | `String` |
| `market_cap_usd_millions` | `Float64` |
| `provider` | `String` |
| `source_symbol` | `String` |
| `ingested_at` | `Datetime` |

## Sector Assignments CSV

| Column | Type |
|---|---|
| `symbol` | `String` |
| `sector` | `String` |
| `effective_start` | `Date` |
| `effective_end` | `Date` |
| `source` | `String` |
| `ingested_at` | `Datetime` |

## Index Returns

| Column | Type |
|---|---|
| `date` | `Datetime` |
| `index_id` | `String` |
| `return` | `Float64` |
| `total_return_level` | `Float64` |
| `provider` | `String` |
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

- Polars-first: public tabular Python APIs return polars.DataFrame objects, schemas are expressed with Polars dtypes, and pandas-shaped provider outputs are normalized at ingestion boundaries.
- One parquet file per symbol. Daily store: <paths.parquet_root>/<SYMBOL>.parquet. Intraday store: <extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet.
- Legacy intraday monitoring cache: <extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet. This thin OHLC-only schema is retained for compatibility; downstream consumers should prefer intraday_research or intraday_live.
- Intraday research store: <intraday.research_root>/<INTERVAL>/<SYMBOL>.parquet.
- Intraday live store: <intraday_live.live_root>/<INTERVAL>/<SYMBOL>.parquet.
- Crypto store: <paths.crypto_root>/<EXCHANGE>/<MARKET_TYPE>/<INTERVAL>/<SYMBOL>.parquet.
- FX daily store: <paths.fx_daily_root>/<PAIR>.parquet.
- Market-cap store: <paths.market_cap_root>/<SYMBOL>.parquet.
- Sector assignments live in <paths.sector_assignments_csv>.
- Index total-return store: <paths.index_returns_root>/<INDEX_ID>.parquet.
- Authoritative symbol metadata lives under <paths.meta_root>/symbol_master.csv, with exchange defaults and symbol overrides as companion CSV artifacts.
- OHLC columns are raw vendor OHLC. adj_close is adjusted close when supplied by the upstream provider. currency is the listing currency when known.
- The legacy intraday parquet contract is an extended-hours monitoring cache with the historical OHLC-only schema. It is retained for compatibility and should not be treated as the preferred downstream analytical contract.
- symbol_master.csv is the authoritative accounting metadata surface. Daily OHLC currency remains diagnostic provider data and is not authoritative accounting metadata. metadata_quality=non_authoritative_country and metadata_quality=non_authoritative_tax_country mark fallback fields derived from exchange_defaults.csv rather than provider-authoritative source data.
- Intraday research parquet persists regular-session raw OHLCV bars with explicit UTC timestamp, session_date, provider, and symbol metadata.
- Intraday live parquet persists session-aware raw OHLCV bars for pre, regular, and post sessions with explicit closed-bar and session metadata.
- Crypto parquet persists closed exchange-native OHLCV bars with explicit exchange, market type, interval, and canonical symbol metadata.
- FX daily parquet persists explicit source-to-target conversion pairs such as USDEUR, meaning EUR value of 1 USD. Consumers must not silently invert pair direction.
- Market-cap parquet persists point-in-time market capitalisation in USD millions for public consumer size splits.
- Sector assignment CSV persists GICS sector names using the fixed 11-sector vocabulary.
- Index return parquet persists daily total returns for supported market indices such as SPX, RTY, and NDX.
- date is stored as Polars Datetime. Daily bars represent session dates. Intraday bars should be normalized to UTC internally and written without mixed timezone types.
- Intraday research timestamp and ingested_at are stored as UTC-normalized datetimes; session_date is the exchange-local trading date.
- Intraday live timestamp and ingested_at are stored as UTC-normalized datetimes; session_date is the exchange-local trading date.
- Crypto timestamp columns are UTC-normalized bar-open timestamps; ingested_at records the last local write time in UTC.
- FX daily date follows the same daily-bar normalization as the existing daily parquet contract; ingested_at is stored in UTC.
- Market-cap date follows the effective trading date of the observation; ingested_at is stored in UTC.
- Sector effective_start and effective_end are inclusive point-in-time dates when history is available.
- Index return date follows the trading date of the total-return observation; ingested_at is stored in UTC.
- Rows must be sorted by date ascending.
- date values must be unique within a file.
- open/high/low/close must be non-null and positive for valid rows.
- high must be >= open, close, low. low must be <= open, close.
- Rows must be sorted by timestamp ascending.
- timestamp values must be unique within a file.
- session must be regular and is_regular_session must be true in the first implementation.
- interval, provider, and symbol metadata must be populated on every row and remain file-consistent.
- Rows must be sorted by timestamp ascending.
- timestamp values must be unique within a file.
- session must be one of pre, regular, post, or unknown.
- interval, provider, symbol, and is_closed_bar metadata must be populated on every row and remain file-consistent.
- Rows must be sorted by timestamp ascending.
- timestamp values must be unique within a file.
- Only closed bars belong in the canonical crypto parquet history.
- exchange, market_type, symbol, interval, and source_symbol must be populated on every row.
- Rows must be sorted by date ascending.
- date values must be unique within a file.
- base_currency + quote_currency must equal pair on every row.
- open, high, low, and close must be positive finite conversion values.
- Rows must be sorted by date ascending within each symbol file.
- date values must be unique within each symbol file.
- market_cap_usd_millions must be strictly positive for valid rows.
- symbol, provider, and source_symbol must be populated on every row.
- symbol and sector must be populated on every row.
- sector must use the fixed 11-sector GICS vocabulary.
- effective_start and effective_end are inclusive when populated.
- Rows must be sorted by date ascending within each index file.
- date values must be unique within each index file.
- return must be a simple daily total return.
- index_id, provider, and source_symbol must be populated on every row.
- All required symbol master columns must be present.
- Active rows must have non-empty symbol, exchange, country, asset_currency, base_listing_currency, tax_country, asset_class, and fx_pair_to_base values.
- lot_size and price_multiplier must be strictly positive.
- fx_pair_to_base must be a six-letter uppercase pair, including explicit identity pairs such as EUREUR.
