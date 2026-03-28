# Data Parquet Schema

Artifact schema version: `v0.1.0`

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

## Storage Layout

- Daily parquet path: `<paths.parquet_root>/<SYMBOL>.parquet`
- Intraday parquet path: `<extended_hours.intraday_root>/<INTERVAL>/<SYMBOL>.parquet`
- One symbol per file
- Rows sorted ascending by `date`
- `date` unique within a file

## Semantics

- `open`, `high`, `low`, `close` are raw vendor OHLC bars
- `adj_close` is the provider-adjusted close when available
- `currency` is listing currency when known, otherwise `"UNKNOWN"`
- Daily bars represent regular-session daily history
- Intraday bars may include pre-market and after-hours data when `prepost=True`

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
