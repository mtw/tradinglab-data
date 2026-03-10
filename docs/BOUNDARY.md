# Package Boundary

`tradinglab-data` is responsible for data acquisition, normalization, storage, verification, and repair.

`tradinglab` is responsible for consuming prepared artifacts for:

- screening
- plotting
- research
- prediction
- experiment analysis

Required artifact inputs for `tradinglab`:

- universe CSVs
- daily parquet store
- optional intraday parquet store

Design rule:

- `tradinglab` should not fetch market data on demand during research workflows.
- Missing parquet is an operational failure, not a signal to call providers from research code.
