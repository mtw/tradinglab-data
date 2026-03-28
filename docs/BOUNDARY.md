# Package Boundary

`tradinglab-data` is responsible for data acquisition, normalization, storage, verification, and repair.

Downstream applications are responsible for consuming prepared artifacts for:

- screening
- plotting
- research
- prediction
- experiment analysis

Required artifact inputs for downstream consumers:

- universe CSVs
- daily parquet store
- optional intraday parquet store

Design rule:

- downstream applications should not fetch market data on demand during research workflows.
- Missing parquet is an operational failure, not a signal to call providers from research code.
