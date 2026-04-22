from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import polars as pl


def crypto_parquet_path(
    root: str | Path,
    *,
    exchange: str,
    market_type: str,
    interval: str,
    symbol: str,
) -> Path:
    return Path(root) / exchange / market_type / interval / f"{symbol}.parquet"


def read_crypto_parquet(path: str | Path) -> pl.DataFrame | None:
    parquet_path = Path(path)
    if not parquet_path.exists():
        return None
    return pl.read_parquet(str(parquet_path))


def atomic_write_parquet(path: str | Path, frame: pl.DataFrame) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=str(out_path.parent), prefix=f".{out_path.stem}.", suffix=".parquet", delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        frame.write_parquet(str(temp_path))
        os.replace(temp_path, out_path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
