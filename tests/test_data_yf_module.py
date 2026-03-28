from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl

import tradinglab_data.data_yf as data_yf


def test_is_rate_limit_error_detection():
    assert data_yf._is_rate_limit_error(RuntimeError("Too many requests")) is True
    assert data_yf._is_rate_limit_error(RuntimeError("429")) is True
    assert data_yf._is_rate_limit_error(RuntimeError("other")) is False


def test_normalize_yf_df_to_polars_with_standard_columns():
    pdf = pd.DataFrame(
        {
            "Date": [datetime(2020, 1, 1), datetime(2020, 1, 2)],
            "Open": [1.0, 2.0],
            "High": [2.0, 3.0],
            "Low": [0.5, 1.5],
            "Close": [1.5, 2.5],
            "Adj Close": [1.4, 2.4],
            "Volume": [100, 200],
        }
    )
    out = data_yf._normalize_yf_df_to_polars(pdf)
    assert out.columns == ["date", "open", "high", "low", "close", "adj_close", "volume"]
    assert out.height == 2


def test_split_bulk_download_falls_back_single_symbol():
    pdf = pd.DataFrame(
        {
            "Date": [datetime(2020, 1, 1)],
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Adj Close": [1.4],
            "Volume": [100],
        }
    )
    out = data_yf._split_bulk_download(pdf, ["AAPL"])
    assert "AAPL" in out
    assert out["AAPL"].height == 1


def test_split_bulk_download_multiindex_symbol_level0():
    cols = pd.MultiIndex.from_product([["AAPL", "MSFT"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]])
    pdf = pd.DataFrame([[1.0, 2.0, 0.5, 1.5, 1.4, 100, 3.0, 4.0, 2.5, 3.5, 3.4, 200]], columns=cols)
    out = data_yf._split_bulk_download(pdf, ["AAPL", "MSFT"])
    assert set(out.keys()) == {"AAPL", "MSFT"}
    assert out["AAPL"].height == 1
    assert out["MSFT"].height == 1


def test_split_bulk_download_multiindex_symbol_level1():
    cols = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["AAPL", "MSFT"]])
    pdf = pd.DataFrame([[1.0, 3.0, 2.0, 4.0, 0.5, 2.5, 1.5, 3.5, 1.4, 3.4, 100, 200]], columns=cols)
    out = data_yf._split_bulk_download(pdf, ["AAPL", "MSFT"])
    assert set(out.keys()) == {"AAPL", "MSFT"}
    assert out["AAPL"].height == 1
    assert out["MSFT"].height == 1


def test_append_update_log_writes_csv(tmp_path: Path):
    log = tmp_path / "update_log.csv"
    data_yf.append_update_log(log, "AAPL", "x", 1)
    text = log.read_text(encoding="utf-8")
    assert "timestamp,symbol,error,attempt_count" in text
    assert "AAPL,x,1" in text


def test_upsert_symbol_parquet_uses_recent_age_for_incremental_fetch(monkeypatch, tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    last_dt = datetime.now() - timedelta(days=1)
    pl.DataFrame(
        {
            "date": [last_dt],
            "open": [1.0],
            "high": [1.2],
            "low": [0.9],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
        }
    ).write_parquet(root / "AAA.parquet")

    seen_lookbacks: list[int] = []

    def fake_fetch(spec: data_yf.YFDownloadSpec) -> pl.DataFrame:
        seen_lookbacks.append(spec.lookback_days)
        return pl.DataFrame(
            {
                "date": [last_dt + timedelta(days=1)],
                "open": [1.1],
                "high": [1.3],
                "low": [1.0],
                "close": [1.2],
                "adj_close": [1.2],
                "volume": [110.0],
            }
        )

    monkeypatch.setattr(data_yf, "fetch_yfinance_history", fake_fetch)

    out_path = data_yf.upsert_symbol_parquet("AAA", "1d", 2000, root)

    assert out_path == root / "AAA.parquet"
    assert seen_lookbacks == [14]


def test_upsert_symbol_parquet_expands_incremental_window_for_stale_history(monkeypatch, tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    last_dt = datetime.now() - timedelta(days=120)
    pl.DataFrame(
        {
            "date": [last_dt],
            "open": [1.0],
            "high": [1.2],
            "low": [0.9],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
        }
    ).write_parquet(root / "AAA.parquet")

    seen_lookbacks: list[int] = []

    def fake_fetch(spec: data_yf.YFDownloadSpec) -> pl.DataFrame:
        seen_lookbacks.append(spec.lookback_days)
        return pl.DataFrame(
            {
                "date": [datetime.now()],
                "open": [1.1],
                "high": [1.3],
                "low": [1.0],
                "close": [1.2],
                "adj_close": [1.2],
                "volume": [110.0],
            }
        )

    monkeypatch.setattr(data_yf, "fetch_yfinance_history", fake_fetch)

    data_yf.upsert_symbol_parquet("AAA", "1d", 2000, root)

    assert len(seen_lookbacks) == 1
    assert seen_lookbacks[0] >= 125
