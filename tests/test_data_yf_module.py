from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

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
