from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import polars as pl
import pytest

import tradinglab_data._intraday_fetch as intraday_fetch


def test_period_for_interval_returns_period_and_reports_supported_values():
    assert intraday_fetch.period_for_interval("5m", {"5m": "60d"}, purpose="test") == "60d"
    with pytest.raises(ValueError, match="Supported intervals: 1m, 5m"):
        intraday_fetch.period_for_interval("15m", {"5m": "60d", "1m": "7d"}, purpose="test")


def test_sanitize_intraday_df_empty_missing_and_sorting():
    assert intraday_fetch.sanitize_intraday_df(None).is_empty()
    frame = pl.DataFrame(
        {
            "date": [datetime(2026, 1, 2), datetime(2026, 1, 1), None],
            "open": [2.0, None, None],
            "high": [3.0, None, None],
            "low": [1.0, None, None],
            "close": [2.5, None, None],
            "adj_close": [2.5, None, None],
            "volume": [200.0, None, None],
            "ignored": ["x", "y", "z"],
        }
    )

    out = intraday_fetch.sanitize_intraday_df(frame)

    assert out.columns == ["date", "open", "high", "low", "close", "adj_close", "volume"]
    assert out.get_column("date").to_list() == [datetime(2026, 1, 2)]


def test_normalize_intraday_pd_converts_timezone_index_to_utc_naive():
    pdf = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Adj Close": [1.4],
            "Volume": [100],
        },
        index=pd.DatetimeIndex(["2026-01-01 09:30"], tz="America/New_York"),
    )

    out = intraday_fetch.normalize_intraday_pd(pdf)

    assert out.get_column("date").to_list() == [datetime(2026, 1, 1, 14, 30)]


def test_fetch_intraday_one_result_returns_empty_with_classified_issue(monkeypatch):
    monkeypatch.setattr(
        intraday_fetch,
        "run_yf_download",
        lambda *args, **kwargs: (pd.DataFrame(), "possibly delisted", None),
    )

    frame, issue = intraday_fetch._fetch_intraday_one_result("AAA", interval="5m", period="1d", prepost=True)

    assert frame.is_empty()
    assert issue == "yahoo_symbol_warning: possibly delisted or no timezone found"


def test_fetch_extended_intraday_uses_single_symbol_fallback(monkeypatch, tmp_path):
    fallback_frame = pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1, 14, 30)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "adj_close": [1.4],
            "volume": [100.0],
        }
    )
    monkeypatch.setattr(intraday_fetch, "fetch_intraday_bulk", lambda **kwargs: {})
    monkeypatch.setattr(intraday_fetch, "_fetch_intraday_one_result", lambda *args, **kwargs: (fallback_frame, None))

    out = intraday_fetch.fetch_extended_intraday(
        ["AAA"],
        interval="5m",
        period="1d",
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
    )

    assert out["AAA"].height == 1


def test_trim_rolling_window_keeps_recent_rows(monkeypatch):
    now = datetime.utcnow()
    frame = pl.DataFrame({"date": [now - timedelta(days=5), now], "open": [1.0, 2.0]})

    assert intraday_fetch.trim_rolling_window(frame, 0).height == 2
    assert intraday_fetch.trim_rolling_window(frame, 1).height == 1
