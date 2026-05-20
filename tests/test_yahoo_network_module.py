from __future__ import annotations

import polars as pl
import pytest

from tradinglab_data import data_yf
from tradinglab_data._intraday_fetch import fetch_extended_intraday


def _skip_for_network_issue(exc: Exception) -> None:
    text = f"{type(exc).__name__}: {exc}".lower()
    transient_markers = [
        "rate limit",
        "network",
        "timed out",
        "temporarily unavailable",
        "service unavailable",
        "forbidden",
        "not known",
        "name or service not known",
        "nodename nor servname provided",
        "could not resolve host",
    ]
    if any(marker in text for marker in transient_markers):
        pytest.skip(f"upstream/network unavailable: {exc}")
    raise exc


def _assert_nonempty_daily_frame(frame: pl.DataFrame, symbol: str) -> None:
    assert frame.is_empty() is False, f"successful Yahoo daily fetch returned empty frame for {symbol}"
    assert "date" in frame.columns
    assert frame.height > 0


def _assert_nonempty_intraday_frame(frame: pl.DataFrame | None, symbol: str, *, prepost: bool) -> None:
    label = "with prepost" if prepost else "without prepost"
    assert frame is not None, f"successful Yahoo intraday fetch returned no frame for {symbol} {label}"
    assert frame.is_empty() is False, f"successful Yahoo intraday fetch returned empty frame for {symbol} {label}"
    assert "date" in frame.columns
    assert frame.height > 0


@pytest.mark.network
@pytest.mark.parametrize("symbol", ["AAPL", "MSFT"])
def test_yahoo_daily_fetch_smoke(symbol: str):
    try:
        frame = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec(symbol=symbol, interval="1d", lookback_days=30))
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    _assert_nonempty_daily_frame(frame, symbol)


@pytest.mark.network
def test_yahoo_share_class_fallback_smoke():
    try:
        frame = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec(symbol="BRK.B", interval="1d", lookback_days=30))
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    _assert_nonempty_daily_frame(frame, "BRK.B")


@pytest.mark.network
def test_yahoo_intraday_fetch_smoke():
    try:
        out = fetch_extended_intraday(
            ["AAPL"],
            interval="5m",
            period="10d",
            prepost=True,
            sleep_seconds=0.0,
            threads=False,
        )
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    _assert_nonempty_intraday_frame(out.get("AAPL"), "AAPL", prepost=True)


@pytest.mark.network
def test_yahoo_intraday_fetch_smoke_without_prepost():
    try:
        out = fetch_extended_intraday(
            ["MSFT"],
            interval="5m",
            period="10d",
            prepost=False,
            sleep_seconds=0.0,
            threads=False,
        )
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    _assert_nonempty_intraday_frame(out.get("MSFT"), "MSFT", prepost=False)


def test_yahoo_daily_smoke_helper_fails_on_empty_success(monkeypatch):
    monkeypatch.setattr(
        data_yf,
        "fetch_yfinance_history",
        lambda spec: pl.DataFrame({"date": [], "open": [], "high": [], "low": [], "close": [], "adj_close": [], "volume": []}).with_columns(
            pl.col("date").cast(pl.Datetime),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("adj_close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
        ),
    )

    frame = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec(symbol="AAPL", interval="1d", lookback_days=30))
    with pytest.raises(AssertionError, match="empty frame"):
        _assert_nonempty_daily_frame(frame, "AAPL")


def test_yahoo_intraday_smoke_helper_fails_on_empty_success():
    empty = pl.DataFrame(
        {
            "date": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "adj_close": [],
            "volume": [],
            "currency": [],
        }
    ).with_columns(
        pl.col("date").cast(pl.Datetime),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("adj_close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("currency").cast(pl.Utf8),
    )
    with pytest.raises(AssertionError, match="empty frame"):
        _assert_nonempty_intraday_frame(empty, "AAPL", prepost=True)
