from __future__ import annotations

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


@pytest.mark.network
@pytest.mark.parametrize("symbol", ["AAPL", "MSFT"])
def test_yahoo_daily_fetch_smoke(symbol: str):
    try:
        frame = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec(symbol=symbol, interval="1d", lookback_days=30))
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    if frame.is_empty():
        pytest.skip(f"no live daily data returned for {symbol}")

    assert "date" in frame.columns
    assert frame.height > 0


@pytest.mark.network
def test_yahoo_share_class_fallback_smoke():
    try:
        frame = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec(symbol="BRK.B", interval="1d", lookback_days=30))
    except Exception as exc:  # pragma: no cover - live-network path
        _skip_for_network_issue(exc)
        return

    if frame.is_empty():
        pytest.skip("no live daily data returned for BRK.B/BRK-B fallback")

    assert "date" in frame.columns
    assert frame.height > 0


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

    frame = out.get("AAPL")
    if frame is None or frame.is_empty():
        pytest.skip("no live 5m intraday data returned for AAPL")

    assert "date" in frame.columns
    assert frame.height > 0


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

    frame = out.get("MSFT")
    if frame is None or frame.is_empty():
        pytest.skip("no live 5m intraday data returned for MSFT without prepost")

    assert "date" in frame.columns
    assert frame.height > 0
