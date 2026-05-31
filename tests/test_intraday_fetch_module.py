from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

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


def test_normalize_intraday_pd_ignores_tz_conversion_failures(monkeypatch):
    class BrokenIndex:
        tz = "UTC"
        def tz_convert(self, zone):
            raise RuntimeError("bad tz")

    class BrokenFrame:
        index = BrokenIndex()
        def copy(self):
            return self

    monkeypatch.setattr(intraday_fetch, "normalize_yf_df_to_polars", lambda df_pd: pl.DataFrame({"date": [datetime(2026, 1, 1, 14, 30)], "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "adj_close": [1.4], "volume": [100.0]}))
    monkeypatch.setattr(intraday_fetch, "coerce_standard_schema", lambda df: df)
    out = intraday_fetch.normalize_intraday_pd(BrokenFrame())
    assert out.height == 1


def test_normalize_intraday_pd_converts_timezone_index_to_utc_naive_across_us_dst_boundaries():
    pdf = pd.DataFrame(
        {
            "Open": [1.0, 2.0],
            "High": [2.0, 3.0],
            "Low": [0.5, 1.5],
            "Close": [1.5, 2.5],
            "Adj Close": [1.4, 2.4],
            "Volume": [100, 200],
        },
        index=pd.DatetimeIndex(["2026-03-09 09:30", "2026-11-02 09:30"], tz="America/New_York"),
    )

    out = intraday_fetch.normalize_intraday_pd(pdf)

    assert out.get_column("date").to_list() == [datetime(2026, 3, 9, 13, 30), datetime(2026, 11, 2, 14, 30)]


def test_fetch_intraday_one_result_returns_empty_with_classified_issue(monkeypatch):
    monkeypatch.setattr(
        intraday_fetch,
        "run_yf_download",
        lambda *args, **kwargs: (pd.DataFrame(), "possibly delisted", None),
    )

    frame, issue = intraday_fetch._fetch_intraday_one_result("AAA", interval="5m", period="1d", prepost=True)

    assert frame.is_empty()
    assert issue == "yahoo_symbol_warning: possibly delisted or no timezone found"


def test_fetch_intraday_one_wraps_normalized_frame_and_raises_unknown_exception(monkeypatch):
    normalized = pl.DataFrame(
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
    monkeypatch.setattr(intraday_fetch, "run_yf_download", lambda *args, **kwargs: ("payload", "", None))
    monkeypatch.setattr(intraday_fetch, "normalize_intraday_pd", lambda payload: normalized)
    frame = intraday_fetch.fetch_intraday_one("AAA", interval="5m", period="1d")
    assert frame.equals(normalized)

    monkeypatch.setattr(intraday_fetch, "run_yf_download", lambda *args, **kwargs: (None, "", RuntimeError("boom")))
    monkeypatch.setattr(intraday_fetch, "classify_yf_download_issue", lambda text: None)
    with pytest.raises(RuntimeError, match="boom"):
        intraday_fetch._fetch_intraday_one_result("AAA", interval="5m", period="1d", prepost=True)


def test_fetch_intraday_bulk_returns_empty_for_no_symbols_and_raises_unknown_issue(monkeypatch):
    assert intraday_fetch.fetch_intraday_bulk([], interval="5m", period="1d") == {}

    log_calls: list[tuple] = []
    monkeypatch.setattr(intraday_fetch, "run_yf_download", lambda *args, **kwargs: (None, "", RuntimeError("boom")))
    monkeypatch.setattr(intraday_fetch, "classify_yf_download_issue", lambda text: None)
    monkeypatch.setattr(intraday_fetch, "append_update_log", lambda *args: log_calls.append(args))
    out = intraday_fetch.fetch_intraday_bulk(["AAA"], interval="5m", period="1d", sleep_seconds=0, log_path=Path("/tmp/log.csv"))
    assert out == {}
    assert any("intraday_5m_error:boom" in call[2] for call in log_calls)


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


def test_fetch_intraday_bulk_covers_empty_issue_retry_and_logging(monkeypatch, tmp_path):
    log_calls: list[tuple] = []
    throttle_calls: list[tuple] = []
    sleep_calls: list[float] = []
    backoff_calls: list[tuple[int, float]] = []
    frame = pl.DataFrame(
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

    attempts: dict[str, int] = {}

    def fake_run(_download, chunk, **kwargs):
        symbol = chunk[0]
        attempts[symbol] = attempts.get(symbol, 0) + 1
        if symbol == "MISS":
            return None, "possibly delisted", None
        if symbol == "RATE":
            if attempts[symbol] == 1:
                raise RuntimeError("rate limit")
            raise RuntimeError("still rate limited")
        if symbol == "ERR":
            raise RuntimeError("hard fail")
        return None, "", None

    monkeypatch.setattr(intraday_fetch, "run_yf_download", fake_run)
    monkeypatch.setattr(intraday_fetch, "split_bulk_download", lambda df_pd, chunk: {"AAA": frame} if chunk == ["AAA"] else {})
    monkeypatch.setattr(intraday_fetch, "coerce_standard_schema", lambda df: df)
    monkeypatch.setattr(intraday_fetch, "classify_yf_download_issue", lambda text: "possibly_delisted" if "possibly delisted" in text else None)
    monkeypatch.setattr(intraday_fetch, "is_rate_limit_error", lambda exc: "rate limit" in str(exc))
    monkeypatch.setattr(intraday_fetch, "backoff_sleep", lambda attempt, max_seconds: backoff_calls.append((attempt, max_seconds)))
    monkeypatch.setattr(intraday_fetch, "append_update_log", lambda *args: log_calls.append(args))
    monkeypatch.setattr(intraday_fetch, "append_update_log_throttled", lambda *args, **kwargs: throttle_calls.append(args))
    monkeypatch.setattr(intraday_fetch.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    out = intraday_fetch.fetch_intraday_bulk(
        ["MISS", "RATE", "ERR", "AAA"],
        interval="5m",
        period="1d",
        chunk_size=1,
        sleep_seconds=0.25,
        max_retries=1,
        log_path=tmp_path / "log.csv",
        warning_state_path=tmp_path / "state.json",
    )

    assert list(out) == ["AAA"]
    assert throttle_calls[0][1] == "MISS"
    assert backoff_calls == [(1, 120.0)]
    assert any(call[1] == "RATE" for call in log_calls)
    assert any(call[1] == "ERR" for call in log_calls)
    assert sleep_calls == [0.25, 0.25, 0.25, 0.25]


def test_fetch_intraday_bulk_skips_coerce_failures_and_logs_missing_issue(monkeypatch, tmp_path):
    throttle_calls: list[tuple] = []
    calls = {"attempt": 0}
    frame = pl.DataFrame({"date": [datetime(2026, 1, 1, 14, 30)], "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "adj_close": [1.4], "volume": [100.0]})

    monkeypatch.setattr(intraday_fetch, "run_yf_download", lambda *args, **kwargs: (None, "warning", None))
    monkeypatch.setattr(intraday_fetch, "classify_yf_download_issue", lambda text: "warning")
    monkeypatch.setattr(intraday_fetch, "split_bulk_download", lambda df_pd, chunk: {"AAA": frame, "BBB": frame})
    def fake_coerce(df):
        calls["attempt"] += 1
        if calls["attempt"] == 1:
            raise RuntimeError("coerce fail")
        return df
    monkeypatch.setattr(intraday_fetch, "coerce_standard_schema", fake_coerce)
    monkeypatch.setattr(intraday_fetch, "append_update_log_throttled", lambda *args, **kwargs: throttle_calls.append(args))

    out = intraday_fetch.fetch_intraday_bulk(["AAA", "BBB"], interval="5m", period="1d", chunk_size=2, sleep_seconds=0, log_path=tmp_path / "log.csv")

    assert list(out) == ["BBB"]
    assert any(call[1] == "AAA" and "warning" in call[2] for call in throttle_calls)


def test_fetch_extended_intraday_logs_fallback_failures(monkeypatch, tmp_path):
    throttle_calls: list[tuple] = []
    monkeypatch.setattr(
        intraday_fetch,
        "fetch_intraday_bulk",
        lambda **kwargs: {"AAA": pl.DataFrame(schema=intraday_fetch.INTRADAY_SCHEMA), "BBB": pl.DataFrame(schema=intraday_fetch.INTRADAY_SCHEMA)},
    )

    def fake_one(symbol: str, **kwargs):
        if symbol == "AAA":
            return pl.DataFrame(schema=intraday_fetch.INTRADAY_SCHEMA), "possibly_delisted"
        raise RuntimeError("single boom")

    monkeypatch.setattr(intraday_fetch, "_fetch_intraday_one_result", fake_one)
    monkeypatch.setattr(intraday_fetch, "append_update_log_throttled", lambda *args, **kwargs: throttle_calls.append(args))

    out = intraday_fetch.fetch_extended_intraday(
        ["AAA", "BBB"],
        interval="5m",
        period="1d",
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
        warning_state_path=tmp_path / "state.json",
    )

    assert "AAA" in out and out["AAA"].is_empty()
    assert any(call[1] == "AAA" and "possibly_delisted" in call[2] for call in throttle_calls)
    assert any(call[1] == "BBB" and "single_error" in call[2] for call in throttle_calls)

def test_trim_rolling_window_keeps_recent_rows(monkeypatch):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    frame = pl.DataFrame({"date": [now - timedelta(days=5), now], "open": [1.0, 2.0]})

    assert intraday_fetch.trim_rolling_window(frame, 0).height == 2
    assert intraday_fetch.trim_rolling_window(frame, 1).height == 1


def test_trim_rolling_window_returns_empty_frame_unchanged():
    empty = pl.DataFrame(schema={"date": pl.Datetime})
    assert intraday_fetch.trim_rolling_window(empty, 5).is_empty()
