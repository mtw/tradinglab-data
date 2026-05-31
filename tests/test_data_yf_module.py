from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

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


def test_append_update_log_throttled_suppresses_recent_duplicate(tmp_path: Path):
    log = tmp_path / "update_log.csv"
    state = tmp_path / "warning_state.json"
    data_yf.append_update_log_throttled(log, "AAPL", "issue_a", 1, cooldown_hours=24.0, state_path=state)
    wrote = data_yf.append_update_log_throttled(log, "AAPL", "issue_a", 1, cooldown_hours=24.0, state_path=state)
    text = log.read_text(encoding="utf-8").strip().splitlines()

    assert wrote is False
    assert len(text) == 2
    assert state.exists()


def test_warning_state_write_prunes_old_entries(tmp_path: Path, monkeypatch):
    state = tmp_path / "warning_state.json"
    now = datetime.now(tz=data_yf.timezone.utc)
    stale = now - timedelta(days=31)
    fresh = now - timedelta(days=1)
    monkeypatch.setattr(data_yf, "_WARNING_STATE_TTL_DAYS", 30)

    data_yf._write_warning_state(
        state,
        {
            ("AAPL", "issue_old"): stale,
            ("MSFT", "issue_new"): fresh,
        },
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert "AAPL␟issue_old" not in payload
    assert "MSFT␟issue_new" in payload


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

    with pytest.warns(DeprecationWarning, match="deprecated"):
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

    with pytest.warns(DeprecationWarning, match="deprecated"):
        data_yf.upsert_symbol_parquet("AAA", "1d", 2000, root)

    assert len(seen_lookbacks) == 1
    assert seen_lookbacks[0] >= 125


def test_fetch_yfinance_history_bulk_classifies_dns_failure_without_delisted_noise(monkeypatch, tmp_path: Path, capsys):
    log_path = tmp_path / "update_log.csv"

    def fake_download(*args, **kwargs):
        print(
            "Failed to get ticker 'HYG' reason: Failed to perform, curl: (6) Could not resolve host: guce.yahoo.com.",
            file=sys.stderr,
        )
        print("$HYG: possibly delisted; no timezone found", file=sys.stderr)
        print("\n1 Failed download:\n['HYG']: possibly delisted; no timezone found", file=sys.stderr)
        return pd.DataFrame()

    monkeypatch.setattr(data_yf.yf, "download", fake_download)

    out = data_yf.fetch_yfinance_history_bulk(
        ["HYG"],
        interval="1d",
        lookback_days=30,
        chunk_size=1,
        sleep_seconds=0.0,
        log_path=log_path,
    )

    captured = capsys.readouterr()
    assert out == {}
    assert captured.out == ""
    assert captured.err == ""
    log_text = log_path.read_text(encoding="utf-8")
    assert "yahoo_connectivity_error: could not resolve host guce.yahoo.com" in log_text
    assert "possibly delisted" not in log_text


def test_fetch_yfinance_history_raises_unclassified_exception(monkeypatch):
    monkeypatch.setattr(
        data_yf,
        "_run_yf_download",
        lambda *args, **kwargs: (None, "", RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec("AAA"))


def test_fetch_yfinance_history_raises_when_share_class_fallback_fails(monkeypatch):
    calls: list[str] = []

    def fake_run(download_fn, symbol, **kwargs):
        calls.append(symbol)
        if symbol == "BRK.B":
            return pd.DataFrame(), "", None
        return None, "", RuntimeError("fallback failed")

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)

    with pytest.raises(RuntimeError, match="fallback failed"):
        data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec("BRK.B"))

    assert calls == ["BRK.B", "BRK-B"]


def test_fetch_yfinance_history_uses_share_class_fallback(monkeypatch):
    calls: list[str] = []
    pdf = pd.DataFrame(
        {
            "Date": [datetime(2026, 1, 1)],
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Adj Close": [1.4],
            "Volume": [100],
        }
    )

    def fake_run(download_fn, symbol, **kwargs):
        calls.append(symbol)
        if symbol == "BRK.B":
            return pd.DataFrame(), "", None
        return pdf, "", None

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)

    out = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec("BRK.B"))

    assert calls == ["BRK.B", "BRK-B"]
    assert out.height == 1


def test_fetch_yfinance_history_returns_empty_for_classified_issue(monkeypatch):
    monkeypatch.setattr(
        data_yf,
        "_run_yf_download",
        lambda *args, **kwargs: (pd.DataFrame(), "possibly delisted", None),
    )

    out = data_yf.fetch_yfinance_history(data_yf.YFDownloadSpec("AAA"))

    assert out.is_empty()
    assert out.columns == list(data_yf.STANDARD_PRICE_SCHEMA)


def test_fetch_symbol_currency_fast_info_get_item_info_and_cache(monkeypatch):
    data_yf.clear_currency_cache()
    ticker_calls: list[str] = []

    class FastInfoGet:
        def get(self, key):
            return " usd "

    class FakeTicker:
        fast_info = FastInfoGet()

        def __init__(self, symbol):
            ticker_calls.append(symbol)

        def get_info(self):
            return {"currency": "EUR"}

    monkeypatch.setattr(data_yf.yf, "Ticker", FakeTicker)

    assert data_yf.fetch_symbol_currency("AAA") == "USD"
    assert data_yf.fetch_symbol_currency("AAA") == "USD"
    assert ticker_calls == ["AAA"]

    data_yf.clear_currency_cache()

    class FastInfoItem:
        def get(self, key):
            raise RuntimeError("no get")

        def __getitem__(self, key):
            return "gbp"

    class FakeTickerItem(FakeTicker):
        fast_info = FastInfoItem()

    monkeypatch.setattr(data_yf.yf, "Ticker", FakeTickerItem)
    assert data_yf.fetch_symbol_currency("BBB") == "GBP"

    data_yf.clear_currency_cache()

    class FakeTickerInfo(FakeTicker):
        fast_info = None

        def get_info(self):
            return {"currency": " jpy "}

    monkeypatch.setattr(data_yf.yf, "Ticker", FakeTickerInfo)
    assert data_yf.fetch_symbol_currency("CCC") == "JPY"


def test_fetch_symbol_currency_handles_non_string_and_exception(monkeypatch):
    data_yf.clear_currency_cache()

    class FakeTickerBad:
        fast_info = {"currency": 123}

        def __init__(self, symbol):
            pass

        def get_info(self):
            return {"currency": 456}

    monkeypatch.setattr(data_yf.yf, "Ticker", FakeTickerBad)
    assert data_yf.fetch_symbol_currency("BAD") is None

    data_yf.clear_currency_cache()

    class ExplodingTicker:
        def __init__(self, symbol):
            raise RuntimeError("no ticker")

    monkeypatch.setattr(data_yf.yf, "Ticker", ExplodingTicker)
    assert data_yf.fetch_symbol_currency("ERR") is None


def test_fetch_symbol_currency_handles_missing_fast_info_get_and_getitem(monkeypatch):
    data_yf.clear_currency_cache()

    class FastInfoBroken:
        def get(self, key):
            raise RuntimeError("no get")

        def __getitem__(self, key):
            raise RuntimeError("no item")

    class FakeTicker:
        fast_info = FastInfoBroken()

        def __init__(self, symbol):
            pass

        def get_info(self):
            return {"currency": None}

    monkeypatch.setattr(data_yf.yf, "Ticker", FakeTicker)
    assert data_yf.fetch_symbol_currency("MISS") is None


def test_fetch_yfinance_history_bulk_returns_empty_for_no_symbols():
    assert data_yf.fetch_yfinance_history_bulk([], interval="1d", lookback_days=5) == {}


def test_fetch_yfinance_history_bulk_logs_chunk_level_issue_for_missing_symbols(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(data_yf, "_classify_yf_download_issue", lambda message: "connectivity issue")
    monkeypatch.setattr(data_yf, "_run_yf_download", lambda *args, **kwargs: (pd.DataFrame(), "connectivity issue", None))

    out = data_yf.fetch_yfinance_history_bulk(
        ["AAA", "BBB"],
        interval="1d",
        lookback_days=5,
        chunk_size=2,
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
    )

    assert out == {}
    log_text = (tmp_path / "log.csv").read_text(encoding="utf-8")
    assert "AAA,connectivity issue,1" in log_text
    assert "BBB,connectivity issue,1" in log_text


def test_fetch_yfinance_history_bulk_logs_unclassified_exception_for_chunk(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(data_yf, "_run_yf_download", lambda *args, **kwargs: (None, "", RuntimeError("boom")))

    out = data_yf.fetch_yfinance_history_bulk(
        ["AAA"],
        interval="1d",
        lookback_days=5,
        chunk_size=1,
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
        max_retries=0,
    )

    assert out == {}
    assert "AAA,boom,1" in (tmp_path / "log.csv").read_text(encoding="utf-8")


def test_read_parquet_if_exists_returns_frame_or_none(tmp_path: Path):
    path = tmp_path / "data.parquet"
    pl.DataFrame({"x": [1]}).write_parquet(path)

    assert data_yf.read_parquet_if_exists(path).to_dict(as_series=False) == {"x": [1]}
    assert data_yf.read_parquet_if_exists(tmp_path / "missing.parquet") is None


def test_fetch_yfinance_history_bulk_share_class_fallback_and_single_issue(monkeypatch, tmp_path: Path):
    pdf = pd.DataFrame(
        {
            "Date": [datetime(2026, 1, 1)],
            "Open": [1.0],
            "High": [2.0],
            "Low": [0.5],
            "Close": [1.5],
            "Adj Close": [1.4],
            "Volume": [100],
        }
    )
    calls: list[object] = []

    def fake_run(download_fn, symbols, **kwargs):
        calls.append(symbols)
        if symbols == ["BRK.B", "FAIL.B"]:
            return pd.DataFrame(), "", None
        if symbols == "BRK-B":
            return pdf, "", None
        if symbols == "FAIL-B":
            return pd.DataFrame(), "possibly delisted", None
        raise AssertionError(symbols)

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)

    out = data_yf.fetch_yfinance_history_bulk(
        ["BRK.B", "FAIL.B"],
        interval="1d",
        lookback_days=5,
        chunk_size=2,
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
    )

    assert out["BRK.B"].height == 1
    assert "FAIL.B" not in out
    assert "possibly delisted" in (tmp_path / "log.csv").read_text(encoding="utf-8")


def test_fetch_yfinance_history_bulk_ignores_fallback_exception(monkeypatch):
    def fake_run(download_fn, symbols, **kwargs):
        if symbols == ["BRK.B"]:
            return pd.DataFrame(), "", None
        raise RuntimeError("single fallback boom")

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)

    assert data_yf.fetch_yfinance_history_bulk(["BRK.B"], interval="1d", lookback_days=5, chunk_size=1, sleep_seconds=0) == {}


def test_fetch_yfinance_history_bulk_ignores_returned_single_fallback_exception(monkeypatch):
    def fake_run(download_fn, symbols, **kwargs):
        if symbols == ["BRK.B"]:
            return pd.DataFrame(), "", None
        return None, "", RuntimeError("single tuple boom")

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)

    assert data_yf.fetch_yfinance_history_bulk(["BRK.B"], interval="1d", lookback_days=5, chunk_size=1, sleep_seconds=0) == {}


def test_fetch_yfinance_history_bulk_retries_rate_limit_then_logs(monkeypatch, tmp_path: Path):
    calls = 0
    sleeps: list[int] = []

    def fake_run(*args, **kwargs):
        nonlocal calls
        calls += 1
        return None, "", RuntimeError("429")

    monkeypatch.setattr(data_yf, "_run_yf_download", fake_run)
    monkeypatch.setattr(data_yf, "_backoff_sleep", lambda attempt, maximum: sleeps.append(attempt))

    out = data_yf.fetch_yfinance_history_bulk(
        ["AAA"],
        interval="1d",
        lookback_days=5,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        log_path=tmp_path / "log.csv",
    )

    assert out == {}
    assert calls == 2
    assert sleeps == [1]
    assert "429" in (tmp_path / "log.csv").read_text(encoding="utf-8")


def test_fetch_yfinance_history_bulk_logs_issue_for_symbols_still_missing_after_partial_success(monkeypatch, tmp_path: Path):
    cols = pd.MultiIndex.from_product([["AAA"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]])
    pdf = pd.DataFrame([[1.0, 2.0, 0.5, 1.5, 1.4, 100]], columns=cols)

    monkeypatch.setattr(data_yf, "_classify_yf_download_issue", lambda message: "partial issue")
    monkeypatch.setattr(data_yf, "_run_yf_download", lambda *args, **kwargs: (pdf, "partial issue", None))

    out = data_yf.fetch_yfinance_history_bulk(
        ["AAA", "BBB"],
        interval="1d",
        lookback_days=5,
        chunk_size=2,
        sleep_seconds=0,
        log_path=tmp_path / "log.csv",
    )

    assert set(out) == {"AAA"}
    assert "BBB,partial issue,1" in (tmp_path / "log.csv").read_text(encoding="utf-8")


def test_upsert_symbol_parquet_writes_initial_history_and_handles_empty_increment(monkeypatch, tmp_path: Path):
    root = tmp_path / "daily"
    fetched = pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1)],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "adj_close": [1.4],
            "volume": [100.0],
        }
    )
    monkeypatch.setattr(data_yf, "fetch_yfinance_history", lambda spec: fetched)

    with pytest.warns(DeprecationWarning):
        out_path = data_yf.upsert_symbol_parquet("AAA", "1d", 10, root)

    assert pl.read_parquet(out_path).height == 1

    monkeypatch.setattr(data_yf, "fetch_yfinance_history", lambda spec: pl.DataFrame(schema=data_yf.STANDARD_PRICE_SCHEMA))
    with pytest.warns(DeprecationWarning):
        assert data_yf.upsert_symbol_parquet("AAA", "1d", 10, root) == out_path


def test_upsert_symbol_parquet_returns_path_when_initial_fetch_is_empty(monkeypatch, tmp_path: Path):
    root = tmp_path / "daily"
    monkeypatch.setattr(data_yf, "fetch_yfinance_history", lambda spec: pl.DataFrame(schema=data_yf.STANDARD_PRICE_SCHEMA))

    with pytest.warns(DeprecationWarning):
        out_path = data_yf.upsert_symbol_parquet("AAA", "1d", 10, root)

    assert out_path == root / "AAA.parquet"
    assert not out_path.exists()


def test_append_update_log_throttled_with_zero_cooldown_always_writes(tmp_path: Path):
    log = tmp_path / "update_log.csv"

    assert data_yf.append_update_log_throttled(log, "AAPL", "issue_a", 1, cooldown_hours=0.0) is True
    assert data_yf.append_update_log_throttled(log, "AAPL", "issue_a", 2, cooldown_hours=-1.0) is True

    text = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(text) == 3


def test_warning_state_path_and_load_state_edge_cases(tmp_path: Path):
    log = tmp_path / "update_log.csv"
    explicit = tmp_path / "custom.json"

    assert data_yf._warning_state_path(log) == tmp_path / "update_log_warning_state.json"
    assert data_yf._warning_state_path(log, state_path=explicit) == explicit

    missing = tmp_path / "missing.json"
    empty = tmp_path / "empty.json"
    empty.write_text("", encoding="utf-8")
    bad = tmp_path / "bad.json"
    bad.write_text('["not-a-dict"]', encoding="utf-8")
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    mixed = tmp_path / "mixed.json"
    mixed.write_text(
        json.dumps(
            {
                "AAPL␟issue": "2026-01-01T00:00:00",
                "missing_separator": "2026-01-01T00:00:00+00:00",
                "␟issue": "2026-01-01T00:00:00+00:00",
                "AAPL␟": "2026-01-01T00:00:00+00:00",
                "MSFT␟issue": "not-a-date",
                "GOOG␟issue": "2026-01-01T00:00:00+01:00",
                "BADTYPE␟issue": 123,
            }
        ),
        encoding="utf-8",
    )

    assert data_yf._load_warning_state(missing) == {}
    assert data_yf._load_warning_state(empty) == {}
    assert data_yf._load_warning_state(bad) == {}
    assert data_yf._load_warning_state(malformed) == {}

    loaded = data_yf._load_warning_state(mixed)
    assert ("AAPL", "issue") in loaded
    assert loaded[("AAPL", "issue")].tzinfo is not None
    assert ("GOOG", "issue") in loaded


def test_warning_state_helpers_handle_invalid_and_naive_timestamps(tmp_path: Path):
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps({"AAA␟issue": "2026-01-01T00:00:00", "bad": "2026-01-01T00:00:00", "BBB␟issue": "not-a-date"}),
        encoding="utf-8",
    )

    loaded = data_yf._load_warning_state(state)

    assert list(loaded) == [("AAA", "issue")]
    assert loaded[("AAA", "issue")].tzinfo is not None
