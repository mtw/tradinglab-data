from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data._intraday_fetch as intraday_fetch
import tradinglab_data.extended_hours_monitor as eh
from tradinglab_data.schema import MOVE_ALERT_FRAME_SCHEMA, validate_alerts_frame, validate_moves_frame


def test_compute_moves_vs_close_and_detect_alerts():
    df = pl.DataFrame(
        {
            "date": [datetime(2026, 2, 27, 14, 30), datetime(2026, 2, 27, 14, 35)],
            "open": [100.0, 101.0],
            "high": [101.0, 103.0],
            "low": [99.0, 100.5],
            "close": [101.0, 103.0],
            "adj_close": [101.0, 103.0],
            "volume": [1000.0, 2000.0],
            "currency": ["USD", "USD"],
        }
    )
    moves = eh.compute_moves_vs_close({"AAPL": df}, {"AAPL": {"close": 100.0, "currency": "USD"}})
    assert moves.height == 1
    assert moves.get_column("symbol").to_list()[0] == "AAPL"
    assert abs(float(moves.get_column("pct_move").to_list()[0]) - 3.0) < 1e-12

    alerts = eh.detect_alerts(moves, threshold=2.0, min_volume=1000.0)
    assert alerts.height == 1
    alerts2 = eh.detect_alerts(moves, threshold=4.0)
    assert alerts2.is_empty()


def test_persist_alerts_csv(tmp_path: Path):
    alerts = pl.DataFrame({"symbol": ["AAPL"], "pct_move": [2.5]})
    out = eh.persist_alerts(alerts, tmp_path / "alerts.csv")
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "symbol,pct_move" in text
    assert "AAPL,2.5" in text


def test_persist_extended_hours_report_html(tmp_path: Path):
    moves = pl.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "ref_close": [100.0, 200.0],
            "last_price": [103.0, 195.0],
            "pct_move": [3.0, -2.5],
            "last_volume": [1000.0, 2000.0],
            "currency": ["USD", "USD"],
            "last_ts": [datetime(2026, 2, 27, 21, 0), datetime(2026, 2, 27, 22, 0)],
            "session": ["post", "post"],
        }
    )
    alerts = moves.filter(pl.col("pct_move").abs() >= 2.0)
    out = eh.persist_extended_hours_report_html(moves, alerts, tmp_path / "report.html", threshold=2.0, top_n=10)
    text = out.read_text(encoding="utf-8")
    assert "Extended-Hours Gap Report" in text
    assert "AAPL" in text
    assert "MSFT" in text
    assert "sessionFilter" in text
    assert 'data-table="moves"' in text


def test_summarize_gap_report_session_filter():
    moves = pl.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "NVDA"],
            "pct_move": [3.0, -2.5, 1.1],
            "ref_close": [100.0, 200.0, 300.0],
            "last_price": [103.0, 195.0, 303.3],
            "last_volume": [1000.0, 2000.0, 3000.0],
            "session": ["post", "pre", "regular"],
        }
    )
    out = eh.summarize_gap_report(moves, threshold=2.0, top_n=10, session_filter="post")
    assert out.get_column("symbol").to_list() == ["AAPL"]


def test_empty_move_and_alert_frames_follow_contract_schema():
    moves = eh.compute_moves_vs_close({}, {})
    alerts = eh.detect_alerts(moves, threshold=2.0)
    summary = eh.summarize_gap_report(moves, threshold=2.0)

    assert moves.columns == list(MOVE_ALERT_FRAME_SCHEMA)
    assert alerts.columns == list(MOVE_ALERT_FRAME_SCHEMA)
    assert summary.columns == list(MOVE_ALERT_FRAME_SCHEMA)
    validate_moves_frame(moves)
    validate_alerts_frame(alerts)
    validate_moves_frame(summary)


def test_update_extended_hours_store_writes_preferred_and_fallback(tmp_path: Path, monkeypatch):
    now = datetime.now().replace(microsecond=0)
    base_df = pl.DataFrame(
        {
            "date": [now - timedelta(minutes=5), now],
            "open": [100.0, 101.0],
            "high": [101.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 104.0],
            "adj_close": [101.0, 104.0],
            "volume": [100.0, 200.0],
        }
    )

    def _fake_fetch(symbols, interval, period, **kwargs):
        out = {}
        for s in symbols:
            if s == "AAA" and interval == "5m":
                out[s] = base_df
            elif s == "BBB" and interval == "1m":
                out[s] = base_df.with_columns(pl.lit(300.0).alias("volume"))
        return out

    monkeypatch.setattr(eh, "fetch_extended_intraday", _fake_fetch)
    monkeypatch.setattr(eh, "fetch_symbol_currency", lambda symbol: "USD")

    daily_root = tmp_path / "daily"
    daily_root.mkdir(parents=True, exist_ok=True)
    for sym, close in {"AAA": 100.0, "BBB": 100.0}.items():
        pl.DataFrame(
            {
                "date": [now - timedelta(days=1)],
                "open": [close],
                "high": [close],
                "low": [close],
                "close": [close],
                "adj_close": [close],
                "volume": [1000.0],
                "currency": ["USD"],
            }
        ).write_parquet(str(daily_root / f"{sym}.parquet"))

    out = eh.update_extended_hours_store(
        symbols=["AAA", "BBB"],
        intraday_root=tmp_path / "intraday",
        daily_root=daily_root,
        preferred_interval="5m",
        fallback_interval="1m",
        retention_days=10,
        pct_move_threshold=2.0,
        alerts_path=tmp_path / "alerts.csv",
    )
    assert (tmp_path / "intraday" / "5m" / "AAA.parquet").exists()
    assert (tmp_path / "intraday" / "1m" / "BBB.parquet").exists()
    assert out["alerts"] == 2
    assert Path(out["alerts_path"]).exists()


def test_update_extended_hours_store_sanitizes_existing_null_rows_without_new_data(tmp_path: Path, monkeypatch):
    now = datetime.now().replace(microsecond=0)
    intraday_root = tmp_path / "intraday"
    pref_dir = intraday_root / "5m"
    pref_dir.mkdir(parents=True, exist_ok=True)
    bad_path = pref_dir / "AAA.parquet"
    pl.DataFrame(
        {
            "date": [now - timedelta(minutes=5), now],
            "open": [100.0, None],
            "high": [101.0, None],
            "low": [99.0, None],
            "close": [100.5, None],
            "adj_close": [100.5, None],
            "volume": [100.0, None],
            "currency": ["USD", "USD"],
        }
    ).write_parquet(str(bad_path))

    monkeypatch.setattr(eh, "fetch_extended_intraday", lambda *args, **kwargs: {})
    monkeypatch.setattr(eh, "fetch_symbol_currency", lambda symbol: "USD")

    daily_root = tmp_path / "daily"
    daily_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": [now - timedelta(days=1)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.0],
            "adj_close": [100.0],
            "volume": [1000.0],
            "currency": ["USD"],
        }
    ).write_parquet(str(daily_root / "AAA.parquet"))

    out = eh.update_extended_hours_store(
        symbols=["AAA"],
        intraday_root=intraday_root,
        daily_root=daily_root,
        preferred_interval="5m",
        fallback_interval="1m",
        retention_days=10,
        pct_move_threshold=2.0,
    )
    repaired = pl.read_parquet(str(bad_path)).sort("date")
    assert repaired.height == 1
    assert out["preferred_written"] == 1


def test_update_intraday_interval_is_testable_in_isolation(tmp_path: Path):
    now = datetime.now().replace(microsecond=0)
    out_dir = tmp_path / "intraday" / "5m"
    out_dir.mkdir(parents=True, exist_ok=True)
    fetched = pl.DataFrame(
        {
            "date": [now - timedelta(minutes=5), now],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "adj_close": [100.5, 101.5],
            "volume": [100.0, 200.0],
        }
    )

    resolved = eh._update_intraday_interval(
        ["AAA"],
        "5m",
        "10d",
        out_dir,
        retention_days=10,
        prepost=True,
        chunk_size=20,
        sleep_seconds=0.0,
        max_retries=1,
        backoff_max_seconds=1.0,
        threads=False,
        log_path=None,
        fetch_intraday_fn=lambda **kwargs: {"AAA": fetched},
        read_frame_fn=lambda path: None,
        fetch_currency_fn=lambda symbol: "USD",
    )

    stored = pl.read_parquet(out_dir / "AAA.parquet").sort("date")
    assert resolved == ["AAA"]
    assert stored.height == 2
    assert stored.get_column("currency").to_list() == ["USD", "USD"]


def test_update_extended_hours_store_rejects_unsupported_interval(tmp_path: Path):
    daily_root = tmp_path / "daily"
    daily_root.mkdir(parents=True, exist_ok=True)

    with pytest.raises(ValueError, match="Unsupported intraday interval"):
        eh.update_extended_hours_store(
            symbols=["AAA"],
            intraday_root=tmp_path / "intraday",
            daily_root=daily_root,
            preferred_interval="15m",
            fallback_interval="1m",
            retention_days=10,
            pct_move_threshold=2.0,
        )


def test_fetch_intraday_bulk_classifies_dns_failure_without_delisted_noise(monkeypatch, tmp_path: Path, capsys):
    log_path = tmp_path / "update_log.csv"

    def fake_download(*args, **kwargs):
        print(
            "Failed to get ticker 'IEF' reason: Failed to perform, curl: (6) Could not resolve host: guce.yahoo.com.",
            file=sys.stderr,
        )
        print("$IEF: possibly delisted; no timezone found", file=sys.stderr)
        print("\n1 Failed download:\n['IEF']: possibly delisted; no timezone found", file=sys.stderr)
        return None

    monkeypatch.setattr(intraday_fetch.yf, "download", fake_download)

    out = intraday_fetch.fetch_intraday_bulk(
        ["IEF"],
        interval="5m",
        period="10d",
        sleep_seconds=0.0,
        log_path=log_path,
    )

    captured = capsys.readouterr()
    assert out == {}
    assert captured.out == ""
    assert captured.err == ""
    log_text = log_path.read_text(encoding="utf-8")
    assert "intraday_5m_yahoo_connectivity_error: could not resolve host guce.yahoo.com" in log_text
    assert "possibly delisted" not in log_text
