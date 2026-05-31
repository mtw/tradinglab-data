from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data.consistency_report as report_mod
from tradinglab_data.config import Config
from tradinglab_data.consistency_report import (
    generate_universe_consistency_report,
    render_universe_consistency_json,
    render_universe_consistency_markdown,
)


def _write_config(tmp_path: Path) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'universe.csv'}",
                f"  universe_dir: {tmp_path / 'meta' / 'universes'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  crypto_root: {tmp_path / 'crypto'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "extended_hours:",
                f"  intraday_root: {tmp_path / 'intraday'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path


def _write_equity_universe(tmp_path: Path) -> None:
    meta = tmp_path / "meta"
    (meta / "universes").mkdir(parents=True, exist_ok=True)
    (meta / "universe.csv").write_text(
        "\n".join(
            [
                "symbol,name,instrument_type,asset_class,active",
                "AAA,Alpha Corp,stock,equity,1",
                "BBB,Beta ETF,etf,equity,1",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_generate_daily_consistency_report_filters_stock_rows(tmp_path: Path):
    _write_equity_universe(tmp_path)
    config_path = _write_config(tmp_path)
    daily_root = tmp_path / "daily"
    daily_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-04-18", "2026-04-21"],
            "open": [10.0, 10.5],
            "high": [10.4, 10.8],
            "low": [9.8, 10.2],
            "close": [10.2, 10.7],
            "adj_close": [10.2, 10.7],
            "volume": [1000.0, 1200.0],
            "currency": ["USD", "USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(daily_root / "AAA.parquet")

    cfg = Config.load(config_path)
    report = generate_universe_consistency_report(cfg, dataset="daily", instrument_type="stock")

    assert report.height == 1
    row = report.row(0, named=True)
    assert row["symbol"] == "AAA"
    assert row["status"] == "ok"
    assert row["rows"] == 2
    assert row["start"].startswith("2026-04-18")
    assert row["end"].startswith("2026-04-21")


def test_generate_intraday_consistency_report_marks_missing_file(tmp_path: Path):
    _write_equity_universe(tmp_path)
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)

    report = generate_universe_consistency_report(
        cfg,
        dataset="intraday",
        interval="5m",
        instrument_type="etf",
    )

    assert report.height == 1
    row = report.row(0, named=True)
    assert row["symbol"] == "BBB"
    assert row["status"] == "missing"
    assert "missing_file" in row["issues"]


def test_generate_crypto_consistency_report_marks_dirty_and_renders(tmp_path: Path):
    config_path = _write_config(tmp_path)
    crypto_root = tmp_path / "crypto" / "binance" / "spot" / "1h"
    crypto_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "timestamp": ["2026-04-21T10:00:00", "2026-04-21T09:00:00"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10.0, 12.0],
            "provider": ["ccxt", "ccxt"],
            "exchange": ["binance", "binance"],
            "market_type": ["spot", "spot"],
            "symbol": ["BTC_USDT", "BTC_USDT"],
            "base_asset": ["BTC", "BTC"],
            "quote_asset": ["USDT", "USDT"],
            "interval": ["1h", "1h"],
            "is_closed": [True, True],
            "ingested_at": ["2026-04-21T12:00:00", "2026-04-21T12:00:00"],
            "source_symbol": ["BTC/USDT", "BTC/USDT"],
        }
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(crypto_root / "BTC_USDT.parquet")

    cfg = Config.load(config_path)
    report = generate_universe_consistency_report(
        cfg,
        dataset="crypto",
        interval="1h",
        symbols_override=["BTC_USDT", "ETH_USDT"],
    )

    rows = {row["symbol"]: row for row in report.iter_rows(named=True)}
    assert rows["BTC_USDT"]["status"] == "dirty"
    assert "unsorted_rows" in rows["BTC_USDT"]["issues"]
    assert rows["ETH_USDT"]["status"] == "missing"

    markdown = render_universe_consistency_markdown(report, dataset="crypto", interval="1h", universe="crypto_core")
    payload = json.loads(render_universe_consistency_json(report))
    assert "| BTC_USDT | dirty |" in markdown
    assert payload[0]["dataset"] == "crypto"


def test_consistency_report_rejects_missing_interval_and_unsupported_dataset(tmp_path: Path):
    _write_equity_universe(tmp_path)
    cfg = Config.load(_write_config(tmp_path))

    with pytest.raises(ValueError, match="--interval is required for intraday"):
        generate_universe_consistency_report(cfg, dataset="intraday")
    with pytest.raises(ValueError, match="--interval is required for crypto"):
        generate_universe_consistency_report(cfg, dataset="crypto")
    with pytest.raises(ValueError, match="Unsupported dataset"):
        generate_universe_consistency_report(cfg, dataset="other")


def test_consistency_report_helpers_cover_empty_rows_and_edge_cases(tmp_path: Path, monkeypatch):
    _write_equity_universe(tmp_path)
    cfg = Config.load(_write_config(tmp_path))

    override_frame = report_mod._load_equity_universe(cfg, instrument_type="stock", symbols_override=[" aaa ", "BBB"])
    assert override_frame.get_column("symbol").to_list() == ["AAA", "BBB"]

    monkeypatch.setattr(report_mod, "load_universe_frame", lambda *args, **kwargs: pl.DataFrame({"symbol": ["AAA"], "instrument_type": ["stock"]}))
    loaded = report_mod._load_equity_universe(cfg, instrument_type="stock", symbols_override=None)
    assert loaded.columns == ["symbol", "instrument_type", "name", "asset_class"]

    empty_md = render_universe_consistency_markdown(pl.DataFrame(schema={column: pl.String for column in ["dataset", "symbol", "status", "rows", "start", "end", "issues", "path"]}), dataset="daily", universe="u", instrument_type="stock")
    assert "scope: `daily universe=u instrument_type=stock`" in empty_md

    row = report_mod._report_row(
        dataset="daily",
        interval=None,
        symbol="AAA",
        name="Alpha",
        instrument_type="stock",
        asset_class="equity",
        exists=True,
        status="dirty",
        rows=1,
        start="s",
        end="e",
        schema_ok=False,
        sorted_ok=False,
        duplicate_rows=1,
        null_ohlc_rows=1,
        bad_ohlc_rows=1,
        stale=True,
        issues=["b", "a", "a"],
        path=tmp_path / "AAA.parquet",
    )
    assert row["issues"] == "a,b"
    assert report_mod._rows_to_frame([]).is_empty()
    assert report_mod._duplicate_count(pl.DataFrame(), time_column="date") == 0
    assert report_mod._null_ohlc_count(pl.DataFrame({"x": [1]})) == 0
    assert report_mod._bad_ohlc_count(pl.DataFrame({"x": [1]})) == 0
    assert report_mod._sorted_times(pl.DataFrame(), time_column="date") is False
    assert report_mod._time_bounds(pl.DataFrame(), time_column="date") == (None, None, None)
    assert report_mod._fmt_datetime(None) is None
    assert report_mod._fmt_datetime("x") == "x"
    partial = report_mod._rows_to_frame([{"dataset": "daily", "symbol": "AAA"}])
    assert "issues" in partial.columns
    assert partial.get_column("symbol").to_list() == ["AAA"]


def test_inspect_ohlc_path_covers_zero_byte_read_error_schema_and_sorted_fallback(tmp_path: Path, monkeypatch):
    missing = report_mod._inspect_ohlc_path(
        tmp_path / "missing.parquet",
        dataset="daily",
        interval=None,
        symbol="AAA",
        name="Alpha",
        instrument_type="stock",
        asset_class="equity",
        time_column="date",
        validator=lambda frame: None,
    )
    assert missing["status"] == "missing"

    zero = tmp_path / "zero.parquet"
    zero.write_bytes(b"")
    zero_issue = report_mod._inspect_ohlc_path(
        zero,
        dataset="daily",
        interval=None,
        symbol="AAA",
        name="Alpha",
        instrument_type="stock",
        asset_class="equity",
        time_column="date",
        validator=lambda frame: None,
    )
    assert "read_error" in zero_issue["issues"]

    bad = tmp_path / "bad.parquet"
    pl.DataFrame({"date": [datetime(2026, 1, 2)], "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "adj_close": [1.5], "volume": [1.0], "currency": ["USD"]}).write_parquet(bad)
    monkeypatch.setattr(report_mod, "_sorted_times", lambda *args, **kwargs: False)
    dirty = report_mod._inspect_ohlc_path(
        bad,
        dataset="daily",
        interval=None,
        symbol="AAA",
        name="Alpha",
        instrument_type="stock",
        asset_class="equity",
        time_column="date",
        validator=lambda frame: (_ for _ in ()).throw(ValueError("bad schema")),
        stale_check=lambda value: True,
    )
    assert dirty["status"] == "dirty"
    assert "schema_mismatch" in dirty["issues"]
    assert "unsorted_rows" in dirty["issues"]
    assert "stale" in dirty["issues"]


def test_inspect_ohlc_path_flags_empty_duplicate_null_bad_and_sorted_exception(tmp_path: Path, monkeypatch):
    empty = tmp_path / "empty.parquet"
    pl.DataFrame(
        {
            "date": pl.Series([], dtype=pl.Datetime),
            "open": pl.Series([], dtype=pl.Float64),
            "high": pl.Series([], dtype=pl.Float64),
            "low": pl.Series([], dtype=pl.Float64),
            "close": pl.Series([], dtype=pl.Float64),
            "adj_close": pl.Series([], dtype=pl.Float64),
            "volume": pl.Series([], dtype=pl.Float64),
            "currency": pl.Series([], dtype=pl.String),
        }
    ).write_parquet(empty)
    empty_issue = report_mod._inspect_ohlc_path(
        empty,
        dataset="daily",
        interval=None,
        symbol="EMPTY",
        name="Empty",
        instrument_type="stock",
        asset_class="equity",
        time_column="date",
        validator=lambda frame: None,
    )
    assert "empty_file" in empty_issue["issues"]

    dirty_path = tmp_path / "dirty.parquet"
    pl.DataFrame(
        {
            "date": [datetime(2026, 1, 2), datetime(2026, 1, 2)],
            "open": [1.0, None],
            "high": [0.5, -1.0],
            "low": [2.0, 0.1],
            "close": [1.5, 0.2],
            "adj_close": [1.5, 0.2],
            "volume": [1.0, 2.0],
            "currency": ["USD", "USD"],
        }
    ).write_parquet(dirty_path)
    monkeypatch.setattr(pl.Series, "is_sorted", lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
    issue = report_mod._inspect_ohlc_path(
        dirty_path,
        dataset="daily",
        interval=None,
        symbol="DIRTY",
        name="Dirty",
        instrument_type="stock",
        asset_class="equity",
        time_column="date",
        validator=lambda frame: None,
    )
    assert "duplicate_rows" in issue["issues"]
    assert "null_ohlc_rows" in issue["issues"]
    assert "bad_ohlc_rows" in issue["issues"]
    assert "unsorted_rows" in issue["issues"]
