from __future__ import annotations

import json
from pathlib import Path

import polars as pl

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
