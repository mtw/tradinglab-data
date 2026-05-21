from __future__ import annotations

import contextlib
import io
import sys
from pathlib import Path

import polars as pl

from tests._load import load_script_module

mod = load_script_module("bootstrap_stooq_history")


def test_symbols_from_universe_csv_filters_inactive_and_blank(tmp_path: Path):
    path = tmp_path / "sp500.csv"
    path.write_text(
        "symbol,active\nAAPL,1\nMSFT,0\n,1\nBAD SYMBOL,1\nTSLA,1\n",
        encoding="utf-8",
    )

    symbols = mod._symbols_from_universe_csv(path)

    assert symbols == ["AAPL", "TSLA"]


def test_main_bootstraps_stooq_history(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "universes"
    parquet_root = tmp_path / "daily"
    meta_root = tmp_path / "meta"
    log_path = meta_root / "update_log.csv"
    universe_dir.mkdir(parents=True)
    parquet_root.mkdir(parents=True)
    meta_root.mkdir(parents=True)
    (universe_dir / "sp500.csv").write_text("symbol,active\nAAPL,1\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'merged.csv'}",
                f"  universe_dir: {universe_dir}",
                f"  parquet_root: {parquet_root}",
                f"  update_log_csv: {log_path}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    df_hist = pl.DataFrame(
        {
            "date": ["2026-01-01"],
            "open": [10.0],
            "high": [11.0],
            "low": [9.5],
            "close": [10.5],
            "adj_close": [10.5],
            "volume": [1000.0],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))

    monkeypatch.setattr(mod, "fetch_stooq_history", lambda spec: df_hist)
    monkeypatch.setattr(mod, "infer_currency_from_symbol", lambda symbol: "USD")
    monkeypatch.setattr(sys, "argv", ["bootstrap_stooq_history.py", "--config", str(config_path), "--skip-yf-recent"])

    mod.main()

    out_path = parquet_root / "AAPL.parquet"
    assert out_path.exists()
    loaded = pl.read_parquet(out_path)
    assert loaded.height == 1
    assert loaded.get_column("currency").to_list() == ["USD"]


def test_main_lists_universes_and_exits(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "universes"
    crypto_universe_dir = tmp_path / "meta" / "crypto" / "universes"
    universe_dir.mkdir(parents=True)
    crypto_universe_dir.mkdir(parents=True)
    (universe_dir / "sp500.csv").write_text("symbol,active\nAAPL,1\n", encoding="utf-8")
    (crypto_universe_dir / "crypto_dynamic.json").write_text(
        '{\n  "universe": "crypto_dynamic",\n  "symbols": ["BTC_USDT"]\n}\n',
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                f"  crypto_universe_dir: {crypto_universe_dir}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  update_log_csv: {tmp_path / 'meta' / 'update_log.csv'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv", ["bootstrap_stooq_history.py", "--config", str(config_path), "--list-universes"])

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        mod.main()

    printed = out.getvalue()
    assert "sp500: 1 symbols" in printed
    assert "crypto_dynamic: 1 symbols" in printed
