from __future__ import annotations

import runpy
import sys
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data.cli as cli


def test_cli_schema_does_not_require_config(capsys):
    rc = cli.main(["schema", "--format", "json"])
    out = capsys.readouterr().out
    assert rc == 0
    assert '"daily"' in out


def test_cli_schema_writes_output(tmp_path: Path):
    out = tmp_path / "schema.md"
    rc = cli.main(["schema", "--format", "markdown", "--out", str(out)])
    assert rc == 0
    assert out.exists()
    assert "Data Parquet Schema" in out.read_text(encoding="utf-8")


def test_cli_update_missing_config_has_clear_error():
    with pytest.raises(FileNotFoundError, match="Create a config from"):
        cli.main(["--config", "does-not-exist.yaml", "update"])


def test_cli_report_parquet_store_writes_outputs(tmp_path: Path, capsys):
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday"
    runs_root = tmp_path / "runs"
    universe_dir = tmp_path / "meta" / "universes"
    universe_csv = tmp_path / "meta" / "merged.csv"
    universe_dir.mkdir(parents=True, exist_ok=True)
    universe_csv.parent.mkdir(parents=True, exist_ok=True)
    universe_csv.write_text("symbol\nAAA\n", encoding="utf-8")
    (universe_dir / "sp500.csv").write_text("symbol\nAAA\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {universe_csv}",
                f"  parquet_root: {daily_root}",
                f"  runs_root: {runs_root}",
                f"  universe_dir: {universe_dir}",
                "extended_hours:",
                f"  intraday_root: {intraday_root}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    daily_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "adj_close": [10.2],
            "volume": [1000.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(daily_root / "AAA.parquet")

    out_dir = tmp_path / "integrity"
    rc = cli.main(["--config", str(config_path), "report-parquet-store", "--out-dir", str(out_dir)])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[PARQUET_STORE_REPORT]" in out
    assert (out_dir / "parquet_store_report.json").exists()
    assert (out_dir / "parquet_store_report.md").exists()


def test_cli_report_universe_consistency_writes_markdown(tmp_path: Path):
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday"
    runs_root = tmp_path / "runs"
    universe_dir = tmp_path / "meta" / "universes"
    universe_csv = tmp_path / "meta" / "merged.csv"
    universe_dir.mkdir(parents=True, exist_ok=True)
    universe_csv.parent.mkdir(parents=True, exist_ok=True)
    universe_csv.write_text("symbol,name,instrument_type,asset_class,active\nAAA,Alpha,stock,equity,1\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {universe_csv}",
                f"  parquet_root: {daily_root}",
                f"  runs_root: {runs_root}",
                f"  universe_dir: {universe_dir}",
                "extended_hours:",
                f"  intraday_root: {intraday_root}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    daily_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "adj_close": [10.2],
            "volume": [1000.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(daily_root / "AAA.parquet")

    out_path = tmp_path / "consistency.md"
    rc = cli.main(
        [
            "--config",
            str(config_path),
            "report-universe-consistency",
            "--dataset",
            "daily",
            "--instrument-type",
            "stock",
            "--out",
            str(out_path),
        ]
    )

    assert rc == 0
    assert out_path.exists()
    assert "Universe Consistency Report" in out_path.read_text(encoding="utf-8")


def test_cli_backfill_extended_hours_dispatch(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'merged.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "extended_hours:",
                f"  intraday_root: {tmp_path / 'intraday'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "backfill_extended_hours_from_config",
        lambda cfg, interval, symbols_override=None: {"interval": interval, "written": 3, "symbols": 5, "root": "/tmp/intraday/5m"},
    )

    rc = cli.main(["--config", str(config_path), "backfill-extended-hours", "--interval", "5m"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[BACKFILL_EXTENDED_HOURS] interval=5m written=3 symbols=5 root=/tmp/intraday/5m" in out


def test_cli_intraday_update_dispatch(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'merged.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "intraday:",
                f"  research_root: {tmp_path / 'intraday_research'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "intraday_research_update_from_config",
        lambda cfg, universe=None, symbols_override=None, full_window=False: {
            "interval": "5m",
            "universe": universe or "intraday_pilot",
            "root": "/tmp/intraday_research/5m",
            "symbols": ["SPY", "QQQ"],
            "files_written": 2,
            "rows_written": 100,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )

    rc = cli.main(["--config", str(config_path), "intraday", "update", "--universe", "intraday_pilot"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[INTRADAY_UPDATE] interval=5m files_written=2 symbols=2 root=/tmp/intraday_research/5m universe=intraday_pilot" in out


def test_cli_intraday_live_update_dispatch(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'merged.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "intraday_live:",
                f"  live_root: {tmp_path / 'intraday_live'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "intraday_live_update_from_config",
        lambda cfg, universe=None, symbols_override=None, full_window=False: {
            "interval": "5m",
            "universe": universe or "intraday_live_core",
            "root": "/tmp/intraday_live/5m",
            "symbols": ["SPY", "QQQ"],
            "files_written": 2,
            "rows_written": 100,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )
    rc = cli.main(["--config", str(config_path), "intraday-live", "update", "--universe", "intraday_live_core"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "[INTRADAY_LIVE_UPDATE] interval=5m files_written=2 symbols=2 root=/tmp/intraday_live/5m universe=intraday_live_core" in out


def test_cli_intraday_sync_update_dispatch(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {tmp_path / 'meta' / 'merged.csv'}",
                f"  parquet_root: {tmp_path / 'daily'}",
                f"  runs_root: {tmp_path / 'runs'}",
                "intraday:",
                f"  research_root: {tmp_path / 'intraday_research'}",
                "intraday_live:",
                f"  live_root: {tmp_path / 'intraday_live'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        cli,
        "intraday_sync_from_config",
        lambda cfg, universe=None, symbols_override=None, full_window=False: {
            "interval": "5m",
            "universe": universe or "intraday_live_core",
            "symbols": ["SPY", "QQQ"],
            "fetched_symbols": 2,
            "live": {
                "interval": "5m",
                "universe": universe or "intraday_live_core",
                "root": "/tmp/intraday_live/5m",
                "symbols": ["SPY", "QQQ"],
                "files_written": 2,
                "rows_written": 100,
                "unchanged_symbols": [],
                "skipped_symbols": [],
            },
            "research": {
                "interval": "5m",
                "universe": universe or "intraday_live_core",
                "root": "/tmp/intraday_research/5m",
                "symbols": ["SPY", "QQQ"],
                "files_written": 2,
                "rows_written": 80,
                "unchanged_symbols": [],
                "skipped_symbols": [],
            },
        },
    )

    rc = cli.main(["--config", str(config_path), "intraday-sync", "update", "--universe", "intraday_live_core"])
    out = capsys.readouterr().out

    assert rc == 0
    assert "[INTRADAY_SYNC_UPDATE] interval=5m live_files_written=2 research_files_written=2 symbols=2 fetched_symbols=2 live_root=/tmp/intraday_live/5m research_root=/tmp/intraday_research/5m universe=intraday_live_core" in out


def test_cli_module_invokes_main_when_run_as_module(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["tradinglab-data", "schema", "--format", "json"])
    with pytest.raises(SystemExit, match="0"):
        runpy.run_module("tradinglab_data.cli", run_name="__main__")
