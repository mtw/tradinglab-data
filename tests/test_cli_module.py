from __future__ import annotations

import argparse
import runpy
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data.cli as cli


def test_cli_helper_functions_cover_display_split_and_markdown():
    assert cli._display_value(None) == "-"
    assert cli._display_value(b"abc") == "abc"
    assert cli._split_cli_values(None) is None
    assert cli._split_cli_values(["a,b", " c "], uppercase=True) == ["A", "B", "C"]

    text = cli._render_symbol_master_markdown(
        pl.DataFrame(
            {
                "symbol": ["AAPL"],
                "exchange": ["NASDAQ"],
                "country": ["US"],
                "asset_currency": ["USD"],
                "fx_pair_to_base": ["USDEUR"],
                "metadata_source": ["universe"],
                "metadata_quality": ["complete"],
            }
        ),
        exchange="nasdaq",
        fx_pair="usdeur",
        issues="defaulted",
    )
    assert "scope: `exchange=NASDAQ, fx_pair=USDEUR, issues=defaulted`" in text


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
    existing_module = sys.modules.pop("tradinglab_data.cli", None)
    with pytest.raises(SystemExit, match="0"):
        try:
            runpy.run_module("tradinglab_data.cli", run_name="__main__")
        finally:
            if existing_module is not None:
                sys.modules["tradinglab_data.cli"] = existing_module


def test_cli_build_symbol_master_writes_output(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    universe_csv = meta / "merged.csv"
    universe_csv.parent.mkdir(parents=True, exist_ok=True)
    universe_csv.write_text("symbol,exchange,country,active\nAAPL,NASDAQ,US,1\n", encoding="utf-8")
    exchange_defaults = meta / "exchange_defaults.csv"
    exchange_defaults.write_text(
        "exchange,country,default_asset_currency,default_tax_country,default_lot_size,default_price_multiplier,default_asset_class\n"
        "NASDAQ,US,USD,US,1,1,stock\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: {universe_csv}",
                f"  exchange_defaults_csv: {exchange_defaults}",
                f"  symbol_master_csv: {meta / 'symbol_master.csv'}",
                f"  store_root: {tmp_path / 'store'}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    rc = cli.main(["--config", str(config_path), "build-symbol-master", "--base-currency", "EUR"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "[BUILD_SYMBOL_MASTER]" in out
    assert (meta / "symbol_master.csv").exists()


def test_cli_validate_symbol_master_returns_zero_for_valid_file(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    rc = cli.main(["--config", str(config_path), "validate-symbol-master", "--path", str(path)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "[VALIDATE_SYMBOL_MASTER]" in out


def test_cli_validate_symbol_master_returns_nonzero_for_invalid_file(tmp_path: Path):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text("symbol,exchange\nAAPL,NASDAQ\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        cli.main(["--config", str(config_path), "validate-symbol-master", "--path", str(path), "--strict"])


def test_cli_validate_symbol_master_non_strict_tolerates_invalid_file(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text("symbol,exchange\nAAPL,NASDAQ\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")

    rc = cli.main(["--config", str(config_path), "validate-symbol-master", "--path", str(path)])

    out = capsys.readouterr().out
    assert rc == 0
    assert "[VALIDATE_SYMBOL_MASTER]" in out


def test_cli_fx_validate_checks_existing_pair(tmp_path: Path, capsys):
    fx_root = tmp_path / "store" / "parquet" / "fx_daily"
    fx_root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [0.92],
            "high": [0.93],
            "low": [0.91],
            "close": [0.925],
            "provider": ["yahoo"],
            "pair": ["USDEUR"],
            "base_currency": ["USD"],
            "quote_currency": ["EUR"],
            "source_symbol": ["USDEUR=X"],
            "ingested_at": ["2026-03-27T20:01:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(fx_root / "USDEUR.parquet")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"paths:\n  store_root: {tmp_path / 'store'}\n  fx_daily_root: {fx_root}\n  runs_root: {tmp_path / 'runs'}\n",
        encoding="utf-8",
    )
    rc = cli.main(["--config", str(config_path), "fx-validate", "--pairs", "USDEUR"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "[FX_VALIDATE] pair=USDEUR ok=1" in out


def test_cli_fx_update_uses_pairs_from_symbol_master_when_pairs_omitted(monkeypatch, tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    symbol_master_csv = meta / "symbol_master.csv"
    symbol_master_csv.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n"
        "EBS.VI,VIE,AT,EUR,EUR,AT,stock,EUREUR,1,1\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"paths:\n  symbol_master_csv: {symbol_master_csv}\n  fx_daily_root: {tmp_path / 'fx_daily'}\n  runs_root: {tmp_path / 'runs'}\n",
        encoding="utf-8",
    )
    seen: list[str] = []

    def _sync(pair, root, *, provider="yahoo", allow_inverse=True, start=None, end=None):
        seen.append(pair)
        return {
            "pair": pair,
            "rows_written": 2,
            "path": str(Path(root) / f"{pair}.parquet"),
            "source_symbol": f"{pair}=X",
            "used_inverse": False,
        }

    monkeypatch.setattr(cli, "sync_fx_pair_yahoo", _sync)
    rc = cli.main(["--config", str(config_path), "fx-update"])
    out = capsys.readouterr().out
    assert rc == 0
    assert seen == ["USDEUR"]
    assert "[FX_UPDATE] pair=USDEUR" in out


def test_cli_fx_backfill_and_inspect(monkeypatch, tmp_path: Path, capsys):
    fx_root = tmp_path / "fx"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  fx_daily_root: {fx_root}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")

    monkeypatch.setattr(
        cli,
        "sync_fx_pair_yahoo",
        lambda pair, root, **kwargs: {
            "pair": pair.upper(),
            "rows_written": 2,
            "path": str(Path(root) / f"{pair.upper()}.parquet"),
            "source_symbol": f"{pair.upper()}=X",
            "used_inverse": False,
        },
    )
    monkeypatch.setattr(
        cli,
        "load_fx_pair",
        lambda root, pair, strict=False: pl.DataFrame(
            {
                "date": [datetime(2026, 3, 27), datetime(2026, 3, 28)],
                "close": [0.92, 0.93],
            }
        ),
    )

    assert cli.main(["--config", str(config_path), "fx-backfill", "--pairs", "USDEUR", "--provider", "yahoo"]) == 0
    assert cli.main(["--config", str(config_path), "fx-inspect", "--pairs", "USDEUR", "--tail", "1"]) == 0
    out = capsys.readouterr().out
    assert "[FX_SYNC] pair=USDEUR rows_written=2" in out
    assert "USDEUR rows=2 start=2026-03-27" in out


def test_cli_inspect_symbol_master_filters_and_renders_markdown(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier,metadata_source,metadata_quality\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1,universe,complete\n"
        "EBS.VI,VIE,AT,EUR,EUR,AT,stock,EUREUR,1,1,universe,defaulted_country\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    rc = cli.main(
        [
            "--config",
            str(config_path),
            "inspect-symbol-master",
            "--exchange",
            "VIE",
            "--issues",
            "defaulted_country",
        ]
    )
    out = capsys.readouterr().out
    assert rc == 0
    assert "Symbol Master Inspection" in out
    assert "EBS.VI" in out
    assert "AAPL" not in out


def test_cli_inspect_symbol_master_renders_json_and_csv(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier\n"
        "AAPL,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")

    json_out = tmp_path / "symbol_master.json"
    rc_json = cli.main(["--config", str(config_path), "inspect-symbol-master", "--format", "json", "--out", str(json_out)])
    rc_csv = cli.main(["--config", str(config_path), "inspect-symbol-master", "--format", "csv"])

    assert rc_json == 0
    assert '"symbol":"AAPL"' in json_out.read_text(encoding="utf-8")
    assert rc_csv == 0
    assert "symbol,exchange,country" in capsys.readouterr().out


def test_cli_inspect_symbol_master_limit_markdown_out_json_stdout_and_csv_out(tmp_path: Path, capsys):
    meta = tmp_path / "meta"
    path = meta / "symbol_master.csv"
    meta.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "symbol,exchange,country,asset_currency,base_listing_currency,tax_country,asset_class,fx_pair_to_base,lot_size,price_multiplier,metadata_source,metadata_quality\n"
        "B,NASDAQ,US,USD,USD,US,stock,USDEUR,1,1,universe,complete\n"
        "A,NYSE,US,USD,USD,US,stock,USDEUR,1,1,universe,complete\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  symbol_master_csv: {path}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")

    markdown_out = tmp_path / "inspect.md"
    csv_out = tmp_path / "inspect.csv"

    assert cli.main(["--config", str(config_path), "inspect-symbol-master", "--limit", "1", "--out", str(markdown_out)]) == 0
    assert cli.main(["--config", str(config_path), "inspect-symbol-master", "--format", "json"]) == 0
    assert cli.main(["--config", str(config_path), "inspect-symbol-master", "--format", "csv", "--out", str(csv_out)]) == 0

    assert markdown_out.read_text(encoding="utf-8").count("| B |") == 1
    assert "| A |" not in markdown_out.read_text(encoding="utf-8")
    assert '"symbol":"A"' in capsys.readouterr().out
    assert "symbol,exchange,country" in csv_out.read_text(encoding="utf-8")


def test_cli_market_data_sync_validate_and_inspect(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  store_root: {tmp_path / 'store'}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    calls: list[dict[str, object]] = []

    def _sync_market_data(*args, **kwargs):
        calls.append({"cmd": "sync", **kwargs})
        return {
            "ok": True,
            "market_caps": {"symbols_written": 1, "skipped": {}},
            "sectors": {"symbols_written": 1, "skipped": {}},
            "index_returns": {"index_ids_written": 1, "skipped": {}},
        }

    def _validate_market_data(*args, **kwargs):
        calls.append({"cmd": "validate", **kwargs})
        return {"ok": True}

    def _inspect_market_data(*args, **kwargs):
        calls.append({"cmd": "inspect", **kwargs})
        return [{"artifact": "market_cap", "id": "AAA", "exists": True, "rows": 2, "path": "/tmp/AAA.parquet"}]

    monkeypatch.setattr(cli, "sync_market_data_from_config", _sync_market_data)
    monkeypatch.setattr(cli, "validate_market_data_from_config", _validate_market_data)
    monkeypatch.setattr(cli, "inspect_market_data_from_config", _inspect_market_data)

    assert cli.main(["--config", str(config_path), "market-data", "sync", "--symbols", "AAPL,MSFT", "NVDA", "--index-ids", "spx,ndx", "rty"]) == 0
    assert cli.main(["--config", str(config_path), "market-data", "validate", "--symbols", "AAPL,MSFT", "NVDA", "--index-ids", "spx,ndx", "rty"]) == 0
    assert cli.main(["--config", str(config_path), "market-data", "inspect", "--symbols", "AAPL,MSFT", "NVDA", "--index-ids", "spx,ndx", "rty"]) == 0
    out = capsys.readouterr().out
    assert "[MARKET_DATA_SYNC] artifact=market_caps written=1 skipped=0" in out
    assert "[MARKET_DATA_VALIDATE] ok=1" in out
    assert "market_cap id=AAA exists=True rows=2" in out
    assert [call["symbols_override"] for call in calls] == [["AAPL", "MSFT", "NVDA"]] * 3
    assert [call["index_ids"] for call in calls] == [["SPX", "NDX", "RTY"]] * 3


def test_cli_market_data_validate_raises_aggregated_errors(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  store_root: {tmp_path / 'store'}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "validate_market_data_from_config",
        lambda *args, **kwargs: {
            "ok": False,
            "market_caps": {"errors": ["market cap failed"]},
            "sectors": {"errors": []},
            "index_returns": {"errors": ["index failed"]},
        },
    )

    with pytest.raises(SystemExit, match="market cap failed\nindex failed"):
        cli.main(["--config", str(config_path), "market-data", "validate"])


def test_cli_intraday_validate_and_inspect(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "intraday_research_validate_from_config", lambda *args, **kwargs: {"ok": True, "interval": "5m", "files_checked": 2, "root": "/tmp/research", "universe": "u"})
    monkeypatch.setattr(
        cli,
        "intraday_research_inspect_from_config",
        lambda *args, **kwargs: [{"symbol": "AAA", "exists": True, "rows": 2, "valid": True, "start": "2026-01-01", "end": "2026-01-02", "path": "/tmp/AAA.parquet"}],
    )

    assert cli.main(["--config", str(config_path), "intraday", "validate"]) == 0
    assert cli.main(["--config", str(config_path), "intraday", "inspect"]) == 0
    out = capsys.readouterr().out
    assert "[INTRADAY_VALIDATE] interval=5m files_checked=2" in out
    assert "AAA exists=True rows=2 valid=True" in out


def test_cli_intraday_backfill_and_validation_failure(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "intraday_research_update_from_config",
        lambda *args, **kwargs: {"interval": "5m", "files_written": 1, "symbols": ["AAA"], "root": "/tmp/research", "universe": "u"},
    )
    monkeypatch.setattr(cli, "intraday_research_validate_from_config", lambda *args, **kwargs: {"ok": False, "errors": ["bad research"]})

    assert cli.main(["--config", str(config_path), "intraday", "backfill"]) == 0
    with pytest.raises(SystemExit, match="bad research"):
        cli.main(["--config", str(config_path), "intraday", "validate"])


def test_cli_intraday_live_backfill_validate_and_inspect(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "intraday_live_update_from_config",
        lambda *args, **kwargs: {"interval": "5m", "files_written": 1, "symbols": ["AAA"], "root": "/tmp/live", "universe": "u"},
    )
    monkeypatch.setattr(cli, "intraday_live_validate_from_config", lambda *args, **kwargs: {"ok": True, "interval": "5m", "files_checked": 2, "root": "/tmp/live", "universe": "u"})
    monkeypatch.setattr(
        cli,
        "intraday_live_inspect_from_config",
        lambda *args, **kwargs: [{"symbol": "AAA", "exists": True, "rows": 2, "valid": True, "start": "2026-01-01", "end": "2026-01-02", "path": "/tmp/AAA.parquet"}],
    )

    assert cli.main(["--config", str(config_path), "intraday-live", "backfill"]) == 0
    assert cli.main(["--config", str(config_path), "intraday-live", "validate"]) == 0
    assert cli.main(["--config", str(config_path), "intraday-live", "inspect"]) == 0
    out = capsys.readouterr().out
    assert "[INTRADAY_LIVE_BACKFILL] interval=5m files_written=1" in out
    assert "[INTRADAY_LIVE_VALIDATE] interval=5m files_checked=2" in out
    assert "AAA exists=True rows=2 valid=True" in out


def test_cli_intraday_live_validate_failure(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "intraday_live_validate_from_config", lambda *args, **kwargs: {"ok": False, "errors": []})

    with pytest.raises(SystemExit, match="intraday live validation failed"):
        cli.main(["--config", str(config_path), "intraday-live", "validate"])


def test_cli_intraday_sync_backfill(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "intraday_sync_from_config",
        lambda *args, **kwargs: {
            "interval": "5m",
            "universe": "u",
            "symbols": ["AAA"],
            "fetched_symbols": 1,
            "live": {"files_written": 1, "root": "/tmp/live"},
            "research": {"files_written": 1, "root": "/tmp/research"},
        },
    )

    assert cli.main(["--config", str(config_path), "intraday-sync", "backfill"]) == 0
    assert "[INTRADAY_SYNC_BACKFILL] interval=5m live_files_written=1 research_files_written=1" in capsys.readouterr().out


def test_cli_crypto_commands_dispatch(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  store_root: {tmp_path / 'store'}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(cli, "crypto_list_symbols_from_config", lambda *args, **kwargs: ["BTC_USDT", "ETH_USDT"])
    monkeypatch.setattr(
        cli,
        "crypto_backfill_from_config",
        lambda *args, **kwargs: calls.append(("backfill", kwargs))
        or {
            "interval": kwargs["interval"],
            "symbols": kwargs.get("symbols_override") or ["BTC_USDT"],
            "files_written": 1,
            "root": "/tmp/crypto",
            "universe": kwargs.get("universe") or "crypto_majors",
        },
    )
    monkeypatch.setattr(cli, "crypto_validate_from_config", lambda *args, **kwargs: {"ok": True, "errors": []})
    monkeypatch.setattr(
        cli,
        "crypto_refresh_universe_from_config",
        lambda *args, **kwargs: {
            "provider": "coingecko",
            "universe": "crypto_dynamic",
            "symbols_selected": ["BTC_USDT"],
            "registry_path": "/tmp/registry.json",
            "universe_path": "/tmp/universe.json",
        },
    )
    monkeypatch.setattr(cli, "crypto_show_universe_from_config", lambda *args, **kwargs: ["BTC_USDT"])
    monkeypatch.setattr(
        cli,
        "crypto_diff_universe_from_config",
        lambda *args, **kwargs: {
            "left_universe": "left",
            "right_universe": "right",
            "left_only": ["BTC_USDT"],
            "right_only": ["ETH_USDT"],
            "shared": ["SOL_USDT"],
        },
    )
    monkeypatch.setattr(
        cli,
        "crypto_inspect_from_config",
        lambda *args, **kwargs: [{"symbol": "BTC_USDT", "exists": True, "rows": 2, "start": "2026-01-01", "end": "2026-01-02", "path": "/tmp/BTC_USDT.parquet"}],
    )
    monkeypatch.setattr(cli, "crypto_prune_from_config", lambda *args, **kwargs: ["/tmp/old.parquet"])

    assert cli.main(["--config", str(config_path), "crypto", "list-symbols"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "backfill", "--interval", "1h", "--symbols", "BTC_USDT"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "update", "--interval", "1h", "--symbols", "BTC_USDT"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "validate", "--interval", "1h", "--symbols", "BTC_USDT"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "refresh-universe", "--universe", "crypto_dynamic"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "show-universe"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "diff-universe", "--left-universe", "left", "--right-universe", "right"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "inspect", "--interval", "1h"]) == 0
    assert cli.main(["--config", str(config_path), "crypto", "prune", "--interval", "1h", "--apply"]) == 0

    out = capsys.readouterr().out
    assert "BTC_USDT" in out
    assert "[CRYPTO_REFRESH_UNIVERSE] provider=coingecko universe=crypto_dynamic symbols=1" in out
    assert "[CRYPTO_DIFF_UNIVERSE] left=left right=right" in out
    assert "[CRYPTO_PRUNE] apply=True files=1" in out
    assert calls[1][1]["incremental"] is True


def test_cli_crypto_validate_raises_on_errors(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  store_root: {tmp_path / 'store'}\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "crypto_validate_from_config", lambda *args, **kwargs: {"ok": False, "errors": ["bad crypto"]})

    with pytest.raises(SystemExit, match="bad crypto"):
        cli.main(["--config", str(config_path), "crypto", "validate", "--interval", "1h"])


def test_cli_report_universe_consistency_json_and_csv(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    frame = pl.DataFrame({"symbol": ["AAA"], "status": ["ok"]})
    monkeypatch.setattr(cli, "generate_universe_consistency_report", lambda *args, **kwargs: frame)
    monkeypatch.setattr(cli, "render_universe_consistency_json", lambda frame: '{"ok": true}')

    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily", "--format", "json"]) == 0
    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily", "--format", "csv"]) == 0
    out = capsys.readouterr().out
    assert '{"ok": true}' in out
    assert "symbol,status" in out
    monkeypatch.setattr(cli, "render_universe_consistency_markdown", lambda *args, **kwargs: "# Markdown\n")
    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily"]) == 0
    assert "# Markdown" in capsys.readouterr().out


def test_cli_report_universe_consistency_markdown_json_and_csv_write_outputs(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    frame = pl.DataFrame({"symbol": ["AAA"], "status": ["ok"]})
    monkeypatch.setattr(cli, "generate_universe_consistency_report", lambda *args, **kwargs: frame)
    monkeypatch.setattr(cli, "render_universe_consistency_markdown", lambda *args, **kwargs: "# Report\n")
    monkeypatch.setattr(cli, "render_universe_consistency_json", lambda frame: '{"ok": true}')

    md_out = tmp_path / "report.md"
    json_out = tmp_path / "report.json"
    csv_out = tmp_path / "report.csv"

    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily", "--out", str(md_out)]) == 0
    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily", "--format", "json", "--out", str(json_out)]) == 0
    assert cli.main(["--config", str(config_path), "report-universe-consistency", "--dataset", "daily", "--format", "csv", "--out", str(csv_out)]) == 0

    assert md_out.read_text(encoding="utf-8") == "# Report\n"
    assert json_out.read_text(encoding="utf-8") == '{"ok": true}'
    assert "symbol,status" in csv_out.read_text(encoding="utf-8")
    assert capsys.readouterr().out == ""


def test_cli_report_parquet_store_markdown_only_and_update_monitor(monkeypatch, tmp_path: Path, capsys):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  runs_root: {tmp_path / 'runs'}\n", encoding="utf-8")
    monkeypatch.setattr(cli, "generate_parquet_store_report", lambda *args, **kwargs: {"json_path": "", "markdown_path": "/tmp/report.md"})
    update_calls: list[tuple[str, object]] = []
    monkeypatch.setattr(cli, "update_from_config", lambda cfg, symbols_override=None: update_calls.append(("update", symbols_override)))
    monkeypatch.setattr(cli, "monitor_extended_hours_from_config", lambda cfg, symbols_override=None, top_n=25, session_filter="all": update_calls.append(("monitor", (symbols_override, top_n, session_filter))))

    assert cli.main(["--config", str(config_path), "report-parquet-store", "--format", "markdown"]) == 0
    assert cli.main(["--config", str(config_path), "update", "--symbols", "AAA", "BBB"]) == 0
    assert cli.main(["--config", str(config_path), "monitor-extended-hours", "--symbols", "AAA", "--top-n", "3", "--session", "pre"]) == 0
    out = capsys.readouterr().out
    assert "[PARQUET_STORE_REPORT] json= markdown=/tmp/report.md" in out
    assert update_calls == [("update", ["AAA", "BBB"]), ("monitor", (["AAA"], 3, "pre"))]


def test_cli_fx_validate_failure_build_universe_and_unknown_command(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
                [
                    "paths:",
                    f"  runs_root: {tmp_path / 'runs'}",
                    f"  store_root: {tmp_path / 'store'}",
                    f"  universe_dir: {tmp_path / 'universes'}",
                    f"  ticker_overrides_yaml: {tmp_path / 'ticker_overrides.yaml'}",
                ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli, "validate_fx_pair", lambda root, pair: ["broken", "missing"])
    with pytest.raises(SystemExit, match="USDEUR: broken\nUSDEUR: missing"):
        cli.main(["--config", str(config_path), "fx-validate", "--pairs", "USDEUR"])

    build_calls: list[dict[str, object]] = []
    monkeypatch.setattr(cli, "build_universe", lambda **kwargs: build_calls.append(kwargs))
    assert cli.main(
        [
            "--config",
            str(config_path),
            "build-universe",
            "--indices",
            "sp500",
            "nasdaq100",
            "--out",
            str(tmp_path / "out.csv"),
            "--inactive-too",
        ]
    ) == 0
    assert build_calls == [
        {
            "indices": ["sp500", "nasdaq100"],
            "out_path": str(tmp_path / "out.csv"),
            "active_only": False,
            "overrides_dir": str(tmp_path / "universes"),
            "ticker_overrides_path": tmp_path / "store" / "meta" / "ticker_overrides.csv",
        }
    ]

    class DummyParser:
        def add_argument(self, *args, **kwargs):
            return None

        def add_subparsers(self, *args, **kwargs):
            class DummySubparsers:
                def add_parser(self, *args, **kwargs):
                    class DummySubparser:
                        def add_argument(self, *args, **kwargs):
                            return None

                        def add_subparsers(self, *args, **kwargs):
                            return DummySubparsers()

                        def set_defaults(self, **kwargs):
                            return None

                    return DummySubparser()

            return DummySubparsers()

        def parse_args(self, argv):
            return argparse.Namespace(cmd="unknown", config=str(config_path))

    monkeypatch.setattr(cli.argparse, "ArgumentParser", lambda *args, **kwargs: DummyParser())
    assert cli.main([]) == 2
