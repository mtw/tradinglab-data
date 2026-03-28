from __future__ import annotations

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
    assert "TradingLab Data Parquet Schema" in out.read_text(encoding="utf-8")


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
