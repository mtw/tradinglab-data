from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from tradinglab_data.config import Config
from tradinglab_data.store_report import generate_parquet_store_report, render_store_integrity_report_markdown


def _write_config(tmp_path: Path) -> Path:
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday"
    runs_root = tmp_path / "runs"
    universe_csv = tmp_path / "meta" / "merged.csv"
    universe_dir = tmp_path / "meta" / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)
    universe_csv.parent.mkdir(parents=True, exist_ok=True)
    universe_csv.write_text("symbol\nAAA\nBBB\nCCC\n", encoding="utf-8")
    (universe_dir / "sp500.csv").write_text("symbol\nAAA\nBBB\nCCC\n", encoding="utf-8")
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
    return config_path


def _write_daily_parquet(path: Path, *, dates: list[str], currency: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": dates,
            "open": [10.0 + idx for idx, _ in enumerate(dates)],
            "high": [10.5 + idx for idx, _ in enumerate(dates)],
            "low": [9.5 + idx for idx, _ in enumerate(dates)],
            "close": [10.2 + idx for idx, _ in enumerate(dates)],
            "adj_close": [10.2 + idx for idx, _ in enumerate(dates)],
            "volume": [1000.0 for _ in dates],
            "currency": currency,
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(path)


def _write_intraday_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": ["2026-03-27T08:00:00", "2026-03-27T08:05:00"],
            "open": [20.0, 20.2],
            "high": [20.4, 20.5],
            "low": [19.8, 20.0],
            "close": [20.2, 20.3],
            "adj_close": [20.2, 20.3],
            "volume": [500.0, 650.0],
            "currency": ["USD", "USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(path)


def test_generate_parquet_store_report_detects_dirty_files(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)

    _write_daily_parquet(
        tmp_path / "daily" / "AAA.parquet",
        dates=["2026-03-25", "2026-03-26"],
        currency=["USD", "USD"],
    )
    _write_daily_parquet(
        tmp_path / "daily" / "BBB.parquet",
        dates=["2026-03-26", "2026-03-25", "2026-03-25"],
        currency=["UNKNOWN", "", "USD"],
    )
    _write_intraday_parquet(tmp_path / "intraday" / "5m" / "AAA.parquet")

    report = generate_parquet_store_report(cfg)

    assert report["json_path"]
    assert report["markdown_path"]
    assert Path(report["json_path"]).exists()
    assert Path(report["markdown_path"]).exists()
    assert {section["section"] for section in report["sections"]} == {"daily", "intraday:5m"}
    assert any(item["symbol"] == "BBB" for item in report["dirty_files"])
    assert any("duplicate_dates" in item["dirty_reasons"] for item in report["dirty_files"] if item["symbol"] == "BBB")
    assert any("unknown_currency_rows" in item["dirty_reasons"] for item in report["dirty_files"] if item["symbol"] == "BBB")

    markdown = render_store_integrity_report_markdown(report)
    assert "Dirty Files" in markdown
    assert "intraday:5m" in markdown

    json_report = json.loads(Path(report["json_path"]).read_text(encoding="utf-8"))
    assert json_report["json_path"].endswith("parquet_store_report.json")
    assert json_report["markdown_path"].endswith("parquet_store_report.md")


def test_generate_parquet_store_report_json_only(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_daily_parquet(
        tmp_path / "daily" / "AAA.parquet",
        dates=["2026-03-25"],
        currency=["USD"],
    )

    report = generate_parquet_store_report(cfg, write_markdown=False)

    assert report["json_path"].endswith("parquet_store_report.json")
    assert report["markdown_path"] == ""


def test_generate_parquet_store_report_flags_corrupted_parquet_file(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    bad_path = tmp_path / "daily" / "BROKEN.parquet"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text("not a parquet file", encoding="utf-8")

    report = generate_parquet_store_report(cfg)

    broken = next(item for item in report["dirty_files"] if item["symbol"] == "BROKEN")
    assert "read_error" in broken["dirty_reasons"]
    assert broken["read_error"] is not None


def test_generate_parquet_store_report_flags_zero_byte_file(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    zero_path = tmp_path / "daily" / "ZERO.parquet"
    zero_path.parent.mkdir(parents=True, exist_ok=True)
    zero_path.write_bytes(b"")

    report = generate_parquet_store_report(cfg)

    zero = next(item for item in report["dirty_files"] if item["symbol"] == "ZERO")
    assert "zero_byte" in zero["dirty_reasons"]
    assert "read_error" in zero["dirty_reasons"]


def test_render_store_integrity_report_markdown_has_stable_sections(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_daily_parquet(
        tmp_path / "daily" / "AAA.parquet",
        dates=["2026-03-25"],
        currency=["USD"],
    )

    report = generate_parquet_store_report(cfg, write_json=False, write_markdown=False)
    markdown = render_store_integrity_report_markdown(report)

    assert markdown.startswith("# Parquet Store Integrity Report\n")
    assert "## Section Summary" in markdown
    assert "| Section | Files | Dirty | Rows | Earliest | Latest | Currencies |" in markdown
    assert "## Section Details" in markdown
    assert "## Dirty Files" in markdown
    assert "## Daily Parquet Sanity" in markdown
