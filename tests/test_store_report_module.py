from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

import tradinglab_data.store_report as store_mod
from tradinglab_data.config import Config
from tradinglab_data.store_report import generate_parquet_store_report, render_store_integrity_report_markdown


def _write_config(tmp_path: Path) -> Path:
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday"
    intraday_research_root = tmp_path / "intraday_research"
    intraday_live_root = tmp_path / "intraday_live"
    crypto_root = tmp_path / "crypto"
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
                f"  crypto_root: {crypto_root}",
                f"  runs_root: {runs_root}",
                f"  universe_dir: {universe_dir}",
                "extended_hours:",
                f"  intraday_root: {intraday_root}",
                "intraday:",
                f"  research_root: {intraday_research_root}",
                "intraday_live:",
                f"  live_root: {intraday_live_root}",
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


def _write_intraday_timestamp_parquet(path: Path, *, live: bool, timestamps: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "timestamp": timestamps,
        "open": [20.0 + idx for idx, _ in enumerate(timestamps)],
        "high": [20.4 + idx for idx, _ in enumerate(timestamps)],
        "low": [19.8 + idx for idx, _ in enumerate(timestamps)],
        "close": [20.2 + idx for idx, _ in enumerate(timestamps)],
        "volume": [500.0 for _ in timestamps],
        "currency": ["USD" for _ in timestamps],
        "symbol": [path.stem for _ in timestamps],
        "interval": ["5m" for _ in timestamps],
        "provider": ["yahoo" for _ in timestamps],
        "session_date": ["2026-03-27" for _ in timestamps],
        "is_regular_session": [True for _ in timestamps],
        "ingested_at": ["2026-03-27T21:00:00" for _ in timestamps],
    }
    if live:
        data["session"] = ["regular" for _ in timestamps]
        data["is_closed_bar"] = [True for _ in timestamps]
    else:
        data["session"] = ["regular" for _ in timestamps]
    pl.DataFrame(data).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("session_date").str.strptime(pl.Date, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(path)


def _write_crypto_parquet(path: Path, *, timestamps: list[str], quote_asset: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    quotes = quote_asset or ["USDT" for _ in timestamps]
    pl.DataFrame(
        {
            "timestamp": timestamps,
            "open": [30.0 + idx for idx, _ in enumerate(timestamps)],
            "high": [30.5 + idx for idx, _ in enumerate(timestamps)],
            "low": [29.5 + idx for idx, _ in enumerate(timestamps)],
            "close": [30.2 + idx for idx, _ in enumerate(timestamps)],
            "volume": [700.0 + idx for idx, _ in enumerate(timestamps)],
            "provider": ["ccxt" for _ in timestamps],
            "exchange": ["binance" for _ in timestamps],
            "market_type": ["spot" for _ in timestamps],
            "symbol": ["BTC_USDT" for _ in timestamps],
            "base_asset": ["BTC" for _ in timestamps],
            "quote_asset": quotes,
            "interval": ["1h" for _ in timestamps],
            "is_closed": [True for _ in timestamps],
            "ingested_at": ["2026-03-27T09:00:00" for _ in timestamps],
            "source_symbol": ["BTC/USDT" for _ in timestamps],
        }
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(path)


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
    _write_crypto_parquet(
        tmp_path / "crypto" / "binance" / "spot" / "1h" / "BTC_USDT.parquet",
        timestamps=["2026-03-26T00:00:00", "2026-03-26T01:00:00"],
    )

    report = generate_parquet_store_report(cfg)

    assert report["json_path"]
    assert report["markdown_path"]
    assert Path(report["json_path"]).exists()
    assert Path(report["markdown_path"]).exists()
    assert {section["section"] for section in report["sections"]} == {"daily", "intraday:5m", "crypto:binance:spot:1h"}
    assert any(item["symbol"] == "BBB" for item in report["dirty_files"])
    assert any("duplicate_dates" in item["dirty_reasons"] for item in report["dirty_files"] if item["symbol"] == "BBB")
    assert any("unknown_currency_rows" in item["dirty_reasons"] for item in report["dirty_files"] if item["symbol"] == "BBB")

    markdown = render_store_integrity_report_markdown(report)
    assert "Dirty Files" in markdown
    assert "intraday:5m" in markdown
    assert "crypto:binance:spot:1h" in markdown

    json_report = json.loads(Path(report["json_path"]).read_text(encoding="utf-8"))
    assert json_report["json_path"].endswith("parquet_store_report.json")
    assert json_report["markdown_path"].endswith("parquet_store_report.md")
    assert json_report["crypto_root"].endswith("crypto")


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


def test_generate_parquet_store_report_flags_crypto_dirty_files(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_crypto_parquet(
        tmp_path / "crypto" / "binance" / "spot" / "1h" / "BTC_USDT.parquet",
        timestamps=["2026-03-27T01:00:00", "2026-03-27T00:00:00", "2026-03-27T00:00:00"],
        quote_asset=["", "USDT", "USDT"],
    )

    report = generate_parquet_store_report(cfg)

    btc = next(item for item in report["dirty_files"] if item["symbol"] == "BTC_USDT")
    assert btc["section"] == "crypto:binance:spot:1h"
    assert "duplicate_timestamps" in btc["dirty_reasons"]
    assert "unsorted_timestamps" in btc["dirty_reasons"]
    assert "missing_quote_asset_rows" in btc["dirty_reasons"]


def test_generate_parquet_store_report_includes_intraday_research_and_live_roots(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_intraday_timestamp_parquet(
        tmp_path / "intraday_research" / "5m" / "AAA.parquet",
        live=False,
        timestamps=["2026-03-27T13:35:00", "2026-03-27T13:30:00"],
    )
    bad_live_path = tmp_path / "intraday_live" / "5m" / "BBB.parquet"
    bad_live_path.parent.mkdir(parents=True)
    bad_live_path.write_bytes(b"not parquet")

    report = generate_parquet_store_report(cfg)

    sections = {section["section"] for section in report["sections"]}
    assert "intraday_research:5m" in sections
    assert "intraday_live:5m" in sections
    assert any(item["section"] == "intraday_live:5m" and item["symbol"] == "BBB" for item in report["dirty_files"])


def test_generate_parquet_store_report_flags_crypto_gap_zero_volume_and_metadata_drift(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    path = tmp_path / "crypto" / "binance" / "spot" / "1h" / "BTC_USDT.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "timestamp": ["2026-03-27T00:00:00", "2026-03-27T03:00:00"],
            "open": [30.0, 31.0],
            "high": [30.5, 31.5],
            "low": [29.5, 30.5],
            "close": [30.2, 31.2],
            "volume": [0.0, 700.0],
            "provider": ["ccxt", "ccxt"],
            "exchange": ["binance", "kraken"],
            "market_type": ["spot", "spot"],
            "symbol": ["BTC_USDT", "BTC_USDT"],
            "base_asset": ["BTC", "BTC"],
            "quote_asset": ["USDT", "USDT"],
            "interval": ["1h", "1h"],
            "is_closed": [True, True],
            "ingested_at": ["2026-03-27T09:00:00", "2026-03-27T09:00:00"],
            "source_symbol": ["BTC/USDT", "BTC/USDT"],
        }
    ).with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(path)

    report = generate_parquet_store_report(cfg)

    btc = next(item for item in report["dirty_files"] if item["symbol"] == "BTC_USDT")
    assert "large_continuity_gap" in btc["dirty_reasons"]
    assert "zero_volume_rows" in btc["dirty_reasons"]
    assert "metadata_inconsistency" in btc["dirty_reasons"]


def test_store_report_helper_functions_cover_edge_cases(tmp_path: Path, monkeypatch):
    aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert store_mod._format_datetime(None) is None
    assert store_mod._format_datetime(aware).endswith("+00:00")
    assert store_mod._format_datetime("x") == "x"
    assert store_mod._scan_parquet_files(tmp_path / "missing") == []
    assert store_mod._file_symbol(tmp_path / "AAA.parquet") == "AAA"
    assert store_mod._currency_stats(pl.DataFrame({"x": [1]})) == ([], 1, 0)
    assert store_mod._sorted_times(pl.DataFrame(), time_column="date") is True
    assert store_mod._history_bounds(pl.DataFrame(), time_column="date") == (None, None)
    assert store_mod._ohlc_quality_counts(pl.DataFrame(), time_column="date") == {"null_ohlc": 0, "bad_ohlc": 0, "dup_times": 0}
    assert store_mod._ohlc_quality_counts(pl.DataFrame({"date": [datetime(2026, 1, 1)]}), time_column="date") == {"null_ohlc": 1, "bad_ohlc": 1, "dup_times": 1}
    assert store_mod._expected_step("crypto:binance:spot:15m") == timedelta(minutes=15)
    assert store_mod._expected_step("crypto:binance:spot:1h") == timedelta(hours=1)
    assert store_mod._expected_step("crypto:binance:spot:1d") == timedelta(days=1)
    assert store_mod._expected_step("crypto:binance:spot:5m") is None
    assert store_mod._expected_step("daily") is None
    assert store_mod._max_gap_multiple(pl.DataFrame(), time_column="timestamp", expected_step=None) == 1.0
    assert store_mod._metadata_inconsistent_columns(pl.DataFrame({"provider": ["x", "y"]}), columns=["provider"]) == ["provider"]

    monkeypatch.setattr(pl.Series, "is_sorted", lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
    assert store_mod._sorted_times(pl.DataFrame({"date": [datetime(2026, 1, 1)]}), time_column="date") is False


def test_store_report_helper_exception_branches(tmp_path: Path, monkeypatch):
    frame = pl.DataFrame({"date": [datetime(2026, 1, 1), datetime(2026, 1, 2)], "provider": [None, None]})
    monkeypatch.setattr(pl.DataFrame, "select", lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert store_mod._history_bounds(frame, time_column="date") == (None, None)
    assert store_mod._metadata_inconsistent_columns(frame, columns=["provider"]) == []

    ordered = pl.DataFrame({"timestamp": [datetime(2026, 1, 1), datetime(2026, 1, 1, 2)]})
    monkeypatch.setattr(pl.DataFrame, "sort", lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert store_mod._max_gap_multiple(ordered, time_column="timestamp", expected_step=timedelta(hours=1)) == 1.0

    class ZeroStep:
        def total_seconds(self):
            return 0

    assert store_mod._max_gap_multiple(ordered, time_column="timestamp", expected_step=ZeroStep()) == 1.0
    assert store_mod._max_gap_multiple(pl.DataFrame({"timestamp": ["a", "b"]}), time_column="timestamp", expected_step=timedelta(hours=1)) == 1.0

    monkeypatch.setattr(pl.DataFrame, "filter", lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert store_mod._metadata_inconsistent_columns(frame, columns=["provider"]) == []


def test_store_report_audit_and_summary_cover_read_schema_and_empty_history(tmp_path: Path):
    bad = tmp_path / "BROKEN.parquet"
    bad.write_text("not parquet", encoding="utf-8")
    audit = store_mod._audit_file(bad, section="daily", validator=lambda df: None, time_column="date")
    assert audit.read_error is not None
    assert "read_error" in audit.dirty_reasons

    empty = tmp_path / "EMPTY.parquet"
    pl.DataFrame({"date": pl.Series([], dtype=pl.Datetime), "open": pl.Series([], dtype=pl.Float64), "high": pl.Series([], dtype=pl.Float64), "low": pl.Series([], dtype=pl.Float64), "close": pl.Series([], dtype=pl.Float64), "adj_close": pl.Series([], dtype=pl.Float64), "volume": pl.Series([], dtype=pl.Float64), "currency": pl.Series([], dtype=pl.String)}).write_parquet(empty)
    empty_audit = store_mod._audit_file(empty, section="daily", validator=lambda df: (_ for _ in ()).throw(ValueError("bad schema")), time_column="date")
    assert "empty_file" in empty_audit.dirty_reasons
    assert "schema_mismatch" in empty_audit.dirty_reasons

    summary, dirty = store_mod._summarize_section(section="daily", root=tmp_path, validator=lambda df: None, time_column="date")
    assert summary["files_total"] >= 2
    assert summary["files_dirty"] >= 1
    assert dirty

    top = store_mod._top_histories([empty_audit], limit=1)
    assert top[0]["symbol"] == "EMPTY"


def test_store_report_audit_flags_null_and_bad_ohlc_rows(tmp_path: Path):
    bad = tmp_path / "BAD.parquet"
    pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1), datetime(2026, 1, 2)],
            "open": [1.0, None],
            "high": [2.0, -1.0],
            "low": [0.5, 0.1],
            "close": [1.5, 0.2],
            "adj_close": [1.5, 0.2],
            "volume": [1.0, 2.0],
            "currency": ["USD", "USD"],
        }
    ).write_parquet(bad)

    audit = store_mod._audit_file(bad, section="daily", validator=lambda df: None, time_column="date")
    assert "null_ohlc_rows" in audit.dirty_reasons
    assert "bad_ohlc_rows" in audit.dirty_reasons


def test_render_store_integrity_report_markdown_no_dirty_files_and_json_toggle(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_daily_parquet(tmp_path / "daily" / "AAA.parquet", dates=["2026-03-25"], currency=["USD"])

    report = generate_parquet_store_report(cfg, write_json=False, write_markdown=True)
    text = render_store_integrity_report_markdown(report)

    assert report["json_path"] == ""
    assert "Errors: none" in text or "Errors:" in text


def test_render_store_integrity_report_markdown_errors_none_branch():
    report = {
        "generated_at": "2026-01-01T00:00:00+00:00",
        "config_path": "/tmp/config.yaml",
        "daily_root": "/tmp/daily",
        "intraday_root": "/tmp/intraday",
        "crypto_root": "/tmp/crypto",
        "json_path": "",
        "markdown_path": "",
        "sections": [
            {
                "section": "daily",
                "root": "/tmp/daily",
                "files_total": 0,
                "files_readable": 0,
                "files_dirty": 0,
                "rows_total": 0,
                "rows_min": 0,
                "rows_median": 0.0,
                "rows_max": 0,
                "earliest_date": None,
                "latest_date": None,
                "currencies_seen": [],
                "missing_currency_rows": 0,
                "unknown_currency_rows": 0,
                "duplicate_rows": 0,
                "null_ohlc_rows": 0,
                "bad_ohlc_rows": 0,
                "zero_volume_rows": 0,
                "stale_files": 0,
                "dirty_reason_counts": {},
                "top_histories": [],
            }
        ],
        "dirty_files": [],
        "parquet_sanity": {"status": "ok", "file_count": 0, "zero_byte": 0, "sample_read_checked": 0, "errors": []},
    }

    text = render_store_integrity_report_markdown(report)
    assert "- Errors: none" in text


def test_store_report_gap_helper_covers_zero_step_and_non_datetime_pairs():
    frame = pl.DataFrame({"timestamp": [datetime(2026, 1, 1), datetime(2026, 1, 1, 2)]})

    class ZeroStep:
        def total_seconds(self):
            return 0

    assert store_mod._max_gap_multiple(frame, time_column="timestamp", expected_step=ZeroStep()) == 1.0
    assert store_mod._max_gap_multiple(pl.DataFrame({"timestamp": ["a", "b"]}), time_column="timestamp", expected_step=timedelta(hours=1)) == 1.0


def test_generate_parquet_store_report_includes_root_intraday_section(tmp_path: Path):
    config_path = _write_config(tmp_path)
    cfg = Config.load(config_path)
    _write_daily_parquet(tmp_path / "daily" / "AAA.parquet", dates=["2026-03-25"], currency=["USD"])
    _write_intraday_parquet(tmp_path / "intraday" / "AAA.parquet")

    report = generate_parquet_store_report(cfg, write_json=False, write_markdown=False)

    assert "intraday" in {section["section"] for section in report["sections"]}
