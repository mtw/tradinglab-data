from __future__ import annotations

import json
import sys
from datetime import datetime
from io import StringIO
from pathlib import Path

from tests._load import load_script_module

mod = load_script_module("check_parquet_status")


def _status(symbol: str = "AAPL"):
    return mod.FileStatus(
        symbol=symbol,
        path=Path(f"/tmp/{symbol}.parquet"),
        exists=True,
        readable=True,
        rows=10,
        start_date="2020-01-01 00:00:00",
        end_date="2020-01-10 00:00:00",
        last_open=10.0,
        last_high=11.0,
        last_low=9.0,
        last_close=10.5,
        period_label="all",
        period_rows=10,
        period_start_date="2020-01-01 00:00:00",
        period_end_date="2020-01-10 00:00:00",
        period_high=11.0,
        period_low=9.0,
        required_cols_ok=True,
        missing_cols=[],
        duplicate_dates=0,
        null_ohlc_rows=0,
        bad_ohlc_rows=0,
        large_gap_count=0,
        extreme_move_count=0,
        sorted_dates=True,
        valid=True,
        error="",
    )


def test_parse_index_from_source():
    assert mod._parse_index_from_source("djia_wikipedia") == "djia"
    assert mod._parse_index_from_source("ATX_override") == "atx"
    assert mod._parse_index_from_source("etf_master") == "etf"
    assert mod._parse_index_from_source("other_source") is None


def test_infer_parquet_mode():
    mode, mins = mod._infer_parquet_mode(Path("/store/parquet/daily"), "auto")
    assert mode == "daily"
    assert mins is None
    mode2, mins2 = mod._infer_parquet_mode(Path("/store/parquet/intraday/5m"), "auto")
    assert mode2 == "intraday"
    assert mins2 == 5


def test_collect_targets_ignores_orphan_parquet_by_default(tmp_path: Path):
    root = tmp_path / "parquet"
    root.mkdir()
    (root / "AAPL.parquet").write_text("x", encoding="utf-8")
    (root / "ORPHAN.parquet").write_text("x", encoding="utf-8")
    universe = tmp_path / "universe.csv"
    mod.pl.DataFrame({"symbol": ["AAPL"], "active": [1]}).write_csv(str(universe))
    targets = mod._collect_targets(root=root, symbols=[], paths=[], universe_csv=universe, ignore_orphans=True)
    assert targets == [("AAPL", root / "AAPL.parquet")]


def test_collect_targets_can_include_orphans(tmp_path: Path):
    root = tmp_path / "parquet"
    root.mkdir()
    (root / "AAPL.parquet").write_text("x", encoding="utf-8")
    (root / "ORPHAN.parquet").write_text("x", encoding="utf-8")
    universe = tmp_path / "universe.csv"
    mod.pl.DataFrame({"symbol": ["AAPL"], "active": [1]}).write_csv(str(universe))
    targets = mod._collect_targets(root=root, symbols=[], paths=[], universe_csv=universe, ignore_orphans=False)
    assert targets == [("AAPL", root / "AAPL.parquet"), ("ORPHAN", root / "ORPHAN.parquet")]


def test_row_has_issue_flags_meta_yf_and_sample():
    row = {"status": "ok", "venue_mismatch": "", "parquet_vs_yf": "ok", "sample_status": "ok"}
    assert mod._row_has_issue(row, include_meta=False, include_yf=False, include_sample=False) is False

    row["venue_mismatch"] = "expected_.VI_suffix"
    assert mod._row_has_issue(row, include_meta=True, include_yf=False, include_sample=False) is True
    row["venue_mismatch"] = ""

    row["parquet_vs_yf"] = "price_mismatch"
    assert mod._row_has_issue(row, include_meta=False, include_yf=True, include_sample=False) is True
    row["parquet_vs_yf"] = "ok"

    row["sample_status"] = "mismatch"
    assert mod._row_has_issue(row, include_meta=False, include_yf=False, include_sample=True) is True
    assert mod._row_has_issue(row, include_meta=False, include_yf=False, include_sample=True, provider_baseline="mixed") is False


def test_row_has_critical_issue_only_on_integrity_status():
    assert mod._row_has_critical_issue({"status": "ok", "venue_mismatch": "x"}) is False
    assert mod._row_has_critical_issue({"status": "issue"}) is False
    assert mod._row_has_critical_issue({"status": "issue", "dup_dates": "1"}) is True


def test_row_has_critical_issue_gap_policy_by_asset_type():
    row = {"status": "issue", "large_gaps": "1"}
    assert mod._row_has_critical_issue(row, symbol_asset_type="stock", etf_large_gap_tolerance=2) is True
    assert mod._row_has_critical_issue(row, symbol_asset_type="etf", etf_large_gap_tolerance=2) is False
    assert mod._row_has_critical_issue(row, parquet_mode="intraday", intraday_large_gaps_critical=False) is False
    assert mod._row_has_critical_issue(row, parquet_mode="intraday", intraday_large_gaps_critical=True, symbol_asset_type="stock") is True
    row2 = {"status": "issue", "large_gaps": "3"}
    assert mod._row_has_critical_issue(row2, symbol_asset_type="etf", etf_large_gap_tolerance=2) is True
    row3 = {
        "status": "issue",
        "large_gaps": "12",
        "start_date": "2020-01-01 00:00:00",
        "end_date": "2025-12-31 00:00:00",
    }
    assert (
        mod._row_has_critical_issue(
            row3,
            symbol_asset_type="etf",
            etf_large_gap_tolerance=2,
            etf_max_large_gaps_per_year=3.0,
        )
        is False
    )


def test_infer_symbol_asset_type_from_index_and_meta():
    idx = {"AAA": {"sp500"}, "ETF1": {"etf_all"}, "ETF2": {"etf"}}
    meta = {
        "ETF3": mod.SymbolMeta(name="", isin="", exchange="", country="", source="etf_master"),
        "UNK": mod.SymbolMeta(name="", isin="", exchange="", country="", source=""),
    }
    assert mod._infer_symbol_asset_type("AAA", idx, meta) == "stock"
    assert mod._infer_symbol_asset_type("ETF1", idx, meta) == "etf"
    assert mod._infer_symbol_asset_type("ETF2", idx, meta) == "etf"
    assert mod._infer_symbol_asset_type("ETF3", idx, meta) == "etf"
    assert mod._infer_symbol_asset_type("ZZZ", idx, meta) == "unknown"


def test_status_rows_includes_sample_fields():
    status = _status("EBS.VI")
    meta = {"EBS.VI": mod.SymbolMeta(name="Erste", isin="AT0000A", exchange="Vienna", country="AT", source="atx")}
    yf = {"EBS.VI": mod.YFLatest(ok=True, date="2026-02-17 00:00:00", close=10.4, currency="EUR", error="")}
    sample = {
        "EBS.VI": mod.YFSampleAudit(
            ok=False,
            checked_days=50,
            mismatch_days=12,
            first_mismatch_dates=["2021-01-01", "2021-02-01"],
            max_abs_ohlc_diff=2.5,
            error="",
        )
    }

    rows = mod._status_rows(
        statuses=[status],
        meta_by_symbol=meta,
        yf_latest_by_symbol=yf,
        sample_audit_by_symbol=sample,
        include_meta=True,
        include_yf=True,
        include_sample=True,
    )
    row = rows[0]
    assert row["company_name"] == "Erste"
    assert row["sample_checked_days"] == "50"
    assert row["sample_mismatch_days"] == "12"
    assert row["sample_status"] == "mismatch"
    assert "2021-01-01" in row["sample_mismatch_dates"]


def test_status_rows_provider_baseline_stooq_marks_expected_difference():
    status = _status("AAPL")
    sample = {
        "AAPL": mod.YFSampleAudit(
            ok=False,
            checked_days=50,
            mismatch_days=10,
            first_mismatch_dates=["2021-01-01"],
            max_abs_ohlc_diff=1.0,
            error="",
        )
    }
    rows = mod._status_rows(
        statuses=[status],
        meta_by_symbol={},
        yf_latest_by_symbol={},
        sample_audit_by_symbol=sample,
        include_meta=False,
        include_yf=False,
        include_sample=True,
        provider_baseline="stooq",
    )
    assert rows[0]["suspected_cause"] == "historical_provider_difference_expected_stooq_vs_yf"


def test_main_fail_severity_critical_does_not_fail_on_noncritical_issue(tmp_path: Path, monkeypatch):
    status = _status("AAA")
    meta = {
        "AAA": mod.SymbolMeta(
            name="AAA",
            isin="",
            exchange="Vienna",
            country="AT",
            source="atx",
        )
    }
    monkeypatch.setattr(mod, "_collect_targets", lambda **kwargs: [("AAA", tmp_path / "AAA.parquet")])
    monkeypatch.setattr(mod, "_validate_file", lambda **kwargs: status)
    monkeypatch.setattr(mod, "_load_symbol_meta", lambda **kwargs: meta)
    monkeypatch.setattr(mod, "run_parquet_sanity_checks", lambda *args, **kwargs: {"ok": True, "errors": []})
    summary_path = tmp_path / "summary.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_parquet_status.py",
            "--with-meta",
            "--summary-json",
            str(summary_path),
            "--fail-on-issues",
            "--fail-severity",
            "critical",
        ],
    )
    mod.main()
    assert summary_path.exists()


def test_main_fail_severity_all_fails_on_noncritical_issue(tmp_path: Path, monkeypatch):
    status = _status("AAA")
    meta = {
        "AAA": mod.SymbolMeta(
            name="AAA",
            isin="",
            exchange="Vienna",
            country="AT",
            source="atx",
        )
    }
    monkeypatch.setattr(mod, "_collect_targets", lambda **kwargs: [("AAA", tmp_path / "AAA.parquet")])
    monkeypatch.setattr(mod, "_validate_file", lambda **kwargs: status)
    monkeypatch.setattr(mod, "_load_symbol_meta", lambda **kwargs: meta)
    monkeypatch.setattr(mod, "run_parquet_sanity_checks", lambda *args, **kwargs: {"ok": True, "errors": []})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_parquet_status.py",
            "--with-meta",
            "--fail-on-issues",
            "--fail-severity",
            "all",
        ],
    )
    try:
        mod.main()
        raise AssertionError("expected SystemExit")
    except SystemExit as e:
        assert int(e.code) == 2


def test_main_summary_json_uses_resolved_root_from_config(tmp_path: Path, monkeypatch):
    status = _status("AAA")
    config_path = tmp_path / "config.yaml"
    parquet_root = tmp_path / "parquet" / "daily"
    summary_path = tmp_path / "summary.json"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  parquet_root: {parquet_root}",
                f"  runs_root: {tmp_path / 'runs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(mod, "_collect_targets", lambda **kwargs: [("AAA", tmp_path / "AAA.parquet")])
    monkeypatch.setattr(mod, "_validate_file", lambda **kwargs: status)
    monkeypatch.setattr(mod, "_load_symbol_meta", lambda **kwargs: {})
    monkeypatch.setattr(mod, "run_parquet_sanity_checks", lambda *args, **kwargs: {"ok": True, "errors": []})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_parquet_status.py",
            "--config",
            str(config_path),
            "--summary-json",
            str(summary_path),
        ],
    )

    mod.main()

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["root"] == str(parquet_root)


def test_validate_file_intraday_same_day_gap(tmp_path: Path):
    p = tmp_path / "AAA.parquet"
    df = mod.pl.DataFrame(
        {
            "date": [
                datetime(2026, 2, 27, 14, 30),
                datetime(2026, 2, 27, 14, 35),
                datetime(2026, 2, 27, 16, 0),
            ],
            "open": [1.0, 1.1, 1.2],
            "high": [1.1, 1.2, 1.3],
            "low": [0.9, 1.0, 1.1],
            "close": [1.05, 1.15, 1.25],
        }
    )
    df.write_parquet(str(p))
    status = mod._validate_file(
        path=p,
        symbol="AAA",
        period_year=None,
        period_month=None,
        mode="intraday",
        intraday_interval_minutes=5,
    )
    assert status.large_gap_count == 1
    assert status.valid is False


def test_validate_file_flags_unsorted_original_dates(tmp_path: Path):
    p = tmp_path / "AAA.parquet"
    mod.pl.DataFrame(
        {
            "date": [datetime(2020, 1, 2), datetime(2020, 1, 1)],
            "open": [1.0, 1.1],
            "high": [1.2, 1.3],
            "low": [0.9, 1.0],
            "close": [1.1, 1.2],
        }
    ).write_parquet(str(p))

    status = mod._validate_file(path=p, symbol="AAA", period_year=None, period_month=None)

    assert status.sorted_dates is False
    assert status.valid is False


def test_main_uses_config_resolved_roots_for_sanity_gate(tmp_path: Path, monkeypatch):
    parquet_root = tmp_path / "daily"
    universe_dir = tmp_path / "meta" / "universes"
    universe_csv = tmp_path / "meta" / "merged.csv"
    universe_dir.mkdir(parents=True)
    parquet_root.mkdir()
    universe_csv.write_text("symbol\nAAA\n", encoding="utf-8")
    (universe_dir / "sp500.csv").write_text("symbol\nAAA\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  parquet_root: {parquet_root}",
                f"  universe_csv: {universe_csv}",
                f"  universe_dir: {universe_dir}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured = {}
    monkeypatch.setattr(mod, "_collect_targets", lambda **kwargs: [])
    monkeypatch.setattr(mod, "_load_symbol_index_map", lambda **kwargs: {})
    def fake_sanity(cfg):
        captured["cfg"] = cfg
        return {"ok": True, "errors": []}

    monkeypatch.setattr(mod, "run_parquet_sanity_checks", fake_sanity)
    monkeypatch.setattr(sys, "argv", ["check_parquet_status.py", "--config", str(config_path)])

    mod.main()

    assert captured["cfg"].root == parquet_root
    assert captured["cfg"].universe_dir == universe_dir


def test_repair_intraday_symbol_from_yf_upserts(tmp_path: Path, monkeypatch):
    p = tmp_path / "AAA.parquet"
    mod.pl.DataFrame(
        {
            "date": [datetime(2026, 2, 27, 14, 30)],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.05],
            "adj_close": [1.05],
            "volume": [100.0],
            "currency": ["USD"],
        }
    ).write_parquet(str(p))
    status = _status("AAA")
    status.path = p

    monkeypatch.setattr(
        mod,
        "fetch_extended_intraday",
        lambda *args, **kwargs: {
            "AAA": mod.pl.DataFrame(
                {
                    "date": [datetime(2026, 2, 27, 14, 35)],
                    "open": [1.1],
                    "high": [1.2],
                    "low": [1.0],
                    "close": [1.15],
                    "adj_close": [1.15],
                    "volume": [120.0],
                }
            )
        },
    )
    monkeypatch.setattr(mod, "fetch_symbol_currency", lambda symbol: "USD")

    result = mod._repair_intraday_symbol_from_yf(status, interval="5m", log_path=tmp_path / "log.csv")
    assert result == "repaired_upsert"
    repaired = mod.pl.read_parquet(str(p)).sort("date")
    assert repaired.height == 2
    assert repaired.get_column("currency").to_list() == ["USD", "USD"]


def test_main_repair_mismatches_intraday_uses_intraday_repair(tmp_path: Path, monkeypatch):
    status = _status("AAA")
    status.path = tmp_path / "AAA.parquet"
    monkeypatch.setattr(mod, "_collect_targets", lambda **kwargs: [("AAA", status.path)])
    monkeypatch.setattr(mod, "_validate_file", lambda **kwargs: status)
    monkeypatch.setattr(mod, "_load_symbol_meta", lambda **kwargs: {})
    monkeypatch.setattr(mod, "run_parquet_sanity_checks", lambda *args, **kwargs: {"ok": True, "errors": []})
    monkeypatch.setattr(
        mod,
        "_fetch_yf_latest",
        lambda *args, **kwargs: mod.YFLatest(ok=True, date="2026-02-28 15:00:00", close=12.0, currency="USD", error=""),
    )
    monkeypatch.setattr(
        mod,
        "_sample_yf_consistency",
        lambda *args, **kwargs: mod.YFSampleAudit(ok=True, checked_days=0, mismatch_days=0, first_mismatch_dates=[], max_abs_ohlc_diff=0.0, error=""),
    )
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        mod,
        "_repair_intraday_symbol_from_yf",
        lambda status, interval, log_path: calls.append((status.symbol, interval)) or "repaired_upsert",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_parquet_status.py",
            "--root",
            str(tmp_path / "intraday" / "5m"),
            "--parquet-kind",
            "intraday",
            "--verify-yf",
            "--no-yf-ignore-current-day",
            "--repair-mismatches",
        ],
    )
    mod.main()
    assert calls == [("AAA", "5m")]


def test_clean_intraday_cache_rewrites_null_rows(tmp_path: Path):
    p = tmp_path / "AAA.parquet"
    mod.pl.DataFrame(
        {
            "date": [datetime.now().replace(microsecond=0, second=0, minute=30, hour=14), datetime.now().replace(microsecond=0, second=0, minute=35, hour=14)],
            "open": [1.0, None],
            "high": [1.1, None],
            "low": [0.9, None],
            "close": [1.05, None],
            "adj_close": [1.05, None],
            "volume": [100.0, None],
            "currency": ["USD", "USD"],
        }
    ).write_parquet(str(p))
    actions = mod._clean_intraday_cache([("AAA", p)], retention_days=10)
    assert actions["AAA"] == "cleaned_removed_1"
    repaired = mod.pl.read_parquet(str(p))
    assert repaired.height == 1


def test_main_clean_intraday_cache_rejects_daily_root(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_parquet_status.py",
            "--root",
            str(tmp_path),
            "--parquet-kind",
            "daily",
            "--clean-intraday-cache",
        ],
    )
    try:
        mod.main()
        raise AssertionError("expected SystemExit")
    except SystemExit as e:
        assert "--clean-intraday-cache is supported only for intraday parquet roots." in str(e)


def test_main_lists_universes_and_exits(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "meta" / "universes"
    crypto_universe_dir = tmp_path / "meta" / "crypto" / "universes"
    universe_dir.mkdir(parents=True)
    crypto_universe_dir.mkdir(parents=True)
    (universe_dir / "intraday_live_core.csv").write_text("symbol,active\nAAA,1\n", encoding="utf-8")
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
                f"  parquet_root: {tmp_path / 'parquet' / 'daily'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "argv", ["check_parquet_status.py", "--config", str(config_path), "--list-universes"])
    out = StringIO()
    monkeypatch.setattr(sys, "stdout", out)

    rc = mod.main()

    assert rc == 0
    printed = out.getvalue()
    assert "intraday_live_core: 1 symbols" in printed
    assert "crypto_dynamic: 1 symbols" in printed
