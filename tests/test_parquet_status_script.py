from __future__ import annotations

import json
import sys
from io import StringIO
from pathlib import Path

import polars as pl

from tests._load import load_script_module

mod = load_script_module("parquet_status")


def test_daily_status_wrapper_runs_check_and_writes_summary(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  parquet_root: {tmp_path / 'parquet' / 'daily'}",
                f"  universe_csv: {tmp_path / 'meta' / 'universe_master.csv'}",
                f"  universe_dir: {tmp_path / 'meta' / 'universes'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: list[list[str]] = []

    class FakeCheck:
        @staticmethod
        def main(argv):
            captured.append(list(argv))
            return 0

    monkeypatch.setattr(mod, "_load_check_module", lambda: FakeCheck())
    monkeypatch.setattr(
        mod,
        "generate_universe_consistency_report",
        lambda *args, **kwargs: pl.DataFrame(
            {"symbol": ["AAA"], "exists": [True], "status": ["ok"], "path": ["/tmp/AAA.parquet"]}
        ),
    )
    summary_path = tmp_path / "summary.json"

    rc = mod.main(["--config", str(config_path), "--daily", "--summary-json", str(summary_path)])

    assert rc == 0
    assert captured and "--parquet-kind" in captured[0] and "daily" in captured[0]
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "daily"
    assert payload["ok"] is True


def test_daily_status_wrapper_fails_when_consistency_report_has_issues(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(f"paths:\n  parquet_root: {tmp_path / 'parquet' / 'daily'}\n", encoding="utf-8")

    class FakeCheck:
        @staticmethod
        def main(argv):
            return 0

    monkeypatch.setattr(mod, "_load_check_module", lambda: FakeCheck())
    monkeypatch.setattr(
        mod,
        "generate_universe_consistency_report",
        lambda *args, **kwargs: pl.DataFrame(
            {"symbol": ["AAA"], "exists": [False], "status": ["missing"], "path": ["/tmp/AAA.parquet"]}
        ),
    )

    rc = mod.main(["--config", str(config_path), "--daily"])

    assert rc == 2


def test_intraday_status_wrapper_checks_research_and_live_roots(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "meta" / "universes"
    universe_dir.mkdir(parents=True)
    (universe_dir / "pilot.csv").write_text("symbol,active\nAAA,1\nBBB,1\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                f"  ticker_overrides_csv: {tmp_path / 'meta' / 'ticker_overrides.csv'}",
                "intraday:",
                f"  research_root: {tmp_path / 'parquet' / 'intraday_research'}",
                "intraday_live:",
                f"  live_root: {tmp_path / 'parquet' / 'intraday_live'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: list[list[str]] = []

    class FakeCheck:
        @staticmethod
        def main(argv):
            captured.append(list(argv))
            return 0

    monkeypatch.setattr(mod, "_load_check_module", lambda: FakeCheck())
    monkeypatch.setattr(
        mod,
        "inspect_intraday_research_store",
        lambda symbols, **kwargs: [
            {"symbol": symbol, "exists": True, "valid": True, "issues": [], "path": f"/tmp/research/{symbol}.parquet"}
            for symbol in symbols
        ],
    )
    monkeypatch.setattr(
        mod,
        "inspect_intraday_live_store",
        lambda symbols, **kwargs: [
            {"symbol": symbol, "exists": True, "valid": True, "issues": [], "path": f"/tmp/live/{symbol}.parquet"}
            for symbol in symbols
        ],
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_research_store",
        lambda symbols, **kwargs: {"ok": True, "dirty_files": [], "errors": [], "symbols": symbols, **kwargs},
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_live_store",
        lambda symbols, **kwargs: {"ok": True, "dirty_files": [], "errors": [], "symbols": symbols, **kwargs},
    )
    summary_path = tmp_path / "intraday_summary.json"

    rc = mod.main(
        [
            "--config",
            str(config_path),
            "--intraday",
            "--universe",
            "pilot",
            "--interval",
            "5m",
            "--summary-json",
            str(summary_path),
        ]
    )

    assert rc == 0
    assert len(captured) == 0
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["mode"] == "intraday"
    assert payload["symbols_checked"] == 2
    assert payload["research_missing_files"] == 0
    assert payload["live_missing_files"] == 0


def test_intraday_status_wrapper_fails_when_store_validation_fails(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "meta" / "universes"
    universe_dir.mkdir(parents=True)
    (universe_dir / "pilot.csv").write_text("symbol,active\nAAA,1\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                "intraday:",
                f"  research_root: {tmp_path / 'parquet' / 'intraday_research'}",
                "intraday_live:",
                f"  live_root: {tmp_path / 'parquet' / 'intraday_live'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeCheck:
        @staticmethod
        def main(argv):
            return 0

    monkeypatch.setattr(mod, "_load_check_module", lambda: FakeCheck())
    monkeypatch.setattr(
        mod,
        "inspect_intraday_research_store",
        lambda symbols, **kwargs: [
            {"symbol": symbol, "exists": True, "valid": False, "issues": ["bad"], "path": f"/tmp/research/{symbol}.parquet"}
            for symbol in symbols
        ],
    )
    monkeypatch.setattr(
        mod,
        "inspect_intraday_live_store",
        lambda symbols, **kwargs: [
            {"symbol": symbol, "exists": True, "valid": True, "issues": [], "path": f"/tmp/live/{symbol}.parquet"}
            for symbol in symbols
        ],
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_research_store",
        lambda symbols, **kwargs: {"ok": False, "dirty_files": ["x"], "errors": ["bad"]},
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_live_store",
        lambda symbols, **kwargs: {"ok": True, "dirty_files": [], "errors": []},
    )

    rc = mod.main(["--config", str(config_path), "--intraday", "--universe", "pilot"])

    assert rc == 2


def test_intraday_status_wrapper_fails_on_missing_files(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / "meta" / "universes"
    universe_dir.mkdir(parents=True)
    (universe_dir / "pilot.csv").write_text("symbol,active\nAAA,1\nBBB,1\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_dir: {universe_dir}",
                "intraday:",
                f"  research_root: {tmp_path / 'parquet' / 'intraday_research'}",
                "intraday_live:",
                f"  live_root: {tmp_path / 'parquet' / 'intraday_live'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class FakeCheck:
        @staticmethod
        def main(argv):
            return 0

    monkeypatch.setattr(mod, "_load_check_module", lambda: FakeCheck())
    monkeypatch.setattr(
        mod,
        "inspect_intraday_research_store",
        lambda symbols, **kwargs: [
            {"symbol": "AAA", "exists": True, "valid": True, "issues": [], "path": "/tmp/research/AAA.parquet"},
            {"symbol": "BBB", "exists": False, "valid": False, "issues": ["missing_file"], "path": "/tmp/research/BBB.parquet"},
        ],
    )
    monkeypatch.setattr(
        mod,
        "inspect_intraday_live_store",
        lambda symbols, **kwargs: [
            {"symbol": symbol, "exists": True, "valid": True, "issues": [], "path": f"/tmp/live/{symbol}.parquet"}
            for symbol in symbols
        ],
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_research_store",
        lambda symbols, **kwargs: {"ok": False, "dirty_files": ["/tmp/research/BBB.parquet"], "errors": ["missing"]},
    )
    monkeypatch.setattr(
        mod,
        "validate_intraday_live_store",
        lambda symbols, **kwargs: {"ok": True, "dirty_files": [], "errors": []},
    )
    summary_path = tmp_path / "intraday_summary.json"

    rc = mod.main(
        [
            "--config",
            str(config_path),
            "--intraday",
            "--universe",
            "pilot",
            "--summary-json",
            str(summary_path),
        ]
    )

    assert rc == 2
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert payload["research_missing_files"] == 1
    assert payload["ok"] is False


def test_parquet_status_list_universes(tmp_path: Path, monkeypatch):
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
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    out = StringIO()
    monkeypatch.setattr(sys, "stdout", out)

    rc = mod.main(["--config", str(config_path), "--list-universes"])

    assert rc == 0
    printed = out.getvalue()
    assert "intraday_live_core: 1 symbols" in printed
    assert "crypto_dynamic: 1 symbols" in printed
