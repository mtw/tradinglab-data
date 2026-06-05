from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import tradinglab_data.cli as cli
from tradinglab_data.config import Config
from tradinglab_data.schema import schema_manifest
from tradinglab_data.store_report import generate_parquet_store_report


def _write_basic_config(tmp_path: Path) -> Path:
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
    return config_path


def test_daily_and_intraday_schema_manifest_remain_stable():
    manifest = schema_manifest()
    assert manifest["daily"] == {
        "date": "Datetime",
        "open": "Float64",
        "high": "Float64",
        "low": "Float64",
        "close": "Float64",
        "adj_close": "Float64",
        "volume": "Float64",
        "currency": "String",
    }
    assert manifest["intraday"] == {
        "date": "Datetime",
        "open": "Float64",
        "high": "Float64",
        "low": "Float64",
        "close": "Float64",
        "adj_close": "Float64",
        "volume": "Float64",
        "currency": "String",
    }


def test_report_json_daily_intraday_only_store_keeps_legacy_sections(tmp_path: Path):
    config_path = _write_basic_config(tmp_path)
    cfg = Config.load(config_path)
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday" / "5m"
    daily_root.mkdir(parents=True, exist_ok=True)
    intraday_root.mkdir(parents=True, exist_ok=True)
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
    pl.DataFrame(
        {
            "date": ["2026-03-27T08:00:00"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "adj_close": [10.2],
            "volume": [1000.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(intraday_root / "AAA.parquet")

    report = generate_parquet_store_report(cfg)
    payload = json.loads(Path(report["json_path"]).read_text(encoding="utf-8"))

    assert set(payload) == {
        "generated_at",
        "config_path",
        "daily_root",
        "intraday_root",
        "crypto_root",
        "sections",
        "dirty_files",
        "parquet_sanity",
        "json_path",
        "markdown_path",
    }
    assert [section["section"] for section in payload["sections"]] == ["daily", "intraday:5m"]
    assert not any(section["section"].startswith("crypto:") for section in payload["sections"])


def test_legacy_cli_commands_still_parse_and_return_zero(monkeypatch, tmp_path: Path):
    config_path = _write_basic_config(tmp_path)
    calls: dict[str, list[dict[str, object]]] = {
        "update": [],
        "monitor_extended_hours": [],
        "build_universe": [],
    }

    def fake_update(cfg, symbols_override=None):
        calls["update"].append({"cfg": cfg, "symbols_override": symbols_override})
        return {"symbols": [], "parquet_root": str(tmp_path / "daily"), "intraday": None}

    monkeypatch.setattr(cli, "update_from_config", fake_update)
    monkeypatch.setattr(
        cli,
        "monitor_extended_hours_from_config",
        lambda cfg, symbols_override=None, top_n=25, session_filter="all": calls["monitor_extended_hours"].append(
            {
                "cfg": cfg,
                "symbols_override": symbols_override,
                "top_n": top_n,
                "session_filter": session_filter,
            }
        )
        or {
            "preferred_interval": "5m",
            "fallback_interval": "1m",
            "symbols": 0,
            "preferred_written": 0,
            "fallback_written": 0,
            "alerts": 0,
            "alerts_path": str(tmp_path / "runs" / "alerts.csv"),
            "moves_df": pl.DataFrame(),
            "alerts_df": pl.DataFrame(),
            "report_html": str(tmp_path / "runs" / "report.html"),
        },
    )
    monkeypatch.setattr(
        cli,
        "build_universe",
        lambda **kwargs: calls["build_universe"].append(kwargs),
    )

    assert cli.main(["--config", str(config_path), "update"]) == 0
    assert cli.main(["--config", str(config_path), "monitor-extended-hours", "--session", "pre", "--top-n", "5"]) == 0
    assert (
        cli.main(
            [
                "--config",
                str(config_path),
                "build-universe",
                "--indices",
                "sp500",
                "--out",
                str(tmp_path / "meta" / "merged_out.csv"),
            ]
        )
        == 0
    )
    assert len(calls["update"]) == 1
    assert calls["update"][0]["symbols_override"] is None
    assert len(calls["monitor_extended_hours"]) == 1
    assert calls["monitor_extended_hours"][0]["session_filter"] == "pre"
    assert calls["monitor_extended_hours"][0]["top_n"] == 5
    assert calls["monitor_extended_hours"][0]["symbols_override"] is None
    assert len(calls["build_universe"]) == 1
    assert calls["build_universe"][0]["active_only"] is True
    assert calls["build_universe"][0]["indices"] == ["sp500"]
    assert calls["build_universe"][0]["out_path"] == str(tmp_path / "meta" / "merged_out.csv")
    assert calls["build_universe"][0]["overrides_dir"] == str(tmp_path / "meta" / "universes")
    assert calls["build_universe"][0]["ticker_overrides_path"] == tmp_path / "meta" / "ticker_overrides.csv"


def test_wrapper_crypto_defaults_can_be_disabled_cleanly():
    script = Path("scripts/run_daily_update_verify.sh").read_text(encoding="utf-8")
    assert 'CRYPTO_REFRESH_UNIVERSE="${TLD_CRYPTO_REFRESH_UNIVERSE:-1}"' in script
    assert 'CRYPTO_UPDATE="${TLD_CRYPTO_UPDATE:-1}"' in script
    assert 'VERIFY_CRYPTO="${TLD_VERIFY_CRYPTO:-1}"' in script
    assert 'CRYPTO_REPAIR="${TLD_CRYPTO_REPAIR:-1}"' in script
    assert "cleanup_lock()" in script
    assert 'if [[ "${CRYPTO_REFRESH_UNIVERSE}" == "1" ]]; then' in script
    assert 'if [[ "${CRYPTO_UPDATE}" == "1" ]]; then' in script
    assert 'if [[ "${VERIFY_CRYPTO}" == "1" ]] && [[ -d "${CRYPTO_ROOT}" ]]; then' in script
    assert 'scripts/check_crypto_status.py' in script
    assert "_write_filtered_quarantine_universe" in script
    assert "symbol_overrides_path" in script
    assert "trap 'rmdir" not in script


def test_ci_matrix_runs_installed_package_and_checks_real_pytest_exit_status():
    makefile = Path("Makefile").read_text(encoding="utf-8")

    assert '$$tmp/venv/bin/pip install -q ".[test,dev]"' in makefile
    assert 'PYTHONPATH=src $$tmp/venv/bin/python -m pytest' not in makefile
    assert '>$$log 2>&1' in makefile
    assert 'tail -3 $$log;' in makefile
    assert 'tail -20 $$log;' in makefile


def test_dedicated_crypto_wrapper_uses_check_verify_fix_check_flow():
    script = Path("scripts/run_crypto_update_verify.sh").read_text(encoding="utf-8")
    assert 'CRYPTO_PREFLIGHT_CHECK="${TLD_CRYPTO_PREFLIGHT_CHECK:-1}"' in script
    assert "cleanup_lock()" in script
    assert 'crypto_precheck_' in script
    assert 'crypto_update_' in script
    assert 'crypto_verify_fix_' in script
    assert 'crypto_postcheck_' in script
    assert 'python scripts/check_crypto_status.py' in script
    assert "trap 'rmdir" not in script
