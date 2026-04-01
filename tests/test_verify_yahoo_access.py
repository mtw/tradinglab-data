from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

from tests._load import load_script_module

mod = load_script_module("verify_yahoo_access")


def test_sample_symbols_is_stable_with_explicit_seed():
    symbols = [f"S{idx:02d}" for idx in range(30)]
    sample1 = mod._sample_symbols(symbols, 10, 42)
    sample2 = mod._sample_symbols(symbols, 10, 42)
    assert sample1 == sample2
    assert len(sample1) == 10


def test_probe_symbol_interval_classifies_connectivity_issue(monkeypatch):
    def fake_download(*args, **kwargs):
        print(
            "Failed to perform, curl: (6) Could not resolve host: guce.yahoo.com.",
            file=sys.stderr,
        )
        return pd.DataFrame()

    monkeypatch.setattr(mod.yf, "download", fake_download)

    result = mod.probe_symbol_interval("HYG", "1d", 30, prepost=True)

    assert result.status == "connectivity_error"
    assert result.ok is False
    assert "guce.yahoo.com" in result.issue


def test_main_writes_json_summary(monkeypatch, tmp_path: Path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "paths:",
                f"  universe_csv: \"{tmp_path / 'universe.csv'}\"",
                f"  universe_dir: \"{tmp_path / 'universes'}\"",
                f"  ticker_overrides_csv: \"{tmp_path / 'ticker_overrides.csv'}\"",
                f"  update_log_csv: \"{tmp_path / 'update_log.csv'}\"",
                f"  parquet_root: \"{tmp_path / 'parquet' / 'daily'}\"",
                f"  runs_root: \"{tmp_path / 'runs'}\"",
                f"  registry_root: \"{tmp_path / 'runs' / 'runs_registry'}\"",
                "extended_hours:",
                f"  intraday_root: \"{tmp_path / 'parquet' / 'intraday'}\"",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "universe.csv").write_text("symbol,active\nAAA,1\nBBB,1\nCCC,1\n", encoding="utf-8")
    json_out = tmp_path / "out" / "summary.json"

    monkeypatch.setattr(
        mod,
        "probe_symbol_interval",
        lambda symbol, interval, lookback_days, prepost=True: mod.ProbeResult(
            symbol=symbol,
            interval=interval,
            ok=True,
            rows=5,
            status="ok",
            issue="",
            used_fallback_symbol="",
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "verify_yahoo_access.py",
            "--config",
            str(config_path),
            "--sample-size",
            "2",
            "--intervals",
            "1d,5m",
            "--json-out",
            str(json_out),
        ],
    )

    rc = mod.main()

    assert rc == 0
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["sample_size"] == 2
    assert payload["intervals"] == ["1d", "5m"]
    assert payload["summary"]["1d"]["ok"] == 2
    assert payload["summary"]["5m"]["ok"] == 2
