from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from tradinglab_data.parquet_verify import ParquetVerifyConfig, run_parquet_sanity_checks, write_verification_summary


def test_run_parquet_sanity_checks_missing_root(tmp_path: Path):
    summary = run_parquet_sanity_checks(ParquetVerifyConfig(root=tmp_path / "missing", universe_dir=tmp_path / "universes"))
    assert summary["ok"] is False
    assert summary["status"] == "fail"
    assert any(str(e).startswith("parquet_root_missing:") for e in summary["errors"])


def test_run_parquet_sanity_checks_success_and_baseline_drop(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"symbol": ["AAA", "BBB"]}).to_csv(universe_dir / "sp500.csv", index=False)
    for name in ["AAA.parquet", "BBB.parquet"]:
        pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "open": [1.0, 1.1],
                "high": [1.2, 1.3],
                "low": [0.9, 1.0],
                "close": [1.1, 1.2],
            }
        ).to_parquet(root / name, index=False)

    base = run_parquet_sanity_checks(
        ParquetVerifyConfig(root=root, universe_dir=universe_dir, universes=("sp500",), min_parquet_files=2, sample_read_files=2, max_missing_ratio=0.0)
    )
    assert base["ok"] is True
    assert base["file_count"] == 2
    assert base["coverage"]["sp500"]["missing_ratio"] == 0.0

    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps({"parquet_sanity": {"file_count": 10}}), encoding="utf-8")
    dropped = run_parquet_sanity_checks(
        ParquetVerifyConfig(root=root, universe_dir=universe_dir, universes=("sp500",), min_parquet_files=1, sample_read_files=1, max_missing_ratio=1.0, max_drop_ratio=0.10, baseline_summary_path=baseline_path)
    )
    assert dropped["ok"] is False
    assert any(str(e).startswith("file_count_drop:") for e in dropped["errors"])


def test_write_verification_summary(tmp_path: Path):
    out = tmp_path / "x" / "summary.json"
    write_verification_summary(out, {"ok": True, "n": 1})
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["ok"] is True
    assert loaded["n"] == 1
