from __future__ import annotations

import json
import random
from pathlib import Path

import pandas as pd

from tradinglab_data.parquet_verify import ParquetVerifyConfig, run_parquet_sanity_checks, write_verification_summary


def test_read_universe_symbols_and_symbol_candidates(tmp_path: Path):
    csv_path = tmp_path / "universe.csv"
    csv_path.write_text("ticker\n AAA \n\nBBB\n", encoding="utf-8")

    from tradinglab_data.parquet_verify import _read_universe_symbols, _symbol_candidates

    assert _read_universe_symbols(tmp_path / "missing.csv") == []
    assert _read_universe_symbols(csv_path) == ["AAA", "BBB"]
    assert _symbol_candidates(tmp_path, "BRK.B") == [
        tmp_path / "BRK.B.parquet",
        tmp_path / "BRK-B.parquet",
        tmp_path / "BRK.B.US.parquet",
        tmp_path / "BRK-B.US.parquet",
    ]


def test_read_universe_symbols_handles_headerless_csv(tmp_path: Path):
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text("", encoding="utf-8")

    from tradinglab_data.parquet_verify import _read_universe_symbols

    assert _read_universe_symbols(csv_path) == []


def test_run_parquet_sanity_checks_missing_root(tmp_path: Path):
    summary = run_parquet_sanity_checks(ParquetVerifyConfig(root=tmp_path / "missing", universe_dir=tmp_path / "universes"))
    assert summary["ok"] is False
    assert summary["status"] == "fail"
    assert any(str(e).startswith("parquet_root_missing:") for e in summary["errors"])
    assert "config" in summary
    assert summary["config"]["sample_read_files"] == 30


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


def test_run_parquet_sanity_checks_reports_too_few_files(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"symbol": ["AAA"]}).to_csv(universe_dir / "sp500.csv", index=False)
    pd.DataFrame(
        {"date": pd.to_datetime(["2025-01-01"]), "open": [1.0], "high": [1.2], "low": [0.9], "close": [1.1]}
    ).to_parquet(root / "AAA.parquet", index=False)

    summary = run_parquet_sanity_checks(
        ParquetVerifyConfig(root=root, universe_dir=universe_dir, universes=("sp500",), min_parquet_files=2, sample_read_files=1)
    )

    assert "too_few_parquet_files:1<2" in summary["errors"]


def test_run_parquet_sanity_checks_does_not_mutate_global_random_state(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"symbol": ["AAA"]}).to_csv(universe_dir / "sp500.csv", index=False)
    pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-01"]),
            "open": [1.0],
            "high": [1.2],
            "low": [0.9],
            "close": [1.1],
        }
    ).to_parquet(root / "AAA.parquet", index=False)

    state_before = random.getstate()
    run_parquet_sanity_checks(
        ParquetVerifyConfig(root=root, universe_dir=universe_dir, universes=("sp500",), min_parquet_files=1, sample_read_files=1)
    )
    state_after = random.getstate()

    assert state_after == state_before


def test_run_parquet_sanity_checks_reports_sample_failures_and_universe_issues(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"date": pd.Series([], dtype="datetime64[ns]"), "open": pd.Series([], dtype="float64")}).to_parquet(
        root / "AAA.parquet", index=False
    )
    (root / "BROKEN.parquet").write_text("not parquet", encoding="utf-8")
    (root / "ZERO.parquet").write_bytes(b"")
    (universe_dir / "sp500.csv").write_text("symbol\nAAA\nBBB\n", encoding="utf-8")

    summary = run_parquet_sanity_checks(
        ParquetVerifyConfig(
            root=root,
            universe_dir=universe_dir,
            universes=("sp500", "missing"),
            min_parquet_files=3,
            max_zero_byte=0,
            max_missing_ratio=0.1,
            sample_read_files=5,
        )
    )

    assert summary["ok"] is False
    assert "too_few_parquet_files:3<3" not in summary["errors"]
    assert "zero_byte_files:1>0" in summary["errors"]
    assert "sample_read_failures:3" in summary["errors"]
    assert any(error.startswith("high_missing_ratio:sp500:") for error in summary["errors"])
    assert any(error.startswith("missing_or_empty_universe_csv:") for error in summary["errors"])
    assert "empty:AAA.parquet" in summary["sample_read_failures"]
    assert "empty:AAA.parquet" in summary["sample_read_failures"]
    assert any(item.startswith("read_error:BROKEN.parquet:") for item in summary["sample_read_failures"])
    assert any(item.startswith("read_error:ZERO.parquet:") for item in summary["sample_read_failures"])
    assert summary["coverage"]["sp500"]["present"] == 1
    assert summary["coverage"]["sp500"]["missing"] == 1
    assert summary["coverage"]["missing"]["symbols"] == 0


def test_run_parquet_sanity_checks_reads_flat_baseline_and_reports_json_error(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir(parents=True, exist_ok=True)
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"symbol": ["AAA"]}).to_csv(universe_dir / "sp500.csv", index=False)
    pd.DataFrame(
        {"date": pd.to_datetime(["2025-01-01"]), "open": [1.0], "high": [1.2], "low": [0.9], "close": [1.1]}
    ).to_parquet(root / "AAA.US.parquet", index=False)

    flat_baseline = tmp_path / "baseline-flat.json"
    flat_baseline.write_text(json.dumps({"file_count": 3}), encoding="utf-8")
    dropped = run_parquet_sanity_checks(
        ParquetVerifyConfig(
            root=root,
            universe_dir=universe_dir,
            universes=("sp500",),
            min_parquet_files=1,
            max_missing_ratio=0.0,
            sample_read_files=1,
            max_drop_ratio=0.1,
            baseline_summary_path=flat_baseline,
        )
    )
    assert dropped["prev_file_count"] == 3
    assert any(error.startswith("file_count_drop:") for error in dropped["errors"])

    bad_baseline = tmp_path / "baseline-bad.json"
    bad_baseline.write_text("{", encoding="utf-8")
    bad = run_parquet_sanity_checks(
        ParquetVerifyConfig(
            root=root,
            universe_dir=universe_dir,
            universes=("sp500",),
            min_parquet_files=1,
            max_missing_ratio=0.0,
            sample_read_files=1,
            baseline_summary_path=bad_baseline,
        )
    )
    assert "baseline_summary_read_error:JSONDecodeError" in bad["errors"]


def test_write_verification_summary(tmp_path: Path):
    out = tmp_path / "x" / "summary.json"
    write_verification_summary(out, {"ok": True, "n": 1})
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["ok"] is True
    assert loaded["n"] == 1
