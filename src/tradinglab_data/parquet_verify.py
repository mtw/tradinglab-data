from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import json
import random

import polars as pl

from .contracts import VerifyResult


@dataclass(frozen=True)
class ParquetVerifyConfig:
    root: Path
    universe_dir: Path
    universes: tuple[str, ...] = ("atx", "dax", "mdax", "djia", "sp500")
    min_parquet_files: int = 400
    max_zero_byte: int = 0
    max_missing_ratio: float = 0.20
    sample_read_files: int = 30
    max_drop_ratio: float = 0.10
    baseline_summary_path: Path | None = None


def _read_universe_symbols(path: Path) -> list[str]:
    if not path.exists():
        return []
    out: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if not rd.fieldnames:
            return out
        key = "symbol" if "symbol" in rd.fieldnames else rd.fieldnames[0]
        for row in rd:
            s = (row.get(key) or "").strip()
            if s:
                out.append(s)
    return out


def _symbol_candidates(root: Path, symbol: str) -> list[Path]:
    return [
        root / f"{symbol}.parquet",
        root / f"{symbol.replace('.', '-')}.parquet",
        root / f"{symbol}.US.parquet",
        root / f"{symbol.replace('.', '-')}.US.parquet",
    ]


def run_parquet_sanity_checks(cfg: ParquetVerifyConfig) -> VerifyResult:
    errors: list[str] = []

    root = Path(cfg.root)
    universe_dir = Path(cfg.universe_dir)
    if not root.exists() or not root.is_dir():
        return {
            "ok": False,
            "status": "fail",
            "errors": [f"parquet_root_missing:{root}"],
            "parquet_root": str(root),
            "file_count": 0,
            "zero_byte": 0,
            "sample_read_checked": 0,
            "sample_read_failures": [],
            "coverage": {},
            "prev_file_count": None,
        }

    files = sorted(root.glob("*.parquet"))
    file_count = len(files)
    if file_count < int(cfg.min_parquet_files):
        errors.append(f"too_few_parquet_files:{file_count}<{int(cfg.min_parquet_files)}")

    zero_byte = sum(1 for p in files if p.stat().st_size == 0)
    if zero_byte > int(cfg.max_zero_byte):
        errors.append(f"zero_byte_files:{zero_byte}>{int(cfg.max_zero_byte)}")

    random.seed(42)
    sample = files if len(files) <= int(cfg.sample_read_files) else random.sample(files, int(cfg.sample_read_files))
    sample_read_failures: list[str] = []
    for p in sample:
        try:
            df = pl.read_parquet(str(p))
            if df.is_empty():
                sample_read_failures.append(f"empty:{p.name}")
        except Exception as e:
            sample_read_failures.append(f"read_error:{p.name}:{type(e).__name__}")
    if sample_read_failures:
        errors.append(f"sample_read_failures:{len(sample_read_failures)}")

    coverage: dict[str, dict[str, float | int]] = {}
    for uname in cfg.universes:
        upath = universe_dir / f"{uname}.csv"
        syms = _read_universe_symbols(upath)
        if not syms:
            coverage[uname] = {"symbols": 0, "present": 0, "missing": 0, "missing_ratio": 1.0}
            errors.append(f"missing_or_empty_universe_csv:{upath}")
            continue

        present = 0
        missing = 0
        for s in syms:
            if any(p.exists() for p in _symbol_candidates(root, s)):
                present += 1
            else:
                missing += 1
        total = len(syms)
        mr = (missing / total) if total else 1.0
        coverage[uname] = {
            "symbols": total,
            "present": present,
            "missing": missing,
            "missing_ratio": float(mr),
        }
        if mr > float(cfg.max_missing_ratio):
            errors.append(f"high_missing_ratio:{uname}:{mr:.4f}>{float(cfg.max_missing_ratio):.4f}")

    prev_file_count: int | None = None
    if cfg.baseline_summary_path and Path(cfg.baseline_summary_path).exists():
        try:
            baseline = json.loads(Path(cfg.baseline_summary_path).read_text(encoding="utf-8"))
            prev_file_count = int(baseline.get("parquet_sanity", {}).get("file_count", baseline.get("file_count", 0)))
            threshold = int((1.0 - float(cfg.max_drop_ratio)) * prev_file_count)
            if prev_file_count > 0 and file_count < threshold:
                errors.append(f"file_count_drop:{file_count} from {prev_file_count}")
        except Exception as e:
            errors.append(f"baseline_summary_read_error:{type(e).__name__}")

    return {
        "ok": len(errors) == 0,
        "status": "ok" if not errors else "fail",
        "errors": errors,
        "parquet_root": str(root),
        "file_count": file_count,
        "zero_byte": zero_byte,
        "sample_read_checked": len(sample),
        "sample_read_failures": sample_read_failures,
        "coverage": coverage,
        "prev_file_count": prev_file_count,
        "config": {
            "min_parquet_files": int(cfg.min_parquet_files),
            "max_zero_byte": int(cfg.max_zero_byte),
            "max_missing_ratio": float(cfg.max_missing_ratio),
            "sample_read_files": int(cfg.sample_read_files),
            "max_drop_ratio": float(cfg.max_drop_ratio),
            "baseline_summary_path": str(cfg.baseline_summary_path) if cfg.baseline_summary_path else "",
        },
    }


def write_verification_summary(path: Path, summary: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
