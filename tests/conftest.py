from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


class DummyCfg:
    def __init__(self, raw: dict[str, object]):
        self.raw = raw

    def get(self, *keys: str, default=None):
        cur: object = self.raw
        for key in keys:
            if not isinstance(cur, dict) or key not in cur:
                return default
            cur = cur[key]
        return cur


def make_history_frame(
    *,
    dates: list[str] | None = None,
    close_start: float = 10.2,
    currency: str | None = None,
) -> pl.DataFrame:
    use_dates = dates or ["2026-03-27"]
    closes = [close_start + idx for idx, _ in enumerate(use_dates)]
    frame = pl.DataFrame(
        {
            "date": use_dates,
            "open": closes,
            "high": [value + 0.5 for value in closes],
            "low": [value - 0.5 for value in closes],
            "close": closes,
            "adj_close": closes,
            "volume": [1000.0 + idx for idx, _ in enumerate(use_dates)],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
    if currency is not None:
        frame = frame.with_columns(pl.lit(currency).alias("currency"))
    return frame


@pytest.fixture
def dummy_cfg_factory():
    def _make(raw: dict[str, object]) -> DummyCfg:
        return DummyCfg(raw)

    return _make


@pytest.fixture
def history_frame_factory():
    return make_history_frame


@pytest.fixture
def patch_workflow_common_paths(monkeypatch, tmp_path: Path):
    def _patch(module, *, symbols: list[str]) -> tuple[Path, Path, Path, Path]:
        parquet_root = tmp_path / "daily"
        log_path = tmp_path / "meta" / "update_log.csv"
        runs_root = tmp_path / "runs"
        intraday_root = tmp_path / "intraday"
        monkeypatch.setattr(module, "_load_active_symbols_from_cfg", lambda cfg, symbols_override=None: symbols)
        monkeypatch.setattr(module, "universe_csv_path", lambda cfg: tmp_path / "meta" / "merged.csv")
        monkeypatch.setattr(module, "parquet_root_path", lambda cfg: parquet_root)
        monkeypatch.setattr(module, "update_log_path", lambda cfg: log_path)
        monkeypatch.setattr(module, "runs_root_path", lambda cfg: runs_root)
        monkeypatch.setattr(module, "intraday_root_path", lambda cfg: intraday_root)
        monkeypatch.setattr(module, "append_update_log", lambda *args, **kwargs: None)
        monkeypatch.setattr(module, "_run_intraday_update", lambda **kwargs: None)
        monkeypatch.setattr(module, "assert_postwrite_integrity", lambda *args, **kwargs: None)
        return parquet_root, log_path, runs_root, intraday_root

    return _patch
