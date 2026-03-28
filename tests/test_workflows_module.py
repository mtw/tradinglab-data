from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data.workflows as workflows


class _DummyCfg:
    def __init__(self, raw: dict[str, object]):
        self.raw = raw

    def get(self, *keys: str, default=None):
        cur: object = self.raw
        for key in keys:
            if not isinstance(cur, dict) or key not in cur:
                return default
            cur = cur[key]
        return cur


def _history_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": ["2026-03-27"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "close": [10.2],
            "adj_close": [10.2],
            "volume": [1000.0],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))


def test_update_from_config_keeps_postwrite_integrity_callable(monkeypatch, tmp_path: Path):
    parquet_root = tmp_path / "daily"
    log_path = tmp_path / "meta" / "update_log.csv"
    runs_root = tmp_path / "runs"
    intraday_root = tmp_path / "intraday"

    monkeypatch.setattr(workflows, "_load_active_symbols_from_cfg", lambda cfg, symbols_override=None: ["AAA"])
    monkeypatch.setattr(workflows, "universe_csv_path", lambda cfg: tmp_path / "meta" / "merged.csv")
    monkeypatch.setattr(workflows, "parquet_root_path", lambda cfg: parquet_root)
    monkeypatch.setattr(workflows, "update_log_path", lambda cfg: log_path)
    monkeypatch.setattr(workflows, "runs_root_path", lambda cfg: runs_root)
    monkeypatch.setattr(workflows, "intraday_root_path", lambda cfg: intraday_root)
    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", lambda symbols, **kwargs: {"AAA": _history_frame()})
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")
    monkeypatch.setattr(workflows, "append_update_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflows, "_run_intraday_update", lambda **kwargs: None)

    cfg = _DummyCfg(
        {
            "timeframe": "1d",
            "lookback_days": 30,
            "update": {
                "history_provider": "yfinance",
                "recent_provider": "yfinance",
                "recent_days": 5,
                "incremental_days": 14,
                "assert_postwrite_integrity": True,
            },
            "extended_hours": {"enabled": False},
        }
    )

    result = workflows.update_from_config(cfg)

    assert result["symbols"] == ["AAA"]
    assert (parquet_root / "AAA.parquet").exists()
