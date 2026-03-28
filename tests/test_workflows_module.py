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


def _history_frame(
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


def _base_cfg(*, history_provider: str = "yfinance") -> _DummyCfg:
    return _DummyCfg(
        {
            "timeframe": "1d",
            "lookback_days": 30,
            "update": {
                "history_provider": history_provider,
                "recent_provider": "yfinance",
                "recent_days": 5,
                "incremental_days": 14,
                "assert_postwrite_integrity": True,
                "stooq_refresh_all": False,
            },
            "extended_hours": {"enabled": False},
        }
    )


def _patch_common_paths(monkeypatch, tmp_path: Path, *, symbols: list[str]) -> tuple[Path, Path, Path, Path]:
    parquet_root = tmp_path / "daily"
    log_path = tmp_path / "meta" / "update_log.csv"
    runs_root = tmp_path / "runs"
    intraday_root = tmp_path / "intraday"
    monkeypatch.setattr(workflows, "_load_active_symbols_from_cfg", lambda cfg, symbols_override=None: symbols)
    monkeypatch.setattr(workflows, "universe_csv_path", lambda cfg: tmp_path / "meta" / "merged.csv")
    monkeypatch.setattr(workflows, "parquet_root_path", lambda cfg: parquet_root)
    monkeypatch.setattr(workflows, "update_log_path", lambda cfg: log_path)
    monkeypatch.setattr(workflows, "runs_root_path", lambda cfg: runs_root)
    monkeypatch.setattr(workflows, "intraday_root_path", lambda cfg: intraday_root)
    monkeypatch.setattr(workflows, "append_update_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflows, "_run_intraday_update", lambda **kwargs: None)
    monkeypatch.setattr(workflows, "assert_postwrite_integrity", lambda *args, **kwargs: None)
    return parquet_root, log_path, runs_root, intraday_root


def _read_symbol(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def test_update_from_config_keeps_postwrite_integrity_callable(monkeypatch, tmp_path: Path):
    parquet_root, _, _, _ = _patch_common_paths(monkeypatch, tmp_path, symbols=["AAA"])
    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", lambda symbols, **kwargs: {"AAA": _history_frame()})
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")
    result = workflows.update_from_config(_base_cfg())

    assert result["symbols"] == ["AAA"]
    assert (parquet_root / "AAA.parquet").exists()


def test_update_from_config_incremental_merge_dedupes_and_preserves_currency(monkeypatch, tmp_path: Path):
    parquet_root, _, _, _ = _patch_common_paths(monkeypatch, tmp_path, symbols=["AAA"])
    parquet_root.mkdir(parents=True, exist_ok=True)
    _history_frame(
        dates=["2026-03-25", "2026-03-26"],
        close_start=10.0,
        currency="USD",
    ).write_parquet(parquet_root / "AAA.parquet")

    def fake_bulk(symbols, **kwargs):
        if kwargs.get("lookback_days") == 14:
            return {
                "AAA": _history_frame(
                    dates=["2026-03-26", "2026-03-27"],
                    close_start=11.0,
                )
            }
        return {}

    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", fake_bulk)
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")

    result = workflows.update_from_config(_base_cfg())
    written = _read_symbol(parquet_root / "AAA.parquet").sort("date")

    assert result["symbols"] == ["AAA"]
    assert written.height == 3
    assert written.get_column("currency").to_list() == ["USD", "USD", "USD"]
    assert written.get_column("date").n_unique() == 3
    assert written.get_column("close").to_list()[-1] == 12.0


def test_update_from_config_stooq_mode_writes_history_and_merges_recent(monkeypatch, tmp_path: Path):
    parquet_root, _, _, _ = _patch_common_paths(monkeypatch, tmp_path, symbols=["AAA"])
    stooq_calls: list[str] = []

    monkeypatch.setattr(
        workflows,
        "fetch_stooq_history",
        lambda spec: stooq_calls.append(spec.symbol) or _history_frame(dates=["2026-03-24", "2026-03-25"]),
    )
    monkeypatch.setattr(workflows, "infer_currency_from_symbol", lambda symbol: "EUR")
    monkeypatch.setattr(
        workflows,
        "fetch_yfinance_history_bulk",
        lambda symbols, **kwargs: {"AAA": _history_frame(dates=["2026-03-25", "2026-03-26"], close_start=20.0)},
    )

    cfg = _base_cfg(history_provider="stooq")
    result = workflows.update_from_config(cfg)
    written = _read_symbol(parquet_root / "AAA.parquet").sort("date")

    assert result["symbols"] == ["AAA"]
    assert stooq_calls == ["AAA"]
    assert written.height == 3
    assert written.get_column("currency").to_list() == ["EUR", "EUR", "EUR"]
    assert written.get_column("close").to_list()[-1] == 21.0
