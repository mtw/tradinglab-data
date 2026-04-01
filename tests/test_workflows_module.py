from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data.workflows as workflows


def _base_cfg(dummy_cfg_factory, *, history_provider: str = "yfinance"):
    return dummy_cfg_factory(
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


def _read_symbol(path: Path) -> pl.DataFrame:
    return pl.read_parquet(path)


def test_update_from_config_keeps_postwrite_integrity_callable(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
    history_frame_factory,
):
    parquet_root, _, _, _ = patch_workflow_common_paths(workflows, symbols=["AAA"])
    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", lambda symbols, **kwargs: {"AAA": history_frame_factory()})
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")
    result = workflows.update_from_config(_base_cfg(dummy_cfg_factory))

    assert result["symbols"] == ["AAA"]
    assert (parquet_root / "AAA.parquet").exists()


def test_update_from_config_incremental_merge_dedupes_and_preserves_currency(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
    history_frame_factory,
):
    parquet_root, _, _, _ = patch_workflow_common_paths(workflows, symbols=["AAA"])
    parquet_root.mkdir(parents=True, exist_ok=True)
    history_frame_factory(
        dates=["2026-03-25", "2026-03-26"],
        close_start=10.0,
        currency="USD",
    ).write_parquet(parquet_root / "AAA.parquet")

    def fake_bulk(symbols, **kwargs):
        if kwargs.get("lookback_days") == 14:
            return {
                "AAA": history_frame_factory(
                    dates=["2026-03-26", "2026-03-27"],
                    close_start=11.0,
                )
            }
        return {}

    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", fake_bulk)
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")

    result = workflows.update_from_config(_base_cfg(dummy_cfg_factory))
    written = _read_symbol(parquet_root / "AAA.parquet").sort("date")

    assert result["symbols"] == ["AAA"]
    assert written.height == 3
    assert written.get_column("currency").to_list() == ["USD", "USD", "USD"]
    assert written.get_column("date").n_unique() == 3
    assert written.get_column("close").to_list()[-1] == 12.0


def test_update_from_config_stooq_mode_writes_history_and_merges_recent(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
    history_frame_factory,
):
    parquet_root, _, _, _ = patch_workflow_common_paths(workflows, symbols=["AAA"])
    stooq_calls: list[str] = []

    monkeypatch.setattr(
        workflows,
        "fetch_stooq_history",
        lambda spec: stooq_calls.append(spec.symbol) or history_frame_factory(dates=["2026-03-24", "2026-03-25"]),
    )
    monkeypatch.setattr(workflows, "infer_currency_from_symbol", lambda symbol: "EUR")
    monkeypatch.setattr(
        workflows,
        "fetch_yfinance_history_bulk",
        lambda symbols, **kwargs: {"AAA": history_frame_factory(dates=["2026-03-25", "2026-03-26"], close_start=20.0)},
    )

    cfg = _base_cfg(dummy_cfg_factory, history_provider="stooq")
    result = workflows.update_from_config(cfg)
    written = _read_symbol(parquet_root / "AAA.parquet").sort("date")

    assert result["symbols"] == ["AAA"]
    assert stooq_calls == ["AAA"]
    assert written.height == 3
    assert written.get_column("currency").to_list() == ["EUR", "EUR", "EUR"]
    assert written.get_column("close").to_list()[-1] == 21.0


def test_monitor_extended_hours_from_config_uses_shared_artifact_writer(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
):
    _, _, runs_root, _ = patch_workflow_common_paths(workflows, symbols=["AAA"])
    intraday_calls: list[dict[str, object]] = []

    def fake_execute_intraday_update(**kwargs):
        intraday_calls.append(kwargs)
        return (
            {
                "preferred_interval": "5m",
                "fallback_interval": "1m",
                "symbols": 1,
                "preferred_written": 1,
                "fallback_written": 0,
                "alerts": 0,
                "alerts_path": "/tmp/alerts.csv",
                "moves_df": pl.DataFrame(),
                "alerts_df": pl.DataFrame(),
            },
            "/tmp/report.html",
        )

    monkeypatch.setattr(workflows, "_execute_intraday_update", fake_execute_intraday_update)
    cfg = _base_cfg(dummy_cfg_factory)

    res = workflows.monitor_extended_hours_from_config(cfg, top_n=12, session_filter="post")

    assert res["report_html"] == "/tmp/report.html"
    assert intraday_calls == [
        {
            "symbols": ["AAA"],
            "runs_root": runs_root,
            "parquet_root": workflows.parquet_root_path(cfg),
            "intraday_cfg": workflows._read_intraday_config(cfg),
            "log_path": workflows.update_log_path(cfg),
            "top_n": 12,
            "session_filter": "post",
        }
    ]
