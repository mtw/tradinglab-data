from __future__ import annotations

from pathlib import Path

import polars as pl

import tradinglab_data.workflows as workflows


def _base_cfg(dummy_cfg_factory, *, history_provider: str = "yfinance"):
    return dummy_cfg_factory(
        {
            "timeframe": "1d",
            "lookback_days": 30,
            "paths": {
                "universe_csv": "/tmp/meta/universe.csv",
                "update_log_csv": "/tmp/meta/update_log.csv",
            },
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
    integrity_calls: list[dict[str, object]] = []
    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", lambda symbols, **kwargs: {"AAA": history_frame_factory()})
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")
    monkeypatch.setattr(
        workflows,
        "assert_postwrite_integrity",
        lambda path, symbol, **kwargs: integrity_calls.append(
            {
                "path": Path(path),
                "symbol": symbol,
                **kwargs,
            }
        ),
    )
    result = workflows.update_from_config(_base_cfg(dummy_cfg_factory))

    assert result["symbols"] == ["AAA"]
    assert (parquet_root / "AAA.parquet").exists()
    assert len(integrity_calls) == 1
    assert integrity_calls[0]["path"] == parquet_root / "AAA.parquet"
    assert integrity_calls[0]["symbol"] == "AAA"
    assert integrity_calls[0]["enabled"] is True


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


def test_read_intraday_config_defaults_to_append_only(dummy_cfg_factory):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "extended_hours": {
                "enabled": True,
            }
        }
    )

    intraday_cfg = workflows._read_intraday_config(cfg)
    assert intraday_cfg.retention_days == 0
    assert intraday_cfg.warning_state_path == Path("/tmp/meta/update_warning_state.json")


def test_backfill_extended_hours_from_config_dispatches(dummy_cfg_factory, monkeypatch):
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(workflows, "_load_active_symbols_from_cfg", lambda cfg, symbols_override=None: ["AAA"])
    monkeypatch.setattr(
        workflows,
        "backfill_intraday_interval_store",
        lambda **kwargs: calls.append(kwargs) or {"interval": kwargs["interval"], "symbols": 1, "written": 1, "root": "/tmp/intraday/5m"},
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "runs_root": "/tmp/runs",
                "update_log_csv": "/tmp/meta/update_log.csv",
            },
            "extended_hours": {
                "enabled": True,
                "intraday_root": "/tmp/intraday",
                "retention_days": 0,
            },
        }
    )

    result = workflows.backfill_extended_hours_from_config(cfg, interval="5m")

    assert result["written"] == 1
    assert calls == [
        {
            "symbols": ["AAA"],
            "intraday_root": "/tmp/intraday",
            "interval": "5m",
            "retention_days": 0,
            "prepost": True,
            "chunk_size": 20,
            "sleep_seconds": 1.0,
            "max_retries": 5,
            "backoff_max_seconds": 120.0,
            "threads": False,
            "warning_state_path": Path("/tmp/meta/update_warning_state.json"),
            "log_repeat_cooldown_hours": 24.0,
            "log_path": Path("/tmp/meta/update_log.csv"),
        }
    ]


def test_read_intraday_research_config_defaults(dummy_cfg_factory):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "intraday": {
                "enabled": True,
            },
        }
    )

    intraday_cfg = workflows._read_intraday_research_config(cfg)
    assert intraday_cfg.root == "/tmp/intraday_research"
    assert intraday_cfg.default_universe == "intraday_pilot"
    assert intraday_cfg.interval == "5m"


def test_intraday_research_update_from_config_dispatches(dummy_cfg_factory, monkeypatch):
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(workflows, "_load_intraday_research_symbols_from_cfg", lambda cfg, intraday_cfg, **kwargs: ["SPY", "QQQ"])
    monkeypatch.setattr(
        workflows,
        "update_intraday_research_store",
        lambda symbols, **kwargs: calls.append({"symbols": symbols, **kwargs})
        or {
            "interval": "5m",
            "universe": "intraday_pilot",
            "root": "/tmp/intraday_research/5m",
            "symbols": list(symbols),
            "files_written": 2,
            "rows_written": 10,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "runs_root": "/tmp/runs",
                "update_log_csv": "/tmp/meta/update_log.csv",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "intraday": {
                "enabled": True,
                "research_root": "/tmp/intraday_research",
            },
        }
    )

    result = workflows.intraday_research_update_from_config(cfg, universe="intraday_pilot", full_window=True)

    assert result["files_written"] == 2
    assert calls == [
        {
            "symbols": ["SPY", "QQQ"],
            "research_root": "/tmp/intraday_research",
            "interval": "5m",
            "provider": "yahoo",
            "session": "regular",
            "exchange_timezone": "America/New_York",
            "universe_name": "intraday_pilot",
            "retention_days": 0,
            "full_window": True,
            "chunk_size": 20,
            "sleep_seconds": 1.0,
            "max_retries": 5,
            "backoff_max_seconds": 120.0,
            "threads": False,
            "log_repeat_cooldown_hours": 24.0,
            "log_path": Path("/tmp/meta/update_log.csv"),
            "warning_state_path": Path("/tmp/meta/update_warning_state.json"),
        }
    ]


def test_read_intraday_live_config_defaults(dummy_cfg_factory):
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "intraday_live": {"enabled": True},
        }
    )
    intraday_cfg = workflows._read_intraday_live_config(cfg)
    assert intraday_cfg.root == "/tmp/intraday_live"
    assert intraday_cfg.default_universe == "intraday_live_core"
    assert intraday_cfg.interval == "5m"


def test_intraday_live_update_from_config_dispatches(dummy_cfg_factory, monkeypatch):
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(workflows, "_load_intraday_live_symbols_from_cfg", lambda cfg, intraday_cfg, **kwargs: ["SPY", "QQQ"])
    monkeypatch.setattr(
        workflows,
        "update_intraday_live_store",
        lambda symbols, **kwargs: calls.append({"symbols": symbols, **kwargs})
        or {
            "interval": "5m",
            "universe": "intraday_live_core",
            "root": "/tmp/intraday_live/5m",
            "symbols": list(symbols),
            "files_written": 2,
            "rows_written": 10,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "runs_root": "/tmp/runs",
                "update_log_csv": "/tmp/meta/update_log.csv",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "intraday_live": {
                "enabled": True,
                "live_root": "/tmp/intraday_live",
            },
        }
    )

    result = workflows.intraday_live_update_from_config(cfg, universe="intraday_live_core", full_window=True)
    assert result["files_written"] == 2
    assert calls == [
        {
            "symbols": ["SPY", "QQQ"],
            "live_root": "/tmp/intraday_live",
            "interval": "5m",
            "provider": "yahoo",
            "exchange_timezone": "America/New_York",
            "universe_name": "intraday_live_core",
            "retention_days": 0,
            "full_window": True,
            "chunk_size": 20,
            "sleep_seconds": 1.0,
            "max_retries": 5,
            "backoff_max_seconds": 120.0,
            "threads": False,
            "log_repeat_cooldown_hours": 24.0,
            "log_path": Path("/tmp/meta/update_log.csv"),
            "warning_state_path": Path("/tmp/meta/update_warning_state.json"),
        }
    ]


def test_intraday_sync_from_config_fetches_once_and_writes_both_stores(dummy_cfg_factory, monkeypatch):
    fetch_calls: list[dict[str, object]] = []
    live_calls: list[dict[str, object]] = []
    research_calls: list[dict[str, object]] = []

    monkeypatch.setattr(workflows, "_load_intraday_live_symbols_from_cfg", lambda cfg, intraday_cfg, **kwargs: ["SPY", "QQQ"])
    monkeypatch.setattr(
        workflows,
        "fetch_extended_intraday",
        lambda **kwargs: fetch_calls.append(kwargs)
        or {
            "SPY": pl.DataFrame({"date": [], "open": [], "high": [], "low": [], "close": [], "adj_close": [], "volume": []}),
            "QQQ": pl.DataFrame({"date": [], "open": [], "high": [], "low": [], "close": [], "adj_close": [], "volume": []}),
        },
    )
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "USD")
    monkeypatch.setattr(
        workflows,
        "update_intraday_live_store",
        lambda symbols, **kwargs: live_calls.append({"symbols": symbols, **kwargs})
        or {
            "interval": "5m",
            "universe": "intraday_live_core",
            "root": "/tmp/intraday_live/5m",
            "symbols": list(symbols),
            "files_written": 2,
            "rows_written": 20,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )
    monkeypatch.setattr(
        workflows,
        "update_intraday_research_store",
        lambda symbols, **kwargs: research_calls.append({"symbols": symbols, **kwargs})
        or {
            "interval": "5m",
            "universe": "intraday_live_core",
            "root": "/tmp/intraday_research/5m",
            "symbols": list(symbols),
            "files_written": 2,
            "rows_written": 10,
            "unchanged_symbols": [],
            "skipped_symbols": [],
        },
    )
    cfg = dummy_cfg_factory(
        {
            "paths": {
                "parquet_root": "/tmp/daily",
                "runs_root": "/tmp/runs",
                "update_log_csv": "/tmp/meta/update_log.csv",
                "universe_csv": "/tmp/meta/universe.csv",
            },
            "intraday": {
                "enabled": True,
                "research_root": "/tmp/intraday_research",
            },
            "intraday_live": {
                "enabled": True,
                "live_root": "/tmp/intraday_live",
            },
        }
    )

    result = workflows.intraday_sync_from_config(cfg, universe="intraday_live_core", full_window=False)

    assert result["fetched_symbols"] == 2
    assert fetch_calls == [
        {
            "symbols": ["SPY", "QQQ"],
            "interval": "5m",
            "period": "60d",
            "prepost": True,
            "chunk_size": 20,
            "sleep_seconds": 1.0,
            "max_retries": 5,
            "backoff_max_seconds": 120.0,
            "threads": False,
            "log_repeat_cooldown_hours": 24.0,
            "log_path": Path("/tmp/meta/update_log.csv"),
            "warning_state_path": Path("/tmp/meta/update_warning_state.json"),
        }
    ]
    assert live_calls[0]["universe_name"] == "intraday_live_core"
    assert research_calls[0]["universe_name"] == "intraday_live_core"
    assert callable(live_calls[0]["fetch_intraday_fn"])
    assert callable(research_calls[0]["fetch_intraday_fn"])


def test_strict_symbol_update_preserves_existing_older_rows(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
    history_frame_factory,
):
    parquet_root, _, _, _ = patch_workflow_common_paths(workflows, symbols=["BAS.VI"])
    parquet_root.mkdir(parents=True, exist_ok=True)
    history_frame_factory(
        dates=["2026-01-15", "2026-01-16"],
        close_start=8.0,
        currency="EUR",
    ).write_parquet(parquet_root / "BAS.VI.parquet")

    monkeypatch.setattr(
        workflows,
        "fetch_yfinance_history_bulk",
        lambda symbols, **kwargs: {"BAS.VI": history_frame_factory(dates=["2026-03-25", "2026-03-26"], close_start=10.0)},
    )
    monkeypatch.setattr(workflows, "fetch_symbol_currency", lambda symbol: "EUR")

    workflows.update_from_config(_base_cfg(dummy_cfg_factory))
    written = _read_symbol(parquet_root / "BAS.VI.parquet").sort("date")

    assert written.height == 4
    assert written.get_column("date").min().isoformat().startswith("2026-01-15")


def test_stooq_refresh_all_preserves_existing_older_rows(
    monkeypatch,
    patch_workflow_common_paths,
    dummy_cfg_factory,
    history_frame_factory,
):
    parquet_root, _, _, _ = patch_workflow_common_paths(workflows, symbols=["AAA"])
    parquet_root.mkdir(parents=True, exist_ok=True)
    history_frame_factory(
        dates=["2026-01-15", "2026-01-16"],
        close_start=8.0,
        currency="EUR",
    ).write_parquet(parquet_root / "AAA.parquet")

    monkeypatch.setattr(
        workflows,
        "fetch_stooq_history",
        lambda spec: history_frame_factory(dates=["2026-03-25", "2026-03-26"], close_start=10.0),
    )
    monkeypatch.setattr(workflows, "infer_currency_from_symbol", lambda symbol: "EUR")
    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", lambda symbols, **kwargs: {})

    cfg = dummy_cfg_factory(
        {
            "timeframe": "1d",
            "lookback_days": 30,
            "paths": {
                "parquet_root": str(parquet_root),
                "runs_root": "/tmp/runs",
                "update_log_csv": "/tmp/meta/update_log.csv",
            },
            "update": {
                "history_provider": "stooq",
                "recent_provider": "yfinance",
                "recent_days": 0,
                "incremental_days": 14,
                "assert_postwrite_integrity": True,
                "stooq_refresh_all": True,
            },
            "extended_hours": {"enabled": False},
        }
    )
    monkeypatch.setattr(workflows, "_load_active_symbols_from_cfg", lambda cfg, symbols_override=None: ["AAA"])

    workflows.update_from_config(cfg)
    written = _read_symbol(parquet_root / "AAA.parquet").sort("date")

    assert written.height == 4
    assert written.get_column("date").min().isoformat().startswith("2026-01-15")
