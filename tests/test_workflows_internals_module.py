from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import tradinglab_data.workflows as workflows
from tradinglab_data.config import Config


def test_load_active_symbols_filters_exchange_applies_overrides_and_warns(tmp_path: Path, dummy_cfg_factory, monkeypatch, capsys):
    universe = tmp_path / "universe.csv"
    overrides = tmp_path / "ticker_overrides.csv"
    universe.write_text(
        "symbol,source,active\nAAA,index,1\nBBB,exchange,1\nBRK-B,index,1\nCCC,index,0\n",
        encoding="utf-8",
    )
    overrides.write_text("raw,yahoo\nBRK.B,BRK-B\n", encoding="utf-8")
    cfg = dummy_cfg_factory({"paths": {"universe_csv": universe, "ticker_overrides_csv": overrides}})
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: tmp_path / "shards")

    selected = workflows._load_active_symbols_from_cfg(cfg, symbols_override=["aaa", "BRK.B", "MISSING"])

    assert selected == ["AAA", "BRK-B"]
    assert "MISSING" in capsys.readouterr().out


def test_load_active_symbols_raises_when_universe_is_empty(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    universe = tmp_path / "universe.csv"
    universe.write_text("symbol\n", encoding="utf-8")
    cfg = dummy_cfg_factory({"paths": {"universe_csv": universe}})
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: tmp_path / "shards")

    with pytest.raises(ValueError, match="No universe data found"):
        workflows._load_active_symbols_from_cfg(cfg)


def test_load_active_symbols_raises_when_override_selects_nothing(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    universe = tmp_path / "universe.csv"
    universe.write_text("symbol,source,active\nAAA,index,1\n", encoding="utf-8")
    cfg = dummy_cfg_factory({"paths": {"universe_csv": universe}})
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: tmp_path / "shards")
    monkeypatch.setattr(workflows, "ticker_overrides_path", lambda cfg: None)

    with pytest.raises(SystemExit, match="No requested symbols"):
        workflows._load_active_symbols_from_cfg(cfg, symbols_override=["ZZZ"])


def test_load_intraday_research_symbols_filters_types_and_overrides(tmp_path: Path, dummy_cfg_factory, monkeypatch, capsys):
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir()
    (universe_dir / "pilot.csv").write_text(
        "symbol,source,instrument_type,active\nSPY,index,etf,1\nAAPL,index,stock,1\nEURUSD,index,fx,1\nX,exchange,stock,1\n",
        encoding="utf-8",
    )
    cfg = dummy_cfg_factory({"paths": {"parquet_root": tmp_path / "daily", "universe_dir": universe_dir, "ticker_overrides_csv": tmp_path / "missing.csv"}})
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: universe_dir)
    monkeypatch.setattr(workflows, "ticker_overrides_path", lambda cfg: tmp_path / "missing.csv")
    intraday_cfg = workflows._read_intraday_research_config(cfg)

    selected = workflows._load_intraday_research_symbols_from_cfg(
        cfg,
        intraday_cfg,
        universe="pilot",
        symbols_override=["AAPL", "MISSING"],
    )

    assert selected == ["AAPL"]
    assert "MISSING" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="No requested symbols found in selected intraday universe."):
        workflows._load_intraday_research_symbols_from_cfg(cfg, intraday_cfg, universe="pilot", symbols_override=["MISSING"])


def test_load_intraday_research_symbols_empty_default_universe_path(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    universe = tmp_path / "universe.csv"
    universe.write_text("symbol,instrument_type,active\nAAA,crypto,1\n", encoding="utf-8")
    cfg = dummy_cfg_factory({"paths": {"universe_csv": universe}})
    monkeypatch.setattr(workflows, "universe_csv_path", lambda cfg: universe)
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: tmp_path / "shards")
    monkeypatch.setattr(workflows, "ticker_overrides_path", lambda cfg: None)
    intraday_cfg = workflows._IntradayResearchConfig(
        enabled=True,
        root="/tmp/research",
        interval="5m",
        provider="yahoo",
        session="regular",
        exchange_timezone="America/New_York",
        default_universe="",
        retention_days=0,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=Path("/tmp/state.json"),
        log_repeat_cooldown_hours=1,
    )

    with pytest.raises(SystemExit, match="No symbols resolved"):
        workflows._load_intraday_research_symbols_from_cfg(cfg, intraday_cfg)


def test_load_intraday_live_symbols_file_not_found_and_empty_selection(tmp_path: Path, dummy_cfg_factory, monkeypatch):
    universe_dir = tmp_path / "universes"
    universe_dir.mkdir()
    cfg = dummy_cfg_factory({"paths": {"parquet_root": tmp_path / "daily", "universe_dir": universe_dir}})
    monkeypatch.setattr(workflows, "universe_dir_path", lambda cfg: universe_dir)
    monkeypatch.setattr(workflows, "ticker_overrides_path", lambda cfg: None)
    live_cfg = workflows._read_intraday_live_config(cfg)

    with pytest.raises(FileNotFoundError):
        workflows._load_intraday_live_symbols_from_cfg(cfg, live_cfg, universe="missing")

    (universe_dir / "live.csv").write_text("symbol,instrument_type,active\nAAA,crypto,1\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="No symbols resolved"):
        workflows._load_intraday_live_symbols_from_cfg(cfg, live_cfg, universe="live")


def test_load_intraday_live_symbols_uses_default_universe_when_name_is_blank(tmp_path: Path, dummy_cfg_factory, monkeypatch, capsys):
    universe = tmp_path / "universe.csv"
    universe.write_text("symbol,source,instrument_type\nAAA,universe,stock\nBBB,exchange,etf\n", encoding="utf-8")
    cfg = dummy_cfg_factory({"paths": {"universe_csv": universe, "universe_dir": tmp_path}})
    monkeypatch.setattr(workflows, "ticker_overrides_path", lambda cfg: None)
    live_cfg = workflows._IntradayLiveConfig(
        enabled=True,
        root=str(tmp_path / "live"),
        interval="5m",
        provider="yahoo",
        exchange_timezone="America/New_York",
        default_universe="",
        retention_days=0,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=tmp_path / "state.json",
        log_repeat_cooldown_hours=1,
    )

    assert workflows._load_intraday_live_symbols_from_cfg(cfg, live_cfg, universe=None, symbols_override=["AAA", "MISSING"]) == ["AAA"]
    assert "MISSING" in capsys.readouterr().out


def test_resolve_intraday_symbol_exclusions_matches_universe_case_insensitively(dummy_cfg_factory):
    cfg = dummy_cfg_factory(
        {
            "intraday_live": {
                "excluded_symbols": ["BASE"],
                "excluded_symbols_by_universe": {"EU_ETF": ["POLY.SW", "MVEU.L"]},
            }
        }
    )

    excluded = workflows._resolve_intraday_symbol_exclusions(cfg, section="intraday_live", universe_name="eu_etf")

    assert excluded == {"BASE", "POLY.SW", "MVEU.L"}


def test_migrate_symbol_alias_parquet_moves_daily_and_intraday(tmp_path: Path, monkeypatch, capsys):
    daily_root = tmp_path / "daily"
    intraday_root = tmp_path / "intraday"
    daily_root.mkdir()
    (intraday_root / "5m").mkdir(parents=True)
    (daily_root / "RAW.parquet").write_text("daily", encoding="utf-8")
    (intraday_root / "5m" / "RAW.parquet").write_text("intraday", encoding="utf-8")
    monkeypatch.setattr(workflows, "load_ticker_overrides", lambda path=None: {"RAW": "CANON"})

    workflows._migrate_symbol_alias_parquet(["CANON"], daily_root, intraday_root)

    assert (daily_root / "CANON.parquet").read_text(encoding="utf-8") == "daily"
    assert (intraday_root / "5m" / "CANON.parquet").read_text(encoding="utf-8") == "intraday"
    assert "migrated daily parquet" in capsys.readouterr().out


def test_migrate_symbol_alias_parquet_skips_noncanonical_and_missing_intraday_root(tmp_path: Path, monkeypatch):
    daily_root = tmp_path / "daily"
    daily_root.mkdir()
    (daily_root / "RAW.parquet").write_text("daily", encoding="utf-8")
    monkeypatch.setattr(workflows, "load_ticker_overrides", lambda path=None: {"RAW": "CANON", "OTHER": "NOPE"})

    workflows._migrate_symbol_alias_parquet(["OTHER"], daily_root, intraday_root=tmp_path / "missing")

    assert (daily_root / "RAW.parquet").exists()


def test_migrate_symbol_alias_parquet_uses_configured_override_path(tmp_path: Path, monkeypatch):
    daily_root = tmp_path / "daily"
    daily_root.mkdir()
    (daily_root / "RAW.parquet").write_text("daily", encoding="utf-8")
    seen: list[Path | None] = []
    override_path = tmp_path / "custom_overrides.csv"

    def fake_load(path=None):
        seen.append(path)
        return {"RAW": "CANON"}

    monkeypatch.setattr(workflows, "load_ticker_overrides", fake_load)

    workflows._migrate_symbol_alias_parquet(["CANON"], daily_root, ticker_overrides_csv=override_path)

    assert seen == [override_path]
    assert (daily_root / "CANON.parquet").read_text(encoding="utf-8") == "daily"


def test_write_extended_hours_artifacts_prints_top_moves(tmp_path: Path, monkeypatch, capsys):
    moves = pl.DataFrame(
        {
            "symbol": ["AAA"],
            "pct_move": [5.0],
            "ref_close": [100.0],
            "last_price": [105.0],
            "last_volume": [1000.0],
            "session": ["post"],
            "last_ts": ["2026-01-01T21:00:00"],
            "currency": ["USD"],
        }
    )
    report_path = tmp_path / "report.html"
    monkeypatch.setattr(workflows, "_run_dir", lambda runs_root: tmp_path)
    monkeypatch.setattr(workflows, "persist_extended_hours_report_html", lambda **kwargs: report_path)
    monkeypatch.setattr(workflows, "summarize_gap_report", lambda **kwargs: moves)

    out = workflows._write_extended_hours_artifacts(
        {"moves_df": moves, "alerts_df": moves, "preferred_written": 1, "fallback_written": 0, "alerts": 1, "alerts_path": "a.csv"},
        runs_root=tmp_path,
        threshold=2.0,
        min_volume=0,
    )

    assert out == str(report_path)
    assert "Extended-Hours Top Movers" in capsys.readouterr().out


def test_run_dir_and_execute_intraday_update_dispatch(tmp_path: Path, monkeypatch):
    calls: list[dict[str, object]] = []
    report_calls: list[dict[str, object]] = []
    intraday_cfg = workflows._IntradayConfig(
        enabled=True,
        root=str(tmp_path / "intraday"),
        preferred_interval="5m",
        fallback_interval="1m",
        retention_days=3,
        prepost=True,
        chunk_size=2,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=tmp_path / "state.json",
        log_repeat_cooldown_hours=1,
        pct_move_threshold=2.5,
        min_volume=100,
    )
    monkeypatch.setattr(workflows, "update_extended_hours_store", lambda **kwargs: calls.append(kwargs) or {"moves_df": pl.DataFrame(), "alerts_df": pl.DataFrame()})
    monkeypatch.setattr(workflows, "_write_extended_hours_artifacts", lambda intraday_res, **kwargs: report_calls.append(kwargs) or str(tmp_path / "report.html"))

    run_dir = workflows._run_dir(tmp_path)
    result, report = workflows._execute_intraday_update(
        symbols=["AAA"],
        runs_root=tmp_path,
        parquet_root=tmp_path / "daily",
        intraday_cfg=intraday_cfg,
        log_path=tmp_path / "log.csv",
        top_n=7,
        session_filter="post",
    )

    assert run_dir.exists()
    assert report.endswith("report.html")
    assert calls[0]["symbols"] == ["AAA"]
    assert calls[0]["alerts_path"].name == "extended_hours_alerts.csv"
    assert report_calls[0]["top_n"] == 7


def test_run_intraday_update_disabled_and_failure_paths(tmp_path: Path, monkeypatch, capsys):
    log_calls: list[tuple[Path, str, str, int]] = []
    cfg = workflows._IntradayConfig(
        enabled=False,
        root=str(tmp_path / "intraday"),
        preferred_interval="5m",
        fallback_interval="1m",
        retention_days=0,
        prepost=True,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=tmp_path / "state.json",
        log_repeat_cooldown_hours=1,
        pct_move_threshold=2,
        min_volume=0,
    )

    assert workflows._run_intraday_update(symbols=["AAA"], runs_root=tmp_path, parquet_root=tmp_path, intraday_cfg=cfg, log_path=tmp_path / "log.csv") is None

    enabled = workflows._IntradayConfig(**{**cfg.__dict__, "enabled": True})
    monkeypatch.setattr(workflows, "_execute_intraday_update", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(workflows, "append_update_log", lambda *args: log_calls.append(args))

    assert workflows._run_intraday_update(symbols=["AAA"], runs_root=tmp_path, parquet_root=tmp_path, intraday_cfg=enabled, log_path=tmp_path / "log.csv") is None
    assert log_calls[0][1] == "__extended_hours__"
    assert "extended-hours update failed" in capsys.readouterr().out
    monkeypatch.setattr(workflows, "_execute_intraday_update", lambda **kwargs: ({"ok": 1}, "report"))
    assert workflows._run_intraday_update(symbols=["AAA"], runs_root=tmp_path, parquet_root=tmp_path, intraday_cfg=enabled, log_path=tmp_path / "log.csv") == {"ok": 1}


def test_intraday_validation_helpers_and_cached_fetchers():
    live = workflows._IntradayLiveConfig(
        enabled=True,
        root="/tmp/live",
        interval="5m",
        provider="yahoo",
        exchange_timezone="America/New_York",
        default_universe="live",
        retention_days=0,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=Path("/tmp/state.json"),
        log_repeat_cooldown_hours=1,
    )
    research = workflows._IntradayResearchConfig(
        enabled=True,
        root="/tmp/research",
        interval="1m",
        provider="yahoo",
        session="regular",
        exchange_timezone="America/New_York",
        default_universe="pilot",
        retention_days=0,
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        warning_state_path=Path("/tmp/state.json"),
        log_repeat_cooldown_hours=1,
    )

    with pytest.raises(ValueError, match="interval"):
        workflows._validate_intraday_dual_store_compatibility(live_cfg=live, research_cfg=research)

    provider_mismatch = workflows._IntradayResearchConfig(**{**research.__dict__, "interval": "5m", "provider": "other"})
    with pytest.raises(ValueError, match="provider"):
        workflows._validate_intraday_dual_store_compatibility(live_cfg=live, research_cfg=provider_mismatch)

    tz_mismatch = workflows._IntradayResearchConfig(**{**research.__dict__, "interval": "5m", "exchange_timezone": "UTC"})
    with pytest.raises(ValueError, match="exchange_timezone"):
        workflows._validate_intraday_dual_store_compatibility(live_cfg=live, research_cfg=tz_mismatch)

    frame = pl.DataFrame({"date": [1]})
    assert workflows._prefetched_intraday_fetcher({"AAA": frame})(["AAA", "BBB"]) == {"AAA": frame}
    assert workflows._cached_currency_fetcher({"AAA": "USD"})("BBB") == "UNKNOWN"


def test_intraday_config_wrappers_validate_inspect_and_disabled(dummy_cfg_factory, monkeypatch):
    cfg = dummy_cfg_factory(
        {
            "paths": {"update_log_csv": "/tmp/meta/log.csv"},
            "intraday": {"enabled": False, "research_root": "/tmp/research"},
            "intraday_live": {"enabled": False, "live_root": "/tmp/live"},
        }
    )
    monkeypatch.setattr(workflows, "_load_intraday_research_symbols_from_cfg", lambda *args, **kwargs: ["AAA"])
    monkeypatch.setattr(workflows, "_load_intraday_live_symbols_from_cfg", lambda *args, **kwargs: ["AAA"])
    monkeypatch.setattr(workflows, "validate_intraday_research_store", lambda symbols, **kwargs: {"symbols": symbols, **kwargs})
    monkeypatch.setattr(workflows, "inspect_intraday_research_store", lambda symbols, **kwargs: [{"symbols": symbols, **kwargs}])
    monkeypatch.setattr(workflows, "validate_intraday_live_store", lambda symbols, **kwargs: {"symbols": symbols, **kwargs})
    monkeypatch.setattr(workflows, "inspect_intraday_live_store", lambda symbols, **kwargs: [{"symbols": symbols, **kwargs}])

    with pytest.raises(SystemExit, match="disabled"):
        workflows.intraday_research_update_from_config(cfg)
    with pytest.raises(SystemExit, match="disabled"):
        workflows.intraday_live_update_from_config(cfg)
    assert workflows.intraday_research_validate_from_config(cfg)["symbols"] == ["AAA"]
    assert workflows.intraday_research_inspect_from_config(cfg)[0]["symbols"] == ["AAA"]
    assert workflows.intraday_live_validate_from_config(cfg)["symbols"] == ["AAA"]
    assert workflows.intraday_live_inspect_from_config(cfg)[0]["symbols"] == ["AAA"]


def test_bulk_fetch_with_retry_refetches_missing_symbol(monkeypatch, tmp_path: Path, dummy_cfg_factory):
    calls: list[list[str]] = []
    frame = pl.DataFrame({"date": ["2026-01-01"], "open": [1.0]}).with_columns(pl.col("date").str.strptime(pl.Datetime))

    def fake_bulk(symbols, **kwargs):
        calls.append(list(symbols))
        return {} if len(symbols) > 1 else {symbols[0]: frame}

    monkeypatch.setattr(workflows, "fetch_yfinance_history_bulk", fake_bulk)
    cfg = workflows._UpdateConfig(
        parquet_root=tmp_path,
        interval="1d",
        lookback_days=10,
        log_path=tmp_path / "log.csv",
        chunk_size=100,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=True,
        history_provider="yfinance",
        recent_provider="yfinance",
        recent_days=5,
        incremental_days=3,
        postwrite_integrity_enabled=False,
        stooq_refresh_all=False,
        runs_root=tmp_path,
            intraday=workflows._read_intraday_config(dummy_cfg_factory({"paths": {"parquet_root": tmp_path / "daily"}, "extended_hours": {"enabled": False}})),
    )

    out = workflows._bulk_fetch_with_retry(["AAA", "BBB"], cfg=cfg, lookback_days=10, chunk_size=2, threads=True)

    assert set(out) == {"AAA", "BBB"}
    assert calls == [["AAA", "BBB"], ["AAA"], ["BBB"]]


def test_run_stooq_update_covers_empty_error_and_recent_merge_paths(monkeypatch, tmp_path: Path):
    log_calls: list[tuple] = []
    parquet_root = tmp_path / "daily"
    parquet_root.mkdir()
    pl.DataFrame(
        {
            "date": ["2026-01-01"],
            "open": [1.0],
            "high": [1.2],
            "low": [0.8],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(parquet_root / "OLD.parquet")

    cfg = workflows._UpdateConfig(
        parquet_root=parquet_root,
        interval="1d",
        lookback_days=10,
        log_path=tmp_path / "log.csv",
        chunk_size=2,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        history_provider="stooq",
        recent_provider="yfinance",
        recent_days=2,
        incremental_days=1,
        postwrite_integrity_enabled=False,
        stooq_refresh_all=False,
        runs_root=tmp_path,
        intraday=workflows._read_intraday_config(Config(raw={"paths": {"parquet_root": str(tmp_path / "daily")}, "extended_hours": {"enabled": False}})),
    )

    def fake_stooq(spec):
        if spec.symbol == "EMPTY":
            return pl.DataFrame()
        if spec.symbol == "ERR":
            raise RuntimeError("boom")
        return pl.DataFrame(
            {
                "date": ["2026-01-02"],
                "open": [2.0],
                "high": [2.2],
                "low": [1.8],
                "close": [2.1],
                "adj_close": [2.1],
                "volume": [120.0],
            }
        ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))

    monkeypatch.setattr(workflows, "fetch_stooq_history", fake_stooq)
    monkeypatch.setattr(workflows, "infer_currency_from_symbol", lambda symbol: "USD")
    monkeypatch.setattr(workflows, "ensure_currency", lambda df, cur: df if df is not None else df)
    monkeypatch.setattr(workflows, "_upsert_symbol_parquet", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        workflows,
        "fetch_yfinance_history_bulk",
        lambda symbols, **kwargs: {
            "OLD": pl.DataFrame({"date": ["2026-01-03"], "open": [3.0], "high": [3.1], "low": [2.9], "close": [3.0], "adj_close": [3.0], "volume": [100.0], "currency": ["USD"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
            "MISS": pl.DataFrame(),
            "NONE": None,
            "BAD": pl.DataFrame({"date": ["2026-01-03"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
        },
    )
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: pl.read_parquet(path) if path.exists() else None)
    monkeypatch.setattr(workflows, "currency_from_df", lambda df: "USD")
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: None if df is None or "open" not in df.columns else df)
    monkeypatch.setattr(workflows, "align_for_concat", lambda left, right, **kwargs: (left, right))
    monkeypatch.setattr(workflows, "sanitize_ohlc_df", lambda df: None if df is not None and df.height > 1 else df)
    monkeypatch.setattr(workflows, "assert_postwrite_integrity", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflows, "_run_intraday_update", lambda **kwargs: None)
    monkeypatch.setattr(workflows, "append_update_log", lambda *args: log_calls.append(args))

    workflows._run_stooq_update(cfg, ["EMPTY", "ERR", "OLD", "MISS", "NONE", "BAD"])

    assert any(call[2] == "stooq_empty_data" for call in log_calls)
    assert any("stooq_error:boom" in call[2] for call in log_calls)
    assert any(call[2] == "stooq_yf_recent_empty_after_sanitize" for call in log_calls)
    log_calls.clear()
    monkeypatch.setattr(
        workflows,
        "fetch_yfinance_history_bulk",
        lambda symbols, **kwargs: {
            "KEEP": pl.DataFrame({"date": ["2026-01-03"], "open": [3.0], "high": [3.1], "low": [2.9], "close": [3.0], "adj_close": [3.0], "volume": [100.0], "currency": ["USD"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
            "ERRWRITE": pl.DataFrame({"date": ["2026-01-03"], "open": [3.0], "high": [3.1], "low": [2.9], "close": [3.0], "adj_close": [3.0], "volume": [100.0], "currency": ["USD"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)),
            "EMPTYRECENT": pl.DataFrame(),
        },
    )
    pl.DataFrame(
        {
            "date": ["2026-01-01"],
            "open": [1.0],
            "high": [1.2],
            "low": [0.8],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(parquet_root / "KEEP.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-01"],
            "open": [1.0],
            "high": [1.2],
            "low": [0.8],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(parquet_root / "ERRWRITE.parquet")
    pl.DataFrame(
        {
            "date": ["2026-01-01"],
            "open": [1.0],
            "high": [1.2],
            "low": [0.8],
            "close": [1.1],
            "adj_close": [1.1],
            "volume": [100.0],
            "currency": ["USD"],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(parquet_root / "EMPTYRECENT.parquet")
    monkeypatch.setattr(
        workflows,
        "read_parquet_if_exists",
        lambda path: pl.DataFrame(
            {
                "date": ["2026-01-01", "2026-01-02"],
                "open": [1.0, 1.0],
                "high": [1.2, 1.2],
                "low": [0.8, 0.8],
                "close": [1.1, 1.1],
                "adj_close": [1.1, 1.1],
                "volume": [100.0, 100.0],
                "currency": ["USD", "USD"],
            }
        ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
        if path.name == "KEEP.parquet"
        else pl.read_parquet(path)
    )
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: None if df.height > 1 else df)
    workflows._run_stooq_update(cfg, ["KEEP", "EMPTYRECENT"])
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: df)
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: pl.read_parquet(path) if path.exists() else None)
    monkeypatch.setattr(workflows, "align_for_concat", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("align boom")))
    workflows._run_stooq_update(cfg, ["ERRWRITE"])
    assert any("stooq_yf_recent_error:align boom" in call[2] for call in log_calls)


def test_run_yfinance_update_and_incremental_helpers_cover_remaining_branches(monkeypatch, tmp_path: Path, capsys):
    root = tmp_path / "daily"
    root.mkdir()
    cfg = workflows._UpdateConfig(
        parquet_root=root,
        interval="1d",
        lookback_days=10,
        log_path=tmp_path / "log.csv",
        chunk_size=2,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        history_provider="yfinance",
        recent_provider="yfinance",
        recent_days=2,
        incremental_days=1,
        postwrite_integrity_enabled=False,
        stooq_refresh_all=False,
        runs_root=tmp_path,
        intraday=workflows._read_intraday_config(Config(raw={"paths": {"parquet_root": str(tmp_path / "daily")}, "extended_hours": {"enabled": False}})),
    )
    original_merge_incremental = workflows._merge_incremental_symbols
    original_fetch_strict = workflows._fetch_and_write_strict_symbols
    monkeypatch.setattr(workflows, "_fetch_and_write_new_symbols", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflows, "_merge_incremental_symbols", lambda *args, **kwargs: 2)
    monkeypatch.setattr(workflows, "_fetch_and_write_strict_symbols", lambda *args, **kwargs: None)
    monkeypatch.setattr(workflows, "_run_intraday_update", lambda **kwargs: None)
    result = workflows._run_yfinance_update(cfg, ["A", "B"])
    assert result is None
    assert "skipped unchanged existing symbols" in capsys.readouterr().out

    log_calls: list[tuple] = []
    monkeypatch.setattr(workflows, "_merge_incremental_symbols", original_merge_incremental)
    monkeypatch.setattr(workflows, "_bulk_fetch_with_retry", lambda *args, **kwargs: {"A": None, "B": pl.DataFrame({"date": ["2026-01-01"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)), "C": pl.DataFrame({"date": ["2026-01-01"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)), "D": pl.DataFrame({"date": ["2026-01-01"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))})
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: None if path.name.startswith("B") else pl.DataFrame({"date": ["2026-01-01"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)))
    monkeypatch.setattr(workflows, "resolve_currency", lambda *args, **kwargs: "USD")
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: None if df is None else df)
    monkeypatch.setattr(workflows, "_write_symbol_parquet", lambda *args, **kwargs: True)
    monkeypatch.setattr(workflows, "needs_incremental_write", lambda old, inc: False)
    monkeypatch.setattr(workflows, "align_for_concat", lambda left, right, **kwargs: (left, right))
    monkeypatch.setattr(workflows, "append_update_log", lambda *args: log_calls.append(args))

    skipped = workflows._merge_incremental_symbols(["A", "B", "C"], root=root, update_cfg=cfg, currency_cache={})
    assert skipped == 1
    assert any(call[2] == "empty_incremental" for call in log_calls)

    monkeypatch.setattr(workflows, "_write_symbol_parquet", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("write boom")))
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: None)
    workflows._merge_incremental_symbols(["D"], root=root, update_cfg=cfg, currency_cache={})
    assert any("write boom" in call[2] for call in log_calls)

    monkeypatch.setattr(workflows, "_bulk_fetch_with_retry", lambda *args, **kwargs: {"S": pl.DataFrame({"date": ["2026-01-01"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))})
    monkeypatch.setattr(workflows, "_fetch_and_write_strict_symbols", original_fetch_strict)
    monkeypatch.setattr(workflows, "_upsert_symbol_parquet", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("strict boom")))
    workflows._fetch_and_write_strict_symbols(["S"], root=root, update_cfg=cfg, currency_cache={})
    assert any("strict boom" in call[2] for call in log_calls)
    monkeypatch.setattr(workflows, "_fetch_and_write_new_symbols", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("new boom")))
    monkeypatch.setattr(workflows, "_merge_incremental_symbols", lambda *args, **kwargs: 0)
    monkeypatch.setattr(workflows, "_fetch_and_write_strict_symbols", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("strict stage boom")))
    with pytest.raises(RuntimeError, match="new boom"):
        workflows._run_yfinance_update(cfg, ["A"])

    monkeypatch.setattr(workflows, "_fetch_and_write_new_symbols", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="strict stage boom"):
        workflows._run_yfinance_update(cfg, ["A"])


def test_write_and_upsert_symbol_parquet_cover_empty_and_sort_branches(monkeypatch, tmp_path: Path):
    log_calls: list[tuple] = []
    integrity_calls: list[tuple] = []
    cfg = workflows._UpdateConfig(
        parquet_root=tmp_path,
        interval="1d",
        lookback_days=10,
        log_path=tmp_path / "log.csv",
        chunk_size=1,
        sleep_seconds=0,
        max_retries=1,
        backoff_max_seconds=1,
        threads=False,
        history_provider="yfinance",
        recent_provider="yfinance",
        recent_days=1,
        incremental_days=1,
        postwrite_integrity_enabled=False,
        stooq_refresh_all=False,
        runs_root=tmp_path,
        intraday=workflows._read_intraday_config(Config(raw={"paths": {"parquet_root": str(tmp_path / "daily")}, "extended_hours": {"enabled": False}})),
    )
    monkeypatch.setattr(workflows, "append_update_log", lambda *args: log_calls.append(args))
    monkeypatch.setattr(workflows, "assert_postwrite_integrity", lambda *args, **kwargs: integrity_calls.append(args))

    assert workflows._write_symbol_parquet(tmp_path / "A.parquet", "A", None, currency="USD", cfg=cfg, empty_reason="empty") is False

    frame = pl.DataFrame({"date": ["2026-01-02", "2026-01-01"], "open": [2.0, 1.0], "high": [2.0, 1.0], "low": [2.0, 1.0], "close": [2.0, 1.0], "adj_close": [2.0, 1.0], "volume": [1.0, 1.0], "currency": ["USD", "USD"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: frame if df is not None else None)
    assert workflows._write_symbol_parquet(tmp_path / "B.parquet", "B", frame, currency="USD", cfg=cfg, empty_reason="empty", sort_before_write=True) is True
    written = pl.read_parquet(tmp_path / "B.parquet")
    assert written.get_column("date").to_list()[0] < written.get_column("date").to_list()[1]

    assert workflows._upsert_symbol_parquet(tmp_path / "C.parquet", "C", None, currency="USD", cfg=cfg, empty_reason="empty") is False
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: None)
    assert workflows._upsert_symbol_parquet(tmp_path / "D.parquet", "D", frame, currency="USD", cfg=cfg, empty_reason="empty", sort_before_write=True) is True
    monkeypatch.setattr(workflows, "read_parquet_if_exists", lambda path: frame)
    monkeypatch.setattr(workflows, "_prepare_history_frame", lambda df, cur: None if df is frame else frame)
    assert workflows._upsert_symbol_parquet(tmp_path / "E.parquet", "E", frame, currency="USD", cfg=cfg, empty_reason="empty", sort_before_write=True) is False
    assert any(call[1] == "A" for call in log_calls)
    assert integrity_calls
