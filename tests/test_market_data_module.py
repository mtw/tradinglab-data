from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from tradinglab_data.config import Config
from tradinglab_data.exceptions import DataNotFoundError, UniverseNotFoundError
from tradinglab_data.market_data import (
    _apply_point_in_time_filter,
    _as_datetime,
    _daily_calendar,
    _daily_market_cap_frame,
    _date_bounds,
    _drop_all_null_value_columns,
    _ensure_value_columns,
    _first_column,
    _is_nullish,
    _matrix_from_long,
    _non_null_numeric_values,
    _normalize_symbol,
    _ordered_unique,
    _read_csv,
    _read_daily_adjusted_close,
    _read_index_return_frame,
    _read_market_cap_frame,
    _resolve_universe_path,
    get_adjusted_prices,
    get_index_returns,
    get_market_caps,
    get_sector_assignments,
    get_total_returns,
    get_universe_symbols,
)


def _write_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    store = tmp_path / "store"
    paths = {
        "store": store,
        "meta": store / "meta",
        "universe_dir": store / "meta" / "universes",
        "daily": store / "parquet" / "daily",
        "market_caps": store / "parquet" / "market_caps",
        "index_returns": store / "parquet" / "index_returns",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    config = tmp_path / "config.yaml"
    config.write_text(
        "\n".join(
            [
                "paths:",
                f"  store_root: {store}",
                f"  universe_csv: {paths['meta'] / 'universe_master.csv'}",
                f"  universe_dir: {paths['universe_dir']}",
                f"  parquet_root: {paths['daily']}",
                f"  market_cap_root: {paths['market_caps']}",
                f"  sector_assignments_csv: {paths['meta'] / 'sector_assignments.csv'}",
                f"  index_returns_root: {paths['index_returns']}",
                f"  runs_root: {store / 'runs'}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("TRADINGLAB_DATA_CONFIG", str(config))
    return paths


def _write_daily(root: Path, symbol: str, dates: list[str], adj_close: list[float | None]) -> None:
    close = [value if value is not None else 1.0 for value in adj_close]
    pl.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 0.5 for value in close],
            "low": [value - 0.5 for value in close],
            "close": close,
            "adj_close": adj_close,
            "volume": [1000.0 + idx for idx, _ in enumerate(dates)],
            "currency": ["USD"] * len(dates),
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(root / f"{symbol}.parquet")


def test_get_universe_symbols_filters_point_in_time_default_and_named_universe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "OLD"],
            "active": [1, 1, 1],
            "listed_date": ["2020-01-01", "2020-01-01", "2020-01-01"],
            "delisted_date": ["", "", "2021-12-31"],
        }
    ).write_csv(paths["meta"] / "universe_master.csv")
    pl.DataFrame({"symbol": ["ZZZ"], "active": [1]}).write_csv(paths["universe_dir"] / "custom.csv")

    assert get_universe_symbols(as_of="2021-06-01") == ["AAA", "BBB", "OLD"]
    assert get_universe_symbols(as_of="2022-06-01") == ["AAA", "BBB"]
    assert get_universe_symbols(universe_id="custom") == ["ZZZ"]
    with pytest.raises(UniverseNotFoundError):
        get_universe_symbols(universe_id="missing")
    assert not issubclass(UniverseNotFoundError, ValueError)


def test_market_data_internal_helpers_cover_nullish_dates_and_ordering(tmp_path: Path):
    assert _is_nullish(None) is True
    assert _is_nullish(float("nan")) is True
    assert _is_nullish("x") is False
    assert _as_datetime(None, name="x") is None
    assert _as_datetime("2026-01-02", name="x") == datetime(2026, 1, 2)
    assert _date_bounds(date(2026, 1, 1), datetime(2026, 1, 2)) == (datetime(2026, 1, 1), datetime(2026, 1, 2))
    with pytest.raises(ValueError, match="start and end are required"):
        _date_bounds(None, "2026-01-02")  # type: ignore[arg-type]
    assert _first_column(["Date", "Close"], ["date", "open"]) == "Date"
    assert _ordered_unique(["AAA", "", "AAA", "BBB"]) == ["AAA", "BBB"]
    assert _normalize_symbol(float("nan")) == ""

    csv_path = tmp_path / "frame.csv"
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    assert _read_csv(csv_path).columns == ["a", "b"]
    with pytest.raises(FileNotFoundError):
        _read_csv(tmp_path / "missing.csv")
    with pytest.raises(UniverseNotFoundError, match="default universe is not available"):
        _resolve_universe_path(Config(raw={"paths": {"universe_csv": str(tmp_path / "default.csv"), "universe_dir": str(tmp_path)}}), "default")


def test_market_data_internal_helpers_cover_empty_and_filter_branches():
    assert _matrix_from_long(pl.DataFrame(schema={"date": pl.Datetime, "symbol": pl.String, "value": pl.Float64}), value_column="value").is_empty()
    with pytest.raises(DataNotFoundError, match="missing"):
        _ensure_value_columns(pl.DataFrame({"date": [datetime(2026, 1, 1)]}), message="missing")

    frame = pl.DataFrame({"date": [datetime(2026, 1, 1)], "AAA": [None], "BBB": [1.0]})
    dropped = _drop_all_null_value_columns(frame, label="symbol")
    assert dropped.columns == ["date", "BBB"]
    assert _non_null_numeric_values(pl.DataFrame({"date": [datetime(2026, 1, 1)]})).is_empty()

    pit = pl.DataFrame({"symbol": ["AAA"], "effective_start": ["2026-01-01"], "effective_end": ["2026-12-31"]})
    filtered = _apply_point_in_time_filter(pit, datetime(2026, 6, 1), warn_current_only=False)
    assert filtered.height == 1


def test_get_universe_symbols_wraps_racy_file_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    (paths["universe_dir"] / "custom.csv").write_text("symbol\nAAA\n", encoding="utf-8")
    monkeypatch.setattr("tradinglab_data.market_data._read_csv", lambda path: (_ for _ in ()).throw(FileNotFoundError("gone")))
    with pytest.raises(UniverseNotFoundError, match="universe not found"):
        get_universe_symbols(universe_id="custom")


def test_read_csv_wraps_parser_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "frame.csv"
    csv_path.write_text("a\n1\n", encoding="utf-8")
    monkeypatch.setattr("tradinglab_data.market_data.pl.read_csv", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad csv")))
    with pytest.raises(DataNotFoundError, match="failed to read"):
        _read_csv(csv_path)


def test_market_data_reader_helpers_cover_window_and_calendar_branches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-06", "2026-01-10"], [1.0, 1.1, 1.2])
    _write_daily(paths["daily"], "SPARSE", ["2026-01-02", "2026-01-06", "2026-01-10"], [1.0, None, None])
    pl.DataFrame({"date": ["2025-01-01"], "adj_close": [1.0]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["daily"] / "OUT.parquet")
    pl.DataFrame({"date": ["2026-01-02"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["daily"] / "INCOMPLETE.parquet")
    (paths["daily"] / "BROKEN.parquet").write_text("not parquet", encoding="utf-8")
    pl.DataFrame({"adj_close": [1.0]}).write_parquet(paths["daily"] / "SCHEMA.parquet")

    incomplete = _read_daily_adjusted_close(paths["daily"], "INCOMPLETE", datetime(2026, 1, 1), datetime(2026, 1, 3))
    outside = _read_daily_adjusted_close(paths["daily"], "OUT", datetime(2026, 1, 1), datetime(2026, 1, 3))
    bad_schema = _read_daily_adjusted_close(paths["daily"], "SCHEMA", datetime(2026, 1, 1), datetime(2026, 1, 3))
    assert incomplete.is_empty()
    assert outside.is_empty()
    assert bad_schema.is_empty()

    caps_out = pl.DataFrame({"date": ["2025-01-01"], "market_cap_usd_millions": [1.0]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
    caps_daily = _daily_market_cap_frame("AAA", caps_out, pl.DataFrame(schema={"date": pl.Datetime}))
    assert caps_daily.is_empty()

    calendar = _daily_calendar(["MISSING", "BROKEN"], datetime(2026, 1, 1), datetime(2026, 1, 3))
    assert calendar.is_empty()

    sparse_returns = get_total_returns(["AAA", "SPARSE"], "2026-01-02", "2026-01-10", max_ffill=0)
    assert sparse_returns.columns == ["date", "AAA"]
    assert "dropping symbol with insufficient adjusted-price coverage: SPARSE" in caplog.text


def test_read_daily_adjusted_close_handles_missing_columns_after_read(tmp_path: Path):
    root = tmp_path / "daily"
    root.mkdir()
    path = root / "AAA.parquet"
    path.write_text("placeholder", encoding="utf-8")
    original = pl.read_parquet

    def fake_read_parquet(target, *args, **kwargs):
        if Path(target) == path:
            return pl.DataFrame({"date": [datetime(2026, 1, 1)]})
        return original(target, *args, **kwargs)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("tradinglab_data.market_data.pl.read_parquet", fake_read_parquet)
        assert _read_daily_adjusted_close(root, "AAA", datetime(2026, 1, 1), datetime(2026, 1, 3)).is_empty()


def test_market_cap_and_index_reader_helpers_cover_remaining_drop_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    pl.DataFrame({"date": ["2025-01-01"], "market_cap_usd_millions": [1.0]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["market_caps"] / "AAA.parquet")
    assert _read_market_cap_frame(paths["market_caps"], "AAA", datetime(2026, 1, 1), datetime(2026, 1, 3)).is_empty()
    assert _read_index_return_frame(paths["index_returns"], "SPX", datetime(2026, 1, 1), datetime(2026, 1, 3)).is_empty()

    pl.DataFrame({"date": ["2025-01-01"], "return": [0.01]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(
        paths["index_returns"] / "NDX.parquet"
    )
    assert _read_index_return_frame(paths["index_returns"], "NDX", datetime(2026, 1, 1), datetime(2026, 1, 3)).is_empty()
    with pytest.raises(DataNotFoundError, match="no requested index_ids could be loaded"):
        get_index_returns(["spx", "SPX", "", "UNKNOWN"], "2026-01-01", "2026-01-03")
    assert "dropping symbol with no market-cap data in requested window: AAA" in caplog.text
    assert "dropping index with no return artifact: SPX" in caplog.text
    assert "dropping index with no data in requested window: NDX" in caplog.text
    assert "dropping unsupported index_id: UNKNOWN" in caplog.text


def test_get_universe_symbols_requires_point_in_time_columns_for_as_of(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"symbol": ["AAA"], "active": [1]}).write_csv(paths["meta"] / "universe_master.csv")

    assert get_universe_symbols() == ["AAA"]
    with pytest.raises(DataNotFoundError, match="point-in-time history"):
        get_universe_symbols(as_of="2026-01-01")


def test_get_universe_symbols_rejects_empty_universe_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _write_config(tmp_path, monkeypatch)

    with pytest.raises(UniverseNotFoundError, match="non-empty"):
        get_universe_symbols(universe_id="")


def test_get_universe_symbols_rejects_missing_symbol_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"ticker": ["AAA"]}).write_csv(paths["meta"] / "universe_master.csv")

    with pytest.raises(UniverseNotFoundError, match="no symbol column"):
        get_universe_symbols()


def test_get_universe_symbols_uses_default_shard_when_primary_csv_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    (paths["meta"] / "universe_master.csv").unlink(missing_ok=True)
    pl.DataFrame({"symbol": ["AAA"], "active": [1]}).write_csv(paths["universe_dir"] / "default.csv")

    assert get_universe_symbols() == ["AAA"]


def test_get_universe_symbols_raises_when_default_universe_has_no_usable_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"symbol": ["", "BAD SYMBOL", "$CASH"], "active": [1, 1, 1]}).write_csv(paths["meta"] / "universe_master.csv")

    with pytest.raises(DataNotFoundError, match="no usable symbols"):
        get_universe_symbols()


def test_prices_and_returns_are_adjusted_aligned_and_consistent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]
    _write_daily(paths["daily"], "AAA", dates, [100.0, 105.0, 110.0, 121.0])
    _write_daily(paths["daily"], "BBB", dates, [50.0, None, 55.0, 60.5])

    prices = get_adjusted_prices(["AAA", "BBB"], "2026-01-02", "2026-01-07", max_ffill=1)
    returns = get_total_returns(["AAA", "BBB"], "2026-01-02", "2026-01-07", max_ffill=1)

    assert isinstance(prices, pl.DataFrame)
    assert prices.columns == ["date", "AAA", "BBB"]
    assert prices.get_column("date").to_list() == returns.get_column("date").to_list()
    assert prices.select(pl.col("AAA", "BBB").drop_nulls().gt(0).all()).row(0) == (True, True)
    expected_returns = prices.with_columns(pl.col("AAA", "BBB").pct_change())
    assert returns.select(["AAA", "BBB"]).slice(1).to_dict(as_series=False) == expected_returns.select(["AAA", "BBB"]).slice(1).to_dict(as_series=False)
    valid_returns = returns.select(pl.concat_list(pl.col("AAA", "BBB")).alias("r")).explode("r").drop_nulls()
    assert valid_returns.select(pl.col("r").is_between(-1, 0.5, closed="right").all()).item() is True


def test_total_returns_drop_missing_symbols_and_raise_when_none_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)

    with pytest.raises(DataNotFoundError):
        get_total_returns(["MISSING"], "2026-01-02", "2026-01-07")
    assert "dropping symbol with no daily parquet" in caplog.text


def test_get_adjusted_prices_rejects_negative_max_ffill(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-05"], [1.0, 1.1])

    with pytest.raises(ValueError, match="max_ffill must be non-negative"):
        get_adjusted_prices(["AAA"], "2026-01-02", "2026-01-05", max_ffill=-1)


def test_get_adjusted_prices_rejects_invalid_date_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-05"], [1.0, 1.1])

    with pytest.raises(ValueError, match="start must be a valid date"):
        get_adjusted_prices(["AAA"], "bad-date", "2026-01-05")

    with pytest.raises(ValueError, match="start must be before end"):
        get_adjusted_prices(["AAA"], "2026-01-05", "2026-01-05")


def test_get_adjusted_prices_drops_unreadable_and_incomplete_daily_parquet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    _write_daily(paths["daily"], "GOOD", ["2026-01-02", "2026-01-05"], [1.0, 1.1])
    pl.DataFrame({"date": ["2026-01-02"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(
        paths["daily"] / "BADSCHEMA.parquet"
    )
    (paths["daily"] / "BROKEN.parquet").write_text("not parquet", encoding="utf-8")

    prices = get_adjusted_prices(["GOOD", "BADSCHEMA", "BROKEN"], "2026-01-02", "2026-01-05")

    assert prices.columns == ["date", "GOOD"]
    assert "dropping symbol with unreadable daily parquet BADSCHEMA" in caplog.text
    assert "dropping symbol with unreadable daily parquet" in caplog.text


def test_total_returns_reject_invalid_adjusted_price_jumps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "BAD", ["2026-01-02", "2026-01-05"], [1.0, 20.0])

    with pytest.raises(ValueError, match="outside"):
        get_total_returns(["BAD"], "2026-01-02", "2026-01-05")


def test_total_returns_allows_overriding_max_daily_return(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "VOL", ["2026-01-02", "2026-01-05"], [1.0, 1.8])

    returns = get_total_returns(["VOL"], "2026-01-02", "2026-01-05", max_daily_return=1.0)

    assert returns.filter(pl.col("date") == datetime(2026, 1, 5)).get_column("VOL").item() == pytest.approx(0.8)


def test_total_returns_rejects_nonpositive_max_daily_return(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-05"], [1.0, 1.1])

    with pytest.raises(ValueError, match="max_daily_return must be positive"):
        get_total_returns(["AAA"], "2026-01-02", "2026-01-05", max_daily_return=0)


def test_get_adjusted_prices_rejects_nonpositive_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "BAD", ["2026-01-02", "2026-01-05"], [1.0, -1.0])

    with pytest.raises(ValueError, match="strictly positive"):
        get_adjusted_prices(["BAD"], "2026-01-02", "2026-01-05")


def test_get_market_caps_monthly_daily_and_invalid_frequency(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    calendar = [datetime(2026, 1, 1) + timedelta(days=idx) for idx in range(42)]
    business_days = [value for value in calendar if value.weekday() < 5][:30]
    _write_daily(paths["daily"], "AAA", [value.strftime("%Y-%m-%d") for value in business_days], list(range(100, 130)))
    pl.DataFrame(
        {
            "date": ["2025-12-31", "2026-01-31"],
            "symbol": ["AAA", "AAA"],
            "market_cap_usd_millions": [1000.0, 1100.0],
            "provider": ["fixture", "fixture"],
            "source_symbol": ["AAA", "AAA"],
            "ingested_at": ["2026-02-01", "2026-02-01"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(paths["market_caps"] / "AAA.parquet")

    monthly = get_market_caps(["AAA"], "2026-01-01", "2026-02-15", frequency="monthly")
    daily = get_market_caps(["AAA"], "2026-01-01", "2026-02-15", frequency="daily")

    assert monthly.filter(pl.col("date") == datetime(2026, 1, 31)).get_column("AAA").item() == 1100.0
    assert daily.get_column("AAA").drop_nulls().gt(0).all()
    assert daily.get_column("AAA").head(21).is_not_null().all()
    assert daily.get_column("AAA").item(21) is None
    with pytest.raises(ValueError):
        get_market_caps(["AAA"], "2026-01-01", "2026-02-15", frequency="weekly")


def test_get_market_caps_raises_when_no_requested_symbols_can_be_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _write_config(tmp_path, monkeypatch)

    with pytest.raises(DataNotFoundError, match="market-cap artifacts"):
        get_market_caps(["MISSING"], "2026-01-01", "2026-02-15")


def test_get_market_caps_rejects_nonpositive_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "date": ["2026-01-31"],
            "symbol": ["AAA"],
            "market_cap_usd_millions": [-1.0],
            "provider": ["fixture"],
            "source_symbol": ["AAA"],
            "ingested_at": ["2026-02-01"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(paths["market_caps"] / "AAA.parquet")

    with pytest.raises(ValueError, match="strictly positive"):
        get_market_caps(["AAA"], "2026-01-01", "2026-02-15")


def test_get_market_caps_reads_market_cap_column_and_drops_bad_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-05"], [10.0, 10.5])
    pl.DataFrame(
        {
            "timestamp": ["2026-01-31"],
            "market_cap": [2_500_000_000.0],
        }
    ).with_columns(pl.col("timestamp").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["market_caps"] / "AAA.parquet")
    pl.DataFrame({"bad": [1]}).write_parquet(paths["market_caps"] / "BAD.parquet")
    (paths["market_caps"] / "BROKEN.parquet").write_text("not parquet", encoding="utf-8")

    caps = get_market_caps(["AAA", "BAD", "BROKEN"], "2026-01-01", "2026-02-15")

    assert caps.filter(pl.col("date") == datetime(2026, 1, 31)).get_column("AAA").item() == 2500.0
    assert "dropping symbol with no market-cap parquet: BAD" in caplog.text or "dropping symbol with incomplete market-cap schema: BAD" in caplog.text


def test_get_market_caps_daily_drops_unreadable_calendar_symbols(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    _write_daily(paths["daily"], "AAA", ["2026-01-02", "2026-01-05"], [10.0, 10.5])
    (paths["daily"] / "BROKEN.parquet").write_text("not parquet", encoding="utf-8")
    pl.DataFrame(
        {
            "date": ["2026-01-02"],
            "market_cap_usd_millions": [1000.0],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["market_caps"] / "AAA.parquet")

    caps = get_market_caps(["BROKEN", "AAA"], "2026-01-01", "2026-01-31", frequency="daily")

    assert "AAA" in caps.columns


def test_get_sector_assignments_respects_order_vocab_and_current_only_warning(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "sector": ["Information Technology", "Financials"],
            "source": ["fixture", "fixture"],
        }
    ).write_csv(paths["meta"] / "sector_assignments.csv")

    with pytest.warns(UserWarning, match="point-in-time history is unavailable"):
        sectors = get_sector_assignments(["BBB", "AAA", "MISSING"], as_of="2026-01-01")

    assert sectors.to_dict(as_series=False) == {
        "symbol": ["BBB", "AAA", "MISSING"],
        "sector": ["Financials", "Information Technology", None],
    }


def test_get_sector_assignments_can_require_history_for_current_only_artifact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "symbol": ["AAA"],
            "sector": ["Information Technology"],
            "source": ["fixture"],
        }
    ).write_csv(paths["meta"] / "sector_assignments.csv")

    with pytest.raises(DataNotFoundError, match="point-in-time history"):
        get_sector_assignments(["AAA"], as_of="2026-01-01", require_history=True)


def test_get_sector_assignments_filters_point_in_time_history(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "symbol": ["AAA", "AAA"],
            "sector": ["Information Technology", "Communication Services"],
            "effective_start": ["2020-01-01", "2026-01-01"],
            "effective_end": ["2025-12-31", ""],
            "source": ["fixture", "fixture"],
            "ingested_at": ["2026-01-01", "2026-01-01"],
        }
    ).write_csv(paths["meta"] / "sector_assignments.csv")

    assert get_sector_assignments(["AAA"], as_of="2025-06-01").get_column("sector").item() == "Information Technology"
    assert get_sector_assignments(["AAA"], as_of="2026-06-01").get_column("sector").item() == "Communication Services"


def test_get_sector_assignments_rejects_invalid_sector_vocab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"symbol": ["AAA"], "sector": ["Not A Sector"], "source": ["fixture"]}).write_csv(paths["meta"] / "sector_assignments.csv")

    with pytest.raises(ValueError, match="fixed GICS sector vocabulary"):
        get_sector_assignments(["AAA"])


def test_get_sector_assignments_rejects_missing_columns_and_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)

    with pytest.raises(DataNotFoundError, match="artifact not found"):
        get_sector_assignments(["AAA"])

    pl.DataFrame({"symbol": ["AAA"]}).write_csv(paths["meta"] / "sector_assignments.csv")
    with pytest.raises(DataNotFoundError, match="no sector column"):
        get_sector_assignments(["AAA"])


def test_get_sector_assignments_rejects_missing_symbol_column(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"sector": ["Information Technology"]}).write_csv(paths["meta"] / "sector_assignments.csv")

    with pytest.raises(DataNotFoundError, match="no symbol column"):
        get_sector_assignments(["AAA"])


def test_get_sector_assignments_raises_when_no_requested_symbols_match(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"symbol": ["AAA"], "sector": ["Information Technology"], "source": ["fixture"]}).write_csv(paths["meta"] / "sector_assignments.csv")

    with pytest.raises(DataNotFoundError, match="no requested symbols have sector assignments"):
        get_sector_assignments(["MISSING"])


def test_get_index_returns_loads_supported_and_drops_unsupported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "index_id": ["SPX", "SPX", "SPX"],
            "total_return_level": [1000.0, 1010.0, 1000.0],
            "provider": ["fixture", "fixture", "fixture"],
            "source_symbol": ["SPXTR", "SPXTR", "SPXTR"],
            "ingested_at": ["2026-01-07"] * 3,
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    ).write_parquet(paths["index_returns"] / "SPX.parquet")
    caplog.set_level(logging.WARNING)

    returns = get_index_returns(["SPX", "UNKNOWN"], "2026-01-02", "2026-01-06")

    assert returns.columns == ["date", "SPX"]
    assert returns.filter(pl.col("date") == datetime(2026, 1, 5)).get_column("SPX").item() == pytest.approx(0.01)
    assert returns.get_column("SPX").drop_nulls().is_between(-0.5, 0.5).all()
    assert "dropping unsupported index_id: UNKNOWN" in caplog.text


def test_get_index_returns_warns_on_price_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "close": [100.0, 102.0],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["index_returns"] / "SPX.parquet")

    with pytest.warns(UserWarning, match="price-return levels"):
        returns = get_index_returns(["SPX"], "2026-01-02", "2026-01-05")

    assert returns.columns == ["date", "SPX"]


def test_get_index_returns_drops_artifacts_with_missing_date_or_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    pl.DataFrame({"close": [100.0]}).write_parquet(paths["index_returns"] / "SPX.parquet")

    with pytest.raises(DataNotFoundError, match="index_ids could be loaded"):
        get_index_returns(["SPX"], "2026-01-02", "2026-01-05")

    assert "dropping index with no date column: SPX" in caplog.text


def test_get_index_returns_drops_artifacts_with_no_return_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    pl.DataFrame({"date": ["2026-01-02"]}).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(
        paths["index_returns"] / "SPX.parquet"
    )

    with pytest.raises(DataNotFoundError, match="index_ids could be loaded"):
        get_index_returns(["SPX"], "2026-01-02", "2026-01-05")

    assert "dropping index with no return or total-return level column: SPX" in caplog.text


def test_get_index_returns_drops_unreadable_and_empty_window_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    paths = _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)
    (paths["index_returns"] / "SPX.parquet").write_text("not parquet", encoding="utf-8")
    pl.DataFrame(
        {
            "date": ["2025-01-02"],
            "return": [0.01],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False)).write_parquet(paths["index_returns"] / "NDX.parquet")

    with pytest.raises(DataNotFoundError, match="index_ids could be loaded"):
        get_index_returns(["SPX", "NDX"], "2026-01-02", "2026-01-05")

    assert "dropping index with unreadable return artifact" in caplog.text
    assert "dropping index with no data in requested window: NDX" in caplog.text
