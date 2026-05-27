from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from tradinglab_data.exceptions import DataNotFoundError, UniverseNotFoundError
from tradinglab_data.market_data import (
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


def test_get_universe_symbols_requires_point_in_time_columns_for_as_of(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    pl.DataFrame({"symbol": ["AAA"], "active": [1]}).write_csv(paths["meta"] / "universe_master.csv")

    assert get_universe_symbols() == ["AAA"]
    with pytest.raises(DataNotFoundError, match="point-in-time history"):
        get_universe_symbols(as_of="2026-01-01")


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
    assert valid_returns.select(pl.col("r").is_between(-1, 10, closed="right").all()).item() is True


def test_total_returns_drop_missing_symbols_and_raise_when_none_load(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    _write_config(tmp_path, monkeypatch)
    caplog.set_level(logging.WARNING)

    with pytest.raises(DataNotFoundError):
        get_total_returns(["MISSING"], "2026-01-02", "2026-01-07")
    assert "dropping symbol with no daily parquet" in caplog.text


def test_total_returns_reject_invalid_adjusted_price_jumps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    paths = _write_config(tmp_path, monkeypatch)
    _write_daily(paths["daily"], "BAD", ["2026-01-02", "2026-01-05"], [1.0, 20.0])

    with pytest.raises(ValueError, match="outside"):
        get_total_returns(["BAD"], "2026-01-02", "2026-01-05")


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
