from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from tradinglab_data.intraday_research import (
    empty_intraday_research_frame,
    inspect_intraday_research_store,
    normalize_intraday_research_frame,
    trim_intraday_research_window,
    update_intraday_research_store,
    validate_intraday_research_store,
)


def _raw_intraday_frame(timestamps: list[str]) -> pl.DataFrame:
    values = [10.0 + idx for idx, _ in enumerate(timestamps)]
    return pl.DataFrame(
        {
            "date": timestamps,
            "open": values,
            "high": [value + 0.5 for value in values],
            "low": [value - 0.5 for value in values],
            "close": values,
            "adj_close": values,
            "volume": [1000.0 + idx for idx, _ in enumerate(values)],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))


def test_normalize_intraday_research_frame_filters_to_regular_session_and_sets_session_date():
    frame = _raw_intraday_frame(
        [
            "2026-03-27T13:25:00",
            "2026-03-27T13:30:00",
            "2026-03-27T19:55:00",
            "2026-03-27T20:00:00",
        ]
    )

    normalized = normalize_intraday_research_frame(frame, symbol="SPY", currency="USD")

    assert normalized.get_column("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-03-27T13:30:00",
        "2026-03-27T19:55:00",
    ]
    assert normalized.get_column("session_date").dt.strftime("%Y-%m-%d").to_list() == ["2026-03-27", "2026-03-27"]
    assert normalized.get_column("session").unique().to_list() == ["regular"]
    assert normalized.get_column("is_regular_session").unique().to_list() == [True]


def test_update_intraday_research_store_merges_existing_and_new_rows(tmp_path: Path):
    root = tmp_path / "intraday_research"
    existing_path = root / "5m" / "AAA.parquet"
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    normalize_intraday_research_frame(
        _raw_intraday_frame(["2026-03-27T13:30:00", "2026-03-27T13:35:00"]),
        symbol="AAA",
        currency="USD",
    ).write_parquet(existing_path)

    def fake_fetch_intraday(**kwargs):
        symbols = kwargs["symbols"]
        if symbols == ["BBB"]:
            return {"BBB": _raw_intraday_frame(["2026-03-27T13:30:00"])}
        if symbols == ["AAA"]:
            return {"AAA": _raw_intraday_frame(["2026-03-27T13:35:00", "2026-03-27T13:40:00"])}
        return {}

    result = update_intraday_research_store(
        ["AAA", "BBB"],
        research_root=root,
        fetch_intraday_fn=fake_fetch_intraday,
        fetch_currency_fn=lambda symbol: "USD",
    )

    written_aaa = pl.read_parquet(existing_path).sort("timestamp")
    written_bbb = pl.read_parquet(root / "5m" / "BBB.parquet").sort("timestamp")

    assert result["files_written"] == 2
    assert written_aaa.height == 3
    assert written_aaa.get_column("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S").to_list() == [
        "2026-03-27T13:30:00",
        "2026-03-27T13:35:00",
        "2026-03-27T13:40:00",
    ]
    assert written_bbb.height == 1


def test_validate_and_inspect_intraday_research_store_report_missing_files(tmp_path: Path):
    root = tmp_path / "intraday_research"

    inspected = inspect_intraday_research_store(["AAA"], research_root=root)
    validated = validate_intraday_research_store(["AAA"], research_root=root)

    assert inspected == [
        {
            "symbol": "AAA",
            "exists": False,
            "rows": 0,
            "start": None,
            "end": None,
            "valid": False,
            "issues": ["missing_file"],
            "path": str(root / "5m" / "AAA.parquet"),
        }
    ]
    assert not validated["ok"]
    assert validated["dirty_files"] == [str(root / "5m" / "AAA.parquet")]


def test_intraday_research_empty_invalid_and_trim_paths():
    assert empty_intraday_research_frame().is_empty()
    assert normalize_intraday_research_frame(None, symbol="AAA", currency="USD").is_empty()
    with pytest.raises(ValueError, match="Unsupported intraday research interval"):
        normalize_intraday_research_frame(_raw_intraday_frame(["2026-03-27T13:30:00"]), symbol="AAA", currency="USD", interval="1m")
    with pytest.raises(ValueError, match="Unsupported intraday research provider"):
        normalize_intraday_research_frame(_raw_intraday_frame(["2026-03-27T13:30:00"]), symbol="AAA", currency="USD", provider="other")
    with pytest.raises(ValueError, match="Unsupported intraday research session"):
        normalize_intraday_research_frame(_raw_intraday_frame(["2026-03-27T13:30:00"]), symbol="AAA", currency="USD", session="all")

    old = normalize_intraday_research_frame(_raw_intraday_frame(["2020-01-01T13:30:00"]), symbol="AAA", currency="USD")
    assert trim_intraday_research_window(old, retention_days=1).is_empty()


def test_update_intraday_research_store_empty_skipped_and_unchanged(tmp_path: Path):
    root = tmp_path / "intraday_research"

    empty_result = update_intraday_research_store([], research_root=root)
    assert empty_result["symbols"] == []

    skipped = update_intraday_research_store(
        ["AAA"],
        research_root=root,
        fetch_intraday_fn=lambda **kwargs: {},
        fetch_currency_fn=lambda symbol: "USD",
    )
    assert skipped["skipped_symbols"] == ["AAA"]

    existing_path = root / "5m" / "BBB.parquet"
    normalize_intraday_research_frame(_raw_intraday_frame(["2026-03-27T13:30:00"]), symbol="BBB", currency="USD").write_parquet(existing_path)
    unchanged = update_intraday_research_store(
        ["BBB"],
        research_root=root,
        fetch_intraday_fn=lambda **kwargs: {},
        fetch_currency_fn=lambda symbol: "USD",
    )
    assert unchanged["unchanged_symbols"] == ["BBB"]


def test_inspect_intraday_research_store_reports_invalid_existing_file(tmp_path: Path):
    root = tmp_path / "intraday_research"
    path = root / "5m" / "BAD.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame({"timestamp": ["not-a-valid-schema"]}).write_parquet(path)

    inspected = inspect_intraday_research_store(["BAD"], research_root=root)

    assert inspected[0]["exists"] is True
    assert inspected[0]["valid"] is False
    assert inspected[0]["issues"]
