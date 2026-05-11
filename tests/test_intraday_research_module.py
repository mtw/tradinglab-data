from __future__ import annotations

from pathlib import Path

import polars as pl

from tradinglab_data.intraday_research import (
    inspect_intraday_research_store,
    normalize_intraday_research_frame,
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
