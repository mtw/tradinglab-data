from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data._move_compute as moves


def _intraday_frame(*, with_currency: bool = True, with_volume: bool = True) -> pl.DataFrame:
    data = {
        "date": [datetime(2026, 3, 27, 12, 0), datetime(2026, 3, 27, 21, 0)],
        "close": [101.0, 105.0],
    }
    if with_volume:
        data["volume"] = [100.0, 200.0]
    if with_currency:
        data["currency"] = ["", " usd "]
    return pl.DataFrame(data)


def test_session_label_handles_boundaries_invalid_and_timezone_aware_values():
    assert moves.session_label(None) == "unknown"
    assert moves.session_label("not-a-date") == "unknown"
    assert moves.session_label(datetime(2026, 3, 27, 14, 0)) == "regular"
    assert moves.session_label(datetime(2026, 3, 27, 8, 0, tzinfo=timezone.utc)) == "pre"
    assert moves.session_label(datetime(2026, 3, 27, 14, 0, tzinfo=timezone.utc)) == "regular"
    assert moves.session_label(datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc)) == "post"
    assert moves.session_label(datetime(2026, 3, 27, 2, 0, tzinfo=timezone.utc)) == "closed"


def test_load_daily_reference_closes_reads_tail_currency_and_skips_bad_files(tmp_path: Path, monkeypatch):
    root = tmp_path / "daily"
    root.mkdir()
    pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1), datetime(2026, 1, 2)],
            "close": [10.0, 11.0],
            "currency": [None, " eur "],
        }
    ).write_parquet(root / "AAA.parquet")
    pl.DataFrame({"date": [datetime(2026, 1, 1)]}).write_parquet(root / "NO_CLOSE.parquet")
    pl.DataFrame({"date": [], "close": []}).write_parquet(root / "EMPTY.parquet")

    out = moves.load_daily_reference_closes(["AAA", "MISSING", "NO_CLOSE", "EMPTY"], root)

    assert out == {"AAA": {"close": 11.0, "currency": "EUR"}}


def test_daily_close_frame_accepts_float_and_dict_values():
    frame = moves._daily_close_frame(
        {
            "AAA": 100.0,
            "BBB": {"close": 0, "currency": "USD"},
            "CCC": {"close": 50.0, "currency": " eur "},
            "DDD": {"close": None, "currency": "USD"},
        }
    )

    assert frame.get_column("symbol").to_list() == ["AAA", "CCC"]
    assert frame.get_column("ref_currency").to_list() == [None, "EUR"]
    assert moves._daily_close_frame({}).is_empty()


def test_compute_moves_vs_close_handles_dict_inputs_missing_columns_and_currency_fallback():
    out = moves.compute_moves_vs_close({"AAA": _intraday_frame(with_currency=False, with_volume=False), "EMPTY": pl.DataFrame()}, {"AAA": {"close": 100.0, "currency": "usd"}})

    assert out.get_column("symbol").to_list() == ["AAA"]
    assert out.get_column("last_volume").to_list() == [None]
    assert out.get_column("currency").to_list() == ["USD"]
    assert out.get_column("pct_move").to_list() == [5.000000000000004]


def test_compute_moves_vs_close_without_date_column_raises_after_symbol_sort():
    frame = pl.DataFrame({"symbol": ["BBB", "AAA"], "close": [50.0, 105.0], "volume": [10.0, 20.0]})

    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        moves.compute_moves_vs_close(frame, {"AAA": 100.0, "BBB": 100.0})


def test_compute_moves_vs_close_returns_empty_for_no_data_no_refs_or_no_join():
    assert moves.compute_moves_vs_close(pl.DataFrame(), {"AAA": 1.0}).is_empty()
    assert moves.compute_moves_vs_close(pl.DataFrame({"symbol": ["AAA"], "close": [None], "date": [datetime(2026, 1, 1)]}), {"AAA": 1.0}).is_empty()
    assert moves.compute_moves_vs_close(_intraday_frame().with_columns(pl.lit("AAA").alias("symbol")), {}).is_empty()
    assert moves.compute_moves_vs_close(_intraday_frame().with_columns(pl.lit("AAA").alias("symbol")), {"BBB": 1.0}).is_empty()


def test_detect_alerts_and_summarize_gap_report_filters_and_sorts():
    frame = pl.DataFrame(
        {
            "symbol": ["AAA", "BBB", "CCC"],
            "last_price": [105.0, 98.0, 120.0],
            "ref_close": [100.0, 100.0, 100.0],
            "pct_move": [5.0, -2.0, 20.0],
            "last_ts": [datetime(2026, 1, 1)] * 3,
            "last_volume": [100.0, 1000.0, 10.0],
            "currency": ["USD"] * 3,
            "session": ["post", "post", "pre"],
        }
    )

    alerts = moves.detect_alerts(frame, threshold=3.0, min_volume=50)
    summary = moves.summarize_gap_report(frame, threshold=0, min_volume=50, top_n=1, session_filter="invalid")
    post = moves.summarize_gap_report(frame, threshold=0, min_volume=None, top_n=10, session_filter="post")

    assert alerts.get_column("symbol").to_list() == ["AAA"]
    assert summary.get_column("symbol").to_list() == ["AAA"]
    assert post.get_column("symbol").to_list() == ["AAA", "BBB"]
    assert moves.detect_alerts(pl.DataFrame(), threshold=1).is_empty()
    assert moves.summarize_gap_report(pl.DataFrame(), threshold=1).is_empty()
