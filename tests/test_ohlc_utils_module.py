from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

import tradinglab_data._ohlc_utils as ohlc


def _frame(dates: list[str] | None = None, *, currency: str | None = None) -> pl.DataFrame:
    use_dates = dates or ["2026-01-01", "2026-01-02"]
    values = [10.0 + idx for idx, _ in enumerate(use_dates)]
    out = pl.DataFrame(
        {
            "date": use_dates,
            "open": values,
            "high": [value + 1.0 for value in values],
            "low": [value - 1.0 for value in values],
            "close": values,
            "adj_close": values,
            "volume": [100.0 + idx for idx, _ in enumerate(use_dates)],
        }
    ).with_columns(pl.col("date").str.strptime(pl.Datetime, strict=False))
    if currency is not None:
        out = out.with_columns(pl.lit(currency).alias("currency"))
    return out


def test_scalar_eq_handles_none_numbers_and_fallback_strings():
    assert ohlc.scalar_eq(None, None)
    assert not ohlc.scalar_eq(None, 1)
    assert ohlc.scalar_eq("1.0000000000001", 1.0)
    assert ohlc.scalar_eq("abc", "abc")
    assert not ohlc.scalar_eq("abc", "def")


def test_currency_helpers_resolve_from_frame_fetcher_and_cache():
    cache: dict[str, str] = {}
    frame = _frame(currency=" usd ")

    assert ohlc.currency_from_df(frame) == "USD"
    assert ohlc.resolve_currency("AAA", lambda symbol: "EUR", df_hint=frame, cache=cache) == "USD"
    assert ohlc.resolve_currency("AAA", lambda symbol: "EUR", cache=cache) == "USD"
    assert ohlc.resolve_currency("BBB", lambda symbol: None, cache=cache) == "UNKNOWN"
    assert ohlc.currency_from_df(_frame(currency="")) is None
    assert ohlc.currency_from_df(pl.DataFrame({"currency": [None]})) is None
    assert ohlc.currency_from_df(_frame().drop("date").drop("open").drop("high").drop("low").drop("close").drop("adj_close").drop("volume")) is None
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("tradinglab_data._ohlc_utils.pl.DataFrame.select", lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad")))
        assert ohlc.currency_from_df(frame) is None


def test_ensure_currency_fills_missing_and_postprocesses():
    frame = _frame().with_columns(pl.lit("").alias("currency"))
    out = ohlc.ensure_currency(frame, "eur", postprocess=lambda df: df.with_columns(pl.lit("x").alias("tag")))

    assert out is not None
    assert out.get_column("currency").to_list() == ["EUR", "EUR"]
    assert out.get_column("tag").to_list() == ["x", "x"]
    assert ohlc.ensure_currency(None, "USD") is None
    existing = _frame(currency="usd")
    assert ohlc.ensure_currency(existing, "EUR").get_column("currency").to_list() == ["usd", "usd"]


def test_align_for_concat_adds_missing_columns_casts_and_orders():
    left = _frame(["2026-01-01"]).drop("adj_close")
    right = _frame(["2026-01-02"]).with_columns(pl.col("volume").cast(pl.Int64), pl.lit("note").alias("extra"))

    left_out, right_out = ohlc.align_for_concat(
        left,
        right,
        schema={"date": pl.Datetime, "adj_close": pl.Float64, "volume": pl.Float64},
        preferred_columns=["date", "open", "adj_close", "volume"],
        postprocess=lambda df: df.with_columns(pl.lit("USD").alias("currency")),
    )

    assert left_out.columns[:4] == ["date", "open", "adj_close", "volume"]
    assert right_out.schema["volume"] == pl.Float64
    assert left_out.get_column("currency").to_list() == ["USD"]
    assert "extra" in right_out.columns
    a, b = ohlc.align_for_concat(pl.DataFrame({"x": [1]}), pl.DataFrame({"y": [2]}), schema={}, preferred_columns=None)
    assert a.columns == ["x", "y"]
    assert b.columns == ["x", "y"]


def test_needs_incremental_write_covers_empty_newer_changed_and_malformed():
    old = _frame(["2026-01-01", "2026-01-02"])
    same = _frame(["2026-01-02"])
    newer = _frame(["2026-01-03"])
    changed = _frame(["2026-01-02"], currency="USD").with_columns(pl.lit(99.0).alias("close"))

    assert ohlc.needs_incremental_write(None, same)
    assert not ohlc.needs_incremental_write(old, None)
    assert ohlc.needs_incremental_write(old, newer)
    assert ohlc.needs_incremental_write(old.with_columns(pl.lit("USD").alias("currency")), changed)
    assert ohlc.needs_incremental_write(old.drop("date"), same)
    assert ohlc.needs_incremental_write(old, _frame(["2025-12-31"])) is False
    same_last = old.tail(1).with_columns(pl.lit(None).alias("currency"))
    assert ohlc.needs_incremental_write(old.with_columns(pl.lit(None).alias("currency")), same_last) is False
    assert ohlc.needs_incremental_write(pl.DataFrame({"date": [None], "open": [1.0]}), pl.DataFrame({"date": [datetime(2026, 1, 1)], "open": [1.0]})) is True
    assert ohlc.needs_incremental_write(
        pl.DataFrame({"date": [datetime(2026, 1, 1)]}),
        pl.DataFrame({"date": [datetime(2026, 1, 1)]}),
        compare_columns=[],
    ) is False
    assert ohlc.needs_incremental_write(
        pl.DataFrame({"date": [datetime(2026, 1, 1)], "open": [1.0]}),
        pl.DataFrame({"date": [datetime(2026, 1, 1)], "open": [1.0]}),
        compare_values=lambda a, b: (_ for _ in ()).throw(RuntimeError("cmp")),
    ) is True
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("tradinglab_data._ohlc_utils.pl.DataFrame.select", lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad max")))
        assert ohlc.needs_incremental_write(old, same) is True
    assert ohlc.needs_incremental_write(pl.DataFrame({"date": [None]}), same) is True


def test_sanitize_and_quality_counts_drop_bad_rows_and_count_issues():
    frame = pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1), datetime(2026, 1, 1), None, datetime(2026, 1, 3)],
            "open": [10.0, 10.0, 10.0, -1.0],
            "high": [11.0, 9.0, 11.0, 1.0],
            "low": [9.0, 10.0, 9.0, 0.5],
            "close": [10.5, 10.5, None, 1.0],
        }
    )

    sanitized = ohlc.sanitize_ohlc_df(frame)
    quality = ohlc.ohlc_quality_counts(frame)

    assert sanitized is not None
    assert sanitized.height == 1
    assert quality == {"null_ohlc": 1, "bad_ohlc": 2, "dup_dates": 1}
    assert ohlc.ohlc_quality_counts(pl.DataFrame({"date": [datetime(2026, 1, 1)]})) == {
        "null_ohlc": 1,
        "bad_ohlc": 1,
        "dup_dates": 1,
    }
    assert ohlc.sanitize_ohlc_df(pl.DataFrame({"x": [1]})).to_dicts() == [{"x": 1}]
    assert ohlc.sanitize_ohlc_df(pl.DataFrame()).is_empty()
    assert ohlc.ohlc_quality_counts(pl.DataFrame()) == {"null_ohlc": 0, "bad_ohlc": 0, "dup_dates": 0}


def test_assert_postwrite_integrity_logs_and_raises(tmp_path: Path):
    log_calls: list[tuple[Path, str, str, int]] = []
    bad = pl.DataFrame(
        {
            "date": [datetime(2026, 1, 1)],
            "open": [10.0],
            "high": [8.0],
            "low": [9.0],
            "close": [10.0],
        }
    )

    with pytest.raises(RuntimeError, match="postwrite_integrity_failed"):
        ohlc.assert_postwrite_integrity(
            tmp_path / "AAA.parquet",
            "AAA",
            enabled=True,
            read_frame=lambda path: bad,
            append_log=lambda *args: log_calls.append(args),
            log_path=tmp_path / "update_log.csv",
        )

    assert "bad_ohlc=1" in log_calls[0][2]
    ohlc.assert_postwrite_integrity(
        tmp_path / "AAA.parquet",
        "AAA",
        enabled=False,
        read_frame=lambda path: bad,
        append_log=lambda *args: log_calls.append(args),
        log_path=tmp_path / "update_log.csv",
    )
    good = _frame()
    ohlc.assert_postwrite_integrity(
        tmp_path / "AAA.parquet",
        "AAA",
        enabled=True,
        read_frame=lambda path: good,
        append_log=lambda *args: log_calls.append(args),
        log_path=tmp_path / "update_log.csv",
    )
