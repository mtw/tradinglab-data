from __future__ import annotations

from datetime import datetime

import pandas as pd
import polars as pl
import pytest

import tradinglab_data._yf_utils as yf_utils


def test_yf_date_window_formats_lookback_window():
    start, end = yf_utils.yf_date_window(10)
    assert len(start) == 10
    assert len(end) == 10
    assert start < end


def test_coerce_standard_schema_adds_missing_casts_and_sorts():
    frame = pl.DataFrame({"date": [datetime(2026, 1, 2), datetime(2026, 1, 1)], "open": ["2.0", "1.0"]})
    out = yf_utils.coerce_standard_schema(frame)

    assert out.columns == list(yf_utils.STANDARD_PRICE_SCHEMA)
    assert out.schema["open"] == pl.Float64
    assert out.get_column("date").to_list() == [datetime(2026, 1, 1), datetime(2026, 1, 2)]
    assert out.get_column("volume").null_count() == 2


def test_normalize_yf_df_to_polars_detects_datetime_column_and_dedupes_columns():
    pdf = pd.DataFrame(
        {
            "datetime": [datetime(2026, 1, 2), datetime(2026, 1, 1)],
            "Open": [2.0, 1.0],
            "High": [3.0, 2.0],
            "Low": [1.5, 0.5],
            "Close": [2.5, 1.5],
            "Volume": [200, 100],
        }
    )

    out = yf_utils.normalize_yf_df_to_polars(pdf)

    assert out.get_column("date").to_list() == [datetime(2026, 1, 1), datetime(2026, 1, 2)]
    assert out.get_column("adj_close").null_count() == 2


def test_normalize_yf_df_to_polars_raises_without_datetime_column():
    with pytest.raises(ValueError, match="Could not identify"):
        pdf = pd.DataFrame({"Open": [1.0]}, index=pd.Index(["not-a-date"], name="rowid"))
        yf_utils.normalize_yf_df_to_polars(pdf)


def test_share_class_fallback_is_strict_uppercase_dot_class():
    assert yf_utils.share_class_fallback("BRK.B") == "BRK-B"
    assert yf_utils.share_class_fallback("brk.b") is None
    assert yf_utils.share_class_fallback("BRK-B") is None


def test_backoff_sleep_uses_exponential_delay_and_jitter(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(yf_utils.random, "uniform", lambda left, right: 0.25)
    monkeypatch.setattr(yf_utils.time, "sleep", lambda value: slept.append(value))

    delay = yf_utils.backoff_sleep(3, 11.0)

    assert delay == 11.25
    assert slept == [11.25]


def test_run_yf_download_captures_stdout_stderr_and_exception():
    def noisy_download():
        print("stdout text")
        raise RuntimeError("boom")

    frame, output, exc = yf_utils.run_yf_download(noisy_download)

    assert frame is None
    assert "stdout text" in output
    assert isinstance(exc, RuntimeError)


@pytest.mark.parametrize(
    ("output", "expected"),
    [
        ("Temporary failure in name resolution", "yahoo_connectivity_error: temporary failure in name resolution"),
        ("Name or service not known", "yahoo_connectivity_error: name or service not known"),
        ("curl: (28) timeout", "yahoo_connectivity_error: curl (28)"),
        ("possibly delisted; no timezone found", "yahoo_symbol_warning: possibly delisted or no timezone found"),
        ("", None),
    ],
)
def test_classify_yf_download_issue_variants(output: str, expected: str | None):
    assert yf_utils.classify_yf_download_issue(output) == expected


def test_split_bulk_download_empty_input_returns_empty():
    assert yf_utils.split_bulk_download(None, ["AAPL"]) == {}
