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


def test_normalize_yf_df_to_polars_handles_multiindex_datetime_detection_and_dedupe_fallback(monkeypatch):
    pdf = pd.DataFrame(
        {
            "when": [datetime(2026, 1, 2), datetime(2026, 1, 1)],
            "Open": [2.0, 1.0],
            "High": [3.0, 2.0],
            "Low": [1.5, 0.5],
            "Close": [2.5, 1.5],
            "Volume": [200, 100],
        }
    ).set_index(pd.Index([1, 2], name="rowid"))
    out = yf_utils.normalize_yf_df_to_polars(pdf)
    assert out.height == 2
    assert out.get_column("date").is_sorted()

    accesses = {"count": 0}
    class BoomColumns:
        def __contains__(self, item):
            return item == "date"
        def __iter__(self):
            yield "date"
        def duplicated(self):
            raise RuntimeError("no dedupe")

    class FakePD:
        def __init__(self):
            self._columns = BoomColumns()
        def copy(self):
            return self
        def reset_index(self, inplace=True):
            return None
        def rename(self, columns=None, inplace=True):
            return None
        @property
        def columns(self):
            accesses["count"] += 1
            if accesses["count"] == 1:
                raise RuntimeError("boom")
            return self._columns

    fake = FakePD()
    monkeypatch.setattr(yf_utils.pl, "from_pandas", lambda df: pl.DataFrame({"date": [datetime(2026, 1, 1)]}))
    out2 = yf_utils.normalize_yf_df_to_polars(fake)
    assert out2.height == 1


def test_normalize_yf_df_to_polars_raises_without_datetime_column():
    with pytest.raises(ValueError, match="Could not identify"):
        pdf = pd.DataFrame({"Open": [1.0]}, index=pd.Index(["not-a-date"], name="rowid"))
        yf_utils.normalize_yf_df_to_polars(pdf)


def test_share_class_fallback_is_strict_uppercase_dot_class():
    assert yf_utils.share_class_fallback("BRK.B") == "BRK-B"
    assert yf_utils.share_class_fallback("brk.b") is None
    assert yf_utils.share_class_fallback("BRK-B") is None


def test_is_rate_limit_error_variants():
    class RateLimitBoom(Exception):
        pass
    assert yf_utils.is_rate_limit_error(RateLimitBoom("ok")) is True
    assert yf_utils.is_rate_limit_error(type("RateLimitError", (Exception,), {})("x")) is True
    assert yf_utils.is_rate_limit_error(RuntimeError("rate limit exceeded")) is True
    assert yf_utils.is_rate_limit_error(RuntimeError("Too many requests")) is True
    assert yf_utils.is_rate_limit_error(RuntimeError("HTTP 429")) is True
    assert yf_utils.is_rate_limit_error(RuntimeError("other")) is False


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
    assert yf_utils.classify_yf_download_issue("random warning") is None


def test_split_bulk_download_empty_input_returns_empty():
    assert yf_utils.split_bulk_download(None, ["AAPL"]) == {}


def test_split_bulk_download_handles_multiindex_levels_exceptions_and_single_symbol_fallback(monkeypatch):
    cols0 = pd.MultiIndex.from_product([["AAPL"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]])
    pdf0 = pd.DataFrame([[1.0, 2.0, 0.5, 1.5, 1.4, 100]], columns=cols0)
    assert set(yf_utils.split_bulk_download(pdf0, ["AAPL"]).keys()) == {"AAPL"}

    cols1 = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["AAPL"]])
    pdf1 = pd.DataFrame([[1.0, 2.0, 0.5, 1.5, 1.4, 100]], columns=cols1)
    assert set(yf_utils.split_bulk_download(pdf1, ["AAPL"]).keys()) == {"AAPL"}
    assert set(yf_utils.split_bulk_download(pdf1, ["MISSING", "AAPL"]).keys()) == {"AAPL"}

    class BadXsFrame(pd.DataFrame):
        @property
        def _constructor(self):
            return BadXsFrame
        def xs(self, *args, **kwargs):
            raise RuntimeError("bad xs")

    cols_bad = pd.MultiIndex.from_product([["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["AAA"]])
    bad = BadXsFrame([[1.0, 2.0, 0.5, 1.5, 1.4, 100]], columns=cols_bad)
    assert set(yf_utils.split_bulk_download(bad, ["AAA"]).keys()) == {"AAA"}

    class BadColumnsFrame:
        def __len__(self):
            return 1
        def copy(self):
            return self
        def reset_index(self, inplace=True):
            return None
        @property
        def columns(self):
            raise RuntimeError("bad cols")

    monkeypatch.setattr(yf_utils, "normalize_yf_df_to_polars", lambda df: pl.DataFrame({"date": [datetime(2026, 1, 1)]}))
    assert yf_utils.split_bulk_download(BadColumnsFrame(), ["AAA", "BBB"]) == {}
