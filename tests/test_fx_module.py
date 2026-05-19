from __future__ import annotations

from pathlib import Path

import pandas as pd
import polars as pl

import tradinglab_data.fx as fx_mod
from tradinglab_data.fx import (
    available_fx_pairs,
    derive_inverse_fx_frame,
    identity_fx_frame,
    load_fx_pair,
    normalize_pair,
    split_pair,
    validate_fx_pair,
    write_fx_pair,
)


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date": ["2026-03-27", "2026-03-28"],
            "open": [0.92, 0.93],
            "high": [0.94, 0.95],
            "low": [0.91, 0.92],
            "close": [0.93, 0.94],
            "provider": ["yahoo", "yahoo"],
            "pair": ["USDEUR", "USDEUR"],
            "base_currency": ["USD", "USD"],
            "quote_currency": ["EUR", "EUR"],
            "source_symbol": ["USDEUR=X", "USDEUR=X"],
            "ingested_at": ["2026-03-28T00:00:00", "2026-03-28T00:00:00"],
        }
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, strict=False),
        pl.col("ingested_at").str.strptime(pl.Datetime, strict=False),
    )


def test_normalize_pair_accepts_six_letter_pair():
    assert normalize_pair("usdEUR") == "USDEUR"


def test_split_pair_returns_source_and_target_currency():
    assert split_pair("USDEUR") == ("USD", "EUR")


def test_write_and_load_fx_pair_roundtrip(tmp_path: Path):
    write_fx_pair(_frame(), tmp_path, "USDEUR")
    loaded = load_fx_pair(tmp_path, "USDEUR")
    assert loaded.height == 2


def test_validate_fx_pair_rejects_nonpositive_close(tmp_path: Path):
    bad = _frame().with_columns(pl.lit(0.0).alias("close"))
    write_fx_pair(bad, tmp_path, "USDEUR")
    errors = validate_fx_pair(tmp_path, "USDEUR")
    assert any("invalid_rows" in error for error in errors)


def test_validate_fx_pair_rejects_mismatched_pair_column(tmp_path: Path):
    bad = _frame().with_columns(pl.lit("EURUSD").alias("pair"))
    write_fx_pair(bad, tmp_path, "USDEUR")
    errors = validate_fx_pair(tmp_path, "USDEUR")
    assert any("invalid_rows" in error for error in errors)


def test_derive_inverse_fx_frame_inverts_high_low_correctly():
    inverse = derive_inverse_fx_frame(_frame().head(1), "EURUSD")
    row = inverse.row(0, named=True)
    assert round(row["open"], 8) == round(1 / 0.92, 8)
    assert round(row["high"], 8) == round(1 / 0.91, 8)
    assert round(row["low"], 8) == round(1 / 0.94, 8)


def test_available_fx_pairs_lists_sorted_pairs(tmp_path: Path):
    write_fx_pair(_frame(), tmp_path, "USDEUR")
    write_fx_pair(_frame().with_columns(pl.lit("CHFEUR").alias("pair"), pl.lit("CHF").alias("base_currency")), tmp_path, "CHFEUR")
    assert available_fx_pairs(tmp_path) == ["CHFEUR", "USDEUR"]


def test_identity_fx_frame_has_close_one():
    frame = identity_fx_frame("EUREUR", ["2026-03-27", "2026-03-28"])
    assert frame.get_column("close").to_list() == [1.0, 1.0]


def test_fetch_yahoo_pair_accepts_multiindex_single_symbol_shape(monkeypatch):
    columns = pd.MultiIndex.from_tuples(
        [
            ("Adj Close", "CHFEUR=X"),
            ("Close", "CHFEUR=X"),
            ("High", "CHFEUR=X"),
            ("Low", "CHFEUR=X"),
            ("Open", "CHFEUR=X"),
            ("Volume", "CHFEUR=X"),
        ],
        names=["Price", "Ticker"],
    )
    frame = pd.DataFrame(
        [[1.08, 1.08, 1.09, 1.07, 1.08, 0], [1.07, 1.07, 1.08, 1.06, 1.07, 0]],
        index=pd.to_datetime(["2026-04-15", "2026-04-16"]),
        columns=columns,
    )
    monkeypatch.setattr(fx_mod.yf, "download", lambda *args, **kwargs: frame)
    out = fx_mod._fetch_yahoo_pair("CHFEUR=X")
    assert out.columns == ["date", "open", "high", "low", "close"]
    assert out.height == 2
