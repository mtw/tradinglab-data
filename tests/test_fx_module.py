from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

import tradinglab_data.fx as fx_mod
from tradinglab_data.fx import (
    available_fx_pairs,
    derive_inverse_fx_frame,
    identity_fx_frame,
    load_fx_pair,
    normalize_pair,
    provider_symbol_for_pair,
    split_pair,
    sync_fx_pair_yahoo,
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


def test_normalize_currency_and_pair_reject_invalid_values():
    assert fx_mod.normalize_currency(None) == ""
    with pytest.raises(ValueError, match="Invalid FX pair"):
        normalize_pair("USD")


def test_split_pair_returns_source_and_target_currency():
    assert split_pair("USDEUR") == ("USD", "EUR")


def test_provider_symbol_for_pair_rejects_unsupported_provider():
    with pytest.raises(ValueError, match="Unsupported FX provider"):
        provider_symbol_for_pair("USDEUR", provider="other")


def test_write_and_load_fx_pair_roundtrip(tmp_path: Path):
    write_fx_pair(_frame(), tmp_path, "USDEUR")
    loaded = load_fx_pair(tmp_path, "USDEUR")
    assert loaded.height == 2


def test_load_fx_pair_raises_for_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_fx_pair(tmp_path, "USDEUR")


def test_load_fx_pair_strict_raises_for_invalid_frame(tmp_path: Path):
    write_fx_pair(_frame().with_columns(pl.lit(0.0).alias("close")), tmp_path, "USDEUR")

    with pytest.raises(ValueError, match="invalid_rows"):
        load_fx_pair(tmp_path, "USDEUR", strict=True)


def test_validate_fx_pair_rejects_nonpositive_close(tmp_path: Path):
    bad = _frame().with_columns(pl.lit(0.0).alias("close"))
    write_fx_pair(bad, tmp_path, "USDEUR")
    errors = validate_fx_pair(tmp_path, "USDEUR")
    assert any("invalid_rows" in error for error in errors)


def test_validate_fx_pair_reports_missing_file(tmp_path: Path):
    errors = validate_fx_pair(tmp_path, "USDEUR")
    assert len(errors) == 1
    assert "USDEUR.parquet" in errors[0]


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


def test_available_fx_pairs_returns_empty_for_missing_root(tmp_path: Path):
    assert available_fx_pairs(tmp_path / "missing") == []


def test_identity_fx_frame_has_close_one():
    frame = identity_fx_frame("EUREUR", ["2026-03-27", "2026-03-28"])
    assert frame.get_column("close").to_list() == [1.0, 1.0]


def test_identity_fx_frame_rejects_non_identity_pair():
    with pytest.raises(ValueError, match="Identity frame requires base == quote"):
        identity_fx_frame("USDEUR", ["2026-03-27"])


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


def test_fetch_yahoo_pair_returns_empty_frame_for_no_data(monkeypatch):
    monkeypatch.setattr(fx_mod.yf, "download", lambda *args, **kwargs: None)

    out = fx_mod._fetch_yahoo_pair("CHFEUR=X")

    assert out.is_empty()
    assert out.columns == ["date", "open", "high", "low", "close"]


def test_sync_fx_pair_yahoo_writes_direct_pair(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        fx_mod,
        "_fetch_yahoo_pair",
        lambda symbol, start=None, end=None: pl.DataFrame(
            {"date": [datetime(2026, 3, 27)], "open": [0.92], "high": [0.93], "low": [0.91], "close": [0.925]}
        ),
    )

    result = sync_fx_pair_yahoo("USDEUR", tmp_path)

    assert result["ok"] is True
    assert result["used_inverse"] is False
    assert result["source_symbol"] == "USDEUR=X"
    assert (tmp_path / "USDEUR.parquet").exists()


def test_sync_fx_pair_yahoo_uses_inverse_when_direct_missing(tmp_path: Path, monkeypatch):
    def _fetch(symbol, start=None, end=None):
        if symbol == "USDEUR=X":
            return pl.DataFrame(schema={"date": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64})
        return pl.DataFrame({"date": [datetime(2026, 3, 27)], "open": [1.08], "high": [1.09], "low": [1.07], "close": [1.08]})

    monkeypatch.setattr(fx_mod, "_fetch_yahoo_pair", _fetch)

    result = sync_fx_pair_yahoo("USDEUR", tmp_path)

    assert result["used_inverse"] is True
    assert result["source_symbol"] == "EURUSD=X inverted"


def test_sync_fx_pair_yahoo_raises_when_direct_and_inverse_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        fx_mod,
        "_fetch_yahoo_pair",
        lambda symbol, start=None, end=None: pl.DataFrame(schema={"date": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64}),
    )

    with pytest.raises(ValueError, match="No FX data returned"):
        sync_fx_pair_yahoo("USDEUR", tmp_path)


def test_sync_fx_pair_yahoo_raises_when_inverse_disabled(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        fx_mod,
        "_fetch_yahoo_pair",
        lambda symbol, start=None, end=None: pl.DataFrame(schema={"date": pl.Datetime, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64}),
    )

    with pytest.raises(ValueError, match="No FX data returned for USDEUR"):
        sync_fx_pair_yahoo("USDEUR", tmp_path, allow_inverse=False)


def test_sync_fx_pair_yahoo_rejects_invalid_provider_output(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(
        fx_mod,
        "_fetch_yahoo_pair",
        lambda symbol, start=None, end=None: pl.DataFrame(
            {"date": [datetime(2026, 3, 27)], "open": [0.0], "high": [0.93], "low": [0.91], "close": [0.925]}
        ),
    )

    with pytest.raises(ValueError, match="invalid_rows"):
        sync_fx_pair_yahoo("USDEUR", tmp_path)
