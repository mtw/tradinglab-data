from __future__ import annotations

from pathlib import Path

import polars as pl

from .contracts import (
    EXCHANGE_DEFAULT_COLUMNS,
    SYMBOL_MASTER_COLUMNS,
    SYMBOL_MASTER_OPTIONAL_COLUMNS,
    SYMBOL_OVERRIDE_COLUMNS,
)
from .schema import validate_symbol_master_frame


def normalize_currency(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_symbol(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def normalize_fx_pair(pair: object) -> str:
    return normalize_currency(pair)


def make_fx_pair_to_base(asset_currency: str, base_currency: str) -> str:
    return normalize_currency(asset_currency) + normalize_currency(base_currency)


def _read_csv(path: str | Path) -> pl.DataFrame:
    return pl.read_csv(str(path))


def _empty_frame(columns: tuple[str, ...]) -> pl.DataFrame:
    return pl.DataFrame({column: pl.Series(name=column, values=[], dtype=pl.String) for column in columns})


def _normalize_string_columns(df: pl.DataFrame, columns: list[str], *, uppercase: bool = False) -> pl.DataFrame:
    expressions = []
    for column in columns:
        if column not in df.columns:
            continue
        expr = pl.col(column).cast(pl.String, strict=False).fill_null("").str.strip_chars()
        if uppercase:
            expr = expr.str.to_uppercase()
        expressions.append(expr.alias(column))
    return df.with_columns(expressions) if expressions else df


def load_exchange_defaults(path: str | Path) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    df = _read_csv(p)
    if df.is_empty():
        return _empty_frame(EXCHANGE_DEFAULT_COLUMNS)
    for column in EXCHANGE_DEFAULT_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit("").alias(column))
    df = _normalize_string_columns(
        df,
        ["exchange", "country", "default_asset_currency", "default_tax_country", "default_asset_class", "notes"],
        uppercase=False,
    )
    df = df.with_columns(
        pl.col("exchange").str.to_uppercase(),
        pl.col("country").str.to_uppercase(),
        pl.col("default_asset_currency").str.to_uppercase(),
        pl.col("default_tax_country").str.to_uppercase(),
        pl.col("default_lot_size").cast(pl.Float64, strict=False),
        pl.col("default_price_multiplier").cast(pl.Float64, strict=False),
    )
    return df.select([c for c in EXCHANGE_DEFAULT_COLUMNS if c in df.columns] + [c for c in df.columns if c not in EXCHANGE_DEFAULT_COLUMNS])


def load_symbol_overrides(path: str | Path) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        return _empty_frame(SYMBOL_OVERRIDE_COLUMNS)
    df = _read_csv(p)
    if df.is_empty():
        return _empty_frame(SYMBOL_OVERRIDE_COLUMNS)
    for column in SYMBOL_OVERRIDE_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit("").alias(column))
    df = _normalize_string_columns(
        df,
        ["symbol", "exchange", "country", "asset_currency", "base_listing_currency", "tax_country", "asset_class", "fx_pair_to_base", "notes"],
        uppercase=False,
    )
    df = df.with_columns(
        pl.col("symbol").str.to_uppercase(),
        pl.col("exchange").str.to_uppercase(),
        pl.col("country").str.to_uppercase(),
        pl.col("asset_currency").str.to_uppercase(),
        pl.col("base_listing_currency").str.to_uppercase(),
        pl.col("tax_country").str.to_uppercase(),
        pl.col("fx_pair_to_base").str.to_uppercase(),
        pl.col("lot_size").cast(pl.Float64, strict=False),
        pl.col("price_multiplier").cast(pl.Float64, strict=False),
    )
    return df.select([c for c in SYMBOL_OVERRIDE_COLUMNS if c in df.columns] + [c for c in df.columns if c not in SYMBOL_OVERRIDE_COLUMNS])


def load_symbol_master_frame(path: str | Path, *, strict: bool = True) -> pl.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    df = _read_csv(p)
    text_columns = [column for column in SYMBOL_MASTER_COLUMNS if column not in {"lot_size", "price_multiplier"}]
    text_columns.extend([column for column in SYMBOL_MASTER_OPTIONAL_COLUMNS if column in df.columns])
    df = _normalize_string_columns(df, text_columns, uppercase=False)
    uppercase_columns = ["symbol", "exchange", "country", "asset_currency", "base_listing_currency", "tax_country", "fx_pair_to_base"]
    present = [column for column in uppercase_columns if column in df.columns]
    if present:
        df = df.with_columns([pl.col(column).str.to_uppercase().alias(column) for column in present])
    for column in ("lot_size", "price_multiplier"):
        if column in df.columns:
            df = df.with_columns(pl.col(column).cast(pl.Float64, strict=False))
    errors = validate_symbol_master_frame(df, strict=strict)
    if strict and errors:
        raise ValueError("\n".join(errors))
    return df


def load_symbol_master_map(path: str | Path, *, strict: bool = True) -> dict[str, dict[str, object]]:
    df = load_symbol_master_frame(path, strict=strict)
    return {normalize_symbol(row["symbol"]): row for row in df.iter_rows(named=True)}


def _optional_column(df: pl.DataFrame, column: str, default: object = "") -> pl.Expr:
    if column in df.columns:
        return pl.col(column)
    return pl.lit(default)


def _string_value_or_null(df: pl.DataFrame, column: str) -> pl.Expr:
    if column not in df.columns:
        return pl.lit(None, dtype=pl.String)
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .fill_null("")
        .str.strip_chars()
        .replace("", None)
    )


def _float_value_or_null(df: pl.DataFrame, column: str) -> pl.Expr:
    if column not in df.columns:
        return pl.lit(None, dtype=pl.Float64)
    return pl.col(column).cast(pl.Float64, strict=False)


def _csv_flags(flags: list[str]) -> str:
    return ",".join(flag for flag in flags if flag)


def build_symbol_master_frame(
    universe_frame: pl.DataFrame,
    *,
    exchange_defaults: pl.DataFrame | None = None,
    symbol_overrides: pl.DataFrame | None = None,
    base_currency: str = "EUR",
    strict: bool = True,
) -> pl.DataFrame:
    if universe_frame.is_empty():
        return pl.DataFrame(schema={column: pl.String for column in SYMBOL_MASTER_COLUMNS + SYMBOL_MASTER_OPTIONAL_COLUMNS})
    if "symbol" not in universe_frame.columns:
        raise ValueError("universe_frame must contain symbol")
    base_currency = normalize_currency(base_currency)
    df = universe_frame.clone()
    if "active" not in df.columns:
        df = df.with_columns(pl.lit(1).alias("active"))
    if "source" not in df.columns:
        df = df.with_columns(pl.lit("").alias("source"))
    text_columns = [column for column in df.columns if column not in {"active", "lot_size", "price_multiplier", "needs_mapping"}]
    df = _normalize_string_columns(df, text_columns, uppercase=False)
    upper_columns = ["symbol", "exchange", "country", "asset_currency", "base_listing_currency", "tax_country", "fx_pair_to_base", "isin"]
    present_upper = [column for column in upper_columns if column in df.columns]
    if present_upper:
        df = df.with_columns([pl.col(column).str.to_uppercase().alias(column) for column in present_upper])
    df = df.with_columns(pl.col("symbol").str.to_uppercase().alias("symbol"))
    if exchange_defaults is None:
        if strict:
            raise FileNotFoundError("exchange_defaults.csv is required in strict mode")
        exchange_defaults = _empty_frame(EXCHANGE_DEFAULT_COLUMNS)
    if symbol_overrides is None:
        symbol_overrides = _empty_frame(SYMBOL_OVERRIDE_COLUMNS)
    if "exchange" not in df.columns:
        df = df.with_columns(pl.lit("").alias("exchange"))
    df = df.with_columns(pl.col("exchange").cast(pl.String, strict=False).fill_null("").str.strip_chars().str.to_uppercase())
    df = df.with_columns(
        _string_value_or_null(df, "country").alias("_source_country"),
        pl.coalesce([_string_value_or_null(df, "asset_currency"), _string_value_or_null(df, "currency")]).alias("_source_asset_currency"),
        _string_value_or_null(df, "tax_country").alias("_source_tax_country"),
        _string_value_or_null(df, "asset_class").alias("_source_asset_class"),
        pl.coalesce([_string_value_or_null(df, "base_listing_currency"), _string_value_or_null(df, "currency")]).alias("_source_base_listing_currency"),
        _string_value_or_null(df, "fx_pair_to_base").alias("_source_fx_pair_to_base"),
    )
    defaults = exchange_defaults.clone()
    if not defaults.is_empty():
        defaults = defaults.with_columns(pl.col("exchange").cast(pl.String, strict=False).fill_null("").str.strip_chars().str.to_uppercase())
        if "country" in defaults.columns:
            defaults = defaults.rename({"country": "default_country"})
        df = df.join(defaults, on="exchange", how="left")
    df = df.with_columns(
        pl.coalesce([_string_value_or_null(df, "country"), _string_value_or_null(df, "default_country")]).alias("country"),
        pl.coalesce([_string_value_or_null(df, "asset_currency"), _string_value_or_null(df, "currency"), _string_value_or_null(df, "default_asset_currency")]).alias("asset_currency"),
        pl.coalesce([_string_value_or_null(df, "tax_country"), _string_value_or_null(df, "default_tax_country"), _string_value_or_null(df, "country")]).alias("tax_country"),
        pl.coalesce([_string_value_or_null(df, "asset_class"), _string_value_or_null(df, "default_asset_class"), _string_value_or_null(df, "instrument_type")]).alias("asset_class"),
        pl.coalesce([_float_value_or_null(df, "lot_size"), _float_value_or_null(df, "default_lot_size"), pl.lit(1.0)]).alias("lot_size"),
        pl.coalesce([_float_value_or_null(df, "price_multiplier"), _float_value_or_null(df, "default_price_multiplier"), pl.lit(1.0)]).alias("price_multiplier"),
        pl.coalesce([_string_value_or_null(df, "base_listing_currency"), _string_value_or_null(df, "asset_currency"), _string_value_or_null(df, "currency"), _string_value_or_null(df, "default_asset_currency")]).alias("base_listing_currency"),
    )
    if not symbol_overrides.is_empty():
        overrides = symbol_overrides.clone().rename({column: f"override_{column}" for column in symbol_overrides.columns if column != "symbol"})
        df = df.join(overrides, on="symbol", how="left")
        for column in ["exchange", "country", "asset_currency", "base_listing_currency", "tax_country", "asset_class", "fx_pair_to_base", "notes"]:
            override_column = f"override_{column}"
            if override_column in df.columns:
                df = df.with_columns(
                    pl.when(pl.col(override_column).cast(pl.String, strict=False).fill_null("").str.strip_chars() != "")
                    .then(pl.col(override_column))
                    .otherwise(_optional_column(df, column))
                    .alias(column)
                )
        for column in ("lot_size", "price_multiplier"):
            override_column = f"override_{column}"
            if override_column in df.columns:
                df = df.with_columns(pl.coalesce([pl.col(override_column).cast(pl.Float64, strict=False), pl.col(column)]).alias(column))
    rows = []
    for row in df.iter_rows(named=True):
        source_country = normalize_currency(row.get("_source_country"))
        default_country = normalize_currency(row.get("default_country"))
        source_asset_currency = normalize_currency(row.get("_source_asset_currency"))
        default_asset_currency = normalize_currency(row.get("default_asset_currency"))
        source_tax_country = normalize_currency(row.get("_source_tax_country"))
        default_tax_country = normalize_currency(row.get("default_tax_country"))
        source_asset_class = str(row.get("_source_asset_class") or "").strip().lower()
        default_asset_class = str(row.get("default_asset_class") or "").strip().lower()
        override_flags = [
            name
            for name in ["exchange", "country", "asset_currency", "base_listing_currency", "tax_country", "asset_class", "fx_pair_to_base", "lot_size", "price_multiplier"]
            if str(row.get(f"override_{name}") or "").strip() != ""
        ]
        asset_currency = normalize_currency(row.get("asset_currency"))
        listing_currency = normalize_currency(row.get("base_listing_currency")) or asset_currency
        pair = normalize_fx_pair(row.get("fx_pair_to_base")) or make_fx_pair_to_base(asset_currency, base_currency)
        quality_flags = []
        if not source_country and default_country:
            quality_flags.append("defaulted_country")
            quality_flags.append("non_authoritative_country")
        if not source_asset_currency and default_asset_currency:
            quality_flags.append("defaulted_asset_currency")
        if not source_tax_country and default_tax_country:
            quality_flags.append("defaulted_tax_country")
            quality_flags.append("non_authoritative_tax_country")
        if not source_asset_class and default_asset_class:
            quality_flags.append("defaulted_asset_class")
        if not str(row.get("_source_base_listing_currency") or "").strip():
            quality_flags.append("derived_base_listing_currency")
        if not str(row.get("_source_fx_pair_to_base") or "").strip():
            quality_flags.append("derived_fx_pair_to_base")
        if override_flags:
            quality_flags.append("has_overrides")
        metadata_source_flags = ["universe"]
        if any(flag.startswith("defaulted_") for flag in quality_flags):
            metadata_source_flags.append("exchange_defaults")
        if override_flags:
            metadata_source_flags.append("symbol_overrides")
        built = {
            "symbol": normalize_symbol(row.get("symbol")),
            "exchange": normalize_currency(row.get("exchange")),
            "country": normalize_currency(row.get("country")),
            "asset_currency": asset_currency,
            "base_listing_currency": listing_currency,
            "tax_country": normalize_currency(row.get("tax_country")) or normalize_currency(row.get("country")),
            "asset_class": str(row.get("asset_class") or "").strip().lower(),
            "fx_pair_to_base": pair,
            "lot_size": float(row.get("lot_size") or 1.0),
            "price_multiplier": float(row.get("price_multiplier") or 1.0),
            "name": str(row.get("name") or "").strip(),
            "isin": normalize_currency(row.get("isin")),
            "instrument_type": str(row.get("instrument_type") or "").strip().lower(),
            "active": int(row.get("active") or 1),
            "source": str(row.get("source") or "").strip(),
            "metadata_source": _csv_flags(metadata_source_flags),
            "metadata_quality": _csv_flags(quality_flags) or "complete",
            "notes": str(row.get("notes") or "").strip(),
        }
        rows.append(built)
    out = pl.DataFrame(rows)
    ordered = list(SYMBOL_MASTER_COLUMNS) + [column for column in SYMBOL_MASTER_OPTIONAL_COLUMNS if column in out.columns]
    out = out.select([column for column in ordered if column in out.columns])
    errors = validate_symbol_master_frame(out, strict=strict)
    if strict and errors:
        raise ValueError("\n".join(errors))
    return out


def write_symbol_master_frame(df: pl.DataFrame, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(p)
    return p


def validate_symbol_master(path: str | Path, *, strict: bool = True) -> list[str]:
    try:
        df = load_symbol_master_frame(path, strict=False)
    except Exception as exc:
        return [str(exc)]
    return validate_symbol_master_frame(df, strict=strict)


def require_exchange_defaults_frame(path: str | Path, *, strict: bool = True) -> pl.DataFrame:
    p = Path(path)
    if p.exists():
        return load_exchange_defaults(p)
    if strict:
        raise FileNotFoundError(f"exchange_defaults.csv is required in strict mode: {p}")
    return _empty_frame(EXCHANGE_DEFAULT_COLUMNS)


def inspect_symbol_master_frame(
    path: str | Path,
    *,
    exchange: str | None = None,
    fx_pair: str | None = None,
    issues: str | None = None,
    symbols: list[str] | None = None,
) -> pl.DataFrame:
    df = load_symbol_master_frame(path, strict=False)
    if exchange:
        df = df.filter(pl.col("exchange") == normalize_currency(exchange))
    if fx_pair:
        df = df.filter(pl.col("fx_pair_to_base") == normalize_fx_pair(fx_pair))
    if issues:
        token = str(issues).strip().lower()
        df = df.filter(pl.col("metadata_quality").cast(pl.String, strict=False).str.to_lowercase().str.contains(token, literal=True))
    if symbols:
        wanted = [normalize_symbol(symbol) for symbol in symbols]
        df = df.filter(pl.col("symbol").is_in(wanted))
    preferred = [
        "symbol",
        "exchange",
        "country",
        "asset_currency",
        "base_listing_currency",
        "tax_country",
        "asset_class",
        "fx_pair_to_base",
        "metadata_source",
        "metadata_quality",
        "source",
        "instrument_type",
        "name",
    ]
    return df.select([column for column in preferred if column in df.columns] + [column for column in df.columns if column not in preferred])
