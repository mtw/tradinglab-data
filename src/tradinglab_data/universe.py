from __future__ import annotations

from pathlib import Path
import polars as pl


def load_ticker_overrides(csv_path: str | Path | None = None) -> dict[str, str]:
    if csv_path is None:
        return {}
    path = Path(csv_path)
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        df = pl.read_csv(str(path))
    except Exception:
        return {}
    cols = {c.lower(): c for c in df.columns}
    raw_col = cols.get("raw")
    yahoo_col = cols.get("yahoo")
    if not raw_col or not yahoo_col:
        return {}
    mapping: dict[str, str] = {}
    for row in df.select([raw_col, yahoo_col]).iter_rows(named=True):
        raw = str(row.get(raw_col) or "").strip().upper()
        yahoo = str(row.get(yahoo_col) or "").strip().upper()
        if raw and yahoo:
            mapping[raw] = yahoo
    return mapping


def canonicalize_symbol(symbol: str, overrides: dict[str, str] | None = None) -> str:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""
    mapping = overrides if overrides is not None else {}
    return mapping.get(sym, sym)


def load_universe_frame(
    csv_path: str | Path,
    universe_dir: str | Path | None = None,
    ticker_overrides_path: str | Path | None = None,
) -> pl.DataFrame:
    try:
        df = pl.read_csv(str(csv_path))
    except Exception:
        df = pl.DataFrame()

    if df.is_empty() and universe_dir is not None:
        universe_dir = Path(universe_dir)
        if universe_dir.exists():
            frames = []
            for p in sorted(universe_dir.glob("*.csv")):
                try:
                    frames.append(pl.read_csv(str(p)))
                except Exception:
                    continue
            if frames:
                all_cols: set[str] = set()
                for f in frames:
                    all_cols.update(f.columns)
                normalized = []
                for f in frames:
                    missing = [c for c in all_cols if c not in f.columns]
                    if missing:
                        f = f.with_columns([pl.lit(None).alias(c) for c in missing])
                    f = f.select(sorted(all_cols))
                    f = f.with_columns([pl.col(c).cast(pl.Utf8) for c in f.columns])
                    normalized.append(f)
                df = pl.concat(normalized, how="vertical")
    if "symbol" not in df.columns:
        raise ValueError("universe.csv must contain a 'symbol' column")
    if "active" in df.columns:
        df = df.with_columns(pl.col("active").cast(pl.Int64, strict=False))
        df = df.filter(pl.col("active") == 1)
    overrides = load_ticker_overrides(ticker_overrides_path)
    df = df.with_columns(pl.col("symbol").cast(pl.Utf8).str.strip_chars().str.to_uppercase().alias("symbol"))
    df = df.filter((pl.col("symbol") != "") & (~pl.col("symbol").str.contains(r"[\\$\\s]")))
    if overrides:
        df = df.with_columns(
            pl.col("symbol").map_elements(lambda s: overrides.get(str(s), str(s)), return_dtype=pl.Utf8).alias("symbol")
        ).unique(subset=["symbol"], keep="first", maintain_order=True)
    return df


def load_universe(
    csv_path: str | Path,
    universe_dir: str | Path | None = None,
    ticker_overrides_path: str | Path | None = None,
) -> list[str]:
    df = load_universe_frame(csv_path, universe_dir=universe_dir, ticker_overrides_path=ticker_overrides_path)
    return df.get_column("symbol").to_list()
