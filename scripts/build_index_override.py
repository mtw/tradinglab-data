#!/usr/bin/env python3
from __future__ import annotations

import argparse
from io import BytesIO, StringIO
from pathlib import Path
from urllib.request import Request, urlopen

import polars as pl


def _read_stoxx_closecomposition(index_symbol: str) -> pl.DataFrame:
    url = (
        "https://www.stoxx.com/documents/stoxxnet/Documents/Indices/Current/"
        f"Composition_Files/closecomposition_{index_symbol}.csv"
    )
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        data = resp.read()
    if not data:
        raise RuntimeError(f"Empty response from {url}")
    df = pl.read_csv(BytesIO(data))
    if df.width == 1:
        df = pl.read_csv(BytesIO(data), separator=";")
    return df


def _fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        data = resp.read()
    return data.decode("utf-8", errors="ignore")


def _from_wikipedia_table(url: str, match: str | None = None) -> pl.DataFrame:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to parse Wikipedia tables") from exc

    html = _fetch_html(url)
    if match:
        try:
            tables = pd.read_html(StringIO(html), match=match)
        except ValueError:
            tables = []
    else:
        tables = pd.read_html(StringIO(html))

    if not tables:
        tables = pd.read_html(StringIO(html))
    if not tables:
        if match:
            raise RuntimeError(f"No tables matched '{match}' at {url}")
        raise RuntimeError(f"No tables found at {url}")
    return pl.from_pandas(tables[0])


def _from_tradingview_components(url: str) -> pl.DataFrame:
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to parse TradingView tables") from exc

    html = _fetch_html(url)
    tables = pd.read_html(StringIO(html))
    if not tables:
        raise RuntimeError(f"No tables found at {url}")

    df = None
    for table in tables:
        cols = {c.lower(): c for c in table.columns}
        if "symbol" in cols or "instrument" in cols:
            df = table
            break
    if df is None:
        df = tables[0]

    cols = {c.lower(): c for c in df.columns}
    sym_col = cols.get("symbol") or cols.get("instrument")
    if sym_col is None:
        raise RuntimeError("Missing Symbol/Instrument column in TradingView table")

    out = pl.from_pandas(df.select([sym_col]))
    out = out.rename({sym_col: "symbol_raw"})
    out = out.with_columns(
        pl.col("symbol_raw")
        .cast(pl.Utf8)
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("symbol_raw")
    )
    out = out.with_columns(
        pl.col("symbol_raw").str.extract(r"^([A-Z0-9._-]+)", 1).alias("symbol"),
        pl.col("symbol_raw").str.replace(r"^[A-Z0-9._-]+\s*", "").alias("name"),
    ).drop("symbol_raw")
    out = out.filter(pl.col("symbol").is_not_null() & (pl.col("symbol") != ""))
    return out


def build_dax_mdax(index_symbol: str) -> pl.DataFrame:
    if index_symbol == "dax":
        url = "https://de.tradingview.com/symbols/XETR-DAX/components/"
    elif index_symbol == "mdax":
        url = "https://de.tradingview.com/symbols/XETR-MDAX/components/"
    else:
        raise RuntimeError("index_symbol must be dax or mdax")

    out = _from_tradingview_components(url)
    out = out.with_columns(
        pl.lit("Xetra").alias("exchange"),
        pl.lit("Germany").alias("country"),
        pl.lit(1).alias("active"),
    )
    return out


def build_sp500() -> pl.DataFrame:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    df = _from_wikipedia_table(url, match="Symbol")
    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol")
    name = cols.get("security")
    if sym is None or name is None:
        raise RuntimeError("Missing Symbol/Security columns in S&P 500 table")
    out = df.select([sym, name]).rename({sym: "symbol", name: "name"})
    out = out.with_columns(
        pl.lit("US").alias("country"),
        pl.lit(1).alias("active"),
    )
    return out


def build_djia() -> pl.DataFrame:
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    df = _from_wikipedia_table(url, match="Symbol")
    cols = {c.lower(): c for c in df.columns}
    sym = cols.get("symbol")
    name = cols.get("company")
    if sym is None or name is None:
        raise RuntimeError("Missing Symbol/Company columns in DJIA table")
    out = df.select([sym, name]).rename({sym: "symbol", name: "name"})
    out = out.with_columns(
        pl.lit("US").alias("country"),
        pl.lit(1).alias("active"),
    )
    return out


def build_atx() -> pl.DataFrame:
    url = "https://de.wikipedia.org/wiki/Austrian_Traded_Index"
    try:
        import pandas as pd
    except Exception as exc:
        raise RuntimeError("pandas is required to parse Wikipedia tables") from exc

    html = _fetch_html(url)
    tables = pd.read_html(StringIO(html))
    picked = None
    for table in tables:
        cols_l = {str(col).lower() for col in table.columns}
        if {"name", "unternehmen", "company"} & cols_l:
            if len(table) >= 10:
                picked = table
                break
            if picked is None:
                picked = table
    if picked is None:
        for table in tables:
            if len(table) >= 10:
                picked = table
                break
    if picked is None:
        raise RuntimeError(f"No usable ATX table found at {url}")

    df = pl.from_pandas(picked)
    cols = {str(c).lower(): c for c in df.columns}
    name = cols.get("name") or cols.get("unternehmen") or cols.get("company")
    if name is None:
        raise RuntimeError("Missing Name/Unternehmen/Company column in ATX table")
    out = df.select([name]).rename({name: "name"})
    out = out.with_columns(
        pl.lit("").alias("symbol"),
        pl.lit("Vienna").alias("exchange"),
        pl.lit("Austria").alias("country"),
        pl.lit(1).alias("active"),
    ).select(["symbol", "name", "exchange", "country", "active"])
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("index", choices=["dax", "mdax", "sp500", "djia", "atx"])
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args()

    if args.index in {"dax", "mdax"}:
        df = build_dax_mdax(args.index)
    elif args.index == "sp500":
        df = build_sp500()
    elif args.index == "djia":
        df = build_djia()
    elif args.index == "atx":
        df = build_atx()
    else:
        raise SystemExit("Unknown index")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(str(out))
    print(f"Wrote {out} ({df.height} rows)")


if __name__ == "__main__":
    main()
