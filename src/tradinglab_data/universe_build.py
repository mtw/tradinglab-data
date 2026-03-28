from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from io import BytesIO
from urllib.request import urlopen, Request

import polars as pl

from .ticker_map import normalize_to_yahoo


@dataclass(frozen=True)
class UniverseRow:
    symbol: str
    name: str | None
    exchange: str | None
    country: str | None
    source: str
    active: int = 1
    isin: str | None = None
    index_memberships: str | None = None
    needs_mapping: int = 0


def _safe_read_html(url: str, match: str | None = None):
    try:
        import pandas as pd
        if match:
            return pd.read_html(url, match=match)
        return pd.read_html(url)
    except Exception:
        return None


def _from_override(index_name: str, overrides_dir: Path) -> list[dict]:
    p = overrides_dir / f"{index_name}.csv"
    if not p.exists():
        return []
    df = pl.read_csv(str(p))
    rows = []
    for row in df.iter_rows(named=True):
        rows.append({
            "symbol": str(row.get("symbol") or "").strip(),
            "name": row.get("name"),
            "exchange": row.get("exchange"),
            "country": row.get("country"),
            "source": f"{index_name}_override",
            "active": int(row.get("active") or 1),
            "isin": row.get("isin"),
        })
    return rows


def _sp500_rows() -> list[dict]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = _safe_read_html(url, match="Symbol")
    if not tables:
        return []
    df = tables[0]
    rows = []
    for _, r in df.iterrows():
        sym = str(r.get("Symbol", "")).strip()
        name = str(r.get("Security", "")).strip() or None
        exchange = str(r.get("Primary exchange", "")).strip() or None
        rows.append({
            "symbol": sym,
            "name": name,
            "exchange": exchange,
            "country": "US",
            "source": "sp500_wikipedia",
            "active": 1,
            "isin": None,
        })
    return rows


def _djia_rows() -> list[dict]:
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    tables = _safe_read_html(url, match="Symbol")
    if not tables:
        return []
    df = tables[0]
    rows = []
    for _, r in df.iterrows():
        sym = str(r.get("Symbol", "")).strip()
        name = str(r.get("Company", "")).strip() or None
        exchange = str(r.get("Exchange", "")).strip() or None
        rows.append({
            "symbol": sym,
            "name": name,
            "exchange": exchange,
            "country": "US",
            "source": "djia_wikipedia",
            "active": 1,
            "isin": None,
        })
    return rows


def _read_stoxx_closecomposition(index_symbol: str) -> pl.DataFrame | None:
    url = (
        "https://www.stoxx.com/documents/stoxxnet/Documents/Indices/Current/"
        f"Composition_Files/closecomposition_{index_symbol}.csv"
    )
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as resp:
            data = resp.read()
        if not data:
            return None
        df = pl.read_csv(BytesIO(data))
        if df.width == 1:
            df = pl.read_csv(BytesIO(data), separator=";")
        return df
    except Exception:
        return None


def _fetch_html(url: str) -> str | None:
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return None


def _tradingview_components_rows(url: str, source: str) -> list[dict]:
    try:
        import pandas as pd
    except Exception:
        return []

    html = _fetch_html(url)
    if not html:
        return []
    try:
        tables = pd.read_html(html)
    except Exception:
        return []
    if not tables:
        return []

    df = None
    for t in tables:
        cols = {c.lower(): c for c in t.columns}
        if "symbol" in cols or "instrument" in cols:
            df = t
            break
    if df is None:
        df = tables[0]

    cols = {c.lower(): c for c in df.columns}
    sym_col = cols.get("symbol") or cols.get("instrument")
    if sym_col is None:
        return []

    out = pl.from_pandas(df[[sym_col]])
    out = out.rename({sym_col: "symbol_raw"})
    out = out.with_columns(
        pl.col("symbol_raw")
        .cast(pl.String)
        .str.replace_all(r"\\s+", " ")
        .str.strip_chars()
        .alias("symbol_raw")
    )
    out = out.with_columns(
        pl.col("symbol_raw").str.extract(r"^([A-Z0-9._-]+)", 1).alias("symbol"),
        pl.col("symbol_raw").str.replace(r"^[A-Z0-9._-]+\\s*", "").alias("name"),
    ).drop("symbol_raw")
    out = out.filter(pl.col("symbol").is_not_null() & (pl.col("symbol") != ""))

    rows = []
    for row in out.iter_rows(named=True):
        rows.append({
            "symbol": row.get("symbol"),
            "name": row.get("name"),
            "exchange": "Xetra",
            "country": "Germany",
            "source": source,
            "active": 1,
            "isin": None,
        })
    return rows


def _stoxx_closecomposition_rows(index_symbol: str, source: str) -> list[dict]:
    df = _read_stoxx_closecomposition(index_symbol)
    if df is None or df.is_empty():
        return []

    cols = {c.lower(): c for c in df.columns}
    sym_col = (
        cols.get("trading symbol")
        or cols.get("tradingsymbol")
        or cols.get("ticker")
        or cols.get("symbol")
        or cols.get("constituent symbol")
        or cols.get("constituent_symbol")
        or cols.get("local code")
        or cols.get("local_code")
        or cols.get("ric")
    )
    name_col = (
        cols.get("instrument")
        or cols.get("constituent name")
        or cols.get("constituent_name")
        or cols.get("name")
    )
    isin_col = cols.get("isin")

    rows = []
    for row in df.iter_rows(named=True):
        sym = str(row.get(sym_col, "")).strip() if sym_col else ""
        name = str(row.get(name_col, "")).strip() if name_col else None
        isin = str(row.get(isin_col, "")).strip() if isin_col else None
        rows.append({
            "symbol": sym,
            "name": name or None,
            "exchange": "Xetra",
            "country": "Germany",
            "source": source,
            "active": 1,
            "isin": isin or None,
        })
    return rows


def _dax_rows_wikipedia() -> list[dict]:
    url = "https://en.wikipedia.org/wiki/DAX"
    tables = _safe_read_html(url, match="Ticker")
    if not tables:
        return []
    df = tables[0]
    cols = {c.lower(): c for c in df.columns}
    sym_col = cols.get("ticker") or cols.get("symbol")
    name_col = cols.get("company")
    rows = []
    for _, r in df.iterrows():
        sym = str(r.get(sym_col, "")).strip() if sym_col else ""
        name = str(r.get(name_col, "")).strip() if name_col else None
        if not sym and not name:
            continue
        rows.append({
            "symbol": sym,
            "name": name or None,
            "exchange": "Xetra",
            "country": "Germany",
            "source": "dax_wikipedia",
            "active": 1,
            "isin": None,
        })
    return rows


def _mdax_rows_wikipedia() -> list[dict]:
    url = "https://en.wikipedia.org/wiki/MDAX"
    tables = _safe_read_html(url, match="Ticker")
    if not tables:
        return []
    df = tables[0]
    cols = {c.lower(): c for c in df.columns}
    sym_col = cols.get("ticker") or cols.get("symbol")
    name_col = cols.get("company")
    rows = []
    for _, r in df.iterrows():
        sym = str(r.get(sym_col, "")).strip() if sym_col else ""
        name = str(r.get(name_col, "")).strip() if name_col else None
        if not sym and not name:
            continue
        rows.append({
            "symbol": sym,
            "name": name or None,
            "exchange": "Xetra",
            "country": "Germany",
            "source": "mdax_wikipedia",
            "active": 1,
            "isin": None,
        })
    return rows


def _atx_rows() -> list[dict]:
    url = "https://www.wienerborse.at/en/market-data/shares-others/atx/atx/composition/"
    tables = _safe_read_html(url)
    if not tables:
        return _atx_rows_wikipedia()
    df = tables[0]
    cols = {c.lower(): c for c in df.columns}
    sym_col = cols.get("symbol") or cols.get("ticker") or cols.get("short name")
    name_col = cols.get("name") or cols.get("company") or cols.get("security")
    isin_col = cols.get("isin")
    rows = []
    for _, r in df.iterrows():
        sym = str(r.get(sym_col, "")).strip() if sym_col else ""
        name = str(r.get(name_col, "")).strip() if name_col else None
        isin = str(r.get(isin_col, "")).strip() if isin_col else None
        rows.append({
            "symbol": sym,
            "name": name or None,
            "exchange": "Vienna",
            "country": "Austria",
            "source": "atx_wienerboerse",
            "active": 1,
            "isin": isin or None,
        })
    if not rows or all(not (r.get("symbol") or r.get("name")) for r in rows):
        return _atx_rows_wikipedia()
    return rows


def _atx_rows_wikipedia() -> list[dict]:
    url = "https://de.wikipedia.org/wiki/Austrian_Traded_Index"
    tables = _safe_read_html(url, match="Zusammensetzung")
    if not tables:
        return []
    df = tables[0]
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("name")
    if name_col is None:
        return []
    rows = []
    for _, r in df.iterrows():
        name = str(r.get(name_col, "")).strip()
        if not name:
            continue
        rows.append({
            "symbol": "",
            "name": name,
            "exchange": "Vienna",
            "country": "Austria",
            "source": "atx_wikipedia_de",
            "active": 1,
            "isin": None,
        })
    return rows


def _merge_rows(rows_by_index: dict[str, list[dict]]) -> list[dict]:
    merged: dict[tuple[str, str | None], dict] = {}
    memberships: dict[tuple[str, str | None], set[str]] = {}
    for idx, rows in rows_by_index.items():
        for row in rows:
            key = (row.get("symbol", ""), row.get("isin"))
            if key not in merged:
                merged[key] = row.copy()
                memberships[key] = set()
            memberships[key].add(idx.upper())

    out = []
    for key, row in merged.items():
        idxs = sorted(memberships.get(key, []))
        row["index_memberships"] = ", ".join(idxs) if idxs else None
        out.append(row)
    return out


def build_universe(
    indices: Iterable[str],
    out_path: str | Path,
    active_only: bool = True,
    overrides_dir: str | Path = "",
    ticker_overrides_path: str | Path | None = None,
) -> pl.DataFrame:
    idx = [i.lower() for i in indices]
    overrides_dir = Path(overrides_dir)

    rows_by_index: dict[str, list[dict]] = {}

    if "sp500" in idx:
        rows = _sp500_rows()
        if not rows:
            rows = _from_override("sp500", overrides_dir)
        rows_by_index["sp500"] = rows

    if "djia" in idx:
        rows = _djia_rows()
        if not rows:
            rows = _from_override("djia", overrides_dir)
        rows_by_index["djia"] = rows

    if "dax" in idx:
        rows = _dax_rows_wikipedia()
        if not rows:
            rows = _from_override("dax", overrides_dir)
        rows_by_index["dax"] = rows

    if "mdax" in idx:
        rows = _mdax_rows_wikipedia()
        if not rows:
            rows = _from_override("mdax", overrides_dir)
        rows_by_index["mdax"] = rows

    if "atx" in idx:
        rows = _atx_rows()
        if not rows:
            rows = _from_override("atx", overrides_dir)
        rows_by_index["atx"] = rows

    merged = _merge_rows(rows_by_index)
    out_rows: list[UniverseRow] = []
    for row in merged:
        raw_symbol = str(row.get("symbol") or "").strip()
        isin = row.get("isin")
        needs_mapping = 0
        symbol_for_norm = raw_symbol
        if raw_symbol == "" and isin:
            symbol_for_norm = str(isin).strip()
            needs_mapping = 1

        try:
            yahoo = normalize_to_yahoo(
                symbol_for_norm,
                row.get("exchange"),
                row.get("country"),
                overrides_path=ticker_overrides_path,
            )
        except TypeError:
            # Keep test doubles and legacy callsites working when they still mock the 3-arg signature.
            yahoo = normalize_to_yahoo(symbol_for_norm, row.get("exchange"), row.get("country"))
        if yahoo == "":
            needs_mapping = 1

        out_rows.append(
            UniverseRow(
                symbol=yahoo,
                name=row.get("name"),
                exchange=row.get("exchange"),
                country=row.get("country"),
                source=row.get("source"),
                active=int(row.get("active") or 1),
                isin=row.get("isin"),
                index_memberships=row.get("index_memberships"),
                needs_mapping=needs_mapping,
            )
        )

    df = pl.DataFrame([r.__dict__ for r in out_rows])
    if df.is_empty():
        raise RuntimeError(
            "No constituents found. Source fetch may have failed; "
            f"try again or provide overrides in {overrides_dir}/<index>.csv."
        )
    ordered = [
        "symbol",
        "name",
        "exchange",
        "country",
        "source",
        "active",
        "isin",
        "index_memberships",
        "needs_mapping",
    ]
    cols = [c for c in ordered if c in df.columns] + [c for c in df.columns if c not in ordered]
    df = df.select(cols)
    if active_only and "active" in df.columns:
        df = df.filter(pl.col("active") == 1)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(str(out_path))

    return df
