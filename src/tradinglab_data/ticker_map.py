from __future__ import annotations

from pathlib import Path
import polars as pl


_OVERRIDE_CACHE: dict[str, str] | None = None


def _load_overrides(path: str | Path | None = None) -> dict[str, str]:
    global _OVERRIDE_CACHE
    if path is None:
        if _OVERRIDE_CACHE is None:
            _OVERRIDE_CACHE = {}
        return dict(_OVERRIDE_CACHE)
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return {}
    try:
        df = pl.read_csv(str(p))
    except Exception:
        return {}
    if not {"raw", "yahoo"}.issubset(set(df.columns)):
        return {}
    overrides = {}
    for raw, yahoo in df.select(["raw", "yahoo"]).iter_rows():
        if raw and yahoo:
            overrides[str(raw).strip()] = str(yahoo).strip()
    if path is None:
        _OVERRIDE_CACHE = dict(overrides)
    return overrides


def normalize_to_yahoo(symbol: str, exchange: str | None, country: str | None, overrides_path: str | Path | None = None) -> str:
    """Best-effort normalization to Yahoo Finance ticker formats."""
    raw = (symbol or "").strip()
    if raw == "":
        return ""

    overrides = _load_overrides(overrides_path)
    if raw in overrides:
        return overrides[raw]

    if "." in raw:
        return raw

    ex = (exchange or "").strip().lower()
    ct = (country or "").strip().lower()

    if "vienna" in ex or "wien" in ex or "wiener" in ex or "atx" in ex or ct in {"austria", "at"}:
        return f"{raw}.VI"

    if (
        "xetra" in ex
        or "frankfurt" in ex
        or "deutsche" in ex
        or "etra" in ex
        or ct in {"germany", "de"}
    ):
        return f"{raw}.DE"

    return raw
