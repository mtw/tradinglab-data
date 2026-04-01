from __future__ import annotations

from pathlib import Path
from threading import Lock
import polars as pl


_OVERRIDE_CACHE: dict[str, str] | None = None
_OVERRIDE_CACHE_SOURCE: str | None = None
_OVERRIDE_CACHE_LOCK = Lock()


def _load_overrides(path: str | Path | None = None) -> dict[str, str]:
    global _OVERRIDE_CACHE, _OVERRIDE_CACHE_SOURCE
    if path is None:
        with _OVERRIDE_CACHE_LOCK:
            if _OVERRIDE_CACHE is None:
                _OVERRIDE_CACHE = {}
            return dict(_OVERRIDE_CACHE)
    p = Path(path)
    cache_key = str(p.resolve(strict=False))
    with _OVERRIDE_CACHE_LOCK:
        if _OVERRIDE_CACHE is not None and _OVERRIDE_CACHE_SOURCE == cache_key:
            return dict(_OVERRIDE_CACHE)
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
    with _OVERRIDE_CACHE_LOCK:
        _OVERRIDE_CACHE = dict(overrides)
        _OVERRIDE_CACHE_SOURCE = cache_key
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


def clear_override_cache() -> None:
    global _OVERRIDE_CACHE, _OVERRIDE_CACHE_SOURCE
    with _OVERRIDE_CACHE_LOCK:
        _OVERRIDE_CACHE = None
        _OVERRIDE_CACHE_SOURCE = None
