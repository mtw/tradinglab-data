from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import ConfigLike, crypto_universe_dir_path, ticker_overrides_path, universe_dir_path
from .crypto.registry import STATIC_CRYPTO_UNIVERSES, load_crypto_universes
from .universe import load_universe_frame


@dataclass(frozen=True)
class UniverseListingEntry:
    family: str
    name: str
    symbol_count: int
    source: str
    path: str


def list_available_universes(cfg: ConfigLike) -> list[UniverseListingEntry]:
    entries: list[UniverseListingEntry] = []
    entries.extend(_list_equity_universes(cfg))
    entries.extend(_list_crypto_universes(cfg))
    return sorted(entries, key=lambda item: (item.family, item.name))


def render_available_universes(entries: list[UniverseListingEntry]) -> str:
    if not entries:
        return "No universes found.\n"
    grouped: dict[str, list[UniverseListingEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.family, []).append(entry)
    lines: list[str] = ["Available universes", ""]
    for family in sorted(grouped):
        lines.append(f"{family.capitalize()} universes:")
        for entry in sorted(grouped[family], key=lambda item: item.name):
            location = entry.path if entry.path else entry.source
            lines.append(f"  {entry.name}: {entry.symbol_count} symbols [{location}]")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _list_equity_universes(cfg: ConfigLike) -> list[UniverseListingEntry]:
    root = universe_dir_path(cfg)
    if not root.exists() or not root.is_dir():
        return []
    try:
        overrides: str | Path | None = ticker_overrides_path(cfg)
    except ValueError:
        overrides = None
    entries: list[UniverseListingEntry] = []
    for path in sorted(root.glob("*.csv")):
        try:
            frame = load_universe_frame(path, universe_dir=None, ticker_overrides_path=overrides)
            symbol_count = int(frame.height)
        except Exception:
            symbol_count = 0
        entries.append(
            UniverseListingEntry(
                family="equity",
                name=path.stem,
                symbol_count=symbol_count,
                source="csv",
                path=str(path),
            )
        )
    return entries


def _list_crypto_universes(cfg: ConfigLike) -> list[UniverseListingEntry]:
    root = crypto_universe_dir_path(cfg)
    dynamic_dir = root if root.exists() and root.is_dir() else None
    universes = load_crypto_universes(cfg)
    entries: list[UniverseListingEntry] = []
    for name, symbols in sorted(universes.items()):
        path = ""
        source = "static"
        if dynamic_dir is not None:
            candidate = dynamic_dir / f"{name}.json"
            if candidate.exists():
                path = str(candidate)
                source = "json"
        if not path and name not in STATIC_CRYPTO_UNIVERSES:
            source = "dynamic"
        entries.append(
            UniverseListingEntry(
                family="crypto",
                name=name,
                symbol_count=len(symbols),
                source=source,
                path=path or source,
            )
        )
    return entries
