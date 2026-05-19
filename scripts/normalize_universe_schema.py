#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tradinglab_data.config import Config, default_config_path, universe_csv_path, universe_dir_path  # noqa: E402

CANONICAL_COLUMNS = [
    "symbol",
    "name",
    "instrument_type",
    "exchange",
    "country",
    "currency",
    "source",
    "active",
    "isin",
    "index_memberships",
    "needs_mapping",
    "region",
    "asset_class",
    "domicile",
    "provider",
    "ucits",
    "inception_date",
    "expense_ratio",
    "avg_dollar_volume_3m",
    "status",
    "notes",
    "asof_date",
]


def _norm(value: str | None) -> str:
    return (value or "").strip()


def _norm_upper(value: str | None) -> str:
    return _norm(value).upper()


def _infer_region(country: str, file_stem: str) -> str:
    country_upper = country.upper()
    if country_upper == "US":
        return "US"
    if country_upper in {"AT", "DE", "FR", "IT", "IE", "LU", "NL", "BE", "ES", "PT", "SE", "FI", "DK", "CH", "UK", "GB"}:
        return "EU"
    if file_stem.lower() in {"etf_eu"}:
        return "EU"
    if file_stem.lower() in {"etf_us"}:
        return "US"
    return ""


def _infer_instrument_type(file_stem: str, source: str) -> str:
    stem = file_stem.lower()
    source_lower = source.lower()
    if stem.startswith("etf") or "etf" in source_lower:
        return "etf"
    return "stock"


def _normalize_asset_class(raw: str, instrument_type: str) -> str:
    value = _norm(raw).lower()
    aliases = {
        "": "equity",
        "etf": "equity",
        "stock": "equity",
        "stocks": "equity",
        "equities": "equity",
        "multi asset": "multi_asset",
        "multi-asset": "multi_asset",
    }
    if value in aliases:
        return aliases[value]
    if instrument_type == "etf" and value == "fund":
        return "multi_asset"
    return value


def _infer_index_memberships(file_stem: str, raw: str) -> str:
    if raw:
        return raw
    stem = file_stem.lower()
    if stem == "etf_master":
        return "ETF"
    return stem.upper()


def _active_from_status(status: str, active: str) -> str:
    if _norm(active) in {"0", "1"}:
        return _norm(active)
    status_lower = status.lower()
    if status_lower in {"active", "seed"}:
        return "1"
    if status_lower in {"inactive", "deprecated", "removed"}:
        return "0"
    return "1"


def _normalize_row(row: dict[str, str], file_stem: str, asof: str) -> dict[str, str]:
    symbol = _norm_upper(row.get("symbol"))
    if not symbol:
        return {}

    source = _norm(row.get("source"))
    if not source:
        source = f"{file_stem}_override"

    country = _norm_upper(row.get("country")) or _norm_upper(row.get("domicile"))
    instrument_type = _infer_instrument_type(file_stem, source)
    status = _norm(row.get("status"))
    active = _active_from_status(status=status, active=_norm(row.get("active")))

    out = {column: "" for column in CANONICAL_COLUMNS}
    out["symbol"] = symbol
    out["name"] = _norm(row.get("name"))
    out["instrument_type"] = instrument_type
    out["exchange"] = _norm_upper(row.get("exchange"))
    out["country"] = country
    out["currency"] = _norm_upper(row.get("currency"))
    out["source"] = source
    out["active"] = active
    out["isin"] = _norm_upper(row.get("isin"))
    out["index_memberships"] = _infer_index_memberships(file_stem, _norm(row.get("index_memberships")))
    out["needs_mapping"] = _norm(row.get("needs_mapping")) or "0"
    out["region"] = _norm_upper(row.get("region")) or _infer_region(country, file_stem)
    out["asset_class"] = _normalize_asset_class(_norm(row.get("asset_class")), instrument_type)
    out["domicile"] = _norm_upper(row.get("domicile")) or country
    out["provider"] = _norm(row.get("provider"))
    out["ucits"] = _norm(row.get("ucits"))
    out["inception_date"] = _norm(row.get("inception_date"))
    out["expense_ratio"] = _norm(row.get("expense_ratio"))
    out["avg_dollar_volume_3m"] = _norm(row.get("avg_dollar_volume_3m"))
    out["status"] = status or ("active" if active == "1" else "inactive")
    out["notes"] = _norm(row.get("notes"))
    out["asof_date"] = _norm(row.get("asof_date")) or asof
    return out


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        return list(reader)


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in CANONICAL_COLUMNS})


def _dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        key = (_norm_upper(row.get("symbol")), _norm_upper(row.get("index_memberships")))
        if not key[0]:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _build_master(all_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_symbol: dict[str, dict[str, str]] = {}
    memberships: dict[str, set[str]] = {}
    for row in all_rows:
        symbol = _norm_upper(row.get("symbol"))
        if not symbol:
            continue
        if symbol not in by_symbol:
            by_symbol[symbol] = dict(row)
        else:
            merged = by_symbol[symbol]
            for key, value in row.items():
                if key in {"symbol", "index_memberships"}:
                    continue
                if _norm(value):
                    merged[key] = value
        memberships.setdefault(symbol, set())
        raw = _norm(row.get("index_memberships"))
        if raw:
            for item in raw.split(","):
                membership = _norm_upper(item)
                if membership:
                    memberships[symbol].add(membership)

    out: list[dict[str, str]] = []
    for symbol, base in by_symbol.items():
        merged_memberships = sorted(memberships.get(symbol, set()))
        base["index_memberships"] = ",".join(merged_memberships)
        out.append(base)
    out.sort(key=lambda row: (row.get("instrument_type", ""), row.get("symbol", "")))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Normalize all universe CSVs to a single canonical schema.")
    ap.add_argument("--config", default=str(default_config_path()), help="Path to configuration YAML")
    ap.add_argument("--universe-dir", type=Path, default=None, help="Universe directory (defaults to paths.universe_dir)")
    ap.add_argument("--master-out", type=Path, default=None, help="Master output CSV (defaults to paths.universe_csv)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    cfg = Config.load(args.config)
    universe_dir = args.universe_dir or universe_dir_path(cfg)
    master_out = args.master_out or universe_csv_path(cfg)
    if not universe_dir.exists():
        raise SystemExit(f"Universe dir not found: {universe_dir}")

    asof = date.today().isoformat()
    all_normalized: list[dict[str, str]] = []
    csv_files = sorted(universe_dir.glob("*.csv"))
    if not csv_files:
        raise SystemExit(f"No CSV files found in {universe_dir}")

    for path in csv_files:
        rows = _read_rows(path)
        normalized = [_normalize_row(row, path.stem, asof) for row in rows]
        normalized = [row for row in normalized if row]
        normalized = _dedupe_rows(normalized)
        all_normalized.extend(normalized)
        if args.dry_run:
            print(f"[dry-run] {path}: rows_in={len(rows)} rows_out={len(normalized)}")
        else:
            _write_rows(path, normalized)
            print(f"[write] {path}: rows={len(normalized)}")

    master_rows = _build_master(all_normalized)
    if args.dry_run:
        print(f"[dry-run] {master_out}: rows={len(master_rows)}")
    else:
        _write_rows(master_out, master_rows)
        print(f"[write] {master_out}: rows={len(master_rows)}")


if __name__ == "__main__":
    main()
