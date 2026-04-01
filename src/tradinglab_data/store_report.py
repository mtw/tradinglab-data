from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from statistics import median
from typing import Callable

import polars as pl

from ._ohlc_utils import ohlc_quality_counts
from .config import Config, intraday_root_path, parquet_root_path, runs_root_path, universe_dir_path
from .contracts import StoreHistoryEntry, StoreIntegrityFileIssue, StoreIntegrityReport, StoreIntegritySection
from .parquet_verify import ParquetVerifyConfig, run_parquet_sanity_checks
from .schema import validate_daily_frame, validate_intraday_frame


@dataclass(frozen=True)
class _FileAudit:
    section: str
    symbol: str
    path: str
    rows: int
    start_date: str | None
    end_date: str | None
    currencies: list[str]
    missing_currency_rows: int
    unknown_currency_rows: int
    duplicate_dates: int
    null_ohlc_rows: int
    bad_ohlc_rows: int
    sorted_dates: bool
    dirty_reasons: list[str]
    schema_error: str | None
    read_error: str | None


def _format_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _dated_report_dir(runs_root: Path) -> Path:
    return runs_root / datetime.now(timezone.utc).strftime("%Y-%m-%d") / "integrity"


def _scan_parquet_files(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted(path for path in root.glob("*.parquet") if path.is_file())


def _file_symbol(path: Path) -> str:
    return path.stem


def _currency_stats(df: pl.DataFrame) -> tuple[list[str], int, int]:
    if "currency" not in df.columns:
        return [], int(df.height), 0
    currency_expr = pl.col("currency").cast(pl.String, strict=False).str.strip_chars()
    currencies = (
        df.select(currency_expr.alias("currency"))
        .filter(pl.col("currency").is_not_null() & (pl.col("currency") != ""))
        .get_column("currency")
        .unique()
        .sort()
        .to_list()
    )
    missing = int(df.select((currency_expr.is_null() | (currency_expr == "")).sum()).item())
    unknown = int(df.select((currency_expr.str.to_uppercase() == "UNKNOWN").sum()).item())
    return [str(value) for value in currencies], missing, unknown


def _sorted_dates(df: pl.DataFrame) -> bool:
    if "date" not in df.columns or df.is_empty():
        return True
    try:
        return bool(df.get_column("date").is_sorted())
    except Exception:
        return False


def _history_bounds(df: pl.DataFrame) -> tuple[str | None, str | None]:
    if "date" not in df.columns or df.is_empty():
        return None, None
    try:
        start_date = df.select(pl.col("date").min()).item()
        end_date = df.select(pl.col("date").max()).item()
    except Exception:
        return None, None
    return _format_datetime(start_date), _format_datetime(end_date)


def _audit_file(
    path: Path,
    *,
    section: str,
    validator: Callable[[pl.DataFrame], None],
) -> _FileAudit:
    dirty_reasons: list[str] = []
    symbol = _file_symbol(path)
    if path.stat().st_size == 0:
        dirty_reasons.append("zero_byte")
    try:
        df = pl.read_parquet(path)
    except Exception as exc:
        return _FileAudit(
            section=section,
            symbol=symbol,
            path=str(path),
            rows=0,
            start_date=None,
            end_date=None,
            currencies=[],
            missing_currency_rows=0,
            unknown_currency_rows=0,
            duplicate_dates=0,
            null_ohlc_rows=0,
            bad_ohlc_rows=0,
            sorted_dates=False,
            dirty_reasons=sorted(set(dirty_reasons + ["read_error"])),
            schema_error=None,
            read_error=f"{type(exc).__name__}: {exc}",
        )

    rows = int(df.height)
    if rows == 0:
        dirty_reasons.append("empty_file")

    schema_error: str | None = None
    try:
        validator(df)
    except Exception as exc:
        schema_error = str(exc)
        dirty_reasons.append("schema_mismatch")

    start_date, end_date = _history_bounds(df)
    currencies, missing_currency_rows, unknown_currency_rows = _currency_stats(df)
    quality = ohlc_quality_counts(df)
    duplicate_dates = int(quality.get("dup_dates", 0))
    null_ohlc_rows = int(quality.get("null_ohlc", 0))
    bad_ohlc_rows = int(quality.get("bad_ohlc", 0))
    sorted_dates = _sorted_dates(df)

    if duplicate_dates > 0:
        dirty_reasons.append("duplicate_dates")
    if null_ohlc_rows > 0:
        dirty_reasons.append("null_ohlc_rows")
    if bad_ohlc_rows > 0:
        dirty_reasons.append("bad_ohlc_rows")
    if not sorted_dates:
        dirty_reasons.append("unsorted_dates")
    if missing_currency_rows > 0:
        dirty_reasons.append("missing_currency_rows")
    if unknown_currency_rows > 0:
        dirty_reasons.append("unknown_currency_rows")

    return _FileAudit(
        section=section,
        symbol=symbol,
        path=str(path),
        rows=rows,
        start_date=start_date,
        end_date=end_date,
        currencies=currencies,
        missing_currency_rows=missing_currency_rows,
        unknown_currency_rows=unknown_currency_rows,
        duplicate_dates=duplicate_dates,
        null_ohlc_rows=null_ohlc_rows,
        bad_ohlc_rows=bad_ohlc_rows,
        sorted_dates=sorted_dates,
        dirty_reasons=sorted(set(dirty_reasons)),
        schema_error=schema_error,
        read_error=None,
    )


def _top_histories(audits: list[_FileAudit], *, limit: int = 10) -> list[StoreHistoryEntry]:
    ranked = sorted(audits, key=lambda item: (-item.rows, item.symbol))
    return [
        {
            "symbol": audit.symbol,
            "path": audit.path,
            "rows": audit.rows,
            "start_date": audit.start_date,
            "end_date": audit.end_date,
            "currencies": audit.currencies,
        }
        for audit in ranked[:limit]
    ]


def _summarize_section(
    *,
    section: str,
    root: Path,
    validator: Callable[[pl.DataFrame], None],
) -> tuple[StoreIntegritySection, list[StoreIntegrityFileIssue]]:
    files = _scan_parquet_files(root)
    audits = [_audit_file(path, section=section, validator=validator) for path in files]
    row_counts = [audit.rows for audit in audits]
    dates = [date for audit in audits for date in [audit.start_date, audit.end_date] if date is not None]
    currencies_seen = sorted({currency for audit in audits for currency in audit.currencies})
    dirty_reason_counts: dict[str, int] = {}
    dirty_files: list[StoreIntegrityFileIssue] = []
    for audit in audits:
        if not audit.dirty_reasons:
            continue
        for reason in audit.dirty_reasons:
            dirty_reason_counts[reason] = dirty_reason_counts.get(reason, 0) + 1
        dirty_files.append(
            {
                "section": audit.section,
                "symbol": audit.symbol,
                "path": audit.path,
                "rows": audit.rows,
                "start_date": audit.start_date,
                "end_date": audit.end_date,
                "dirty_reasons": list(audit.dirty_reasons),
                "schema_error": audit.schema_error,
                "read_error": audit.read_error,
            }
        )

    summary: StoreIntegritySection = {
        "section": section,
        "root": str(root),
        "files_total": len(files),
        "files_readable": sum(1 for audit in audits if audit.read_error is None),
        "files_dirty": len(dirty_files),
        "rows_total": int(sum(row_counts)),
        "rows_min": min(row_counts) if row_counts else 0,
        "rows_median": float(median(row_counts)) if row_counts else 0.0,
        "rows_max": max(row_counts) if row_counts else 0,
        "earliest_date": min(dates) if dates else None,
        "latest_date": max(dates) if dates else None,
        "currencies_seen": currencies_seen,
        "dirty_reason_counts": dirty_reason_counts,
        "top_histories": _top_histories(audits),
    }
    return summary, dirty_files


def render_store_integrity_report_markdown(report: StoreIntegrityReport) -> str:
    lines = [
        "# Parquet Store Integrity Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Config: `{report['config_path']}`",
        f"- Daily root: `{report['daily_root']}`",
        f"- Intraday root: `{report['intraday_root']}`",
        "",
        "## Section Summary",
        "",
        "| Section | Files | Dirty | Rows | Earliest | Latest | Currencies |",
        "|---|---:|---:|---:|---|---|---|",
    ]
    for section in report["sections"]:
        currencies = ", ".join(section["currencies_seen"]) if section["currencies_seen"] else "-"
        lines.append(
            f"| `{section['section']}` | {section['files_total']} | {section['files_dirty']} | "
            f"{section['rows_total']} | {section['earliest_date'] or '-'} | {section['latest_date'] or '-'} | {currencies} |"
        )

    lines.extend(["", "## Section Details", ""])
    for section in report["sections"]:
        lines.extend(
            [
                f"### {section['section']}",
                "",
                f"- Root: `{section['root']}`",
                f"- Readable files: `{section['files_readable']}/{section['files_total']}`",
                f"- Dirty files: `{section['files_dirty']}`",
                f"- Row distribution: min `{section['rows_min']}`, median `{section['rows_median']:.1f}`, max `{section['rows_max']}`",
            ]
        )
        if section["dirty_reason_counts"]:
            reason_text = ", ".join(f"`{key}`={value}" for key, value in sorted(section["dirty_reason_counts"].items()))
            lines.append(f"- Dirty reason counts: {reason_text}")
        else:
            lines.append("- Dirty reason counts: none")
        lines.extend(["", "| Symbol | Rows | Start | End | Currencies | Path |", "|---|---:|---|---|---|---|"])
        for entry in section["top_histories"]:
            currencies = ", ".join(entry["currencies"]) if entry["currencies"] else "-"
            lines.append(
                f"| `{entry['symbol']}` | {entry['rows']} | {entry['start_date'] or '-'} | "
                f"{entry['end_date'] or '-'} | {currencies} | `{entry['path']}` |"
            )
        if not section["top_histories"]:
            lines.append("| - | 0 | - | - | - | - |")
        lines.append("")

    lines.extend(["## Dirty Files", ""])
    if report["dirty_files"]:
        lines.extend(["| Section | Symbol | Rows | Reasons | Start | End | Path |", "|---|---|---:|---|---|---|---|"])
        for item in report["dirty_files"]:
            reasons = ", ".join(item["dirty_reasons"])
            lines.append(
                f"| `{item['section']}` | `{item['symbol']}` | {item['rows']} | {reasons} | "
                f"{item['start_date'] or '-'} | {item['end_date'] or '-'} | `{item['path']}` |"
            )
            if item["schema_error"]:
                lines.append(f"|  |  |  | schema: `{item['schema_error']}` |  |  |  |")
            if item["read_error"]:
                lines.append(f"|  |  |  | read: `{item['read_error']}` |  |  |  |")
    else:
        lines.append("No dirty files detected.")

    sanity = report["parquet_sanity"]
    lines.extend(
        [
            "",
            "## Daily Parquet Sanity",
            "",
            f"- Status: `{sanity['status']}`",
            f"- File count: `{sanity['file_count']}`",
            f"- Zero-byte files: `{sanity['zero_byte']}`",
            f"- Sample reads checked: `{sanity['sample_read_checked']}`",
        ]
    )
    if sanity["errors"]:
        lines.append(f"- Errors: {', '.join(f'`{item}`' for item in sanity['errors'])}")
    else:
        lines.append("- Errors: none")
    return "\n".join(lines) + "\n"


def generate_parquet_store_report(
    cfg: Config,
    *,
    out_dir: str | Path | None = None,
    write_json: bool = True,
    write_markdown: bool = True,
) -> StoreIntegrityReport:
    daily_root = parquet_root_path(cfg)
    intraday_root = intraday_root_path(cfg)
    runs_root = runs_root_path(cfg)
    report_dir = Path(out_dir) if out_dir is not None else _dated_report_dir(runs_root)
    report_dir.mkdir(parents=True, exist_ok=True)

    sections: list[StoreIntegritySection] = []
    dirty_files: list[StoreIntegrityFileIssue] = []

    daily_section, daily_dirty = _summarize_section(section="daily", root=daily_root, validator=validate_daily_frame)
    sections.append(daily_section)
    dirty_files.extend(daily_dirty)

    root_intraday_files = _scan_parquet_files(intraday_root)
    if root_intraday_files:
        section, items = _summarize_section(section="intraday", root=intraday_root, validator=validate_intraday_frame)
        sections.append(section)
        dirty_files.extend(items)

    interval_dirs = sorted(path for path in intraday_root.iterdir() if path.is_dir()) if intraday_root.exists() and intraday_root.is_dir() else []
    for interval_dir in interval_dirs:
        section, items = _summarize_section(
            section=f"intraday:{interval_dir.name}",
            root=interval_dir,
            validator=validate_intraday_frame,
        )
        sections.append(section)
        dirty_files.extend(items)

    parquet_sanity = run_parquet_sanity_checks(
        ParquetVerifyConfig(
            root=daily_root,
            universe_dir=universe_dir_path(cfg),
        )
    )

    json_path = report_dir / "parquet_store_report.json"
    markdown_path = report_dir / "parquet_store_report.md"

    report: StoreIntegrityReport = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config_path": str(cfg.source_path or ""),
        "daily_root": str(daily_root),
        "intraday_root": str(intraday_root),
        "sections": sections,
        "dirty_files": sorted(dirty_files, key=lambda item: (item["section"], item["symbol"], item["path"])),
        "parquet_sanity": parquet_sanity,
        "json_path": "",
        "markdown_path": "",
    }

    if write_json:
        report["json_path"] = str(json_path)
    if write_markdown:
        report["markdown_path"] = str(markdown_path)
        markdown_path.write_text(render_store_integrity_report_markdown(report), encoding="utf-8")
    if write_json:
        json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return report
