from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

import polars as pl

from .config import (
    Config,
    crypto_root_path,
    intraday_live_root_path,
    intraday_research_root_path,
    intraday_root_path,
    parquet_root_path,
    runs_root_path,
    universe_dir_path,
)
from .contracts import StoreHistoryEntry, StoreIntegrityFileIssue, StoreIntegrityReport, StoreIntegritySection
from .parquet_verify import ParquetVerifyConfig, run_parquet_sanity_checks
from .schema import (
    validate_crypto_frame,
    validate_daily_frame,
    validate_intraday_frame,
    validate_intraday_live_frame,
    validate_intraday_research_frame,
)


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
    max_gap_multiple: float
    zero_volume_rows: int
    metadata_inconsistent_columns: list[str]
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


def _currency_stats(df: pl.DataFrame, *, value_column: str = "currency") -> tuple[list[str], int, int]:
    if value_column not in df.columns:
        return [], int(df.height), 0
    currency_expr = pl.col(value_column).cast(pl.String, strict=False).str.strip_chars()
    currencies = (
        df.select(currency_expr.alias(value_column))
        .filter(pl.col(value_column).is_not_null() & (pl.col(value_column) != ""))
        .get_column(value_column)
        .unique()
        .sort()
        .to_list()
    )
    missing = int(df.select((currency_expr.is_null() | (currency_expr == "")).sum()).item())
    unknown = int(df.select((currency_expr.str.to_uppercase() == "UNKNOWN").sum()).item())
    return [str(value) for value in currencies], missing, unknown


def _sorted_times(df: pl.DataFrame, *, time_column: str) -> bool:
    if time_column not in df.columns or df.is_empty():
        return True
    try:
        return bool(df.get_column(time_column).is_sorted())
    except Exception:
        return False


def _history_bounds(df: pl.DataFrame, *, time_column: str) -> tuple[str | None, str | None]:
    if time_column not in df.columns or df.is_empty():
        return None, None
    try:
        start_date = df.select(pl.col(time_column).min()).item()
        end_date = df.select(pl.col(time_column).max()).item()
    except Exception:
        return None, None
    return _format_datetime(start_date), _format_datetime(end_date)


def _ohlc_quality_counts(df: pl.DataFrame, *, time_column: str) -> dict[str, int]:
    if df.is_empty():
        return {"null_ohlc": 0, "bad_ohlc": 0, "dup_times": 0}
    required = {time_column, "open", "high", "low", "close"}
    if not required.issubset(set(df.columns)):
        return {"null_ohlc": 1, "bad_ohlc": 1, "dup_times": 1}
    null_ohlc = int(
        df.select(
            pl.any_horizontal(
                [
                    pl.col("open").is_null(),
                    pl.col("high").is_null(),
                    pl.col("low").is_null(),
                    pl.col("close").is_null(),
                ]
            ).sum()
        ).item()
    )
    bad_ohlc = int(
        df.select(
            (
                (pl.col("open") <= 0)
                | (pl.col("high") <= 0)
                | (pl.col("low") <= 0)
                | (pl.col("close") <= 0)
                | (pl.col("high") < pl.col("low"))
                | (pl.col("high") < pl.col("open"))
                | (pl.col("high") < pl.col("close"))
                | (pl.col("low") > pl.col("open"))
                | (pl.col("low") > pl.col("close"))
            ).sum()
        ).item()
    )
    dup_times = int(df.height - df.select(pl.col(time_column).n_unique()).item())
    return {"null_ohlc": null_ohlc, "bad_ohlc": bad_ohlc, "dup_times": dup_times}


def _expected_step(section: str) -> timedelta | None:
    if not section.startswith("crypto:"):
        return None
    interval = section.rsplit(":", 1)[-1]
    if interval == "15m":
        return timedelta(minutes=15)
    if interval == "1h":
        return timedelta(hours=1)
    if interval == "1d":
        return timedelta(days=1)
    return None


def _max_gap_multiple(df: pl.DataFrame, *, time_column: str, expected_step: timedelta | None) -> float:
    if expected_step is None or df.height < 2 or time_column not in df.columns:
        return 1.0
    try:
        ordered = df.sort(time_column).get_column(time_column).to_list()
    except Exception:
        return 1.0
    expected_seconds = expected_step.total_seconds()
    if expected_seconds <= 0:
        return 1.0
    max_multiple = 1.0
    for left, right in zip(ordered, ordered[1:]):
        if not isinstance(left, datetime) or not isinstance(right, datetime):
            continue
        multiple = (right - left).total_seconds() / expected_seconds
        if multiple > max_multiple:
            max_multiple = multiple
    return float(max_multiple)


def _metadata_inconsistent_columns(df: pl.DataFrame, *, columns: list[str]) -> list[str]:
    inconsistent: list[str] = []
    for column in columns:
        if column not in df.columns:
            continue
        try:
            non_null = df.filter(pl.col(column).is_not_null())
            if non_null.is_empty():
                continue
            unique_count = int(non_null.select(pl.col(column).n_unique()).item())
        except Exception:
            continue
        if unique_count > 1:
            inconsistent.append(column)
    return inconsistent


def _audit_file(
    path: Path,
    *,
    section: str,
    validator: Callable[[pl.DataFrame], object],
    time_column: str,
    value_column: str = "currency",
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
            max_gap_multiple=1.0,
            zero_volume_rows=0,
            metadata_inconsistent_columns=[],
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

    start_date, end_date = _history_bounds(df, time_column=time_column)
    currencies, missing_currency_rows, unknown_currency_rows = _currency_stats(df, value_column=value_column)
    quality = _ohlc_quality_counts(df, time_column=time_column)
    duplicate_dates = int(quality.get("dup_times", 0))
    null_ohlc_rows = int(quality.get("null_ohlc", 0))
    bad_ohlc_rows = int(quality.get("bad_ohlc", 0))
    sorted_dates = _sorted_times(df, time_column=time_column)
    expected_step = _expected_step(section)
    max_gap_multiple = _max_gap_multiple(df, time_column=time_column, expected_step=expected_step)
    zero_volume_rows = int(df.select((pl.col("volume") <= 0).sum()).item()) if "volume" in df.columns else 0
    metadata_inconsistent_columns = _metadata_inconsistent_columns(
        df,
        columns=["provider", "exchange", "market_type", "symbol", "base_asset", "quote_asset", "interval", "source_symbol"],
    )

    if duplicate_dates > 0:
        dirty_reasons.append("duplicate_dates" if time_column == "date" else "duplicate_timestamps")
    if null_ohlc_rows > 0:
        dirty_reasons.append("null_ohlc_rows")
    if bad_ohlc_rows > 0:
        dirty_reasons.append("bad_ohlc_rows")
    if not sorted_dates:
        dirty_reasons.append("unsorted_dates" if time_column == "date" else "unsorted_timestamps")
    if missing_currency_rows > 0:
        dirty_reasons.append("missing_currency_rows" if value_column == "currency" else f"missing_{value_column}_rows")
    if unknown_currency_rows > 0:
        dirty_reasons.append("unknown_currency_rows" if value_column == "currency" else f"unknown_{value_column}_rows")
    if section.startswith("crypto:") and max_gap_multiple > 2.0:
        dirty_reasons.append("large_continuity_gap")
    if section.startswith("crypto:") and zero_volume_rows > 0:
        dirty_reasons.append("zero_volume_rows")
    if section.startswith("crypto:") and metadata_inconsistent_columns:
        dirty_reasons.append("metadata_inconsistency")

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
        max_gap_multiple=max_gap_multiple,
        zero_volume_rows=zero_volume_rows,
        metadata_inconsistent_columns=metadata_inconsistent_columns,
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
    validator: Callable[[pl.DataFrame], object],
    time_column: str,
    value_column: str = "currency",
) -> tuple[StoreIntegritySection, list[StoreIntegrityFileIssue]]:
    files = _scan_parquet_files(root)
    audits = [_audit_file(path, section=section, validator=validator, time_column=time_column, value_column=value_column) for path in files]
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
        f"- Crypto root: `{report['crypto_root']}`",
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
    intraday_research_root = intraday_research_root_path(cfg)
    intraday_live_root = intraday_live_root_path(cfg)
    crypto_root = crypto_root_path(cfg)
    runs_root = runs_root_path(cfg)
    report_dir = Path(out_dir) if out_dir is not None else _dated_report_dir(runs_root)
    report_dir.mkdir(parents=True, exist_ok=True)

    sections: list[StoreIntegritySection] = []
    dirty_files: list[StoreIntegrityFileIssue] = []

    daily_section, daily_dirty = _summarize_section(
        section="daily",
        root=daily_root,
        validator=validate_daily_frame,
        time_column="date",
        value_column="currency",
    )
    sections.append(daily_section)
    dirty_files.extend(daily_dirty)

    root_intraday_files = _scan_parquet_files(intraday_root)
    if root_intraday_files:
        section, items = _summarize_section(
            section="intraday",
            root=intraday_root,
            validator=validate_intraday_frame,
            time_column="date",
            value_column="currency",
        )
        sections.append(section)
        dirty_files.extend(items)

    interval_dirs = sorted(path for path in intraday_root.iterdir() if path.is_dir()) if intraday_root.exists() and intraday_root.is_dir() else []
    for interval_dir in interval_dirs:
        section, items = _summarize_section(
            section=f"intraday:{interval_dir.name}",
            root=interval_dir,
            validator=validate_intraday_frame,
            time_column="date",
            value_column="currency",
        )
        sections.append(section)
        dirty_files.extend(items)

    research_interval_dirs = (
        sorted(path for path in intraday_research_root.iterdir() if path.is_dir())
        if intraday_research_root.exists() and intraday_research_root.is_dir()
        else []
    )
    for interval_dir in research_interval_dirs:
        section, items = _summarize_section(
            section=f"intraday_research:{interval_dir.name}",
            root=interval_dir,
            validator=validate_intraday_research_frame,
            time_column="timestamp",
            value_column="currency",
        )
        sections.append(section)
        dirty_files.extend(items)

    live_interval_dirs = (
        sorted(path for path in intraday_live_root.iterdir() if path.is_dir())
        if intraday_live_root.exists() and intraday_live_root.is_dir()
        else []
    )
    for interval_dir in live_interval_dirs:
        section, items = _summarize_section(
            section=f"intraday_live:{interval_dir.name}",
            root=interval_dir,
            validator=validate_intraday_live_frame,
            time_column="timestamp",
            value_column="currency",
        )
        sections.append(section)
        dirty_files.extend(items)

    crypto_exchange_dirs = sorted(path for path in crypto_root.iterdir() if path.is_dir()) if crypto_root.exists() and crypto_root.is_dir() else []
    for exchange_dir in crypto_exchange_dirs:
        market_dirs = sorted(path for path in exchange_dir.iterdir() if path.is_dir())
        for market_dir in market_dirs:
            interval_dirs = sorted(path for path in market_dir.iterdir() if path.is_dir())
            for interval_dir in interval_dirs:
                section, items = _summarize_section(
                    section=f"crypto:{exchange_dir.name}:{market_dir.name}:{interval_dir.name}",
                    root=interval_dir,
                    validator=validate_crypto_frame,
                    time_column="timestamp",
                    value_column="quote_asset",
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
        "crypto_root": str(crypto_root),
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
