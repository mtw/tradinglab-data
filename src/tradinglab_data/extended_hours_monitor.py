from __future__ import annotations

from datetime import datetime, timedelta, timezone
import html
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import time

import polars as pl
import yfinance as yf
from tqdm import tqdm

from ._ohlc_utils import align_for_concat, ensure_currency, needs_incremental_write
from ._yf_utils import (
    backoff_sleep,
    coerce_standard_schema,
    is_rate_limit_error,
    normalize_yf_df_to_polars,
    split_bulk_download,
)
from .contracts import DailyCloseInfo, ExtendedHoursResult
from .data_yf import (
    append_update_log,
    fetch_symbol_currency,
    read_parquet_if_exists,
)
from .schema import MOVE_ALERT_FRAME_SCHEMA


INTRADAY_SCHEMA = {
    "date": pl.Datetime,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adj_close": pl.Float64,
    "volume": pl.Float64,
    "currency": pl.String,
}

MAX_PERIOD_BY_INTERVAL = {
    "5m": "60d",
    "1m": "7d",
}

UPDATE_PERIOD_BY_INTERVAL = {
    "5m": "10d",
    "1m": "2d",
}


def _period_for_interval(interval: str, mapping: dict[str, str], *, purpose: str) -> str:
    period = mapping.get(interval)
    if period is None:
        supported = ", ".join(sorted(mapping))
        raise ValueError(f"Unsupported intraday interval for {purpose}: {interval!r}. Supported intervals: {supported}.")
    return period


def _empty_move_alert_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=MOVE_ALERT_FRAME_SCHEMA)


def _sanitize_intraday_df(df: pl.DataFrame | None) -> pl.DataFrame:
    if df is None or df.is_empty():
        return pl.DataFrame(schema=INTRADAY_SCHEMA)
    out = df
    needed = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in out.columns]
    if needed:
        out = out.select(needed + ([c for c in ["currency"] if c in out.columns]))
    out = out.filter(
        ~(
            pl.col("open").is_null()
            & pl.col("high").is_null()
            & pl.col("low").is_null()
            & pl.col("close").is_null()
        )
    )
    if "date" in out.columns:
        out = out.filter(pl.col("date").is_not_null())
    return out.sort("date")


def _normalize_intraday_pd(df_pd) -> pl.DataFrame:
    try:
        idx = getattr(df_pd, "index", None)
        if idx is not None and getattr(idx, "tz", None) is not None:
            df_pd = df_pd.copy()
            df_pd.index = idx.tz_convert("UTC").tz_localize(None)
    except Exception:
        pass
    df = normalize_yf_df_to_polars(df_pd)
    df = coerce_standard_schema(df)
    return _sanitize_intraday_df(df.select(["date", "open", "high", "low", "close", "adj_close", "volume"]))


def _fetch_intraday_bulk(
    symbols: list[str],
    interval: str,
    period: str,
    prepost: bool = True,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
    show_progress: bool = False,
    progress_desc: str = "extended-hours fetch",
) -> dict[str, pl.DataFrame]:
    if not symbols:
        return {}

    results: dict[str, pl.DataFrame] = {}
    chunk_starts = range(0, len(symbols), chunk_size)
    iterator = tqdm(chunk_starts, desc=progress_desc, unit="chunk") if show_progress else chunk_starts
    for i in iterator:
        chunk = symbols[i : i + chunk_size]
        attempt = 0
        while True:
            try:
                df_pd = yf.download(
                    chunk,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    prepost=prepost,
                    progress=False,
                    group_by="column",
                    threads=threads,
                )
                chunk_map = split_bulk_download(df_pd, chunk)
                out_chunk: dict[str, pl.DataFrame] = {}
                for sym, df_one in chunk_map.items():
                    try:
                        out_chunk[sym] = coerce_standard_schema(df_one)
                    except Exception:
                        continue
                results.update(out_chunk)
                break
            except Exception as e:
                attempt += 1
                if is_rate_limit_error(e) and attempt <= max_retries:
                    backoff_sleep(attempt, backoff_max_seconds)
                    continue
                if log_path is not None:
                    for sym in chunk:
                        append_update_log(log_path, sym, f"intraday_{interval}_error:{e}", attempt)
                break
        time.sleep(sleep_seconds)
    return results


def _fetch_intraday_one(
    symbol: str,
    interval: str,
    period: str,
    prepost: bool = True,
) -> pl.DataFrame:
    df_pd = yf.download(
        symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        prepost=prepost,
        progress=False,
        group_by="column",
        threads=False,
    )
    if df_pd is None or len(df_pd) == 0:
        return pl.DataFrame(schema=INTRADAY_SCHEMA)
    return _normalize_intraday_pd(df_pd)


def fetch_extended_intraday(
    symbols: list[str],
    interval: str,
    period: str,
    prepost: bool = True,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
) -> dict[str, pl.DataFrame]:
    out = _fetch_intraday_bulk(
        symbols=symbols,
        interval=interval,
        period=period,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
        show_progress=True,
        progress_desc=f"YF intraday {interval}",
    )
    missing = [s for s in symbols if s not in out or out[s].is_empty()]
    for sym in missing:
        try:
            df = _fetch_intraday_one(sym, interval=interval, period=period, prepost=prepost)
            if not df.is_empty():
                out[sym] = df
        except Exception as e:
            if log_path is not None:
                append_update_log(log_path, sym, f"intraday_{interval}_single_error:{e}", 1)
    return out


def _trim_rolling_window(df: pl.DataFrame, retention_days: int) -> pl.DataFrame:
    if df.is_empty():
        return df
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=max(1, retention_days))
    return df.filter(pl.col("date") >= pl.lit(cutoff)).sort("date")


def _session_label(dt_value: Any) -> str:
    if dt_value is None:
        return "unknown"
    try:
        dt = dt_value if isinstance(dt_value, datetime) else datetime.fromisoformat(str(dt_value))
    except Exception:
        return "unknown"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    et = dt.astimezone(ZoneInfo("America/New_York"))
    hm = et.hour * 60 + et.minute
    if 4 * 60 <= hm < 9 * 60 + 30:
        return "pre"
    if 9 * 60 + 30 <= hm < 16 * 60:
        return "regular"
    if 16 * 60 <= hm < 20 * 60:
        return "post"
    return "closed"


def load_daily_reference_closes(
    symbols: list[str],
    daily_root: str | Path,
) -> dict[str, DailyCloseInfo]:
    root = Path(daily_root)
    out: dict[str, DailyCloseInfo] = {}
    for sym in symbols:
        path = root / f"{sym}.parquet"
        df = read_parquet_if_exists(path)
        if df is None or df.is_empty() or "close" not in df.columns:
            continue
        tail = df.sort("date").tail(1)
        if tail.is_empty():
            continue
        close_v = tail.get_column("close").to_list()[0]
        currency = None
        if "currency" in tail.columns:
            vals = [v for v in tail.get_column("currency").to_list() if v is not None and str(v).strip()]
            if vals:
                currency = str(vals[0]).strip().upper()
        out[sym] = {"close": float(close_v), "currency": currency}
    return out


def _daily_close_frame(daily_close_map: dict[str, DailyCloseInfo | float]) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for sym, info in daily_close_map.items():
        if isinstance(info, dict):
            ref_close = info.get("close")
            ref_currency = info.get("currency")
        else:
            ref_close = info
            ref_currency = None
        if ref_close in {None, 0}:
            continue
        rows.append(
            {
                "symbol": str(sym),
                "ref_close": float(ref_close),
                "ref_currency": str(ref_currency).strip().upper() if ref_currency is not None and str(ref_currency).strip() else None,
            }
        )
    if not rows:
        return pl.DataFrame(schema={"symbol": pl.String, "ref_close": pl.Float64, "ref_currency": pl.String})
    return pl.DataFrame(rows, schema={"symbol": pl.String, "ref_close": pl.Float64, "ref_currency": pl.String})


def compute_moves_vs_close(
    intraday_df: pl.DataFrame | dict[str, pl.DataFrame],
    daily_close_map: dict[str, DailyCloseInfo | float],
) -> pl.DataFrame:
    if isinstance(intraday_df, dict):
        frames: list[pl.DataFrame] = []
        for sym, df in intraday_df.items():
            if df is None or df.is_empty():
                continue
            frames.append(df.with_columns(pl.lit(sym).alias("symbol")))
        data = pl.concat(frames, how="vertical") if frames else _empty_move_alert_frame()
    else:
        data = intraday_df

    if data.is_empty() or "symbol" not in data.columns:
        return _empty_move_alert_frame()

    valid = data.filter(pl.col("close").is_not_null())
    if valid.is_empty():
        return _empty_move_alert_frame()

    valid = valid.with_columns(pl.col("symbol").cast(pl.String, strict=False))
    if "date" in valid.columns:
        valid = valid.sort(["symbol", "date"])
    else:
        valid = valid.sort("symbol")

    aggregate_exprs: list[pl.Expr] = [
        pl.col("close").last().cast(pl.Float64).alias("last_price"),
        pl.col("date").last().alias("last_ts"),
    ]
    if "volume" in valid.columns:
        aggregate_exprs.append(pl.col("volume").last().cast(pl.Float64, strict=False).alias("last_volume"))
    else:
        aggregate_exprs.append(pl.lit(None, dtype=pl.Float64).alias("last_volume"))
    if "currency" in valid.columns:
        aggregate_exprs.append(pl.col("currency").last().cast(pl.String, strict=False).alias("last_currency"))
    else:
        aggregate_exprs.append(pl.lit(None, dtype=pl.String).alias("last_currency"))

    last_per_symbol = valid.group_by("symbol").agg(*aggregate_exprs)
    ref_df = _daily_close_frame(daily_close_map)
    if ref_df.is_empty():
        return _empty_move_alert_frame()

    moves = last_per_symbol.join(ref_df, on="symbol", how="inner")
    if moves.is_empty():
        return _empty_move_alert_frame()

    cleaned_last_currency = pl.col("last_currency").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
    cleaned_ref_currency = pl.col("ref_currency").cast(pl.String, strict=False).str.strip_chars().str.to_uppercase()
    moves = moves.with_columns(
        pl.when(cleaned_last_currency.is_null() | (cleaned_last_currency == ""))
        .then(
            pl.when(cleaned_ref_currency.is_null() | (cleaned_ref_currency == ""))
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(cleaned_ref_currency)
        )
        .otherwise(cleaned_last_currency)
        .alias("currency"),
        (((pl.col("last_price") / pl.col("ref_close")) - 1.0) * 100.0).alias("pct_move"),
        pl.col("last_ts").map_elements(_session_label, return_dtype=pl.String).alias("session"),
    )
    return moves.select(list(MOVE_ALERT_FRAME_SCHEMA)).sort("symbol")


def detect_alerts(
    moves_df: pl.DataFrame,
    threshold: float,
    min_volume: float | None = None,
) -> pl.DataFrame:
    if moves_df is None or moves_df.is_empty():
        return _empty_move_alert_frame()
    out = moves_df.filter(pl.col("pct_move").abs() >= float(threshold))
    if min_volume is not None and float(min_volume) > 0:
        out = out.filter(pl.col("last_volume").fill_null(0.0) >= float(min_volume))
    return out.sort("pct_move", descending=True)


def persist_alerts(alerts: pl.DataFrame, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() == ".parquet":
        alerts.write_parquet(str(out))
    else:
        alerts.write_csv(str(out))
    return out


def summarize_gap_report(
    moves_df: pl.DataFrame,
    threshold: float,
    min_volume: float | None = None,
    top_n: int = 25,
    session_filter: str = "all",
) -> pl.DataFrame:
    if moves_df is None or moves_df.is_empty():
        return _empty_move_alert_frame()
    out = moves_df
    wanted = str(session_filter or "all").strip().lower()
    if wanted not in {"all", "pre", "post", "regular", "closed"}:
        wanted = "all"
    if wanted != "all" and "session" in out.columns:
        out = out.filter(pl.col("session").cast(pl.String, strict=False) == wanted)
    if min_volume is not None and float(min_volume) > 0:
        out = out.filter(pl.col("last_volume").fill_null(0.0) >= float(min_volume))
    out = out.with_columns(pl.col("pct_move").abs().alias("abs_pct_move")).sort(
        ["abs_pct_move", "pct_move"], descending=[True, True]
    )
    if top_n and int(top_n) > 0:
        out = out.head(int(top_n))
    return out


def render_extended_hours_report_html(
    moves_df: pl.DataFrame,
    alerts_df: pl.DataFrame,
    threshold: float,
    generated_at: str | None = None,
    top_n: int = 50,
    session_filter: str = "all",
) -> str:
    wanted = str(session_filter or "all").strip().lower()
    all_moves = summarize_gap_report(moves_df, threshold=threshold, top_n=0, session_filter="all")
    records = all_moves.to_dicts() if all_moves is not None and not all_moves.is_empty() else []
    alerts = alerts_df.to_dicts() if alerts_df is not None and not alerts_df.is_empty() else []
    stamp = generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Extended-Hours Gap Report</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: #fffaf2;
      --ink: #1e1d1b;
      --muted: #6a645b;
      --line: #d9cfbf;
      --up: #0f7b6c;
      --dn: #b03a2e;
      --accent: #b7791f;
    }}
    body {{ margin: 0; background: radial-gradient(circle at top, #fff7e8 0%, var(--bg) 55%); color: var(--ink); font: 15px/1.5 Georgia, "Times New Roman", serif; }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px; }}
    .hero {{ background: linear-gradient(135deg, #fffaf2 0%, #f3e7d1 100%); border: 1px solid var(--line); padding: 24px; border-radius: 20px; box-shadow: 0 14px 50px rgba(90,72,41,0.08); }}
    h1 {{ margin: 0 0 8px; font-size: 32px; }}
    .sub {{ color: var(--muted); margin-bottom: 18px; }}
    .stats {{ display: flex; gap: 12px; flex-wrap: wrap; }}
    .stat {{ background: rgba(255,255,255,0.7); border: 1px solid var(--line); border-radius: 14px; padding: 10px 14px; min-width: 120px; }}
    .stat b {{ display: block; font-size: 18px; }}
    .grid {{ display: grid; grid-template-columns: 1.2fr 2fr; gap: 18px; margin-top: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 18px; }}
    .controls {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 14px; align-items: center; }}
    .controls label {{ color: var(--muted); font-size: 13px; }}
    select {{ border: 1px solid var(--line); border-radius: 10px; padding: 8px 10px; background: #fff; color: var(--ink); }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }}
    th {{ font-size: 12px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); cursor: pointer; user-select: none; }}
    th.active {{ color: var(--accent); }}
    tr:last-child td {{ border-bottom: none; }}
    .up {{ color: var(--up); font-weight: 700; }}
    .dn {{ color: var(--dn); font-weight: 700; }}
    .muted {{ color: var(--muted); }}
    .pill {{ display: inline-block; border: 1px solid var(--line); border-radius: 999px; padding: 2px 8px; font-size: 12px; }}
    .footer {{ margin-top: 14px; color: var(--muted); font-size: 13px; }}
    @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} .wrap {{ padding: 16px; }} h1 {{ font-size: 26px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Extended-Hours Gap Report</h1>
      <div class="sub">Pre-market and after-hours moves versus the latest regular-session close. Generated {html.escape(stamp)}.</div>
      <div class="stats">
        <div class="stat"><span class="muted">Threshold</span><b>{float(threshold):.2f}%</b></div>
        <div class="stat"><span class="muted">Session</span><b>{html.escape(wanted)}</b></div>
        <div class="stat"><span class="muted">Alert Symbols</span><b>{len(alerts)}</b></div>
        <div class="stat"><span class="muted">Reported Movers</span><b>{len(records)}</b></div>
      </div>
      <div class="controls">
        <label for="sessionFilter">Session</label>
        <select id="sessionFilter">
          <option value="all">all</option>
          <option value="pre">pre</option>
          <option value="post">post</option>
          <option value="regular">regular</option>
          <option value="closed">closed</option>
        </select>
      </div>
    </section>
    <section class="grid">
      <article class="card">
        <h2>Alerts</h2>
        <table>
          <thead><tr><th data-table="alerts" data-key="symbol">Symbol</th><th data-table="alerts" data-key="pct_move">Move</th><th data-table="alerts" data-key="session">Session</th><th data-table="alerts" data-key="last_price">Last</th></tr></thead>
          <tbody id="alerts-body"></tbody>
        </table>
      </article>
      <article class="card">
        <h2>Top Movers</h2>
        <table>
          <thead><tr><th data-table="moves" data-key="symbol">Symbol</th><th data-table="moves" data-key="pct_move">Move</th><th data-table="moves" data-key="ref_close">Ref Close</th><th data-table="moves" data-key="last_price">Last Price</th><th data-table="moves" data-key="last_volume">Volume</th><th data-table="moves" data-key="session">Session</th><th data-table="moves" data-key="last_ts">Timestamp</th></tr></thead>
          <tbody id="moves-body"></tbody>
        </table>
      </article>
    </section>
    <div class="footer">Artifact source: <code>&lt;paths.runs_root&gt;/YYYY-MM-DD/monitor/extended_hours_alerts.csv</code> and intraday parquet cache.</div>
  </div>
  <script>
    const alerts = {json.dumps(alerts, default=str)};
    const moves = {json.dumps(records, default=str)};
    const topN = {int(top_n)};
    const initialSession = {json.dumps(wanted)};
    const fmt = (v, digits=2) => (v === null || v === undefined || Number.isNaN(Number(v))) ? "-" : Number(v).toFixed(digits);
    const cls = v => Number(v) >= 0 ? "up" : "dn";
    const alertsBody = document.getElementById("alerts-body");
    const movesBody = document.getElementById("moves-body");
    const state = {{
      alerts: {{ key: "pct_move", dir: "desc" }},
      moves: {{ key: "pct_move", dir: "desc" }},
    }};
    const coerce = (v) => {{
      const n = Number(v);
      return Number.isNaN(n) ? String(v || "").toLowerCase() : n;
    }};
    const bySession = (rows, session) => session === "all" ? rows.slice() : rows.filter(r => (r.session || "unknown") === session);
    const sortRows = (rows, cfg) => rows.slice().sort((a, b) => {{
      const av = coerce(a[cfg.key]);
      const bv = coerce(b[cfg.key]);
      if (av < bv) return cfg.dir === "asc" ? -1 : 1;
      if (av > bv) return cfg.dir === "asc" ? 1 : -1;
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    }});
    const markHeaders = () => {{
      document.querySelectorAll("th[data-table]").forEach(th => {{
        const cfg = state[th.dataset.table];
        th.classList.toggle("active", th.dataset.key === cfg.key);
        const arrow = th.dataset.key === cfg.key ? (cfg.dir === "asc" ? " ▲" : " ▼") : "";
        th.textContent = th.textContent.replace(/[ ▲▼]+$/, "") + arrow;
      }});
    }};
    const render = () => {{
      const session = document.getElementById("sessionFilter").value;
      alertsBody.innerHTML = "";
      movesBody.innerHTML = "";
      sortRows(bySession(alerts, session), state.alerts).forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${{r.symbol || "-"}}</td><td class="${{cls(r.pct_move)}}">${{fmt(r.pct_move)}}%</td><td><span class="pill">${{r.session || "-"}}</span></td><td>${{fmt(r.last_price, 4)}}</td>`;
        alertsBody.appendChild(tr);
      }});
      let moveRows = sortRows(bySession(moves, session), state.moves);
      if (topN > 0) moveRows = moveRows.slice(0, topN);
      moveRows.forEach(r => {{
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${{r.symbol || "-"}}</td><td class="${{cls(r.pct_move)}}">${{fmt(r.pct_move)}}%</td><td>${{fmt(r.ref_close, 4)}}</td><td>${{fmt(r.last_price, 4)}}</td><td>${{fmt(r.last_volume, 0)}}</td><td><span class="pill">${{r.session || "-"}}</span></td><td>${{r.last_ts || "-"}}</td>`;
        movesBody.appendChild(tr);
      }});
      markHeaders();
    }};
    document.getElementById("sessionFilter").value = initialSession;
    document.querySelectorAll("th[data-table]").forEach(th => {{
      th.addEventListener("click", () => {{
        const cfg = state[th.dataset.table];
        cfg.dir = cfg.key === th.dataset.key && cfg.dir === "desc" ? "asc" : "desc";
        cfg.key = th.dataset.key;
        render();
      }});
    }});
    document.getElementById("sessionFilter").addEventListener("change", render);
    render();
  </script>
</body>
</html>"""


def persist_extended_hours_report_html(
    moves_df: pl.DataFrame,
    alerts_df: pl.DataFrame,
    path: str | Path,
    threshold: float,
    top_n: int = 50,
    session_filter: str = "all",
) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        render_extended_hours_report_html(
            moves_df=moves_df,
            alerts_df=alerts_df,
            threshold=threshold,
            top_n=top_n,
            session_filter=session_filter,
        ),
        encoding="utf-8",
    )
    return out


def _update_intraday_interval(
    target_symbols: list[str],
    interval: str,
    period: str,
    out_dir: Path,
    *,
    retention_days: int,
    prepost: bool,
    chunk_size: int,
    sleep_seconds: float,
    max_retries: int,
    backoff_max_seconds: float,
    threads: bool,
    log_path: Path | None,
    fetch_intraday_fn=None,
    read_frame_fn=None,
    fetch_currency_fn=None,
) -> list[str]:
    if fetch_intraday_fn is None:
        fetch_intraday_fn = fetch_extended_intraday
    if read_frame_fn is None:
        read_frame_fn = read_parquet_if_exists
    if fetch_currency_fn is None:
        fetch_currency_fn = fetch_symbol_currency
    fetched = fetch_intraday_fn(
        symbols=target_symbols,
        interval=interval,
        period=period,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    resolved: list[str] = []
    for sym in target_symbols:
        df_new = fetched.get(sym)
        path = out_dir / f"{sym}.parquet"
        df_old = read_frame_fn(path)
        cur = fetch_currency_fn(sym) or "UNKNOWN"
        if df_new is None or df_new.is_empty():
            if df_old is None or df_old.is_empty():
                continue
            df_old_raw = coerce_standard_schema(df_old)
            df_old_clean = _trim_rolling_window(
                ensure_currency(df_old_raw, cur, postprocess=_sanitize_intraday_df),
                retention_days=retention_days,
            )
            if df_old_clean.height != df_old_raw.height:
                df_old_clean.write_parquet(str(path))
                resolved.append(sym)
            continue
        df_new = _trim_rolling_window(
            ensure_currency(df_new, cur, postprocess=_sanitize_intraday_df),
            retention_days=retention_days,
        )
        if df_new.is_empty():
            continue
        if df_old is None or df_old.is_empty():
            df_new.write_parquet(str(path))
            resolved.append(sym)
            continue
        df_old_raw = coerce_standard_schema(df_old)
        old_rows_before = df_old_raw.height
        df_old = _trim_rolling_window(
            ensure_currency(df_old_raw, cur, postprocess=_sanitize_intraday_df),
            retention_days=retention_days,
        )
        old_sanitized = df_old.height != old_rows_before
        df_old, df_new = align_for_concat(
            df_old,
            df_new,
            schema=INTRADAY_SCHEMA,
            postprocess=_sanitize_intraday_df,
        )
        if not old_sanitized and not needs_incremental_write(df_old, df_new):
            resolved.append(sym)
            continue
        combined = (
            pl.concat([df_old, df_new], how="vertical")
            .unique(subset=["date"], keep="last")
            .sort("date")
        )
        combined = _trim_rolling_window(combined, retention_days=retention_days)
        combined.write_parquet(str(path))
        resolved.append(sym)
    return resolved


def update_extended_hours_store(
    symbols: list[str],
    intraday_root: str | Path,
    daily_root: str | Path,
    preferred_interval: str = "5m",
    fallback_interval: str = "1m",
    retention_days: int = 10,
    prepost: bool = True,
    pct_move_threshold: float = 2.0,
    min_volume: float = 0.0,
    alerts_path: str | Path | None = None,
    chunk_size: int = 20,
    sleep_seconds: float = 1.0,
    max_retries: int = 5,
    backoff_max_seconds: float = 120.0,
    threads: bool = False,
    log_path: Path | None = None,
) -> ExtendedHoursResult:
    root = Path(intraday_root)
    pref_dir = root / preferred_interval
    fb_dir = root / fallback_interval
    pref_dir.mkdir(parents=True, exist_ok=True)
    fb_dir.mkdir(parents=True, exist_ok=True)

    pref_missing: list[str] = []
    pref_existing: list[str] = []
    for sym in symbols:
        if (pref_dir / f"{sym}.parquet").exists():
            pref_existing.append(sym)
        else:
            pref_missing.append(sym)

    pref_resolved_missing = _update_intraday_interval(
        pref_missing,
        preferred_interval,
        _period_for_interval(preferred_interval, MAX_PERIOD_BY_INTERVAL, purpose="initial fetch"),
        pref_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    pref_resolved_existing = _update_intraday_interval(
        pref_existing,
        preferred_interval,
        _period_for_interval(preferred_interval, UPDATE_PERIOD_BY_INTERVAL, purpose="incremental fetch"),
        pref_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    unresolved = [s for s in symbols if s not in set(pref_resolved_missing) | set(pref_resolved_existing)]

    fb_missing: list[str] = []
    fb_existing: list[str] = []
    for sym in unresolved:
        if (fb_dir / f"{sym}.parquet").exists():
            fb_existing.append(sym)
        else:
            fb_missing.append(sym)
    fb_resolved_missing = _update_intraday_interval(
        fb_missing,
        fallback_interval,
        _period_for_interval(fallback_interval, MAX_PERIOD_BY_INTERVAL, purpose="initial fetch"),
        fb_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )
    fb_resolved_existing = _update_intraday_interval(
        fb_existing,
        fallback_interval,
        _period_for_interval(fallback_interval, UPDATE_PERIOD_BY_INTERVAL, purpose="incremental fetch"),
        fb_dir,
        retention_days=retention_days,
        prepost=prepost,
        chunk_size=chunk_size,
        sleep_seconds=sleep_seconds,
        max_retries=max_retries,
        backoff_max_seconds=backoff_max_seconds,
        threads=threads,
        log_path=log_path,
    )

    latest_frames: dict[str, pl.DataFrame] = {}
    for sym in symbols:
        candidates: list[tuple[datetime | None, str, pl.DataFrame]] = []
        for interval, d in ((preferred_interval, pref_dir), (fallback_interval, fb_dir)):
            p = d / f"{sym}.parquet"
            df = read_parquet_if_exists(p)
            if df is None or df.is_empty():
                continue
            try:
                last_ts = df.select(pl.col("date").max()).item()
            except Exception:
                last_ts = None
            candidates.append((last_ts, interval, df.with_columns(pl.lit(interval).alias("interval"))))
        if not candidates:
            continue
        candidates.sort(key=lambda x: (x[0] is not None, x[0], x[1] == preferred_interval), reverse=True)
        latest_frames[sym] = candidates[0][2]

    daily_close_map = load_daily_reference_closes(symbols, daily_root=daily_root)
    moves_df = compute_moves_vs_close(latest_frames, daily_close_map)
    alerts = detect_alerts(moves_df, threshold=pct_move_threshold, min_volume=min_volume)

    alert_file = None
    if alerts_path is not None:
        alert_file = persist_alerts(alerts, alerts_path)

    return {
        "preferred_interval": preferred_interval,
        "fallback_interval": fallback_interval,
        "symbols": len(symbols),
        "preferred_written": len(set(pref_resolved_missing) | set(pref_resolved_existing)),
        "fallback_written": len(set(fb_resolved_missing) | set(fb_resolved_existing)),
        "alerts": alerts.height if alerts is not None and not alerts.is_empty() else 0,
        "alerts_path": str(alert_file) if alert_file is not None else "",
        "moves_df": moves_df,
        "alerts_df": alerts,
    }
