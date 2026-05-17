from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path

import polars as pl

from ._move_compute import summarize_gap_report


def _json_for_script(value: object) -> str:
    return (
        json.dumps(value, default=str)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def persist_alerts(alerts: pl.DataFrame, path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() == ".parquet":
        alerts.write_parquet(str(out))
    else:
        alerts.write_csv(str(out))
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
    const alerts = {_json_for_script(alerts)};
    const moves = {_json_for_script(records)};
    const topN = {int(top_n)};
    const initialSession = {_json_for_script(wanted)};
    const fmt = (v, digits=2) => (v === null || v === undefined || Number.isNaN(Number(v))) ? "-" : Number(v).toFixed(digits);
    const cls = v => Number(v) >= 0 ? "up" : "dn";
    const alertsBody = document.getElementById("alerts-body");
    const movesBody = document.getElementById("moves-body");
    const appendCell = (tr, value, className="") => {{
      const td = document.createElement("td");
      if (className) td.className = className;
      td.textContent = value ?? "-";
      tr.appendChild(td);
      return td;
    }};
    const appendPillCell = (tr, value) => {{
      const td = document.createElement("td");
      const span = document.createElement("span");
      span.className = "pill";
      span.textContent = value || "-";
      td.appendChild(span);
      tr.appendChild(td);
    }};
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
        appendCell(tr, r.symbol || "-");
        appendCell(tr, `${{fmt(r.pct_move)}}%`, cls(r.pct_move));
        appendPillCell(tr, r.session || "-");
        appendCell(tr, fmt(r.last_price, 4));
        alertsBody.appendChild(tr);
      }});
      let moveRows = sortRows(bySession(moves, session), state.moves);
      if (topN > 0) moveRows = moveRows.slice(0, topN);
      moveRows.forEach(r => {{
        const tr = document.createElement("tr");
        appendCell(tr, r.symbol || "-");
        appendCell(tr, `${{fmt(r.pct_move)}}%`, cls(r.pct_move));
        appendCell(tr, fmt(r.ref_close, 4));
        appendCell(tr, fmt(r.last_price, 4));
        appendCell(tr, fmt(r.last_volume, 0));
        appendPillCell(tr, r.session || "-");
        appendCell(tr, r.last_ts || "-");
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
