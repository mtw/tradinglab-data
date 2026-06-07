from __future__ import annotations

import csv
import html
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from pathlib import Path

DEFAULT_YAHOO_QUOTE_BASE_URL = "https://finance.yahoo.com/quote/"
DEFAULT_ETF_MASTER_PATH_NAME = "etf_all.csv"
YAHOO_FETCH_MODES = ("http", "browser")

_HEADER_PAIR_PATTERN = re.compile(
    r"([A-Za-z][A-Za-z0-9 .&/_-]{1,80}?)\s*-\s*(?:Delayed Quote|(?:[A-Za-z]+(?: [A-Za-z]+)*)? Real Time Price)\s*(?:[•\-]\s*)?([A-Z]{3})"
)
_JSON_PAIR_PATTERNS = (
    re.compile(r'"exchangeName":"([^"]+)".{0,300}?"currency":"([A-Z]{3})"', re.DOTALL),
    re.compile(r'"fullExchangeName":"([^"]+)".{0,300}?"currency":"([A-Z]{3})"', re.DOTALL),
)
_PAGE_SYMBOL_PATTERNS = (
    re.compile(r"# [^(]+\(([A-Z0-9.\-=\^]+)\)"),
    re.compile(r'"symbol":"([A-Z0-9.\-=\^]+)"'),
)
_PAREN_SYMBOL_PATTERN = re.compile(r"\(([A-Z0-9.\-=\^]+)\)")


@dataclass(frozen=True)
class YahooQuoteSnapshot:
    requested_symbol: str
    requested_url: str
    final_url: str
    page_symbol: str
    page_name: str
    exchange_display: str
    exchange_canonical: str
    currency: str
    ambiguous: bool
    parse_issue: str


@dataclass(frozen=True)
class YahooQuoteAuditRow:
    symbol: str
    local_exchange: str
    local_currency: str
    local_name: str
    yahoo_exchange_display: str
    yahoo_exchange: str
    yahoo_currency: str
    yahoo_name: str
    status: str
    issue: str
    requested_url: str
    final_url: str
    page_symbol: str
    isin: str


SnapshotFetcher = Callable[[str], YahooQuoteSnapshot]
FetchClose = Callable[[], None]


def yahoo_quote_url(symbol: str, *, base_url: str = DEFAULT_YAHOO_QUOTE_BASE_URL) -> str:
    normalized = str(symbol or "").strip().upper()
    return urllib.parse.urljoin(base_url, urllib.parse.quote(normalized, safe="") + "/")


def canonicalize_yahoo_exchange(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    upper = normalized.upper().replace(" ", "")
    mapping = {
        "LSE": "LSE",
        "LONDONSTOCKEXCHANGE": "LSE",
        "XETRA": "XETRA",
        "NASDAQ": "NASDAQ",
        "NASDAQGM": "NASDAQ",
        "NASDAQGS": "NASDAQ",
        "NASDAQCM": "NASDAQ",
        "NYSEARCA": "ARCA",
        "ARCA": "ARCA",
        "NYSE": "NYSE",
        "NYSEMKT": "NYSE",
        "NYSEAMERICAN": "NYSE",
        "SW": "SW",
        "SIX": "SW",
        "SIXSWISSEXCHANGE": "SW",
        "SWX": "SW",
        "VIENNA": "VIENNA",
        "WIENERBORSE": "VIENNA",
        "EURONEXT": "EURONEXT",
        "PARIS": "FP",
        "EURONEXTPARIS": "FP",
        "FP": "FP",
        "CBOE": "CBOE",
    }
    return mapping.get(upper, normalized.upper())


def normalize_name_for_compare(value: str) -> str:
    text = str(value or "").strip().upper().replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return " ".join(text.split())


def extract_yahoo_name(symbol: str, text: str, *, page_title: str = "") -> str:
    wanted = str(symbol or "").strip().upper()
    title = str(page_title or "").strip()
    if title:
        title_match = re.match(r"^(.*?) \(([A-Z0-9.\-=\^]+)\) ", title)
        if title_match and str(title_match.group(2) or "").strip().upper() == wanted:
            return str(title_match.group(1) or "").strip()
    suffix = f"({wanted})"
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or not line.endswith(suffix):
            continue
        name = line[: -len(suffix)].strip().lstrip("#").strip()
        if name and normalize_name_for_compare(name) != normalize_name_for_compare(wanted):
            return name
    return ""


def fetch_yahoo_quote_html(
    symbol: str,
    *,
    timeout: float = 20.0,
    base_url: str = DEFAULT_YAHOO_QUOTE_BASE_URL,
    opener: urllib.request.OpenerDirector | None = None,
) -> tuple[str, str, int]:
    url = yahoo_quote_url(symbol, base_url=base_url)
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    client = opener or urllib.request.build_opener()
    try:
        with client.open(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8", errors="replace")
            final_url = response.geturl()
            status = getattr(response, "status", 200)
    except urllib.error.HTTPError as exc:
        try:
            payload = exc.read().decode("utf-8", errors="replace")
            final_url = exc.geturl()
            status = int(exc.code)
        finally:
            exc.close()
    return payload, final_url, status


def make_browser_snapshot_fetcher(
    *,
    timeout: float = 20.0,
    base_url: str = DEFAULT_YAHOO_QUOTE_BASE_URL,
) -> tuple[SnapshotFetcher, FetchClose]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # pragma: no cover - exercised only when runtime lacks playwright
        raise RuntimeError("Playwright is not installed. Install it before using --fetcher browser.") from exc

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(locale="en-US")
    page = context.new_page()
    page.set_default_timeout(int(timeout * 1000))

    def _handle_consent(target_url: str) -> None:
        if "consent.yahoo.com" not in page.url:
            return
        for label in ("Reject all", "Accept all"):
            try:
                page.get_by_role("button", name=label).click(timeout=3000)
                break
            except Exception:
                continue
        page.wait_for_timeout(500)
        if "consent.yahoo.com" in page.url:
            page.goto(target_url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
            page.wait_for_timeout(500)

    def _fetch(symbol: str) -> YahooQuoteSnapshot:
        requested_url = yahoo_quote_url(symbol, base_url=base_url)
        response = page.goto(requested_url, wait_until="domcontentloaded", timeout=int(timeout * 1000))
        _handle_consent(requested_url)
        page.wait_for_timeout(500)
        rendered_text = page.locator("body").inner_text(timeout=min(int(timeout * 1000), 5000))
        page_title = page.title()
        status = response.status if response is not None else 0
        snapshot = parse_yahoo_quote_snapshot(
            symbol,
            rendered_text,
            requested_url=requested_url,
            final_url=page.url,
            page_title=page_title,
        )
        if status >= 400:
            snapshot = replace(
                snapshot,
                parse_issue=",".join(filter(None, [snapshot.parse_issue, f"http_status_{status}"])),
            )
        return snapshot

    def _close() -> None:
        context.close()
        browser.close()
        pw.stop()

    return _fetch, _close


def parse_yahoo_quote_snapshot(
    symbol: str,
    html_text: str,
    *,
    requested_url: str = "",
    final_url: str = "",
    page_title: str = "",
) -> YahooQuoteSnapshot:
    text = html.unescape(str(html_text or ""))
    pairs: list[tuple[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    for match in _HEADER_PAIR_PATTERN.finditer(text):
        pair = (match.group(1).strip(), match.group(2).strip().upper())
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            pairs.append(pair)
    if not pairs:
        for pattern in _JSON_PAIR_PATTERNS:
            for match in pattern.finditer(text):
                pair = (match.group(1).strip(), match.group(2).strip().upper())
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    pairs.append(pair)

    page_symbol = ""
    for pattern in _PAGE_SYMBOL_PATTERNS:
        page_match = pattern.search(text)
        if page_match:
            page_symbol = str(page_match.group(1) or "").strip().upper()
            if page_symbol:
                break
    if not page_symbol:
        requested_symbol = str(symbol or "").strip().upper()
        candidates = [str(match.group(1) or "").strip().upper() for match in _PAREN_SYMBOL_PATTERN.finditer(text)]
        if requested_symbol in candidates:
            page_symbol = requested_symbol
        elif candidates:
            page_symbol = candidates[0]

    parse_issue = ""
    ambiguous = False
    page_name = extract_yahoo_name(symbol, text, page_title=page_title)
    exchange_display = ""
    exchange_canonical = ""
    currency = ""
    if not pairs:
        parse_issue = "no_exchange_currency_pair_found"
    else:
        exchange_values = {exchange for exchange, _ in pairs}
        currency_values = {curr for _, curr in pairs}
        if len(exchange_values) > 1 or len(currency_values) > 1:
            ambiguous = True
            parse_issue = "multiple_exchange_currency_pairs_found"
        exchange_display, currency = pairs[0]
        exchange_canonical = canonicalize_yahoo_exchange(exchange_display)

    return YahooQuoteSnapshot(
        requested_symbol=str(symbol or "").strip().upper(),
        requested_url=requested_url,
        final_url=final_url,
        page_symbol=page_symbol,
        page_name=page_name,
        exchange_display=exchange_display,
        exchange_canonical=exchange_canonical,
        currency=currency,
        ambiguous=ambiguous,
        parse_issue=parse_issue,
    )


def classify_quote_audit_row(
    *,
    symbol: str,
    local_exchange: str,
    local_currency: str,
    local_name: str,
    snapshot: YahooQuoteSnapshot,
    isin: str = "",
) -> YahooQuoteAuditRow:
    wanted_exchange = str(local_exchange or "").strip().upper()
    wanted_currency = str(local_currency or "").strip().upper()
    wanted_name = str(local_name or "").strip()
    actual_exchange = str(snapshot.exchange_canonical or "").strip().upper()
    actual_currency = str(snapshot.currency or "").strip().upper()
    actual_name = str(snapshot.page_name or "").strip()

    status = "match"
    issues: list[str] = []
    if snapshot.parse_issue:
        issues.append(snapshot.parse_issue)
    if snapshot.ambiguous:
        status = "ambiguous"
    elif not actual_exchange or not actual_currency:
        status = "parse_error"
    else:
        mismatch_fields: list[str] = []
        if actual_exchange != wanted_exchange:
            mismatch_fields.append("exchange")
        if actual_currency != wanted_currency:
            mismatch_fields.append("currency")
        if normalize_name_for_compare(wanted_name) and normalize_name_for_compare(actual_name):
            if normalize_name_for_compare(wanted_name) != normalize_name_for_compare(actual_name):
                mismatch_fields.append("name")
        elif wanted_name and not actual_name:
            issues.append("missing_yahoo_name")
        elif actual_name and not wanted_name:
            issues.append("missing_local_name")
        if mismatch_fields:
            status = "_".join(mismatch_fields) + "_mismatch"
    if snapshot.page_symbol and snapshot.page_symbol != str(symbol or "").strip().upper():
        issues.append(f"page_symbol={snapshot.page_symbol}")

    return YahooQuoteAuditRow(
        symbol=str(symbol or "").strip().upper(),
        local_exchange=wanted_exchange,
        local_currency=wanted_currency,
        local_name=wanted_name,
        yahoo_exchange_display=snapshot.exchange_display,
        yahoo_exchange=actual_exchange,
        yahoo_currency=actual_currency,
        yahoo_name=actual_name,
        status=status,
        issue=",".join(issues),
        requested_url=snapshot.requested_url,
        final_url=snapshot.final_url,
        page_symbol=snapshot.page_symbol,
        isin=str(isin or ""),
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def audit_universe_file(
    path: str | Path,
    *,
    symbols: list[str] | None = None,
    timeout: float = 20.0,
    sleep_seconds: float = 0.0,
    fetcher: SnapshotFetcher | None = None,
) -> list[YahooQuoteAuditRow]:
    csv_path = Path(path)
    rows = _read_csv_rows(csv_path)
    requested_symbols = {str(symbol or "").strip().upper() for symbol in (symbols or []) if str(symbol or "").strip()}
    out: list[YahooQuoteAuditRow] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        if requested_symbols and symbol not in requested_symbols:
            continue
        if fetcher is None:
            requested_url = yahoo_quote_url(symbol)
            html_text, final_url, status_code = fetch_yahoo_quote_html(symbol, timeout=timeout)
            snapshot = parse_yahoo_quote_snapshot(symbol, html_text, requested_url=requested_url, final_url=final_url)
            if status_code >= 400:
                status_issue = f"http_status_{status_code}"
                snapshot = replace(snapshot, parse_issue=",".join(filter(None, [snapshot.parse_issue, status_issue])))
        else:
            try:
                snapshot = fetcher(symbol)
            except Exception as exc:
                snapshot = YahooQuoteSnapshot(
                    requested_symbol=symbol,
                    requested_url=yahoo_quote_url(symbol),
                    final_url="",
                    page_symbol="",
                    page_name="",
                    exchange_display="",
                    exchange_canonical="",
                    currency="",
                    ambiguous=False,
                    parse_issue=f"fetch_error_{exc.__class__.__name__.lower()}",
                )
        out.append(
            classify_quote_audit_row(
                symbol=symbol,
                local_exchange=str(row.get("exchange") or ""),
                local_currency=str(row.get("currency") or ""),
                local_name=str(row.get("name") or ""),
                snapshot=snapshot,
                isin=str(row.get("isin") or ""),
            )
        )
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return out


def audit_rows_to_markdown(rows: list[YahooQuoteAuditRow]) -> str:
    lines = [
        "# Yahoo Quote Metadata Audit",
        "",
        f"- rows: `{len(rows)}`",
        "",
        "| Symbol | Status | Local Exchange | Yahoo Exchange | Local Currency | Yahoo Currency | Issue |",
        "|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row.symbol} | {row.status} | {row.local_exchange or '-'} | {row.yahoo_exchange or row.yahoo_exchange_display or '-'} | "
            f"{row.local_currency or '-'} | {row.yahoo_currency or '-'} | {row.issue or '-'} |"
        )
    return "\n".join(lines) + "\n"


def audit_rows_to_json(rows: list[YahooQuoteAuditRow]) -> str:
    return json.dumps([asdict(row) for row in rows], indent=2)


def audit_rows_to_csv(rows: list[YahooQuoteAuditRow]) -> str:
    if not rows:
        return (
            "symbol,local_exchange,local_currency,local_name,yahoo_exchange_display,yahoo_exchange,"
            "yahoo_currency,yahoo_name,status,issue,requested_url,final_url,page_symbol,isin\n"
        )
    columns = list(asdict(rows[0]).keys())
    output: list[str] = [",".join(columns)]
    for row in rows:
        values = []
        payload = asdict(row)
        for column in columns:
            value = str(payload.get(column, "") or "")
            if any(ch in value for ch in [",", '"', "\n"]):
                value = '"' + value.replace('"', '""') + '"'
            values.append(value)
        output.append(",".join(values))
    return "\n".join(output) + "\n"
