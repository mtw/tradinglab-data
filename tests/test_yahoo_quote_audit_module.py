from __future__ import annotations

import csv
import io
import sys
import types
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from tradinglab_data.yahoo_quote_audit import (
    YahooQuoteAuditRow,
    YahooQuoteSnapshot,
    audit_rows_to_csv,
    audit_rows_to_json,
    audit_rows_to_markdown,
    audit_universe_file,
    canonicalize_yahoo_exchange,
    classify_quote_audit_row,
    extract_yahoo_name,
    fetch_yahoo_quote_html,
    make_browser_snapshot_fetcher,
    normalize_name_for_compare,
    parse_yahoo_quote_snapshot,
    yahoo_quote_url,
)


def test_yahoo_quote_url_normalizes_symbol():
    assert yahoo_quote_url(" mveu.l ") == "https://finance.yahoo.com/quote/MVEU.L/"


def test_canonicalize_yahoo_exchange_collapses_common_aliases():
    assert canonicalize_yahoo_exchange("NasdaqGM") == "NASDAQ"
    assert canonicalize_yahoo_exchange("NYSEArca") == "ARCA"
    assert canonicalize_yahoo_exchange("LSE") == "LSE"
    assert canonicalize_yahoo_exchange("") == ""


def test_normalize_name_for_compare_collapses_punctuation_and_case():
    assert normalize_name_for_compare("iShares & Co., PLC") == "ISHARES AND CO PLC"


def test_extract_yahoo_name_prefers_page_title():
    text = "LSE - Delayed Quote\nEUR\nIgnored Body Name (MVEU.L)\n"
    title = "iShares Edge MSCI Europe Minimum Volatility UCITS ETF EUR (Acc) (MVEU.L) Stock Price, News, Quote & History - Yahoo Finance"
    assert extract_yahoo_name("MVEU.L", text, page_title=title) == "iShares Edge MSCI Europe Minimum Volatility UCITS ETF EUR (Acc)"


def test_parse_yahoo_quote_snapshot_from_header_line():
    html = """
    <html><body>
    # iShares Edge MSCI Europe Minimum Volatility UCITS ETF EUR (Acc) (MVEU.L)
    MVEU.L
    iShares Edge MSCI Europe Minimum Volatility UCITS ETF EUR (Acc)
    LSE - Delayed Quote EUR
    </body></html>
    """
    snapshot = parse_yahoo_quote_snapshot("MVEU.L", html, requested_url="u1", final_url="u2")
    assert snapshot.page_symbol == "MVEU.L"
    assert snapshot.page_name == "iShares Edge MSCI Europe Minimum Volatility UCITS ETF EUR (Acc)"
    assert snapshot.exchange_display == "LSE"
    assert snapshot.exchange_canonical == "LSE"
    assert snapshot.currency == "EUR"
    assert snapshot.ambiguous is False


def test_parse_yahoo_quote_snapshot_marks_ambiguous_when_multiple_pairs_found():
    html = """
    <html><body>
    ABC - Delayed Quote USD
    XETRA - Delayed Quote EUR
    </body></html>
    """
    snapshot = parse_yahoo_quote_snapshot("ABC", html)
    assert snapshot.ambiguous is True
    assert snapshot.parse_issue == "multiple_exchange_currency_pairs_found"


def test_parse_yahoo_quote_snapshot_uses_json_and_parenthesis_fallbacks():
    html = '{"exchangeName":"NasdaqGM","currency":"USD"} Name (AAA)'
    snapshot = parse_yahoo_quote_snapshot("AAA", html)
    assert snapshot.exchange_canonical == "NASDAQ"
    assert snapshot.page_symbol == "AAA"
    fallback = parse_yahoo_quote_snapshot("ZZZ", "First (BBB) Second (CCC)")
    assert fallback.page_symbol == "BBB"


def test_fetch_yahoo_quote_html_covers_success_and_http_error():
    class SuccessResponse:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return b"<html>ok</html>"
        def geturl(self):
            return "https://final/success"

    class SuccessOpener:
        def open(self, request, timeout=20.0):
            return SuccessResponse()

    payload, final_url, status = fetch_yahoo_quote_html("AAA", opener=SuccessOpener())
    assert payload == "<html>ok</html>"
    assert final_url == "https://final/success"
    assert status == 200

    class ErrorResponse(io.BytesIO):
        def __init__(self):
            super().__init__(b"bad")
        def geturl(self):
            return "https://final/error"

    req = urllib.request.Request("https://finance.yahoo.com/quote/AAA/")
    err = urllib.error.HTTPError(req.full_url, 404, "not found", hdrs=None, fp=ErrorResponse())

    class ErrorOpener:
        def open(self, request, timeout=20.0):
            raise err

    payload, final_url, status = fetch_yahoo_quote_html("AAA", opener=ErrorOpener())
    assert payload == "bad"
    assert final_url == "https://finance.yahoo.com/quote/AAA/"
    assert status == 404


def test_make_browser_snapshot_fetcher_supports_fake_playwright(monkeypatch):
    closed: list[str] = []

    class FakeButton:
        def __init__(self, name: str):
            self.name = name
        def click(self, timeout=3000):
            if self.name == "Reject all":
                raise RuntimeError("missing")
            return None

    class FakePage:
        def __init__(self):
            self.url = "https://consent.yahoo.com"
            self.calls = 0
        def set_default_timeout(self, timeout):
            self.timeout = timeout
        def goto(self, target_url, wait_until=None, timeout=None):
            self.calls += 1
            if self.calls == 1:
                self.url = "https://consent.yahoo.com"
                return type("Resp", (), {"status": 404})()
            self.url = target_url
            return type("Resp", (), {"status": 200})()
        def wait_for_timeout(self, ms):
            return None
        def locator(self, selector):
            return type("Loc", (), {"inner_text": lambda self, timeout=None: '# Test Name (AAA)\nNYSE - Delayed Quote USD'})()
        def title(self):
            return "Test Name (AAA) Stock Price - Yahoo Finance"
        def get_by_role(self, role, name):
            return FakeButton(name)

    class FakeContext:
        def new_page(self):
            return FakePage()
        def close(self):
            closed.append("context")

    class FakeBrowser:
        def new_context(self, locale="en-US"):
            return FakeContext()
        def close(self):
            closed.append("browser")

    class FakeChromium:
        def launch(self, headless=True):
            return FakeBrowser()

    class FakePW:
        chromium = FakeChromium()
        def stop(self):
            closed.append("pw")

    class FakeSyncPlaywright:
        def start(self):
            return FakePW()

    monkeypatch.setitem(sys.modules, "playwright.sync_api", types.SimpleNamespace(sync_playwright=lambda: FakeSyncPlaywright()))
    fetch, close = make_browser_snapshot_fetcher()
    snapshot = fetch("AAA")
    close()

    assert snapshot.exchange_canonical == "NYSE"
    assert snapshot.currency == "USD"
    assert "http_status_404" in snapshot.parse_issue
    assert closed == ["context", "browser", "pw"]


def test_make_browser_snapshot_fetcher_skips_consent_when_not_on_consent_domain(monkeypatch):
    closed: list[str] = []

    class FakePage:
        def __init__(self):
            self.url = "https://finance.yahoo.com/quote/AAA/"

        def set_default_timeout(self, timeout):
            self.timeout = timeout

        def goto(self, target_url, wait_until=None, timeout=None):
            self.url = target_url
            return type("Resp", (), {"status": 200})()

        def wait_for_timeout(self, ms):
            return None

        def locator(self, selector):
            return type("Loc", (), {"inner_text": lambda self, timeout=None: '# Test Name (AAA)\nNYSE - Delayed Quote USD'})()

        def title(self):
            return "Test Name (AAA) Stock Price - Yahoo Finance"

        def get_by_role(self, role, name):
            raise AssertionError("consent handler should not click buttons")

    class FakeContext:
        def new_page(self):
            return FakePage()

        def close(self):
            closed.append("context")

    class FakeBrowser:
        def new_context(self, locale="en-US"):
            return FakeContext()

        def close(self):
            closed.append("browser")

    class FakeChromium:
        def launch(self, headless=True):
            return FakeBrowser()

    class FakePW:
        chromium = FakeChromium()

        def stop(self):
            closed.append("pw")

    class FakeSyncPlaywright:
        def start(self):
            return FakePW()

    monkeypatch.setitem(sys.modules, "playwright.sync_api", types.SimpleNamespace(sync_playwright=lambda: FakeSyncPlaywright()))
    fetch, close = make_browser_snapshot_fetcher()
    snapshot = fetch("AAA")
    close()

    assert snapshot.exchange_canonical == "NYSE"
    assert closed == ["context", "browser", "pw"]


def test_make_browser_snapshot_fetcher_requires_playwright(monkeypatch):
    monkeypatch.delitem(sys.modules, "playwright.sync_api", raising=False)
    import builtins
    original_import = builtins.__import__
    def fake_import(name, *args, **kwargs):
        if name == "playwright.sync_api":
            raise ImportError("missing")
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="Playwright is not installed"):
        make_browser_snapshot_fetcher()


def test_audit_universe_file_reports_mismatch_and_match(tmp_path: Path):
    path = tmp_path / "etf_all.csv"
    path.write_text(
        "\n".join(
            [
                "symbol,exchange,currency,name,isin",
                "MVEU.L,LSE,EUR,MVEU,IE00A",
                "IQQ5.DE,XETRA,EUR,IQQ5,IE00B",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    snapshots = {
        "MVEU.L": YahooQuoteSnapshot(
            requested_symbol="MVEU.L",
            requested_url="r1",
            final_url="f1",
            page_symbol="MVEU.L",
            page_name="MVEU",
            exchange_display="LSE",
            exchange_canonical="LSE",
            currency="EUR",
            ambiguous=False,
            parse_issue="",
        ),
        "IQQ5.DE": YahooQuoteSnapshot(
            requested_symbol="IQQ5.DE",
            requested_url="r2",
            final_url="f2",
            page_symbol="IQQ5.DE",
            page_name="IQQ5",
            exchange_display="LSE",
            exchange_canonical="LSE",
            currency="GBP",
            ambiguous=False,
            parse_issue="",
        ),
    }

    rows = audit_universe_file(path, fetcher=lambda symbol: snapshots[symbol])
    assert [row.symbol for row in rows] == ["MVEU.L", "IQQ5.DE"]
    assert rows[0].status == "match"
    assert rows[1].status == "exchange_currency_mismatch"


def test_audit_universe_file_keeps_running_on_http_404(tmp_path: Path, monkeypatch):
    path = tmp_path / "etf_all.csv"
    path.write_text("symbol,exchange,currency,name,isin\nMISS,LSE,USD,Miss,IE00X\n", encoding="utf-8")

    monkeypatch.setattr(
        "tradinglab_data.yahoo_quote_audit.fetch_yahoo_quote_html",
        lambda symbol, timeout=20.0, base_url="", opener=None: ("", yahoo_quote_url(symbol), 404),
    )

    rows = audit_universe_file(path)
    assert rows[0].status == "parse_error"
    assert "http_status_404" in rows[0].issue


def test_classify_quote_audit_row_covers_ambiguous_name_and_page_symbol_cases():
    ambiguous = classify_quote_audit_row(
        symbol="AAA",
        local_exchange="NYSE",
        local_currency="USD",
        local_name="Test Name",
        snapshot=YahooQuoteSnapshot(
            requested_symbol="AAA",
            requested_url="r",
            final_url="f",
            page_symbol="BBB",
            page_name="Other Name",
            exchange_display="NYSE",
            exchange_canonical="NYSE",
            currency="USD",
            ambiguous=True,
            parse_issue="multiple",
        ),
    )
    assert ambiguous.status == "ambiguous"
    assert "page_symbol=BBB" in ambiguous.issue

    name_mismatch = classify_quote_audit_row(
        symbol="AAA",
        local_exchange="NYSE",
        local_currency="USD",
        local_name="Local Name",
        snapshot=YahooQuoteSnapshot(
            requested_symbol="AAA",
            requested_url="r",
            final_url="f",
            page_symbol="AAA",
            page_name="Yahoo Name",
            exchange_display="NYSE",
            exchange_canonical="NYSE",
            currency="USD",
            ambiguous=False,
            parse_issue="",
        ),
    )
    assert name_mismatch.status == "name_mismatch"

    missing_names = classify_quote_audit_row(
        symbol="AAA",
        local_exchange="NYSE",
        local_currency="USD",
        local_name="",
        snapshot=YahooQuoteSnapshot(
            requested_symbol="AAA",
            requested_url="r",
            final_url="f",
            page_symbol="AAA",
            page_name="Yahoo Name",
            exchange_display="NYSE",
            exchange_canonical="NYSE",
            currency="USD",
            ambiguous=False,
            parse_issue="",
        ),
    )
    assert "missing_local_name" in missing_names.issue
    missing_yahoo = classify_quote_audit_row(
        symbol="AAA",
        local_exchange="NYSE",
        local_currency="USD",
        local_name="Local Name",
        snapshot=YahooQuoteSnapshot("AAA", "r", "f", "AAA", "", "NYSE", "NYSE", "USD", False, ""),
    )
    assert "missing_yahoo_name" in missing_yahoo.issue


def test_audit_renderers_include_statuses(tmp_path: Path):
    path = tmp_path / "etf_all.csv"
    path.write_text("symbol,exchange,currency,name,isin\nMVEU.L,LSE,EUR,MVEU,IE00A\n", encoding="utf-8")
    rows = audit_universe_file(
        path,
        fetcher=lambda symbol: YahooQuoteSnapshot(
            requested_symbol=symbol,
            requested_url="r1",
            final_url="f1",
            page_symbol=symbol,
            page_name="MVEU",
            exchange_display="LSE",
            exchange_canonical="LSE",
            currency="EUR",
            ambiguous=False,
            parse_issue="",
        ),
    )
    markdown = audit_rows_to_markdown(rows)
    csv_text = audit_rows_to_csv(rows)
    assert "| MVEU.L | match |" in markdown
    assert "symbol,local_exchange,local_currency,local_name" in csv_text
    assert '"status": "match"' in audit_rows_to_json(rows)


def test_audit_rows_to_csv_round_trips_quoted_fields():
    rows = [
        YahooQuoteAuditRow(
            symbol="MVEU.L",
            local_exchange="LSE",
            local_currency="EUR",
            local_name='Name, "Quoted"\nLine',
            yahoo_exchange_display="LSE",
            yahoo_exchange="LSE",
            yahoo_currency="EUR",
            yahoo_name='Yahoo, "Quoted"\nLine',
            status="name_mismatch",
            issue='needs "manual", review',
            requested_url="https://finance.yahoo.com/quote/MVEU.L/",
            final_url="https://finance.yahoo.com/quote/MVEU.L/",
            page_symbol="MVEU.L",
            isin="IE00A",
        )
    ]

    csv_text = audit_rows_to_csv(rows)
    parsed = list(csv.DictReader(io.StringIO(csv_text)))

    assert len(parsed) == 1
    assert parsed[0]["symbol"] == "MVEU.L"
    assert parsed[0]["local_name"] == 'Name, "Quoted"\nLine'
    assert parsed[0]["yahoo_name"] == 'Yahoo, "Quoted"\nLine'
    assert parsed[0]["issue"] == 'needs "manual", review'


def test_audit_rows_to_csv_and_json_handle_empty_rows():
    assert audit_rows_to_csv([]).startswith("symbol,local_exchange")
    assert audit_rows_to_json([]) == "[]"


def test_audit_universe_file_filters_symbols_and_handles_fetcher_errors_and_sleep(tmp_path: Path, monkeypatch):
    path = tmp_path / "etf_all.csv"
    path.write_text(
        "symbol,exchange,currency,name,isin\nAAA,NYSE,USD,Name A,ISIN1\n,NYSE,USD,Skip,ISIN0\nBBB,NYSE,USD,Name B,ISIN2\nCCC,NYSE,USD,Name C,ISIN3\n",
        encoding="utf-8",
    )
    sleep_calls: list[float] = []
    monkeypatch.setattr("tradinglab_data.yahoo_quote_audit.time.sleep", lambda seconds: sleep_calls.append(seconds))

    def fetcher(symbol: str):
        if symbol == "AAA":
            return YahooQuoteSnapshot(symbol, "r", "f", "AAA", "Name A", "NYSE", "NYSE", "USD", False, "")
        raise RuntimeError("boom")

    rows = audit_universe_file(path, symbols=["AAA", "BBB"], sleep_seconds=0.2, fetcher=fetcher)

    assert [row.symbol for row in rows] == ["AAA", "BBB"]
    assert rows[1].status == "parse_error"
    assert "fetch_error_runtimeerror" in rows[1].issue
    assert sleep_calls == [0.2, 0.2]
