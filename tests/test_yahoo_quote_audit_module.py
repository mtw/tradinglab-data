from __future__ import annotations

from pathlib import Path

from tradinglab_data.yahoo_quote_audit import (
    YahooQuoteSnapshot,
    audit_rows_to_csv,
    audit_rows_to_markdown,
    audit_universe_file,
    canonicalize_yahoo_exchange,
    extract_yahoo_name,
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
