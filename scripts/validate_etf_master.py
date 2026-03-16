#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tradinglab_data.config import Config, default_config_path, universe_dir_path


DEFAULT_PATH_NAME = 'etf_master.csv'
REQUIRED_COLUMNS = (
    'symbol',
    'isin',
    'name',
    'region',
    'asset_class',
    'domicile',
    'exchange',
    'currency',
    'provider',
    'ucits',
    'inception_date',
    'expense_ratio',
    'avg_dollar_volume_3m',
    'status',
    'notes',
    'source',
)
REQUIRED_NON_EMPTY = ('symbol', 'region', 'asset_class', 'currency', 'status')
ALLOWED_REGION = {'US', 'EU'}
ALLOWED_ASSET_CLASS = {'equity', 'bond', 'commodity', 'multi_asset', 'sector', 'factor'}
ALLOWED_STATUS = {'seed', 'active', 'holdout', 'inactive'}
ALLOWED_UCITS = {'true', 'false', ''}


def _read_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        columns = list(reader.fieldnames or [])
    return rows, columns


def main() -> None:
    ap = argparse.ArgumentParser(description='Validate and summarize ETF master universe CSV.')
    ap.add_argument('--config', default=str(default_config_path()), help='Path to configuration YAML')
    ap.add_argument('--path', type=Path, default=None, help='ETF master CSV path (defaults to paths.universe_dir/etf_master.csv)')
    ap.add_argument('--show-missing', type=int, default=20, help='How many missing-field rows to print')
    ap.add_argument('--show-duplicates', type=int, default=20, help='How many duplicate groups to print')
    args = ap.parse_args()

    cfg = Config.load(args.config)
    path = args.path or (universe_dir_path(cfg) / DEFAULT_PATH_NAME)
    if not path.exists():
        print(f'ERROR: file does not exist: {path}')
        raise SystemExit(2)

    rows, cols = _read_rows(path)

    missing_columns = [c for c in REQUIRED_COLUMNS if c not in cols]
    extra_columns = [c for c in cols if c not in REQUIRED_COLUMNS]

    missing_required_values: list[tuple[int, str, str]] = []
    invalid_region: list[tuple[int, str, str]] = []
    invalid_asset: list[tuple[int, str, str]] = []
    invalid_status: list[tuple[int, str, str]] = []
    invalid_ucits: list[tuple[int, str, str]] = []

    symbol_counter: Counter[str] = Counter()
    symbol_exchange_counter: Counter[tuple[str, str]] = Counter()
    isin_counter: Counter[str] = Counter()

    for idx, row in enumerate(rows, start=2):
        symbol = str(row.get('symbol', '')).strip()
        exchange = str(row.get('exchange', '')).strip()
        isin = str(row.get('isin', '')).strip()

        if symbol:
            symbol_counter[symbol] += 1
        if symbol and exchange:
            symbol_exchange_counter[(symbol, exchange)] += 1
        if isin:
            isin_counter[isin] += 1

        for key in REQUIRED_NON_EMPTY:
            val = str(row.get(key, '')).strip()
            if not val:
                missing_required_values.append((idx, symbol or '-', key))

        region = str(row.get('region', '')).strip()
        if region and region not in ALLOWED_REGION:
            invalid_region.append((idx, symbol or '-', region))

        asset = str(row.get('asset_class', '')).strip()
        if asset and asset not in ALLOWED_ASSET_CLASS:
            invalid_asset.append((idx, symbol or '-', asset))

        status = str(row.get('status', '')).strip()
        if status and status not in ALLOWED_STATUS:
            invalid_status.append((idx, symbol or '-', status))

        ucits = str(row.get('ucits', '')).strip().lower()
        if ucits not in ALLOWED_UCITS:
            invalid_ucits.append((idx, symbol or '-', ucits))

    dup_symbol = [(k, v) for k, v in symbol_counter.items() if v > 1]
    dup_symbol_exchange = [(k, v) for k, v in symbol_exchange_counter.items() if v > 1]
    dup_isin = [(k, v) for k, v in isin_counter.items() if v > 1]

    print('ETF Master Validation')
    print(f'  path: {path}')
    print(f'  rows: {len(rows)}')
    print(f'  columns: {len(cols)}')
    print(f'  missing_columns: {len(missing_columns)}')
    if missing_columns:
        print(f"    {', '.join(missing_columns)}")
    print(f'  extra_columns: {len(extra_columns)}')
    if extra_columns:
        print(f"    {', '.join(extra_columns)}")
    print(f'  missing_required_values: {len(missing_required_values)}')
    print(f'  invalid_region: {len(invalid_region)}')
    print(f'  invalid_asset_class: {len(invalid_asset)}')
    print(f'  invalid_status: {len(invalid_status)}')
    print(f'  invalid_ucits: {len(invalid_ucits)}')
    print(f'  duplicate_symbol: {len(dup_symbol)}')
    print(f'  duplicate_symbol_exchange: {len(dup_symbol_exchange)}')
    print(f'  duplicate_isin: {len(dup_isin)}')

    hard_error = bool(
        missing_columns
        or missing_required_values
        or invalid_region
        or invalid_asset
        or invalid_status
        or invalid_ucits
        or dup_symbol_exchange
    )
    if hard_error:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
