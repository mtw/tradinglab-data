from __future__ import annotations

import sys
from pathlib import Path

from tests._load import load_script_module

mod = load_script_module('validate_etf_master')


def test_validate_etf_master_ok(tmp_path: Path, monkeypatch):
    universe_dir = tmp_path / 'meta' / 'universes'
    universe_dir.mkdir(parents=True, exist_ok=True)
    path = universe_dir / 'etf_master.csv'
    path.write_text(
        'symbol,isin,name,region,asset_class,domicile,exchange,currency,provider,ucits,inception_date,expense_ratio,avg_dollar_volume_3m,status,notes,source\n'
        'SPY,US78462F1030,SPDR S&P 500 ETF,US,equity,US,NYSEARCA,USD,State Street,false,1993-01-22,0.09,1000000,active,,seed\n',
        encoding='utf-8',
    )

    class DummyCfg:
        pass

    monkeypatch.setattr(mod.Config, 'load', staticmethod(lambda _path: DummyCfg()))
    monkeypatch.setattr(mod, 'universe_dir_path', lambda _cfg: universe_dir)
    monkeypatch.setattr(sys, 'argv', ['validate_etf_master.py'])

    mod.main()
