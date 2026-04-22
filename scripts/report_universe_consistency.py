#!/usr/bin/env python
from __future__ import annotations

import sys

from tradinglab_data.cli import main

if __name__ == "__main__":
    raise SystemExit(main(["report-universe-consistency", *sys.argv[1:]]))
