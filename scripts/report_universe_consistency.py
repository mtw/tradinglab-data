#!/usr/bin/env python
from __future__ import annotations

import sys

from tradinglab_data.cli import main
from tradinglab_data.config import Config, default_config_path
from tradinglab_data.universe_listing import list_available_universes, render_available_universes

if __name__ == "__main__":
    argv = sys.argv[1:]
    if "--list-universes" in argv:
        config_path = str(default_config_path())
        for idx, token in enumerate(argv):
            if token == "--config" and idx + 1 < len(argv):
                config_path = argv[idx + 1]
                break
        cfg = Config.load(config_path)
        print(render_available_universes(list_available_universes(cfg)), end="")
        raise SystemExit(0)
    raise SystemExit(main(["report-universe-consistency", *sys.argv[1:]]))
