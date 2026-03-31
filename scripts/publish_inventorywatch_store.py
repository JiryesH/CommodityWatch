#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from inventory_watch_published_db import publish_inventory_store


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish InventoryWatch local read model")
    parser.add_argument(
        "--data-root",
        default=str(APP_ROOT / "backend"),
        help="InventoryWatch backend artifact root",
    )
    parser.add_argument(
        "--output",
        default=str(APP_ROOT / "data" / "inventorywatch.db"),
        help="Published InventoryWatch SQLite read-model path",
    )
    args = parser.parse_args()

    summary = publish_inventory_store(
        Path(args.data_root).expanduser().resolve(),
        Path(args.output).expanduser().resolve(),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
