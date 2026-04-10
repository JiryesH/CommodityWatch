from __future__ import annotations

import sys
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

try:
    from app.modules.demandwatch.cli import main
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Backend dependencies are not available in this interpreter. "
        "Activate the backend virtualenv or run `.venv/bin/python scripts/demandwatch.py ...`."
    ) from exc


if __name__ == "__main__":
    raise SystemExit(main())
