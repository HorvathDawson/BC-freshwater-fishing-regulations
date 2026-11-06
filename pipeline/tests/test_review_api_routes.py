"""T434: curation-review/API.md names every route the app serves — its generated table (`api_routes.py`) is current."""
from __future__ import annotations

import sys
from pathlib import Path

from pipeline.tests.conftest import curated_root

CR = Path(__file__).resolve().parents[2] / "curation-review"


def test_api_md_route_table_is_current(tmp_path, monkeypatch):
    curated_root(monkeypatch, tmp_path / "curated")
    monkeypatch.syspath_prepend(str(CR / "backend"))
    monkeypatch.syspath_prepend(str(CR))
    for mod in ("app", "reuse", "api_routes"):
        sys.modules.pop(mod, None)
    import api_routes
    import app
    assert api_routes.current() == api_routes.table(app.app), (
        "API.md's route table is stale: PYTHONPATH=. .venv/bin/python curation-review/api_routes.py --write")
    for mod in ("app", "reuse", "api_routes"):
        sys.modules.pop(mod, None)
