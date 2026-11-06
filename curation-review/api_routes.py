"""THE ROUTE TABLE in curation-review/API.md (T434), generated from `app.routes`: every route, its method and the first
line of its docstring, between the `routes:start` / `routes:end` markers. `pipeline/tests/test_review_api_routes.py`
fails when it drifts.

    PYTHONPATH=<repo> python curation-review/api_routes.py --write

Outside backend/: the backend's only file writer is writes.py (test_curation_review_safety).
"""
from __future__ import annotations

import sys
from pathlib import Path

API_MD = Path(__file__).resolve().parent / "API.md"
BACKEND = Path(__file__).resolve().parent / "backend"
START, END = "<!-- routes:start (curation-review/api_routes.py --write) -->", "<!-- routes:end -->"


def table(app) -> str:
    rows = []
    for r in app.routes:
        methods = sorted(getattr(r, "methods", None) or ()) or ["—"]
        if not r.path.startswith(("/api", "/basemap")) or methods == ["HEAD"]:
            continue
        doc = (getattr(r, "endpoint", None).__doc__ or "").strip().split("\n")[0].replace("|", "\\|")
        rows.append(f"| {' '.join(m for m in methods if m != 'HEAD')} | `{r.path}` | {doc} |")
    return "\n".join([START, "| method | path | what |", "|---|---|---|", *sorted(set(rows), key=lambda x: x.split("`")[1]),
                      END])


def current() -> str:
    s = API_MD.read_text(encoding="utf-8")
    return s[s.index(START):s.index(END) + len(END)] if START in s else ""


def main(argv: list[str]) -> int:
    sys.path.insert(0, str(BACKEND))
    import app
    want = table(app.app)
    if "--write" in argv:
        s = API_MD.read_text(encoding="utf-8")
        s = s.replace(current(), want) if current() else s.rstrip("\n") + "\n\n## Every route (generated)\n\n" + want + "\n"
        API_MD.write_text(s, encoding="utf-8")
        return 0
    return 0 if current() == want else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
