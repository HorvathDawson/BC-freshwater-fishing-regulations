"""NO SILENT FALLBACK IN THE REVIEW APP'S FRONTEND (T434, audit 2026-11-05 review-deploy #4).

About 47 `.catch(() => …)` in `curation-review/frontend/src` turned a failed read into an empty answer: a failed list
became `[]` ("nothing here"), a failed reaches read an invented empty verdict, a failed policy read a copy kept in the
frontend. Now every `.catch(` handler must SHOW the failure — report it (`reportFailure(...)`, `failed(...)`, the
notice stack's FailuresBanner), set the screen's own error (`setError`, `setErrors`, ...), or throw it on — unless its
file is listed below with the exact count and why. A new silent fallback, or one more in a listed file, fails here.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "curation-review" / "frontend" / "src"

#: a handler that shows or passes the failure on
HANDLES = re.compile(r"\breportFailure\(|\bfailed\(|\bthrow\b|\bset(?:Error|Errors|IdsError|ViewError|Msg|Pv)\(")

_BODY = "parsing a FAILED answer's body, which may not be JSON: the status is still thrown / returned with it"
#: file (relative to src) -> (exact number of non-showing `.catch(` sites, why)
ALLOWLIST: dict[str, tuple[int, str]] = {
    "api.ts": (6, _BODY),
    "components/ReportsView.tsx": (1, _BODY),
}


def _code(src: str) -> str:
    """Comments out, lines kept (a `.catch` named in a comment is not a site)."""
    src = re.sub(r"/\*[\s\S]*?\*/", lambda m: re.sub(r"[^\n]", " ", m.group(0)), src)
    return re.sub(r"(^|[\s;{}),])//.*$", r"\1", src, flags=re.M)


def _balanced(src: str, i: int) -> str:
    """The text inside the parenthesis opening at src[i], strings skipped."""
    depth, j = 0, i
    while j < len(src):
        c = src[j]
        if c in "\"'`":
            j += 1
            while j < len(src) and src[j] != c:
                j += 2 if src[j] == "\\" else 1
        elif c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth == 0:
                return src[i + 1:j]
        j += 1
    return src[i + 1:]


def silent_catches(src: str) -> list[int]:
    """The lines of every `.catch(` whose handler neither reports, sets an error, nor throws."""
    code = _code(src)
    out = []
    for m in re.finditer(r"\.catch\s*\(", code):
        handler = _balanced(code, m.end() - 1)
        if not HANDLES.search(handler):
            out.append(code.count("\n", 0, m.start()) + 1)
    return out


def _files() -> list[Path]:
    return sorted(p for p in SRC.rglob("*") if p.suffix in (".ts", ".tsx"))


def test_every_catch_shows_its_failure_or_is_listed():
    got = {p.relative_to(SRC).as_posix(): silent_catches(p.read_text(encoding="utf-8")) for p in _files()}
    got = {f: lines for f, lines in got.items() if lines}
    off = [f"{f}: {len(got.get(f, []))} silent .catch @ {got.get(f, [])}, allowlist says {ALLOWLIST.get(f, (0,))[0]}"
           for f in sorted(set(got) | set(ALLOWLIST)) if len(got.get(f, [])) != ALLOWLIST.get(f, (0, ""))[0]]
    assert not off, ("a .catch that turns a failure into an empty answer: report it (reportFailure / failed(where), "
                     "src/failures.ts) or set the screen's error — " + "; ".join(off))


def test_no_policy_copy_in_the_frontend():
    """The extent ops and the split-anchor policy come from the server only (GET /api/extent-fix/ops, /api/split-anchors)."""
    for name in ("FALLBACK_OPS", "OFFSET_FALLBACK"):
        assert not [p.name for p in _files() if name in p.read_text(encoding="utf-8")], name


def test_the_scan_names_a_planted_fallback():
    src = "\n".join([
        "api.queue().then(setRows).catch(() => setRows([]));",          # 1: an empty list for a failure
        "api.queue().then(setRows).catch(() => {});",                   # 2: swallowed
        "api.queue().then(setRows).catch(failed(\"the queue\"));",      # shown
        "api.queue().then(setRows).catch((e) => setError(String(e)));",  # shown
        "api.queue().catch((e) => { reportFailure(\"q\", e); return []; });",  # shown
        "// api.queue().catch(() => [])",                               # a comment
        "p.catch((e) => { pending = null; throw e; });",                # passed on
    ])
    assert silent_catches(src) == [1, 2]
