"""NO ORPHAN MODULES (T436, audit 2026-11-05 pipeline #7/#17). Six run-timing modules (the frozen-index path the feed
replaced) and three audit tools sat in `pipeline/` with nothing naming them: no importer, no `python -m` in a script, a
workflow or a doc. Dead code reads as live and is kept "in sync" by hand. Every pipeline module must now be NAMED
somewhere outside itself — imported (absolute or relative), run (`python -m …`), or cited by path in a tracked script,
workflow, test, tool or doc — or be listed below with why.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
SELF = "pipeline/tests/test_no_orphan_modules.py"
#: where a module may be named from (tracked files only)
WHERE = ("pipeline", "scripts", "curation-review", ".github", "app/tools", "app/workers", "deploy", "conftest.py",
         "handoff/PLAN/runbooks")
SUFFIXES = (".py", ".sh", ".yml", ".yaml", ".mjs", ".ts", ".toml", ".json", ".md")

#: module (repo-relative) -> why it is kept although nothing names it
ALLOWLIST: dict[str, str] = {
    "pipeline/tools/audit/slough_fold_preview.py":
        "the T322 slough-fold dry run (heavy, by hand); T322 is still PARTIAL, its ticket cites the command",
}

_TOKEN = re.compile(r"[A-Za-z_][\w./-]*")
_FROM = re.compile(r"from\s+(\.*[\w.]*)\s+import\s+(\([^)]*\)|[^\n]+)")


def mentions(texts: dict[str, str]) -> dict[str, set[str]]:
    """name -> the files mentioning it: every dotted name (and each `pipeline.` prefix of one), path and `pkg.name`
    (relative imports resolved against the file's package)."""
    out: dict[str, set[str]] = {}
    for f, t in texts.items():
        got = set(_TOKEN.findall(t))
        pkg = PurePosixPath(f).parent.as_posix().replace("/", ".")
        for base, names in _FROM.findall(t):
            if base.startswith("."):
                dots = len(base) - len(base.lstrip("."))
                parts = pkg.split(".")
                up = parts[: len(parts) - (dots - 1)] if dots > 1 else parts
                base = ".".join(up + ([base.lstrip(".")] if base.lstrip(".") else []))
            got.add(base)
            for n in re.findall(r"\w+", names):
                got.add(f"{base}.{n}")
        for t in got:
            out.setdefault(t, set()).add(f)
            if t.startswith("pipeline."):
                parts = t.split(".")
                for i in range(2, len(parts)):
                    out.setdefault(".".join(parts[:i]), set()).add(f)
    return out


def named(module: str, index: dict[str, set[str]]) -> bool:
    """Is `module` (a repo-relative .py path) named by any file but itself?"""
    dotted = module[:-3].replace("/", ".")
    short = module.removeprefix("pipeline/")
    return any(index.get(w, set()) - {module} for w in (dotted, module, short, short[:-3], module[:-3]))


def _orphans() -> list[str]:
    files = subprocess.run(["git", "-C", str(ROOT), "ls-files", *WHERE], capture_output=True, text=True,
                           check=True).stdout.split()
    texts = {f: (ROOT / f).read_text(encoding="utf-8", errors="ignore")
             for f in files if f.endswith(SUFFIXES) and "/fixtures/" not in f and (ROOT / f).is_file()
             and f != SELF}   # its own allowlist is not a mention
    index = mentions(texts)
    mods = [f for f in files if f.startswith("pipeline/") and f.endswith(".py") and "/tests/" not in f
            and not f.endswith(("__init__.py", "__main__.py", "conftest.py"))]
    return [m for m in mods if not named(m, index)]


def test_every_pipeline_module_is_named_somewhere():
    orphans = _orphans()
    assert not [m for m in orphans if m not in ALLOWLIST], (
        "a pipeline module nothing imports, runs or cites: delete it (git keeps it) or list it with a reason: "
        f"{[m for m in orphans if m not in ALLOWLIST]}")
    assert not [m for m in ALLOWLIST if m not in orphans], "an allowlisted module is named now (or gone): drop its line"


def test_the_check_sees_an_unnamed_module_and_the_ways_one_is_named():
    texts = {"pipeline/x/a.py": "from pipeline.x.b import f\n", "pipeline/x/c.py": "from .d import g\n",
             "scripts/run.sh": "python -m pipeline.x.e\n", "pipeline/x/f.py": "from pipeline.x import (\n    h,\n)\n",
             "pipeline/docs/MAP.md": "| `x/i.py` | a tool |\n", "pipeline/y/j.py": "from ..x import k\n"}
    index = mentions(texts)
    for m in ("b", "d", "e", "h", "i", "k"):
        assert named(f"pipeline/x/{m}.py", index), m
    assert not named("pipeline/x/z.py", index)
