#!/usr/bin/env python3
"""NO HARD-CODED DATA PATH (T1021, AGENTS W46 / rules 36, 41): pipeline-ci's "No hardcoded curated or source paths"
step runs this. Standard library only.

    python3 scripts/check_data_paths.py            # every offender (file:line: the string), then the labels let through

WHAT IS REFUSED: in the tracked pipeline / tools/curation-review / project_config.py code (tests exempt: they pin the real
files), any STRING THAT STARTS A PATH UNDER data/ OR cache/ — read by the AST, so a string is a string however it is
written: `"data/source/x.json"`, `"./data/x"`, `f"data/{x}"`, `"data/" + "source/x"`, `Path("data")`,
`os.path.join("data", "source", …)`, `"Cache/dfo"` (any case). (`ROOT / "data" / …` is anchored at the repo root:
the step's older checks hold that form.) Relative to the cwd, each names a
second tree from any other directory; a path read or written comes from config.yaml (CURATED / GENERATED / SOURCE,
`get_config()`).

WHAT IS NOT: a docstring (the first string of a module, class or function) and a comment. Nothing else is exempt by
its shape — a code line that happens to hold a backtick is code (the grep this replaced dropped every such line).

A NAMED SOURCE FILE TYPED BY ITS FILE NAME (T1192) is refused too: the FWA GeoPackages are read as `SOURCE.fwa_gpkg` /
`SOURCE.coast_gpkg` (config.yaml `source:`), so a string that IS one of their file names, or ends a path in one
(`SOURCE / "bc_fisheries_data.gpkg"`, `f"{d}/fwa_coast.gpkg"`), is a second spelling of a name the config owns. Prose
that mentions the file (`"copy into bc_fisheries_data.gpkg"`) is not a path and passes.

A STRING THAT ONLY NAMES A PATH (provenance written into an output, git pathspecs, a repo-relative name a repo-root
command prints) carries `# path-label: <why>` (10+ characters) on one of its lines. The labels let through are printed,
counted, and pinned by `pipeline/tests/test_data_path_gate.py` (a new label is a visible diff); a label on a line with
no such string is refused (stale).
"""
from __future__ import annotations

import ast
import io
import re
import subprocess
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRS = ("pipeline", "data", "tools/curation-review", "project_config.py")
SKIP = re.compile(r"(^|/)(archive|\.venv|node_modules|__pycache__|graphify-out)/")
EXEMPT = re.compile(r"^pipeline/tests/")
LABEL = re.compile(r"#\s*path-label:\s*(\S.{9,})$")
#: a string that starts a path under the tree (`data/…`, `./cache/…`, any case)
UNDER = re.compile(r"^(\./)*(data|cache)/", re.I)
#: the tree's own name, alone: a path only where it STARTS one (`Path("data")`, `os.path.join("data", …)`,
#: `"data" / x`); `ROOT / "data" / …` is anchored at the repo root and is the older checks' business
BARE = re.compile(r"^(\./)*(data|cache)/?$", re.I)
PATH_CALLS = {"Path", "PurePath", "PosixPath", "join"}
#: (T1192) the named source files a string may not spell, and how they are read (`pipeline/tests/test_data_path_gate.py`
#: pins this against config.yaml's `source:` block)
NAMED = {"bc_fisheries_data.gpkg": "SOURCE.fwa_gpkg", "fwa_coast.gpkg": "SOURCE.coast_gpkg"}
#: a string that is one of `NAMED`, or a path ending in one (a string with whitespace is prose that mentions the file:
#: `"copy into data/source/bc_fisheries_data.gpkg"` names it, opens nothing)
SPELLS = re.compile(r"(?:^|/)(" + "|".join(re.escape(n) for n in NAMED) + r")$")
#: the two kinds of offender `check` returns: a data/ or cache/ path, a named source file spelt by its file name
PATH, NAMED_FILE = "path", "named source file"


def files(root: Path = ROOT) -> list[str]:
    out = subprocess.run(["git", "ls-files", "--", *DIRS], cwd=root, capture_output=True, text=True, check=True).stdout
    return sorted(f for f in out.split() if f.endswith(".py") and not SKIP.search(f) and not EXEMPT.match(f))


def _docstrings(tree: ast.AST) -> set[int]:
    """id() of every docstring node (the first statement of a module / class / function, a string)."""
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                out.add(id(first.value))
    return out


def strings(tree: ast.AST):
    """(node, text) of every string a path could start: each constant, and an f-string by its leading literal (its
    later parts follow a value, so they start nothing)."""
    inner = {id(v) for n in ast.walk(tree) if isinstance(n, ast.JoinedStr) for v in n.values}
    for n in ast.walk(tree):
        if isinstance(n, ast.JoinedStr):
            head = n.values[0] if n.values else None
            if isinstance(head, ast.Constant) and isinstance(head.value, str):
                yield n, head.value
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in inner:
            yield n, n.value


def every_string(tree: ast.AST):
    """(node, text) of every string constant, the literal parts of an f-string included (a file name can end one)."""
    for n in ast.walk(tree):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            yield n, n.value


def _starts_a_path(node: ast.AST, parent: dict[int, ast.AST]) -> bool:
    """A bare `"data"`: the first argument of Path(...) / os.path.join(...), or the left of a `/`."""
    up = parent.get(id(node))
    if isinstance(up, ast.Call) and up.args and up.args[0] is node:
        f = up.func
        return (f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else "") in PATH_CALLS
    return isinstance(up, ast.BinOp) and isinstance(up.op, ast.Div) and up.left is node


def check(rel: str, src: str) -> tuple[list[tuple[str, str]], list[str], list[str]]:
    """(offenders, labels let through, stale labels) of one file, each `file:line: …`; an offender is (its kind, that
    line): `PATH` (a data/ or cache/ path) or `NAMED_FILE` (a named source file spelt by its file name, `NAMED`)."""
    tree = ast.parse(src)
    labels = {}
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT and "path-label" in tok.string:
            m = LABEL.search(tok.string)
            labels[tok.start[0]] = m.group(1) if m else None          # None: a label with no reason
    docs = _docstrings(tree)
    parent = {id(c): n for n in ast.walk(tree) for c in ast.iter_child_nodes(n)}
    bad, let, used = [], [], set()
    paths = [(node, text, PATH, "") for node, text in strings(tree)
             if UNDER.match(text) or (BARE.match(text) and _starts_a_path(node, parent))]
    seen = {id(n) for n, *_ in paths}
    named = [(node, text, NAMED_FILE, f"  (a named source file: read it as {NAMED[m.group(1)]})")
             for node, text in every_string(tree)
             if (m := SPELLS.search(text)) and not re.search(r"\s", text) and id(node) not in seen]
    for node, text, kind, note in sorted(paths + named, key=lambda x: (x[0].lineno, x[0].col_offset)):
        if id(node) in docs:
            continue
        span = range(node.lineno, (node.end_lineno or node.lineno) + 1)
        why = next((labels[ln] for ln in span if labels.get(ln)), None)
        if why:
            used |= {ln for ln in span if labels.get(ln)}
            let.append(f"{rel}:{node.lineno}: {text!r}  ({why})")
        else:
            bad.append((kind, f"{rel}:{node.lineno}: {text!r}{note}"))
    stale = [f"{rel}:{ln}: a path-label {'with no reason (10+ characters)' if why is None else 'on no data/cache string'}"
             for ln, why in sorted(labels.items()) if ln not in used]
    return bad, let, stale


def main(argv: list[str] | None = None) -> int:
    bad, let, stale = [], [], []
    for rel in files():
        b, lt, st = check(rel, (ROOT / rel).read_text(encoding="utf-8"))
        bad, let, stale = bad + b, let + lt, stale + st
    for _kind, x in bad:
        print(f"      {x}")
    for x in stale:
        print(f"      {x}")
    print(f"  data/… or cache/… string in code       {sum(k == PATH for k, _ in bad):3}")
    print(f"  named source file spelt in code        {sum(k == NAMED_FILE for k, _ in bad):3}")
    print(f"  stale or reasonless path-label         {len(stale):3}")
    print(f"  path-labels let through                {len(let):3}")
    for x in let:
        print(f"      {x}")
    return 1 if bad or stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
