"""TEST ANCHORS, the pipeline half (T667 work item 4; the app half is `app/tools/check-test-anchors.mjs`).

A test stands on ONE of four anchors (T667-audit §3 "Rules for new tests"): (a) data it builds itself, (b) a frozen
committed fixture, (c) THE DATA UNDER TEST read through `pipeline/tests/under_test.py`, (d) a generated expectations
file. This scanner finds the test files that stand on something else, or that name a position of one build. It reads
the AST, so a comment or a docstring is never an offender: only code is.

WHAT IS AN OFFENDER (every `test_*.py` under the pytest.ini testpaths; `conftest.py` and `under_test.py` are the
allowed helpers and are not scanned)

READS, in any test file — the live tree reached without the helper:
  generated   the name `GENERATED` (pipeline.common.curated) used in code.          -> `under_test.bundle()/atlas()/…`
  curated     the name `CURATED` used in a file that never sandboxes the tree (`curated_root(` / `sandbox_tree(` /
              `make_root(` / `CURATED_ROOT`): the live curated tree read in place.  -> `under_test.curated()`
  env         a read of an override the helper owns: os.environ[...] / .get(...) / getenv(...) of `ENV_OWNED`
              (setting one with monkeypatch is not a read).                         -> the helper reads it
  live-const  a product module's live path constant: `<module>.BUNDLE` / `<module>.OUT` (however long the dotted
              name), or `BUNDLE` / `OUT` imported from a `pipeline.` module (the bundle's and the export's default
              paths, a review tool's output file).                                   -> `under_test.bundle()/export_dir()`
              NOT one: the `OUT` of an EMITTER, a module of the package `pipeline.tools.emit` — the committed
              generated contract file under `app/` that the emitter writes and its `--check` compares (rule 40), named
              `E.OUT` after `from pipeline.tools.emit import gauge_policy as E` (or `import pipeline.tools.emit.pack as
              E`, or `OUT` imported from the emitter). It is in git and follows no build. The name must be bound by
              that import ONLY: one also bound by another import or assigned in the file is judged as any other.
              `test_test_anchors.py` holds every emitter's `OUT` to a path under `app/`, so the class cannot grow a
              live-data path.
  path        a string naming `data/generated|release|curated|source/...` that is anchored on the repo: the left end
              of a `/` chain that starts at a repo-root name (`REPO`, `REPO_ROOT`, `ROOT`) or at `__file__`, or a bare
              "data/..." handed straight to a call. A chain that starts anywhere else (`tmp_path / "data/curated"`,
              `tree / ...`) is the test's own tree and is fine; so is the string inside a dict, a list or an f-string.
  pointer     `release_root` (conftest's live release root) used in code: which release is under test is the
              helper's answer (`latest(release_root())` compares with LATEST during a release).
                                                                                    -> `under_test.release()`

PINS, only in a LIVE file — one with any read above, a `needs_*` marker, `need(`, or `under_test` — because a number in
a file that builds its own data names that data (a `sid = 99_999` in a synthetic test is fine):
  release-id  a string holding a release id `r-<12 hex>` that looks like a real one (5 or more different hex digits;
              `r-000000000001`, `r-aaaaaaaaaaaa` are invented).
  digest      a string that IS a build digest: 12-64 hex digits with 6 or more different ones and both letters and
              digits, or a tiles id `<16 hex>.<12 hex>`. A counting run (`0123456789abcdef`, `fedcba9876543210`) is
              invented and fine, and so is a tiles id whose two halves are both counting runs
              (`0123456789abcdef.0123456789ab`).
              NOT one: THE BOOK'S OWN SHA — a full SHA-256 (64 hex digits) that is one side of an `==` whose other
              side is read by a source-document key (`SOURCE_SHA_KEYS`: `m["pdf_sha256"]`). It names the published
              document a committed baseline was cut from; no build moves it. The same string anywhere else (assigned,
              passed, compared with another key such as `sha256` or `raw_data_sha256`) is a digest.
  raw-section a section HANDLE typed as a number: an int of 3 or more digits compared with, assigned to, passed as,
              or keyed by a name that says section (`sid`, `sids`, `handle`, `handles`, `section_id`, `dense_id`),
              or written into SQL text (`sid = 316756`, `sid IN (1, 2)`).            -> `under_test.sections().stream(blk, m)`
A section named by its id STRING (`"356075101:0"`, `"lake:-1174"`) is a name, not a position, and is fine.

Not scanned here (later T667 slices): literal counts on live data (D3), entry ids, format versions.

THE GUARD IS ON (2026-11-11, owner D4: every offender fails, no baseline, no list of excused files).
`python -m pipeline.tests.anchors` exits 1 while one offender is left, naming each hit and what to write instead
(`--json` prints every hit; `--strict` is accepted and means the same). `--report` only counts and exits 0: it is for
reading, no check may use it. `scripts/fast-fail.sh` runs the failing form, and
`test_test_anchors.py::test_the_live_tree_has_no_offender` fails the suite on the same hits. That file also plants one
offender of each kind. A rule that is wrong for a case is changed HERE, with a test for the accepted case and for the
near-miss that must still fail; a hit is never reworded past the scanner.
"""
from __future__ import annotations

import ast
import configparser
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

#: what to write instead, per kind (printed with a failure)
INSTEAD = {
    "generated": "read the build through the helper: under_test.bundle(request) / .atlas(request) / .reach_run(request) "
                 "/ .tiles(request) / .generated()",
    "curated": "under_test.curated().<field> (a test that WRITES sandboxes the tree first: curated_root(monkeypatch, "
               "tmp_path))",
    "env": "the helper reads the override: under_test.bundle(request) / .export_dir(request) / .atlas(request) / "
           ".release(request), or under_test.path(\"<name>\") at module level",
    "live-const": "under_test.bundle(request) / .export_dir(request) (module level: under_test.path(\"bundle\")); a "
                  "tool's own output file: under_test.generated().<…>",
    "path": "under_test.curated().<field> / under_test.generated().<…> / under_test.release(request); a tree the test "
            "builds starts at tmp_path",
    "pointer": "under_test.release(request): the release under test (the candidate during a release, else LATEST)",
    "release-id": "read the id from the data: under_test.release(request).name; an invented one is r-000000000001",
    "digest": "read it from the data under test (the bundle's meta, the sidecar, the manifest); an invented one is a "
              "counting run: \"0123456789abcdef\"",
    "raw-section": "name the section: under_test.sections(request).stream(<blue line>, <metre>) / .lake(<wbk>)",
}

READ_KINDS = ("generated", "curated", "env", "live-const", "path", "pointer")
PIN_KINDS = ("release-id", "digest", "raw-section")
KINDS = READ_KINDS + PIN_KINDS

#: the overrides `under_test` reads; a test reading one itself has its own idea of where the data is
ENV_OWNED = frozenset({"UI_EXPORT_BUNDLE", "UI_EXPORT_VERDICTS", "STATUS_INDEX", "ANSWERS_EXPORT_DIR", "UI_EXPORT_DIR",
                       "ATLAS_BUILD", "UI_EXPORT_REACHES", "UI_EXPORT_TILES", "RELEASE_DIR"})
LIVE_CONSTS = frozenset({"BUNDLE", "OUT"})
#: the emitters: each module's OUT is a committed generated contract file under app/ (header, live-const)
EMITTERS = "pipeline.tools.emit"
#: keys that hold the sha of a published source document (header, digest)
SOURCE_SHA_KEYS = frozenset({"pdf_sha256"})
REPO_NAMES = frozenset({"REPO", "REPO_ROOT", "ROOT"})
SANDBOXES = frozenset({"curated_root", "sandbox_tree", "make_root"})
SECTION_NAMES = frozenset({"sid", "sids", "handle", "handles", "section_id", "section_ids", "dense_id"})

_DATA = re.compile(r"(^|/)data/(generated|release|curated|source)(/|$)")
_RID = re.compile(r"\br-([0-9a-f]{12})\b")
_HEX = re.compile(r"^[0-9a-f]{12,64}$")
_TILES = re.compile(r"^[0-9a-f]{16}\.[0-9a-f]{12}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SQL_SID = re.compile(r"\bsid\s*(?:=|IN\s*\()\s*\d{3,}", re.I)


def real_release_id(text: str) -> bool:
    """Does `text` hold a release id that looks real (5+ different hex digits)?"""
    return any(len(set(m.group(1))) >= 5 for m in _RID.finditer(text))


_RUN = "0123456789abcdef" * 5


def _counting_run(text: str) -> bool:
    return text in _RUN or text[::-1] in _RUN


def is_digest(text: str) -> bool:
    if _TILES.match(text):
        return not all(_counting_run(half) for half in text.split("."))
    if _counting_run(text):
        return False
    return bool(_HEX.match(text)) and len(set(text)) >= 6 and any(c.isdigit() for c in text) \
        and any(c.isalpha() for c in text)


def _docstrings(tree: ast.AST) -> set[int]:
    out: set[int] = set()
    for n in ast.walk(tree):
        if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.body \
                and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant) \
                and isinstance(n.body[0].value.value, str):
            out.add(id(n.body[0].value))
    return out


def _leftmost(node: ast.AST) -> ast.AST:
    while isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        node = node.left
    return node


def _says_section(node: ast.AST) -> bool:
    """A name, attribute, string key or subscript-by-string-key that says "section handle"."""
    if isinstance(node, ast.Name):
        return node.id.lower() in SECTION_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr.lower() in SECTION_NAMES
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.lower() in SECTION_NAMES
    if isinstance(node, ast.Subscript):
        return _says_section(node.slice)
    return False


def _emitter_names(tree: ast.AST) -> set[str]:
    """The names this file binds to an emitter module (`pipeline.tools.emit.<module>`) and to nothing else."""
    emitters: set[str] = set()
    others: set[str] = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom):
            for a in n.names:
                (emitters if (n.module or "") == EMITTERS and not n.level else others).add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.asname:
                    (emitters if a.name.startswith(EMITTERS + ".") else others).add(a.asname)
                else:
                    others.add(a.name.split(".")[0])
        elif isinstance(n, ast.Name) and not isinstance(n.ctx, ast.Load):
            others.add(n.id)
        elif isinstance(n, (ast.arg, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            others.add(n.arg if isinstance(n, ast.arg) else n.name)
    return emitters - others


def _source_sha(s: ast.Constant, up: ast.AST | None) -> bool:
    """Is the string `s` a full SHA-256 compared (`==`) with a value read by a source-document key?"""
    if not _SHA256.match(s.value) or not isinstance(up, ast.Compare) or len(up.ops) != 1 \
            or not isinstance(up.ops[0], ast.Eq):
        return False
    other = up.comparators[0] if up.left is s else up.left
    return isinstance(other, ast.Subscript) and isinstance(other.slice, ast.Constant) \
        and other.slice.value in SOURCE_SHA_KEYS


def _handle_ints(node: ast.AST) -> bool:
    """An int of 3+ digits, or a list / tuple / set holding one."""
    if isinstance(node, ast.Constant):
        return type(node.value) is int and node.value >= 100
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return any(_handle_ints(e) for e in node.elts)
    return False


def scan_source(source: str) -> list[tuple[int, str, str]]:
    """Every offender of one test file's source: `(line, kind, what)`, in line order."""
    tree = ast.parse(source)
    parent: dict[int, ast.AST] = {}
    for n in ast.walk(tree):
        for c in ast.iter_child_nodes(n):
            parent[id(c)] = n
    doc = _docstrings(tree)
    reads: list[tuple[int, str, str]] = []
    pins: list[tuple[int, str, str]] = []
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
            {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
            {(a.asname or a.name).split(".")[-1] for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
             for a in n.names}
    strings = [n for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc]
    sandboxed = bool(names & SANDBOXES) or any(n.value == "CURATED_ROOT" for n in strings) or "ROOT_ENV" in names
    emitters = _emitter_names(tree)
    live = bool(names & {"need", "under_test"}) or any(a.startswith("needs_") for a in names)

    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load):
            if n.id == "GENERATED":
                reads.append((n.lineno, "generated", "GENERATED"))
            elif n.id == "CURATED" and not sandboxed:
                reads.append((n.lineno, "curated", "CURATED"))
            elif n.id == "release_root":
                reads.append((n.lineno, "pointer", "release_root"))
        elif isinstance(n, ast.ImportFrom):
            mod = n.module or ""
            for a in n.names:
                if a.name in LIVE_CONSTS and mod.startswith("pipeline.") \
                        and not (a.name == "OUT" and mod.startswith(EMITTERS + ".") and not n.level):
                    reads.append((n.lineno, "live-const", f"{mod}.{a.name}"))
        elif isinstance(n, ast.Attribute) and isinstance(n.ctx, ast.Load) and n.attr in LIVE_CONSTS \
                and isinstance(n.value, (ast.Name, ast.Attribute)):
            if not (n.attr == "OUT" and isinstance(n.value, ast.Name) and n.value.id in emitters):
                reads.append((n.lineno, "live-const", ast.unparse(n)[:60]))
        elif isinstance(n, ast.Call):
            f = n.func
            env_get = isinstance(f, ast.Attribute) and (
                (f.attr == "get" and isinstance(f.value, ast.Attribute) and f.value.attr == "environ")
                or (f.attr == "get" and isinstance(f.value, ast.Name) and f.value.id == "environ")
                or f.attr == "getenv")
            if env_get and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value in ENV_OWNED:
                reads.append((n.lineno, "env", str(n.args[0].value)))
            for k in n.keywords:
                if k.arg and k.arg.lower() in SECTION_NAMES and _handle_ints(k.value):
                    pins.append((k.value.lineno, "raw-section", f"{k.arg}={ast.unparse(k.value)[:40]}"))
        elif isinstance(n, ast.Subscript) and isinstance(n.ctx, ast.Load) and isinstance(n.slice, ast.Constant) \
                and n.slice.value in ENV_OWNED and "environ" in ast.unparse(n.value):
            reads.append((n.lineno, "env", str(n.slice.value)))
        elif isinstance(n, ast.Compare):
            sides = [n.left, *n.comparators]
            if any(_says_section(s) for s in sides) and any(_handle_ints(s) for s in sides):
                pins.append((n.lineno, "raw-section", ast.unparse(n)[:60]))
        elif isinstance(n, (ast.Assign, ast.AnnAssign)):
            targets = n.targets if isinstance(n, ast.Assign) else [n.target]
            if n.value is not None and any(_says_section(t) for t in targets) and _handle_ints(n.value):
                pins.append((n.lineno, "raw-section", ast.unparse(n)[:60]))
        elif isinstance(n, ast.Dict):
            for k, v in zip(n.keys, n.values):
                if k is not None and isinstance(k, ast.Constant) and _says_section(k) and _handle_ints(v):
                    pins.append((v.lineno, "raw-section", f"{k.value!r}: {ast.unparse(v)[:40]}"))

    for s in strings:
        text = s.value
        if _DATA.search(text):
            up = parent.get(id(s))
            top: ast.AST = s
            while isinstance(up, ast.BinOp) and isinstance(up.op, ast.Div):
                top, up = up, parent.get(id(up))
            left = _leftmost(top)
            anchored = (isinstance(left, ast.Name) and left.id in REPO_NAMES) or \
                       (left is not s and "__file__" in ast.unparse(left))
            bare = left is s and text.startswith("data/") and isinstance(up, ast.Call) and top in up.args
            if anchored or bare:
                reads.append((s.lineno, "path", text[:60]))
        if real_release_id(text):
            pins.append((s.lineno, "release-id", _RID.search(text).group(0)))
        elif is_digest(text) and not _source_sha(s, parent.get(id(s))):
            pins.append((s.lineno, "digest", text[:16]))
        if _SQL_SID.search(text):
            pins.append((s.lineno, "raw-section", _SQL_SID.search(text).group(0)[:40]))

    out = reads + (pins if (live or reads) else [])
    return sorted(set(out))


def test_files(root: Path = ROOT) -> list[str]:
    ini = configparser.ConfigParser()
    ini.read(root / "pytest.ini")
    trees = ini["pytest"]["testpaths"].split()
    return sorted(p.relative_to(root).as_posix() for t in trees for p in (root / t).rglob("test_*.py")
                  if "__pycache__" not in p.parts)


def scan(root: Path = ROOT, files: list[str] | None = None) -> dict[str, list[tuple[int, str, str]]]:
    """`{file: hits}` for every offending test file under `root` (files with no hit are left out)."""
    out = {}
    for f in files if files is not None else test_files(root):
        hits = scan_source((root / f).read_text(encoding="utf-8"))
        if hits:
            out[f] = hits
    return out


def totals(found: dict[str, list[tuple[int, str, str]]]) -> dict[str, tuple[int, int]]:
    """`{kind: (files, hits)}` in `KINDS` order."""
    return {k: (sum(1 for h in found.values() if any(x[1] == k for x in h)),
                sum(1 for h in found.values() for x in h if x[1] == k)) for k in KINDS}


def report(found: dict[str, list[tuple[int, str, str]]], n_files: int) -> str:
    lines = [f"test anchors (pipeline): {len(found)} of {n_files} test files offend"]
    lines += [f"  {k:12} {f:4} files {h:5} hits" for k, (f, h) in totals(found).items()]
    return "\n".join(lines)


def strict(argv=()) -> bool:
    """The guard is on: every run fails on an offender. `--report` (counts only, exit 0) is for reading."""
    return "--report" not in argv


def failure(found: dict[str, list[tuple[int, str, str]]]) -> str:
    """Every hit, then what to write instead for each kind hit."""
    lines = [f"{f}:{line}  {kind}: {what}" for f, hits in found.items() for line, kind, what in hits]
    hit = [k for k in KINDS if any(x[1] == k for h in found.values() for x in h)]
    lines += [f"test anchors: {len(found)} test file(s) off their anchor (T667, AGENTS W46). A test stands on data it "
              "builds, a frozen fixture, or the data under test through pipeline/tests/under_test.py. Instead:"]
    lines += [f"  {k}: {INSTEAD[k]}" for k in hit]
    lines += ["  the whole rule: the header of pipeline/tests/anchors.py. If the rule is wrong for your case, change it "
              "there with a test; never reword the hit past the scanner, and there is no list of excused files."]
    return "\n".join(lines)


def main(argv: list[str], root: Path | None = None) -> int:
    root = root or ROOT
    files = test_files(root)
    found = scan(root, files)
    if "--json" in argv:
        print(json.dumps({f: [list(h) for h in hits] for f, hits in found.items()}, indent=1))
    else:
        print(report(found, len(files)))
    if strict(argv) and found:
        print(failure(found), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
