"""WHAT FOLLOWS THE RELEASE (T666 item 1): every committed artifact cut from the live release, and every test that
compares with it — ONE table, read by `pipeline/release.py` (its `follows` stage refreshes them against the CANDIDATE,
before the promote) and by nothing else.

    python -m pipeline.release_follows            # the table, and the test files the rules below find today

Why (build 27, r-303b4007a70a): each of these was refreshed by hand, after the promote, found one at a time — the
goldens re-cut, the in-season matches, the reach baseline, the tests that compare with LATEST. A thing that follows the
release and is not in this table is the next hand step: add a row (or a rule), never a line in a runbook.

THE TESTS ARE FOUND BY RULE, not listed (T667 D6): a test follows the release when it asks for THE RELEASE UNDER TEST
through the one helper — `under_test.release` / `release_at` in the pipeline suite, `liveRelease` / `namedRelease` (or a
`*.release.test.ts` name) in the app suite. `RELEASE_DIR` names that release in both (the env contract:
`pipeline/tests/under_test.py`); the release command sets it to its candidate, so nothing compares with LATEST before
the promote.

(T680) AND THEN EVERYTHING ELSE: a re-cut also changes fixtures that tests read with no rule above (the regs fixture, a
golden page's words), so after the re-cut the release runs the whole app check too — every `check:fast` gate one by one,
the whole app suite, the feeds worker's suite — and lists every failure together (`app_check_steps`, `app_suite_cmd`,
`worker_suite_cmd`, `report_failures`).
"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pipeline.common.curated import CURATED, REPO_ROOT

#: the variable that names the release under test in both suites (`under_test.RELEASE_ENV`, `fixtureRelease.ts`)
RELEASE_ENV = "RELEASE_DIR"


def _rel(p) -> str:
    return str(Path(p).resolve().relative_to(Path(REPO_ROOT).resolve()))


@dataclass(frozen=True)
class Follow:
    """One thing that follows the release. `paths`: the committed paths its refresh rewrites (repo-relative; a folder
    means everything under it). `refresh`: the key of the command that re-makes it (`REFRESH`), or None when the
    release keeps it without a command. `answers`: a change here is a change in what an angler is told (the release
    stops and shows it; every other row's change is committed with the release id). `pins` (T1135): the paths are
    release-pins files, judged pin by pin (`moved_answer_pins`): only a moved `answers: true` pin is an answer change."""
    name: str
    what: str
    paths: tuple[str, ...]
    refresh: str | None
    answers: bool = False
    pins: bool = False


#: the app's release-backed fixtures, all re-cut by ONE command (`app/tools/regs-recut/sync.mjs`, T441), in its order
RECUT = "recut"
#: (T1135) the command that writes the release-pins files (the re-cut's last step)
REFRESH_PINS = "node app/tools/regs-recut/sync.mjs"
#: the in-season notice matches, re-resolved on the release (T537)
INSEASON = "inseason"
#: (T791 #4, T993 item 16) the pipeline's live-data tier, run on the release
DATA_TIER = "data-tier"

FOLLOWS: tuple[Follow, ...] = (
    Follow("goldens", "the tap oracle: every golden state's answer keys on the release's own store",
           ("app/fixtures/golden/taps.json",), RECUT),
    Follow("page-oracle", "page v36's own code over the release: every golden state's settle, card and year strip, rules "
           "by durable key (T667 5b: replaces the frozen page's hand-amended rulings)",
           ("app/fixtures/golden/page-oracle.json",), RECUT, answers=True),
    Follow("regs-fixture", "the committed fixture release and its section lines, cut from the release",
           ("app/packages/data/dev/regs-release", "app/packages/data/dev/section-lines.json"), RECUT),
    Follow("golden-pages", "the frozen golden pages (the words of each golden state's page)",
           ("app/packages/ui-native/src/regs/fixtures",), RECUT, answers=True),
    # (T1135) values copied from a release into tests (`pin(name, value)`), recorded on the release by the re-cut's last
    # step (`app/tools/regs-recut/pins.mjs`): a moved `answers: true` pin stops with the golden pages, under one token
    Follow("pins", "the release pins: each value a release-backed test compares with, recorded on the release",
           ("app/fixtures/golden/release-pins.json", "pipeline/tests/fixtures/release-pins.json"), RECUT, pins=True),
    Follow("inseason", "the in-season notice matches, their ranges re-derived on the release (T679 #11: into the "
           "generated resolution, data/generated/inseason/resolved.json; not in git — the curated matches are only read)",
           (), INSEASON),
    # (T791 #4, T993 item 16; T931: ~55 stale pins accumulated where nothing ran the data tier) a test RUN, not an
    # artifact: no paths; the follows stage runs it with the other checks on the candidate, once per release
    Follow("data-tier", "the pipeline's live-data tests (every needs_* tier, not slow) run on the candidate in chunks, "
           "once per release, the files found by rule above left out (they run whole)", (), DATA_TIER),
    Follow("baseline", "the reach run the live release was cut from (the binding diff's \"before\"; not in git: "
           "data/generated/release/baseline/<LATEST>/, kept by the baseline stage and by `pipeline build`)", (), None),
)


def data_tier_markers(ini: Path | None = None) -> str:
    """(T993 item 16) the live-data tier's marker expression: every `needs_*` marker (`tests.tiers.data_markers`, from
    pytest.ini), never `slow` — the tests CI's no-data tier leaves out and nothing else ran."""
    from pipeline.tests.tiers import data_markers
    needs = [m for m in (data_markers(ini) if ini else data_markers()) if m != "slow"]
    return "not slow and (" + " or ".join(needs) + ")"


def data_tier_cmd(whole: list[str]) -> list[str]:
    """(T993 item 16) the live-data tier in chunks (scripts/pytest-chunked.sh: one process for the suite grew to
    ~111 GB). The files in `whole` (found by rule) run whole in their own step, so they are deselected here — nothing
    runs twice in one release (T993 item 15)."""
    return ["bash", "scripts/pytest-chunked.sh", "--", "-q", "-p", "no:cacheprovider", "-m", data_tier_markers(),
            *(f"--deselect={f}" for f in whole)]


def refresh_cmd(key: str, release_dir: Path) -> list[str]:
    """The ONE command that refreshes every row with `refresh == key`, against the release at `release_dir` (the data
    tier reads it as RELEASE_DIR, `test_env`, so the command itself does not name it)."""
    if key == DATA_TIER:
        return data_tier_cmd(pipeline_tests())
    if key == RECUT:
        return ["node", "app/tools/regs-recut/sync.mjs", "--release", str(release_dir)]
    if key == INSEASON:
        return [sys.executable, "-m", "pipeline.regs.inseason.resolve", "--release", str(release_dir)]
    raise KeyError(f"release_follows: no refresh {key!r}")


def paths_of(key: str) -> tuple[str, ...]:
    """Every committed path the refresh `key` may rewrite."""
    return tuple(p for f in FOLLOWS if f.refresh == key for p in f.paths)


def under(path: str, roots: tuple[str, ...]) -> bool:
    return any(path == r or path.startswith(r.rstrip("/") + "/") for r in roots)


def row_of(path: str) -> Follow | None:
    """The row a changed path belongs to (None: not something that follows the release)."""
    return next((f for f in FOLLOWS if under(path, f.paths)), None)


def answer_paths(changed: list[str]) -> list[str]:
    """The changed paths that are a change in what an angler is told (rows with `answers`; a pins file whose
    `answers: true` pins moved, T1135). Empty: the change only renumbers — ids, handles, counts, the fixture release's
    files (T667 D5: committed without a stop)."""
    def answers(p: str) -> bool:
        row = row_of(p)
        return bool(row and (row.answers or (row.pins and moved_answer_pins(p))))
    return sorted(p for p in changed if answers(p))


def answer_roots() -> list[str]:
    """Every path whose change can stop the release: what its stop tells you to `git diff`."""
    return [p for f in FOLLOWS if f.answers or f.pins for p in f.paths]


class PinsFileError(ValueError):
    """(T1135) a release-pins file that is not one (bad JSON, no "pins" object, an entry that is not
    {"answers": bool, "value": ...}): the release STOPS on it, naming the file and the entry — never a pass, never an
    accept (a token covers a re-cut's content, not a broken file)."""


def is_pins(path: str) -> bool:
    row = row_of(path)
    return bool(row and row.pins)


def _pins_at(path: str, rev: str | None) -> dict:
    """{name: {value, answers}} of the pins file `path` at `rev` (None: on disk); {} when it is not there. Raises
    PinsFileError naming the file (and the entry) when it is not a pins file."""
    where = path if rev is None else f"{path} at {rev}"
    fix = f"it is written by `{REFRESH_PINS}` (its pins step), never by hand: re-cut, or restore it from git"
    if rev is None:
        f = Path(REPO_ROOT) / path
        try:
            text = f.read_text(encoding="utf-8") if f.is_file() else None
        except OSError as e:
            raise PinsFileError(f"{where}: cannot be read ({e}); {fix}") from None
    else:
        got = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=REPO_ROOT, capture_output=True, text=True)
        text = None if got.returncode else got.stdout
    if text is None:
        return {}
    try:
        doc = json.loads(text)
    except ValueError as e:
        raise PinsFileError(f"{where}: not JSON ({e}); {fix}") from None
    pins = doc.get("pins") if isinstance(doc, dict) else None
    if not isinstance(pins, dict):
        raise PinsFileError(f"{where}: no \"pins\" object; {fix}")
    for n, p in pins.items():
        if not (isinstance(p, dict) and isinstance(p.get("answers"), bool) and "value" in p):
            raise PinsFileError(f"{where}: pin {n!r} is {json.dumps(p)[:80]}, not {{\"answers\": true|false, "
                                f"\"value\": ...}}; {fix}")
    return pins


def moved_answer_pins(path: str, rev: str = "HEAD") -> list[str]:
    """(T1135) the `answers: true` pins (words or pages an angler reads) that the pins file `path` adds, drops or moves
    between `rev` and the working tree, sorted. A pin with `answers: false` (a count, an id) never stops. A file that
    is not a pins file raises PinsFileError (the release stops on it: `stage_recut`)."""
    old, new = _pins_at(path, rev), _pins_at(path, None)
    return sorted(n for n in set(old) | set(new)
                  if (old.get(n) or {}).get("answers") or (new.get(n) or {}).get("answers")
                  if old.get(n) != new.get(n))


def said(pages: list[str]) -> list[str]:
    """Each answer path as a stop shows it: a pins file with the moved pins it holds."""
    out = []
    for p in pages:
        out.append(p + (f" — answer pin(s) moved: {', '.join(moved_answer_pins(p))}" if is_pins(p) else ""))
    return out


#: (T993 item 8) the keys whose int lists are SOURCE INDEXES on a golden page (`src`: rows of the release's own source
#: table, ascending). A rule added before them renumbers every one after it — 0.8.5's nine pages (1363 -> 1364, 3144 ->
#: 3145) — while nothing an angler reads changes
SOURCE_INDEX_KEYS = frozenset({"src"})


def _index_list(v) -> bool:
    return isinstance(v, list) and all(isinstance(x, int) and not isinstance(x, bool) for x in v)


def _rule_table(manifest: dict, read) -> dict[int, object]:
    """{rix: the rule's JSON} of a release's water file (`rule` table, its blobs through the `zdict` dictionary),
    the file read with `read(<path under the regs fixture root>)` -> bytes."""
    import gzip
    import sqlite3
    import tempfile
    from pipeline.deliver.store.common import unpack
    url = manifest["files"]["water"]["url"]
    raw = gzip.decompress(read(url)) if url.endswith(".gz") else read(url)
    with tempfile.TemporaryDirectory(prefix="renumber-") as td:
        p = Path(td) / "water.sqlite"
        p.write_bytes(raw)
        con = sqlite3.connect(p)
        try:
            zd = dict(con.execute("SELECT tbl, d FROM zdict")).get("rule")
            return {int(ix): json.loads(unpack(j, zd)) for ix, j in con.execute("SELECT rix, j FROM rule")}
        finally:
            con.close()


def fixture_rules(rev: str | None = None) -> dict[int, object]:
    """(T993 item 8) the source table the golden pages' `src` indexes read: the committed regs fixture release's
    rules (row `regs-fixture`, re-cut with the pages) — at `rev`, or on disk (None). Raises when it cannot be read."""
    root = next(f for f in FOLLOWS if f.name == "regs-fixture").paths[0]
    if rev is None:
        mans = sorted((Path(REPO_ROOT) / root).glob("r-*/manifest.json"))
        if len(mans) != 1:
            raise RuntimeError(f"{root}: {len(mans)} fixture releases on disk, not one")
        return _rule_table(json.loads(mans[0].read_text(encoding="utf-8")),
                           lambda u: (Path(REPO_ROOT) / root / u).read_bytes())
    names = subprocess.run(["git", "ls-tree", "-r", "--name-only", rev, "--", root], cwd=REPO_ROOT,
                           capture_output=True, text=True, check=True).stdout.split()
    mans = [n for n in names if n.endswith("/manifest.json")]
    if len(mans) != 1:
        raise RuntimeError(f"{root} at {rev}: {len(mans)} fixture releases, not one")

    def show(rel: str) -> bytes:
        return subprocess.run(["git", "show", f"{rev}:{rel}"], cwd=REPO_ROOT, capture_output=True, check=True).stdout
    return _rule_table(json.loads(show(mans[0])), lambda u: show(f"{root}/{u}"))


def renumbered_only(paths: list[str], rev: str = "HEAD", rules=None) -> tuple[bool, str]:
    """(T993 item 8) do the golden pages `paths` differ from `rev` ONLY in source indexes renumbered one for one? The
    page at `rev` and on disk must be the same JSON once each `SOURCE_INDEX_KEYS` int list is read through ONE mapping
    old -> new, shared by every page (one source table) and one-to-one (two sources never become one, one never
    becomes two) — AND each old index must name the same source as its new one (review 2026-11-21: two sources that
    swap places map one-to-one and say something else): the rule at old in the source table at `rev` equals the rule
    at new on disk (`fixture_rules`; `rules` = (before, after) tables stands in). (True, the note naming the mapping)
    or (False, the first difference). A page added or removed is never a renumber; a table that cannot be read is
    never one either."""
    if any(is_pins(p) for p in paths):     # (T1135) a pin's value is what a test compares, never a source index
        return False, f"{next(p for p in paths if is_pins(p))}: a release-pins file is never a renumber"
    mapping: dict[int, int] = {}
    diff: list[str] = []

    def same(a, b, where: str, key: str | None = None) -> bool:
        if key in SOURCE_INDEX_KEYS and _index_list(a) and _index_list(b):
            if len(a) != len(b):
                diff.append(f"{where}: {len(a)} source(s) -> {len(b)}")
                return False
            for x, y in zip(a, b):
                if mapping.setdefault(x, y) != y:
                    diff.append(f"{where}: source {x} is {mapping[x]} elsewhere, {y} here")
                    return False
            return True
        if type(a) is not type(b):
            diff.append(f"{where}: {type(a).__name__} -> {type(b).__name__}")
            return False
        if isinstance(a, dict):
            if set(a) != set(b):
                diff.append(f"{where}: keys {sorted(set(a) ^ set(b))[:3]}")
                return False
            return all(same(a[k], b[k], f"{where}.{k}", k) for k in sorted(a))
        if isinstance(a, list):
            if len(a) != len(b):
                diff.append(f"{where}: {len(a)} item(s) -> {len(b)}")
                return False
            return all(same(x, y, f"{where}[{i}]", key) for i, (x, y) in enumerate(zip(a, b)))
        if a != b:
            diff.append(f"{where}: {str(a)[:60]!r} -> {str(b)[:60]!r}")
            return False
        return True

    for p in sorted(paths):
        got = subprocess.run(["git", "show", f"{rev}:{p}"], cwd=REPO_ROOT, capture_output=True, text=True)
        f = Path(REPO_ROOT) / p
        if got.returncode or not f.is_file():
            return False, f"{p}: {'added' if got.returncode else 'removed'} (never a renumber)"
        if not same(json.loads(got.stdout), json.loads(f.read_text(encoding="utf-8")), p):
            return False, diff[0] if diff else p
    moved = {a: b for a, b in mapping.items() if a != b}
    if len(set(mapping.values())) != len(mapping):
        return False, "two sources renumbered to one (not one-to-one)"
    try:
        before, after = rules if rules is not None else (fixture_rules(rev), fixture_rules(None))
    except Exception as e:  # noqa: BLE001 — unread is never a renumber: the stop stands, with its cause
        return False, f"the source table could not be read to compare the sources ({type(e).__name__}: {e})"
    for a, b in sorted(mapping.items()):
        if a not in before or b not in after:
            return False, f"source {a} -> {b}: not in the source table ({'before' if a not in before else 'after'})"
        if json.dumps(before[a], sort_keys=True) != json.dumps(after[b], sort_keys=True):
            return False, f"source {a} -> {b} is another source (its rule differs): not a renumber"
    sample = ", ".join(f"{a}->{b}" for a, b in sorted(moved.items())[:4])
    return True, (f"{len(paths)} golden page(s) differ only in source indexes renumbered one for one "
                  f"({len(moved)} index(es): {sample}{' …' if len(moved) > 4 else ''})")


# ------------------------------------------------------------------------------------------------- the working tree
def changed(key: str) -> list[str]:
    """The paths under `paths_of(key)` that differ from HEAD in the working tree (changed, new or deleted)."""
    roots = paths_of(key)
    out = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all", "--", *roots], cwd=REPO_ROOT,
                         capture_output=True, text=True, check=True).stdout
    return sorted(ln[3:].split(" -> ")[-1].strip().strip('"') for ln in out.splitlines() if ln.strip())


def content_token(paths: list[str]) -> str:
    """A change by CONTENT: each path with its bytes' digest (`gone` when deleted). What `--accept-fixtures` names, so
    an accept given for one re-cut never passes another."""
    h = hashlib.sha256()
    for p in sorted(paths):
        f = Path(REPO_ROOT) / p
        h.update(p.encode() + b"\0" + (hashlib.sha256(f.read_bytes()).digest() if f.is_file() else b"gone") + b"\0")
    return h.hexdigest()[:12]


def commit(key: str, rid: str, kind: str) -> str:
    """Commit what the refresh `key` rewrote, THOSE PATHS ONLY (other lanes' edits stay theirs), with the release id;
    the new HEAD."""
    paths = changed(key)        # the paths themselves: a table path with nothing under it is no pathspec for git
    if not paths:
        raise RuntimeError(f"release_follows.commit({key!r}): nothing changed under {', '.join(paths_of(key))}")
    subprocess.run(["git", "add", "-A", "--", *paths], cwd=REPO_ROOT, check=True)
    subprocess.run(["git", "commit", "-q", "-m",
                    # (T998) a Conventional Commits subject: the commit-msg hook and the merge-back PR's check read it
                    f"test(T666): the fixtures re-cut on the candidate {rid}\n\n"
                    f"{kind[:1].upper() + kind[1:]}; made by `python -m pipeline release --staging`.\n\n"
                    "Co-Authored-By: Claude <noreply@anthropic.com>", "--", *paths], cwd=REPO_ROOT, check=True)
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True,
                          check=True).stdout.strip()


# -------------------------------------------------------------------------------------------- the tests, found by rule
PY_RULE = re.compile(r"\bunder_test\.release\(|\brelease_at\(")
APP_RULE = re.compile(r"\b(liveRelease|liveReleaseAt|namedRelease)\b")
APP_SKIP = ("node_modules", "dist", ".expo", "ios", "android")


def pipeline_tests(root: Path | None = None) -> list[str]:
    """The pipeline test files that ask for the release under test (repo-relative, sorted)."""
    root = Path(root or REPO_ROOT)
    return sorted(str(p.relative_to(root)) for p in (root / "pipeline" / "tests").glob("test_*.py")
                  if PY_RULE.search(p.read_text(encoding="utf-8")))


def app_tests(root: Path | None = None) -> list[str]:
    """The app test files that ask for the release under test, relative to `app/` (what vitest takes), sorted."""
    app = Path(root or REPO_ROOT) / "app"
    out = []
    for pat in ("*.test.ts", "*.test.tsx"):
        for p in app.rglob(pat):
            if any(part in APP_SKIP for part in p.relative_to(app).parts):
                continue
            if ".release.test." in p.name or APP_RULE.search(p.read_text(encoding="utf-8")):
                out.append(str(p.relative_to(app)))
    return sorted(set(out))


def test_env(release_dir: Path) -> dict:
    """The environment that points both suites at `release_dir`. REGS_STORE_REQUIRED: a parity suite that cannot pair
    with that release FAILS (a skip would read as green). PIN_RECORD empty (T1135): the pins COMPARE here, whatever the
    shell exported — record mode belongs to the re-cut alone."""
    return {RELEASE_ENV: str(release_dir), "REGS_STORE_REQUIRED": "1", "PIN_RECORD": ""}


def pipeline_tests_cmd(files: list[str], junit: Path | None = None) -> list[str]:
    return [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
            *([f"--junitxml={junit}"] if junit else []), *files]


def app_tests_cmd(files: list[str]) -> list[str]:
    return ["pnpm", "-C", "app", "exec", "vitest", "run", *files]


# ------------------------------------------------------------------------- (T680) the whole app check, after the re-cut
# r-483b295de57c: the re-cut changed fixtures that tests found by NO rule above (a WaterMode test on the regs fixture,
# a wording pinned on the live release) — each failed in CI, after the push, one round at a time. After the re-cut the
# release runs EVERYTHING `pnpm check` and the app CI run (`app-ci.yml`: gates, vitest, bundles; `feeds-worker-ci.yml`; `docs-ci.yml`) run, each gate on its own, and
# lists every failure together, before anything is pushed.

def app_check_steps(root: Path | None = None) -> list[str]:
    """The `pnpm check:fast` gates in order, from their one home (`app/package.json` `check:fast`). Run one by one, so
    every failing gate is named — the `&&` chain stops at the first. A step that is not `pnpm run <gate>` raises: a
    gate this cannot run is never left out."""
    pkg = json.loads((Path(root or REPO_ROOT) / "app" / "package.json").read_text(encoding="utf-8"))
    out = []
    for part in pkg["scripts"]["check:fast"].split("&&"):
        m = re.fullmatch(r"\s*pnpm run (\S+)\s*", part)
        if not m:
            raise ValueError(f"release_follows: app/package.json check:fast step {part.strip()!r} is not "
                             "`pnpm run <gate>` — teach app_check_steps to run it")
        out.append(m.group(1))
    return out


def app_check_cmd(step: str) -> list[str]:
    return ["pnpm", "-C", "app", "run", "-s", step]


def app_suite_cmd(report: Path) -> list[str]:
    """The WHOLE app vitest suite (`pnpm check`'s `test`), its results also written as JSON to `report`."""
    return ["pnpm", "-C", "app", "exec", "vitest", "run", "--reporter=default", "--reporter=json",
            f"--outputFile.json={report}"]


def worker_suite_cmd(report: Path) -> list[str]:
    """The feeds worker's suite (feeds-worker-ci.yml's "Feeds worker tests": not in the pnpm workspace, so `pnpm check` skips it)."""
    return ["pnpm", "-C", "app", "exec", "vitest", "run", "--root", "workers/feeds", "--reporter=default",
            "--reporter=json", f"--outputFile.json={report}"]


def vitest_failures(report: Path) -> list[str]:
    """Every failed test in a vitest JSON report, `<file> › <test>` (a file that failed with no test: its error)."""
    data = json.loads(Path(report).read_text(encoding="utf-8"))
    root = Path(REPO_ROOT).resolve()
    out = []
    for f in data.get("testResults", []):
        name = Path(f.get("name", "?"))
        name = str(name.relative_to(root)) if name.is_absolute() and name.is_relative_to(root) else str(name)
        bad = [a for a in f.get("assertionResults", []) if a.get("status") == "failed"]
        out += [f"{name} › {a.get('fullName') or a.get('title')}" for a in bad]
        if not bad and f.get("status") == "failed":
            out.append(f"{name}: {(f.get('message') or 'failed').strip().splitlines()[0][:200]}")
    return out


def junit_failures(report: Path) -> list[str]:
    """Every failed / erroring test in a pytest junit XML report, `<class>::<test>`."""
    import xml.etree.ElementTree as ET
    out = []
    for case in ET.parse(report).getroot().iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            out.append(f"{case.get('classname', '?')}::{case.get('name', '?')}")
    return out


def report_failures(report: Path) -> list[str]:
    return junit_failures(report) if report.suffix == ".xml" else vitest_failures(report)


def main() -> int:
    print("what follows the release (pipeline/release_follows.py FOLLOWS):")
    for f in FOLLOWS:
        print(f"  {f.name:13} {f.what}\n{'':16}refresh: {f.refresh or '(kept by the release, no command)'}"
              + ("  — a change STOPS the release (an answer changed)" if f.answers else "")
              + ("  — a moved `answers: true` pin STOPS the release" if f.pins else "")
              + "".join(f"\n{'':16}{p}" for p in f.paths))
    py, app = pipeline_tests(), app_tests()
    print(f"\ntests that ask for the release under test ({RELEASE_ENV}; found by rule):")
    print(f"  pipeline ({len(py)}):" + "".join(f"\n    {p}" for p in py))
    print(f"  app ({len(app)}):" + "".join(f"\n    app/{p}" for p in app))
    print("\n(T680) then the whole app check on the candidate, every failure listed together:\n  the app suite, the "
          "feeds worker suite, and each check:fast gate: " + ", ".join(app_check_steps()))
    print(f"\n(T993 item 16) and the pipeline's live-data tier: -m \"{data_tier_markers()}\" in chunks, on the candidate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
