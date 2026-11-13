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
    stops and shows it; every other row's change is committed with the release id)."""
    name: str
    what: str
    paths: tuple[str, ...]
    refresh: str | None
    answers: bool = False


#: the app's release-backed fixtures, all re-cut by ONE command (`app/tools/regs-recut/sync.mjs`, T441), in its order
RECUT = "recut"
#: the in-season notice matches, re-resolved on the release (T537)
INSEASON = "inseason"

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
    Follow("inseason", "the in-season notice matches, their ranges re-derived on the release",
           (_rel(CURATED.inseason.notice_waters),), INSEASON),
    Follow("baseline", "the reach run the live release was cut from (the binding diff's \"before\"; not in git: "
           "data/generated/release/baseline/<LATEST>/, kept by the baseline stage and by `pipeline build`)", (), None),
)


def refresh_cmd(key: str, release_dir: Path) -> list[str]:
    """The ONE command that refreshes every row with `refresh == key`, against the release at `release_dir`."""
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
    """The changed paths that are a change in what an angler is told (rows with `answers`). Empty: the change only
    renumbers — ids, handles, the fixture release's files (T667 D5: committed without a stop)."""
    return sorted(p for p in changed if (row_of(p) or Follow("", "", (), None)).answers)


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
                    f"release {rid}: fixtures re-cut on the candidate ({kind}; pipeline release --staging, T666)\n\n"
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
    with that release FAILS (a skip would read as green)."""
    return {RELEASE_ENV: str(release_dir), "REGS_STORE_REQUIRED": "1"}


def pipeline_tests_cmd(files: list[str], junit: Path | None = None) -> list[str]:
    return [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
            *([f"--junitxml={junit}"] if junit else []), *files]


def app_tests_cmd(files: list[str]) -> list[str]:
    return ["pnpm", "-C", "app", "exec", "vitest", "run", *files]


# ------------------------------------------------------------------------- (T680) the whole app check, after the re-cut
# r-483b295de57c: the re-cut changed fixtures that tests found by NO rule above (a WaterMode test on the regs fixture,
# a wording pinned on the live release) — each failed in CI, after the push, one round at a time. After the re-cut the
# release runs EVERYTHING `pnpm check` and the app CI run (`.github/workflows/app-ci.yml`) run, each gate on its own, and
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
    """The feeds worker's suite (app-ci's "Feeds worker tests": not in the pnpm workspace, so `pnpm check` skips it)."""
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
              + "".join(f"\n{'':16}{p}" for p in f.paths))
    py, app = pipeline_tests(), app_tests()
    print(f"\ntests that ask for the release under test ({RELEASE_ENV}; found by rule):")
    print(f"  pipeline ({len(py)}):" + "".join(f"\n    {p}" for p in py))
    print(f"  app ({len(app)}):" + "".join(f"\n    app/{p}" for p in app))
    print("\n(T680) then the whole app check on the candidate, every failure listed together:\n  the app suite, the "
          "feeds worker suite, and each check:fast gate: " + ", ".join(app_check_steps()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
