#!/usr/bin/env python3
"""THE MUTATION REPORT (T1041, owner 2026-11-23 "Report only"): on a pull request, mutate ONLY its changed lines and
list the mutants its tests let live. Never a required check, never a red PR: it says, the reviewer reads.

    python scripts/mutation_report.py py --base <sha> --head <sha> [--budget 900]   # mutmut 2.5.1, pipeline Python
    python scripts/mutation_report.py ts --base <sha> --head <sha> [--budget 900]   # Stryker 10.1.1, app TypeScript

Writes markdown to $GITHUB_STEP_SUMMARY (else stdout) and always exits 0 once it has started — a tool that broke is
SAID in the report (and as a ::warning::), never hidden and never a failed PR. A bad argument exits 2.

Why: reviews this session kept finding new branches with no test that could fail (AGENTS rule 33: "a test must be able
to fail"). A survivor here is a changed line whose behaviour no test pins.

WHAT IS MUTATED: the lines the PR adds (`git diff -U0 <base>...<head>`), in
  - py: pipeline/**/*.py and scripts/*.py that are not tests or conftest — mutmut's `--use-patch-file` (only the patch's added lines),
    one file at a time, its runner the tests that import that file directly (scripts/related_tests.py, CI's no-data
    markers, `-x`), so a mutant costs one short pytest run;
  - ts: app/packages/** and app/apps/** .ts/.tsx that are not tests — Stryker's `--mutate <file>:<from>-<to>` line
    ranges, its tests the file's sibling tests (X.test.ts / X.test.tsx / X.render.test.tsx) and the PR's changed app
    test files, through a vitest config that includes only those (the whole suite is ~21 min on one runner; a core
    file's import graph is the whole suite).
A changed file with no such test is listed as having none (every mutant would live), and not run.

BOUNDED: each part stops at --budget seconds (the job's timeout-minutes is above it). mutmut keeps its results as it
goes, so what finished is reported; Stryker reports only at its end, so a stopped Stryker says so. A part also takes at
most MAX_LINES changed lines (the rest named).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import related_tests as RT  # noqa: E402

#: changed lines mutated at most per part (the rest are named, not mutated)
MAX_LINES = 400
#: the pinned tools (the workflow installs these; test_mutation_report.py holds the workflow to them)
STRYKER = "10.1.1"
MUTMUT = "2.5.1"
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
TS_SRC = re.compile(r"^app/(packages|apps)/.+\.(ts|tsx)$")
TS_TEST = re.compile(r"\.test\.(ts|tsx)$")


def bounded(cmd: list[str], secs: float | None, cwd: Path, env: dict | None = None) -> tuple[int, str]:
    """(exit code, stdout + stderr) of `cmd`, stopped at `secs` (124, as coreutils' timeout: macOS has none): the
    whole process group goes, the tool's own children (pytest, vitest workers) with it. SIGINT first (a tool stops as
    on Ctrl-C and keeps what it finished: mutmut's cache), then TERM, then KILL."""
    import signal
    p = subprocess.Popen(cmd, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                         start_new_session=True)
    try:
        out, _ = p.communicate(timeout=secs)
        return p.returncode, out
    except subprocess.TimeoutExpired:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                os.killpg(p.pid, sig)
                out, _ = p.communicate(timeout=20)
                return 124, out
            except subprocess.TimeoutExpired:
                continue
            except ProcessLookupError:       # it ended between the timeout and the signal
                out, _ = p.communicate()
                return 124, out
        os.killpg(p.pid, signal.SIGKILL)
        out, _ = p.communicate()
        return 124, out


def git(*args: str) -> str:
    got = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
    if got.returncode:
        raise RuntimeError(f"git {' '.join(args)} failed (rc={got.returncode}): {got.stderr.strip()[-300:]}")
    return got.stdout


def added_lines(diff: str) -> dict[str, list[int]]:
    """file -> the line numbers (in the new file) a `git diff -U0` adds or changes."""
    out: dict[str, list[int]] = {}
    cur = None
    for ln in diff.splitlines():
        if ln.startswith("+++ "):
            cur = None if ln[4:] == "/dev/null" else ln[6:] if ln.startswith("+++ b/") else ln[4:]
        elif cur and (m := HUNK.match(ln)):
            start, n = int(m.group(1)), int(m.group(2) if m.group(2) is not None else 1)
            out.setdefault(cur, []).extend(range(start, start + n))
    return {f: ls for f, ls in out.items() if ls}


def ranges(lines: list[int]) -> list[tuple[int, int]]:
    """Consecutive line numbers as (first, last) ranges."""
    out: list[tuple[int, int]] = []
    for n in sorted(set(lines)):
        if out and n == out[-1][1] + 1:
            out[-1] = (out[-1][0], n)
        else:
            out.append((n, n))
    return out


def cap_lines(files: dict[str, list[int]], cap: int = MAX_LINES) -> tuple[dict[str, list[int]], list[str]]:
    """At most `cap` changed lines, file by file in path order; (kept, the files left out or cut, named)."""
    kept, left, n = {}, [], 0
    for f in sorted(files):
        room = cap - n
        if room <= 0:
            left.append(f"{f} ({len(files[f])} lines)")
            continue
        kept[f] = sorted(files[f])[:room]
        if len(files[f]) > room:
            left.append(f"{f} (lines after {kept[f][-1]})")
        n += len(kept[f])
    return kept, left


@dataclass
class Report:
    title: str
    lines: list[str] = field(default_factory=list)
    survivors: list[tuple[str, str, str]] = field(default_factory=list)     # (where, mutant, diff or replacement)
    counts: dict[str, int] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)

    def markdown(self, secs: float) -> str:
        c = self.counts
        head = (f"## {self.title}\n\nReport only (T1041): changed lines only, never fails the PR. {round(secs)} s. "
                + (", ".join(f"{k} {v}" for k, v in c.items()) if c else "no mutant run") + ".\n")
        body = [head, *(f"- {x}" for x in self.lines)]
        for p in self.problems:
            body.append(f"- **could not finish:** {p}")
        if self.survivors:
            body.append(f"\n### {len(self.survivors)} survived — no test failed with this change in place\n")
            body.append("| where | mutant |\n|---|---|")
            for where, what, _ in self.survivors[:60]:
                body.append(f"| `{where}` | {what.replace('|', '\\|')} |")
            if len(self.survivors) > 60:
                body.append(f"\n… and {len(self.survivors) - 60} more (the job log lists every one)")
            body.append("\n<details><summary>the survivors' diffs</summary>\n")
            for where, _, diff in self.survivors[:60]:
                body.append(f"`{where}`\n```diff\n{diff.strip()}\n```")
            body.append("</details>")
        return "\n".join(body) + "\n"


def write(rep: Report, t0: float) -> None:
    md = rep.markdown(time.time() - t0)
    print(md)
    for p in rep.problems:
        print(f"::warning title=mutation report::{p}")
    out = os.environ.get("GITHUB_STEP_SUMMARY")
    if out:
        with open(out, "a", encoding="utf-8") as f:
            f.write(md)


# ── Python (mutmut) ─────────────────────────────────────────────────────────────────────────────────────────────────

def py_files(changed: dict[str, list[int]]) -> dict[str, list[int]]:
    tps = RT.testpaths(ROOT)
    return {f: ls for f, ls in changed.items() if f.startswith(("pipeline/", "scripts/")) and f.endswith(".py")
            and Path(f).name != "conftest.py" and not RT._is_test(f, tps) and (ROOT / f).is_file()}


def mutmut_survivors(xml: str) -> tuple[dict[str, int], list[tuple[str, str, str]]]:
    """`mutmut junitxml --untested-policy skipped --suspicious-policy failure`: (counts by outcome, the survivors as
    (file:line, the mutated line, the diff)). A survivor is a <failure> bad_survived, a suspicious one a <failure>
    ok_suspicious, a timeout an <error>, a mutant a stopped run never reached a <skipped> (untested: never "killed")."""
    counts = {"killed": 0, "survived": 0, "timeout": 0, "suspicious": 0, "untested": 0}
    out = []
    for tc in ET.fromstring(xml).iter("testcase"):
        fail, err, skip = tc.find("failure"), tc.find("error"), tc.find("skipped")
        if skip is not None:
            counts["untested"] += 1
        elif err is not None:
            counts["timeout"] += 1
        elif fail is None:
            counts["killed"] += 1
        else:
            kind = "suspicious" if "suspicious" in (fail.get("message") or "") else "survived"
            counts[kind] += 1
            src = (tc.findtext("system-out") or "").strip()
            out.append((f"{tc.get('file')}:{tc.get('line')}", f"{kind}: `{src[:120]}`", fail.text or ""))
    return counts, out


def run_py(base: str, head: str, budget: int, rep: Report, runner=bounded, mutmut: str | None = None) -> None:
    import before_push as BP
    markers = BP.tier_markers(BP.workflow_steps())
    files, left = cap_lines(py_files(added_lines(git("diff", "-U0", "--no-renames", f"{base}...{head}", "--",
                                                    "pipeline/", "scripts/"))))
    if not files:
        rep.lines.append("no changed pipeline or scripts source line (tests and conftest are not mutated)")
        return
    rep.lines += [f"left out (past {MAX_LINES} changed lines): {x}" for x in left]
    deadline = time.time() + budget
    py = os.environ.get("PIPELINE_PY") or str(ROOT / ".venv" / "bin" / "python")
    mutmut = mutmut or str(Path(py).parent / "mutmut")
    total: dict[str, int] = {}
    plan = []
    for f, lines in files.items():
        tests, said = RT.pytest_related([f], ROOT)
        if not tests:
            rep.lines.append(f"`{f}`: {len(lines)} changed line(s), NO test imports or names it — every mutant "
                             "would live (not run)")
            continue
        plan.append((f, lines, tests))
    # each file an equal share of what is left (CI, 2026-11-23: one 190-line file took the whole budget, two files got
    # none), the smaller changes first
    plan.sort(key=lambda x: len(x[1]))
    for i, (f, lines, tests) in enumerate(plan):
        left_s = int((deadline - time.time()) / (len(plan) - i))
        if left_s < 30:
            rep.problems.append(f"the {budget} s budget ran out before `{f}` ({len(lines)} changed lines)")
            continue
        patch = ROOT / ".mutation.patch"
        patch.write_text(git("diff", "-U0", "--no-renames", f"{base}...{head}", "--", f), encoding="utf-8")
        cmd = f"{shlex.quote(py)} -m pytest -x -q -p no:cacheprovider -m {shlex.quote(markers)} " + " ".join(tests)
        rep.lines.append(f"`{f}`: {len(lines)} changed line(s); tests: {', '.join(Path(t).name for t in tests)}")
        (ROOT / ".mutmut-cache").unlink(missing_ok=True)
        orig = (ROOT / f).read_bytes()        # put back as it was (an uncommitted edit too), never `git checkout`
        try:
            rc, said_run = runner([mutmut, "run", "--paths-to-mutate", f, "--tests-dir", str(Path(tests[0]).parent),
                                   "--runner", cmd, "--use-patch-file", str(patch), "--no-progress"], left_s, ROOT)
            if rc == 124:
                rep.problems.append(f"`{f}`: stopped at its share of the budget ({left_s} s); what finished is "
                                    "counted below, if mutmut kept it")
            elif rc & 1:                      # mutmut's bit 1: a fatal error (a red baseline, a tool failure)
                rep.problems.append(f"`{f}`: mutmut stopped (rc={rc}): {said_run.strip()[-400:]}")
            xrc, xml = runner([mutmut, "junitxml", "--untested-policy", "skipped", "--suspicious-policy", "failure"],
                              120, ROOT)
            if xrc or "<testsuites" not in xml:
                rep.problems.append(f"`{f}`: no mutmut results to read (rc={xrc}): {xml.strip()[-200:]}")
                continue
            counts, surv = mutmut_survivors(xml[xml.index("<"):])
            for k, v in counts.items():
                total[k] = total.get(k, 0) + v
            rep.survivors += surv
        finally:
            # mutmut mutates in place: a stopped run can leave its mutant on disk
            (ROOT / f).write_bytes(orig)
            patch.unlink(missing_ok=True)
            (ROOT / ".mutmut-cache").unlink(missing_ok=True)
    rep.counts = total


# ── TypeScript (Stryker) ────────────────────────────────────────────────────────────────────────────────────────────

def ts_tests(src: str, changed_tests: list[str]) -> list[str]:
    """The tests a changed app file is mutated against, relative to app/: its siblings and the PR's changed tests."""
    stem = src[:-len(Path(src).suffix)]
    near = [f"{stem}{x}" for x in (".test.ts", ".test.tsx", ".render.test.tsx")]
    return sorted({t[len("app/"):] for t in [*near, *changed_tests] if (ROOT / t).is_file()})


def source(path: str, line) -> str | None:
    """That line of a file in the checkout (the mutant's original), or None when it cannot be read."""
    try:
        return (ROOT / path).read_text(encoding="utf-8").splitlines()[int(line) - 1]
    except (OSError, ValueError, TypeError, IndexError):
        return None


def stryker_survivors(report: dict) -> tuple[dict[str, int], list[tuple[str, str, str]]]:
    """Stryker's JSON report (mutation-testing-report-schema): (counts by status, the survivors)."""
    counts: dict[str, int] = {}
    out = []
    for name, f in sorted(report.get("files", {}).items()):
        for m in f.get("mutants", []):
            st = m.get("status", "?")
            counts[st] = counts.get(st, 0) + 1
            if st in ("Survived", "NoCoverage"):
                loc = m.get("location", {}).get("start", {})
                where = f"app/{name}:{loc.get('line')}"
                was = (source(f"app/{name}", loc.get("line")) or "?").strip()
                out.append((where, f"{st}: {m.get('mutatorName')} → `{(m.get('replacement') or '')[:100]}`",
                            f"- {was}\n+ (at column {loc.get('column')}) {m.get('replacement') or ''}"))
    return counts, out


VITEST_CONFIG = """// (T1041) written by scripts/mutation_report.py for one Stryker run, then removed: the app's config, its tests only,
// each in the project whose own globs take it (a ui-native test in the node project errors outside any test)
import { matchesGlob } from "node:path";
import base from "./vitest.config";
const only: string[] = JSON.parse(process.env.MUTATION_TESTS ?? "[]");
const projects = (base.test!.projects as any[])
  .map((p) => ({ ...p, test: { ...p.test, include: only.filter((f) => p.test.include.some((g: string) => matchesGlob(f, g))) } }))
  .filter((p) => p.test.include.length);
export default { ...base, test: { ...base.test, globalSetup: [], projects } };
"""


def run_ts(base: str, head: str, budget: int, rep: Report, runner=bounded) -> None:
    changed = added_lines(git("diff", "-U0", "--no-renames", f"{base}...{head}", "--", "app/"))
    tests_changed = [f for f in changed if TS_TEST.search(f) and f.startswith("app/") and (ROOT / f).is_file()]
    src = {f: ls for f, ls in changed.items() if TS_SRC.match(f) and not TS_TEST.search(f)
           and not f.endswith(".d.ts") and "/node_modules/" not in f and (ROOT / f).is_file()}
    files, left = cap_lines(src)
    if not files:
        rep.lines.append("no changed app source line (app/packages, app/apps; tests are not mutated)")
        return
    rep.lines += [f"left out (past {MAX_LINES} changed lines): {x}" for x in left]
    mutate, tests = [], set()
    for f, lines in files.items():
        near = ts_tests(f, tests_changed)
        if not near:
            rep.lines.append(f"`{f}`: {len(lines)} changed line(s), NO sibling or changed test — every mutant would "
                             "live (not run)")
            continue
        rel = f[len("app/"):]
        mutate += [f"{rel}:{a}-{b}" for a, b in ranges(lines)]
        tests |= set(near)
        rep.lines.append(f"`{f}`: {len(lines)} changed line(s); tests: {', '.join(Path(t).name for t in near)}")
    if not mutate:
        return
    app = ROOT / "app"
    cfg, vcfg, out = app / ".mutation-stryker.json", app / ".mutation-vitest.config.mts", app / ".mutation-report.json"
    vcfg.write_text(VITEST_CONFIG, encoding="utf-8")
    orig = {f: (ROOT / f).read_bytes() for f in files}
    cfg.write_text(json.dumps({
        "testRunner": "vitest", "plugins": ["@stryker-mutator/vitest-runner"],
        "vitest": {"configFile": vcfg.name, "related": False}, "coverageAnalysis": "perTest", "inPlace": True,
        "reporters": ["json", "clear-text"], "jsonReporter": {"fileName": out.name}, "timeoutMS": 30000,
        "concurrency": 2}), encoding="utf-8")
    try:
        rc, said = runner(["./node_modules/.bin/stryker", "run", cfg.name, "--mutate", ",".join(mutate)], budget, app,
                          {**os.environ, "MUTATION_TESTS": json.dumps(sorted(tests))})
        print(said[-4000:])                  # the job log: Stryker's own table
        if rc == 124:
            rep.problems.append(f"Stryker stopped at the {budget} s budget: it reports only at its end, so no mutant is "
                                f"counted ({len(mutate)} line range(s) asked)")
            return
        if rc or not out.is_file():
            rep.problems.append(f"Stryker failed (rc={rc}): {said.strip()[-600:]}")
            return
        rep.counts, rep.survivors = stryker_survivors(json.loads(out.read_text(encoding="utf-8")))
    finally:
        for p in (cfg, vcfg, out):
            p.unlink(missing_ok=True)
        # inPlace: Stryker restores the files itself; a stopped run may not have (as they were, uncommitted edits too)
        for f, b in orig.items():
            (ROOT / f).write_bytes(b)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("part", choices=("py", "ts"))
    ap.add_argument("--base", required=True)
    ap.add_argument("--head", required=True)
    ap.add_argument("--budget", type=int, default=900, help="seconds this part may take (default 900)")
    ap.add_argument("--mutmut", default=None, help="py: the mutmut to run (default: beside the pipeline's interpreter)")
    a = ap.parse_args(argv)
    if a.budget < 60:
        print("mutation-report: --budget below 60 s leaves no room for one mutant", file=sys.stderr)
        return 2
    rep = Report(f"Mutation report: {'pipeline Python (mutmut ' + MUTMUT if a.part == 'py' else 'app TypeScript (Stryker ' + STRYKER})")
    t0 = time.time()
    try:
        if a.part == "py":
            run_py(a.base, a.head, a.budget, rep, mutmut=a.mutmut)
        else:
            run_ts(a.base, a.head, a.budget, rep)
    except Exception as e:                   # report-only: a broken tool is SAID in the report and the log, never red
        rep.problems.append(f"the report itself failed: {type(e).__name__}: {e}")
    write(rep, t0)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
