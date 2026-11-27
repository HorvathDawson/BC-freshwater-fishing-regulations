"""(T1193, owner 2026-11-26 "yes with loud default") A PLAIN `pytest` IS CI'S NO-DATA TIER, AND SAYS SO: with no `-m`
the root `conftest.py` deselects what CI's "Tests" step deselects (`tiers.ci_markers`, read from the workflow), and the
run's last line counts the data tests it left out and names `--with-data`. They are deselected, never skipped
(test_no_silent_skips.py). `--with-data` is `-m "not slow"` (the default before); an explicit `-m` is left as given.
Real pytest runs over four planted tests (`fixtures/default_tier/planted.py`: no-data, needs_tiles, slow, slow+needs_cache).
And every pytest a script starts names its tier (`-m` or `--with-data`), so none leans on the default."""
from __future__ import annotations

import io
import os
import re
import subprocess
import tokenize
import sys
from pathlib import Path

from pipeline.tests import tiers

ROOT = Path(__file__).resolve().parents[2]
PLANTED = "pipeline/tests/fixtures/default_tier/planted.py"
LOUD = re.compile(r"^!+ (.*NOT RUN.*) !+$")


def _ci_markers_from_the_shell() -> str:
    out = subprocess.run(["bash", "-c", ". scripts/test-env.sh && no_data_markers"], cwd=ROOT, capture_output=True,
                         text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def _run(*args: str, target: str = PLANTED) -> tuple[int, set[str], list[str], str]:
    """(exit code, the planted tests that RAN, the loud lines, the whole output) of one real pytest run."""
    out = subprocess.run([sys.executable, "-m", "pytest", "-v", "-p", "no:cacheprovider", *args, target], cwd=ROOT,
                         capture_output=True, text=True, timeout=120)
    text = out.stdout + out.stderr
    ran = set(re.findall(r"planted\.py::(\w+) (?:PASSED|FAILED|ERROR)", text))
    return out.returncode, ran, [m.group(1) for ln in text.splitlines() if (m := LOUD.match(ln))], text


def test_the_default_expression_is_ci_s():
    """The pin: what a plain run deselects by is CI's "Tests" step expression, the one the shell runners read too, and
    it is a no-data tier (it leaves out slow and every needs_* kind)."""
    expr = tiers.ci_markers()
    assert expr == _ci_markers_from_the_shell()
    assert tiers.is_no_data(expr, tiers.data_markers()), expr
    assert set(re.findall(r"not (\w+)", expr)) == set(tiers.data_markers())


def test_a_plain_run_is_the_no_data_tier_and_counts_what_it_left_out():
    rc, ran, loud, text = _run()
    assert rc == 0, text
    assert ran == {"test_no_data"}, text
    assert loud == ["2 data tests NOT RUN (they need built data): add --with-data; 1 slow test NOT RUN: -m slow"], text
    assert "3 deselected" in text and "skipped" not in text, text           # deselected and counted, never a skip
    assert tiers.HIDDEN in text, text


def test_with_data_runs_the_data_tests_and_is_quiet():
    rc, ran, loud, text = _run("--with-data")
    assert rc == 0, text
    assert ran == {"test_no_data", "test_needs_data"}, text                 # -m "not slow": today's data tier
    assert loud == [] and tiers.VISIBLE in text, text


def test_an_explicit_m_is_left_alone():
    """A script's or CI's `-m` decides alone: no default applied on top, no loud line."""
    rc, ran, loud, text = _run("-m", tiers.ci_markers())
    assert (rc, ran, loud) == (0, {"test_no_data"}, []), text
    rc, ran, loud, text = _run("-m", "needs_cache")
    assert (rc, ran, loud) == (0, {"test_slow_and_needs_data"}, []), text
    assert tiers.VISIBLE in text, text


def test_minus_k_narrows_first_so_only_the_tier_s_leavings_are_counted():
    rc, ran, loud, text = _run("-k", "needs or no_data")                    # -k drops test_slow before the tier runs
    assert rc == 0 and ran == {"test_no_data"}, text
    assert loud == ["2 data tests NOT RUN (they need built data): add --with-data"], text


def test_with_data_and_minus_m_together_is_refused():
    rc, ran, _, text = _run("--with-data", "-m", "slow")
    assert rc == 4 and not ran, text
    assert "--with-data and -m 'slow' together" in text, text


def test_a_test_named_by_its_node_id_runs():
    """Naming `file::test` is the choice: it runs whatever its markers; the file named whole still deselects."""
    rc, ran, loud, text = _run(target=f"{PLANTED}::test_needs_data")
    assert (rc, ran, loud) == (0, {"test_needs_data"}, []), text
    rc, ran, loud, text = _run(target=f"{PLANTED}::test_slow_and_needs_data")
    assert (rc, ran, loud) == (0, {"test_slow_and_needs_data"}, []), text


def test_the_line_says_only_what_was_left_out():
    assert tiers.left_out_line(0, 0) == ""
    assert tiers.left_out_line(0, 2) == "2 slow tests NOT RUN: -m slow"
    assert tiers.left_out_line(1, 0) == "1 data test NOT RUN (they need built data): add --with-data"


def test_a_chunked_run_ends_with_one_total():
    """scripts/pytest-chunked.sh with no -m / --with-data: each chunk prints its own line, and the run ends with ONE
    line, their sum. With CI's -m, neither."""
    files = "test_(sidecars_agree|answers_year_strip_live|northrop_side_channel)\\.py$"   # three small files with data tests
    env = os.environ | {"PIPELINE_PY": sys.executable}

    def chunked(*args):
        out = subprocess.run(["bash", "scripts/pytest-chunked.sh", "--files", "1", "--only", files, *args], cwd=ROOT,
                             env=env, capture_output=True, text=True, timeout=300)
        assert out.returncode == 0, out.stdout[-3000:] + out.stderr
        return out.stdout

    text = chunked()
    each = [int(m.group(1)) for m in re.finditer(r"^!+ (\d+) data tests? NOT RUN", text, re.M)]
    total = re.findall(r"^pytest-chunked: !!! (\d+) data test\(s\) NOT RUN in all .*!!!$", text, re.M)
    assert len(each) == 3 and sum(each) > 0, text
    assert total == [str(sum(each))], text
    text = chunked("--", "-m", tiers.ci_markers())
    assert "NOT RUN" not in text, text


#: what starts pytest in a script: `-m pytest` (shell, or a Python/JS argument list) or the chunked runner
_INVOKES = re.compile(r"""-m pytest\b|["']-m["'],\s*["']pytest["']|pytest-chunked\.sh""")
#: a tier named: `--with-data`, or a `-m <expr>` that is not `-m pytest`
_TIER = re.compile(r"""--with-data|(?:^|[\s"'(\[,])-m(?:["']\s*,\s*|\s+)(?!["']?pytest\b)\S""")


def _prose(text: str) -> list[tuple[int, int]]:
    """The spans of a Python file's triple-quoted strings (docstrings: prose, never a call)."""
    starts, n = [0], 0
    for ln in text.splitlines(keepends=True):
        n += len(ln)
        starts.append(n)
    return [(starts[t.start[0] - 1] + t.start[1], starts[t.end[0] - 1] + t.end[1])
            for t in tokenize.generate_tokens(io.StringIO(text).readline)
            if t.type == tokenize.STRING and t.string.lstrip("rbuRBU").startswith(('"""', "'''"))]


def _statement(text: str, start: int, shell: bool) -> str:
    """From `start` to the end of its statement: a newline outside brackets (and, in shell, not after `\\`)."""
    depth, j = 0, start
    while j < len(text):
        c = text[j]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
            if depth < 0:
                break
        elif c == "\n" and depth <= 0 and not (shell and text[j - 1] == "\\"):
            break
        j += 1
    return text[start:j]


def untiered(name: str, text: str) -> list[str]:
    """Each pytest a file starts that names neither `-m` nor `--with-data`, `file:line: the line`."""
    spans, out = (_prose(text) if name.endswith(".py") else []), []
    for m in _INVOKES.finditer(text):
        if any(a <= m.start() < b for a, b in spans):
            continue
        begin = text.rfind("\n", 0, m.start()) + 1
        line = text[begin:text.find("\n", m.start())]
        if re.match(r"\s*(#|//|\*)", line):
            continue
        if not _TIER.search(_statement(text, begin, name.endswith(".sh"))[m.end() - begin:]):
            out.append(f"{name}:{text.count(chr(10), 0, m.start()) + 1}: {line.strip()}")
    return out


def _callers() -> list[str]:
    got = subprocess.run(["git", "ls-files", "scripts/*.sh", "scripts/*.py", "pipeline/**.py", "app/tools/**.mjs"],
                         cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    return [f for f in got if not f.startswith("pipeline/tests/") and f != "scripts/pytest-chunked.sh"
            and not re.search(r"(^|/)(test_[^/]*|conftest)\.py$", f)]


def test_every_pytest_a_script_starts_names_its_tier():
    """A plain pytest is the no-data tier, so a script that leans on the default silently changes tier when the
    default moves: every caller says `-m <expr>` or `--with-data` (scripts/pytest-chunked.sh passes its caller's
    arguments on, so its callers are checked instead)."""
    found = {f: (ROOT / f).read_text(encoding="utf-8") for f in _callers()}
    bad = [b for f, text in found.items() for b in untiered(f, text)]
    assert not bad, "name the tier (-m \"<expr>\" or --with-data):\n" + "\n".join(bad)
    named = {f for f, text in found.items() if _INVOKES.search(text)}
    assert {"pipeline/release_follows.py", "scripts/fast-fail.sh", "app/tools/regs-recut/pins.mjs",
            "scripts/test-full.sh", "scripts/before_push.py"} <= named, named        # the scan sees the callers
    assert len(re.findall(r"-m pytest .*--with-data", found["scripts/fast-fail.sh"])) == 8     # + the Makefile's step


def test_the_tier_scan_names_a_caller_that_leans_on_the_default():
    """Mutation: each form, without its tier, is named; with it, passes; prose is not a call."""
    for name, text in [("a.sh", '"$PY" -m pytest -q pipeline/tests/test_x.py\n'),
                       ("a.sh", 'step "x" bash scripts/pytest-chunked.sh\n'),
                       ("a.sh", '"$PY" -m pytest -q \\\n  pipeline/tests/test_x.py\n'),
                       ("a.py", 'CMD = [sys.executable, "-m", "pytest", "-q",\n       *files]\n'),
                       ("a.mjs", "const a = ['-m', 'pytest', '-q', ...files];\n")]:
        assert len(untiered(name, text)) == 1, (name, text)
        tiered = text.replace("-q", "-q --with-data") if name != "a.py" else text.replace('"-q",', '"-q", "--with-data",')
        if "pytest-chunked" in text:
            tiered = text.replace("pytest-chunked.sh", "pytest-chunked.sh -- -m slow")
        assert untiered(name, tiered) == [], (name, tiered)
    assert untiered("a.py", '"""runs `python -m pytest -q` in chunks"""\n') == []
