"""(T1193, owner 2026-11-26 "A Makefile (Recommended)") THE ROOT MAKEFILE: one verb per task, each wrapping a command
the repo already has. Pinned here: `make help` lists every verb (from the file's `## ` comments), every verb's recipe
resolves (`make -n <verb>`: each script, module and pnpm script it names exists), a missing prerequisite stops naming
its fix, and production has no verb that runs (AGENTS A7). Real `make` runs, dry (`-n`) except the guards and `help`."""
from __future__ import annotations

import importlib.util
import json
import re
import shlex
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = ROOT / "Makefile"


def _make(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["make", "--no-print-directory", "-C", str(ROOT), *args], capture_output=True, text=True,
                          timeout=60)


def _verbs() -> list[str]:
    """The .PHONY verbs, in the file's order (the helper targets start with `_`)."""
    m = re.search(r"^\.PHONY:(.*)$", MAKEFILE.read_text(encoding="utf-8"), re.M)
    assert m, "the Makefile declares no .PHONY verbs"
    return m.group(1).split()


def test_make_help_lists_every_verb_with_its_line():
    text = MAKEFILE.read_text(encoding="utf-8")
    described = re.findall(r"^([a-z][a-z-]*):[^=\n]*## \S", text, re.M)
    assert described == _verbs(), "every .PHONY verb has a `## ` line, in the same order, and nothing else does"
    for goal in ((), ("help",)):                                  # `make` alone is `make help`
        out = _make(*goal)
        assert out.returncode == 0, out.stderr
        listed = [ln.split()[0] for ln in out.stdout.splitlines()[1:] if ln.startswith("  ")]
        assert listed == _verbs(), out.stdout


def _commands(verb: str, *extra: str) -> list[str]:
    out = _make("-n", verb, "UNAME=Darwin", *extra)
    assert out.returncode == 0, f"make -n {verb}: {out.stderr}"
    return [ln for ln in out.stdout.splitlines() if ln.strip()]


def _scripts() -> dict:
    return json.loads((ROOT / "app" / "package.json").read_text(encoding="utf-8"))["scripts"]


def test_every_verb_s_recipe_resolves():
    """Each command `make -n` prints names only what exists: a repo path (script, page), a Python module after `-m`,
    an app pnpm script after `pnpm -C app`."""
    for verb in _verbs():
        lines = _commands(verb)
        assert lines, f"make {verb} runs nothing"
        for ln in lines:
            for path in re.findall(r"(?<![\w/.])((?:\./)?(?:scripts|tools|docs)/[\w./-]+\.(?:sh|py|md))", ln):
                assert (ROOT / path).is_file(), f"make {verb} names {path}, which does not exist"
            for mod in re.findall(r"-m (pipeline[\w.]*)", ln):
                assert importlib.util.find_spec(mod) is not None, f"make {verb} runs -m {mod}: no such module"
            for part in re.split(r"[;&|]+", ln):
                words = shlex.split(part) if part.lstrip().startswith("pnpm -C app ") else []
                if words and words[3] != "exec":
                    assert words[3] in _scripts(), f"make {verb}: app/package.json has no script {words[3]!r}"


def test_the_verbs_wrap_today_s_commands():
    """The exact commands (fresh-machine.md §2, §6; README), so a verb cannot drift into a second copy of a script."""
    assert _commands("setup")[-1] == "bash scripts/bootstrap.sh --setup-only"
    assert _commands("build-first")[-1] == ("bash scripts/heavy.sh first-build 50 -- .venv/bin/python -m pipeline build "
                                           "--atlas --promote --jobs 2")
    assert _commands("test-py")[-1].endswith("bash scripts/heavy.sh tests 30 -- bash scripts/pytest-chunked.sh -- -q")
    assert _commands("test-py", "ARGS=pipeline/tests/test_x.py -k foo")[-1] == (
        ".venv/bin/python -m pytest -q pipeline/tests/test_x.py -k foo")
    assert _commands("test-app", "ARGS=tools/x.test.ts")[-1] == "pnpm -C app exec vitest run tools/x.test.ts"
    assert _commands("release")[-1] == ".venv/bin/python -m pipeline release --cut --dry-run"


def test_production_has_no_verb_that_runs():
    """AGENTS A7: `make prod` only prints; no recipe line outside an echo names CONFIRM_PROD or `--prod`."""
    assert all(ln.startswith("echo ") for ln in _commands("prod"))
    for ln in MAKEFILE.read_text(encoding="utf-8").splitlines():
        if ln.startswith("\t") and ("CONFIRM_PROD" in ln or "--prod" in ln):
            assert ln.lstrip("\t@").startswith("echo "), ln


def test_a_missing_prerequisite_stops_naming_its_fix(tmp_path):
    missing = str(tmp_path / "none")
    out = _make("test-py", "ARGS=-k x", f"PY={missing}")
    assert out.returncode != 0 and "make setup" in out.stderr, out.stderr
    out = _make("test-app", "ARGS=x", f"APP_MODULES={missing}")
    assert out.returncode != 0 and "make setup" in out.stderr and "pnpm -C app install" in out.stderr, out.stderr
    out = _make("build", "UNAME=Linux")
    assert out.returncode != 0, out.stdout
    assert "macOS-only" in out.stderr and ".venv/bin/python -m pipeline build" in out.stderr, out.stderr
    out = _make("test", "ARGS=x")
    assert out.returncode != 0 and "make test-py ARGS=" in out.stderr, out.stderr
