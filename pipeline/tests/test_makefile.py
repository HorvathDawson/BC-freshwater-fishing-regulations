"""(T1193, owner 2026-11-26 "A Makefile (Recommended)") THE ROOT MAKEFILE: one verb per task, each wrapping a command
the repo already has. Pinned here: `make help` lists every verb (from the file's `## ` comments), every verb's recipe
resolves (`make -n <verb>`: each script, module and pnpm script it names exists), a missing prerequisite stops naming
its fix, and production has no verb that runs (AGENTS A7). Real `make` runs, dry (`-n`) except the guards and `help`."""
from __future__ import annotations

import importlib.util
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = ROOT / "Makefile"


def _make(*args: str) -> subprocess.CompletedProcess:
    """`make` as a person runs it: PIPELINE_PY unset (CI sets it to `python`; the Makefile's PY follows it), so the
    recipes print the checkout's `.venv/bin/python`."""
    env = {k: v for k, v in os.environ.items() if k != "PIPELINE_PY"}
    return subprocess.run(["make", "--no-print-directory", "-C", str(ROOT), *args], capture_output=True, text=True,
                          timeout=60, env=env)


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
    assert ".venv/bin/python -m pipeline release --cut --dry-run 2>&1" in "\n".join(_commands("release"))
    assert _commands("app")[-1] == "bash scripts/app-dev.sh"


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


def _fake_python(tmp_path: Path, rc: int, says: str) -> str:
    py = tmp_path / "python"
    py.write_text(f"#!/bin/sh\necho {shlex.quote(says)} >&2\nexit {rc}\n")
    py.chmod(0o755)
    return str(py)


def test_release_says_nothing_to_cut_plainly(tmp_path):
    """The cut's dry run exits 1 when no commit bumps the app: `make release` says so in one line and exits 0; any
    other failure keeps its exit code."""
    py = _fake_python(tmp_path, 1, "release --cut: no app release: nothing since v0.9.0 bumps the app (docs, CI)")
    out = _make("release", f"PY={py}")
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.splitlines()[-1] == "make release: nothing to release since v0.9.0", out.stdout
    out = _make("release", f"PY={_fake_python(tmp_path, 1, 'release --cut: cannot read origin (offline)')}")
    assert out.returncode != 0 and "nothing to release" not in out.stdout, out.stdout


def test_make_app_refuses_a_taken_tile_port():
    """`make app` never runs half-working: a listener on the tile port (the owner's own tile server, say) stops it up
    front, naming the PID; nothing is started and nothing is stopped."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        # APP_MODULES=app: the guard for the app's install passes on a runner without one (pipeline-ci installs none)
        out = subprocess.run(["make", "--no-print-directory", "-C", str(ROOT), "app", "APP_MODULES=app"],
                             capture_output=True, text=True,
                             timeout=60, env=os.environ | {"TILE_PORT": str(port)})          # refused before any start
    assert out.returncode != 0, out.stdout + out.stderr
    # the port check needs no tool (bash's /dev/tcp); the PID is named when lsof is there, and its absence is said
    who = f"PID {os.getpid()}" if shutil.which("lsof") else "PID unknown: lsof is not installed"
    assert f"port {port} is taken by {who}" in out.stderr, out.stderr
    assert "your own tile server is already up: run `pnpm -C app dev:web` alone, or stop it" in out.stderr
    assert "pnpm -C app tiles" not in out.stdout


# ---- scripts/app-dev.sh stops what it started (T1193 second review): stand-in servers, never the real ones ----------

APP_DEV = ROOT / "scripts" / "app-dev.sh"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _stand_in(record: Path, tail: str) -> str:
    """A stand-in server: a shell that starts a grandchild `sleep`, records its own and the sleep's PID, then `tail`
    (the way `pnpm` is a wrapper whose child is the real server)."""
    return f"sh -c 'sleep 300 & echo $$ $! >> {shlex.quote(str(record))}; {tail}'"


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _run_app_dev(tmp_path: Path, tiles_tail: str, web_tail: str, signal_to_send=None, wait_s: float = 8):
    """Start the script with the stand-ins; send `signal_to_send` once both stand-ins have recorded their PIDs (or none,
    and let it end by itself). Returns (rc, stderr, the stand-in PIDs). Cleans up by PID, only those recorded."""
    import signal
    import time
    tiles_rec, web_rec = tmp_path / "tiles.pids", tmp_path / "web.pids"
    env = {k: v for k, v in os.environ.items() if k != "PIPELINE_PY"} | {
        "APP_DEV_TILES_CMD": _stand_in(tiles_rec, tiles_tail), "APP_DEV_WEB_CMD": _stand_in(web_rec, web_tail),
        "TILE_PORT": str(_free_port()), "APP_DEV_WEB_PORT": str(_free_port())}
    proc = subprocess.Popen(["bash", str(APP_DEV)], cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)
    pids: list[int] = []

    def recorded() -> list[int]:
        return [int(x) for f in (tiles_rec, web_rec) if f.exists() for x in f.read_text().split()]
    try:
        deadline = time.time() + wait_s
        if signal_to_send is not None:
            while time.time() < deadline and len(recorded()) < 4:
                time.sleep(0.05)
            pids = recorded()
            assert len(pids) == 4, "both stand-ins should have started"
            proc.send_signal(signal_to_send)
        _, err = proc.communicate(timeout=wait_s + 5)
        pids = recorded()
        time.sleep(0.5)
        return proc.returncode, err, pids, [p for p in pids if _alive(p)]
    finally:
        for p in recorded() + ([proc.pid] if proc.poll() is None else []):       # only PIDs this test started
            if _alive(p):
                try:
                    os.kill(p, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_app_dev_term_stops_every_server_process(tmp_path):
    import signal
    rc, err, pids, alive = _run_app_dev(tmp_path, "wait", "wait", signal.SIGTERM)
    assert rc == 130, (rc, err)
    assert len(pids) == 4 and alive == [], f"left running: {alive}"


def test_app_dev_hangup_stops_every_server_process(tmp_path):
    import signal
    rc, err, pids, alive = _run_app_dev(tmp_path, "wait", "wait", signal.SIGHUP)
    assert rc == 129, (rc, err)
    assert len(pids) == 4 and alive == [], f"left running: {alive}"


def test_app_dev_stops_the_web_server_when_the_tile_server_dies(tmp_path):
    rc, err, pids, alive = _run_app_dev(tmp_path, "sleep 0.3; exit 3", "wait")
    assert rc != 0 and "the tile server stopped (rc=3)" in err, (rc, err)
    assert len(pids) == 4 and alive == [], f"left running: {alive}"


def test_app_dev_refuses_with_no_tiles_built(tmp_path):
    """The script, copied into an empty tree, finds none of the four files the tile server serves: one plain line naming
    the fix, non-zero, nothing started (no stand-in is set, so a start would be the real pnpm: the check stops it)."""
    (tmp_path / "scripts").mkdir()
    shutil.copy(APP_DEV, tmp_path / "scripts" / "app-dev.sh")
    env = {k: v for k, v in os.environ.items() if not k.startswith("APP_DEV_")} | {
        "TILE_PORT": str(_free_port()), "APP_DEV_WEB_PORT": str(_free_port())}
    out = subprocess.run(["bash", str(tmp_path / "scripts" / "app-dev.sh")], capture_output=True, text=True, timeout=30,
                         env=env)
    assert out.returncode == 1 and out.stdout == "", out
    assert "data/generated/tiles/atlas.pmtiles is not built" in out.stderr and "make build-first" in out.stderr
    assert out.stderr.count("\n") == 1, out.stderr


def test_app_dev_names_a_bad_tile_port(tmp_path):
    out = subprocess.run(["bash", str(APP_DEV)], capture_output=True, text=True, timeout=30,
                         env=os.environ | {"TILE_PORT": "80a"})
    assert out.returncode == 2 and "TILE_PORT" in out.stderr, out
