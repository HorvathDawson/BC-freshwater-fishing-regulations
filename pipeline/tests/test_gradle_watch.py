"""T1058: build-apk.sh's Gradle builds end (deploy/lib/gradle-watch.sh), with a fake `gradlew`.

Recorded (0.9.0 production build, 2026-11-24 09:14): Gradle printed BUILD SUCCESSFUL, its daemon stopped before it sent
the client the build's result, and the `gradlew` client waited on the closed connection for over an hour, holding the
heavy lock (~/.gradle/daemon/9.4.1/daemon-53034.out.log)."""
from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIB = ROOT / "deploy/lib/gradle-watch.sh"
FAST = {"GRADLE_DONE_GRACE_S": "2", "GRADLE_POLL_S": "1", "GRADLE_STOP_GRACE_S": "2", "GRADLE_MAX_S": "20"}


def gradlew(tmp: Path, body: str) -> Path:
    """an android/ dir whose gradlew writes its args and pid, then runs <body>"""
    d = tmp / "android"
    d.mkdir()
    g = d / "gradlew"
    g.write_text(f'#!/bin/bash\necho "$@" > "{tmp}/args"\necho $$ > "{tmp}/pid"\n{body}\n')
    g.chmod(0o755)
    return d


def watched(tmp: Path, d: Path, **env) -> tuple[subprocess.CompletedProcess, float]:
    e = {**{k: v for k, v in os.environ.items() if not k.startswith("GRADLE_")}, **FAST, **env,
         "GRADLE_USER_HOME": env.get("GRADLE_USER_HOME", str(tmp / "gh"))}
    t = time.monotonic()
    got = subprocess.run(["bash", "-c", 'set -euo pipefail; source "$0"; gradle_watched "$1" assembleRelease -x lint',
                          str(LIB), str(d)], env=e, capture_output=True, text=True, timeout=60)
    return got, time.monotonic() - t


def alive(pid: int) -> bool:
    st = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return bool(st) and not st.startswith("Z")


def test_a_client_left_waiting_after_build_successful_is_stopped_and_the_build_fails(tmp_path):
    """the recorded hang: the result line printed, the client never exits; it and what it started are stopped."""
    d = gradlew(tmp_path, f'sleep 300 & echo $! > "{tmp_path}/child"\necho "BUILD SUCCESSFUL in 3m 27s"\nsleep 300')
    got, took = watched(tmp_path, d)
    assert got.returncode == 124, (got.stdout, got.stderr)
    assert took < 30
    assert "BUILD SUCCESSFUL in 3m 27s" in got.stdout                       # its output was shown as it came
    assert re.search(r'Gradle printed "BUILD SUCCESSFUL in 3m 27s" \d+ s ago and its client \(pid \d+\) has not exited',
                     got.stderr), got.stderr
    assert "The build is NOT trusted" in got.stderr and "this build FAILED (stopped)" in got.stderr
    for f in ("pid", "child"):                                              # no orphan left to hold the lock
        assert not alive(int((tmp_path / f).read_text())), f


def test_a_build_past_its_time_is_stopped(tmp_path):
    d = gradlew(tmp_path, "echo '> Task :app:minifyReleaseWithR8'\nsleep 300")
    got, took = watched(tmp_path, d, GRADLE_MAX_S="3")
    assert got.returncode == 124 and took < 30
    assert re.search(r"Gradle \(pid \d+\) ran \d+ s, past GRADLE_MAX_S=3, without finishing", got.stderr), got.stderr
    assert not alive(int((tmp_path / "pid").read_text()))


def test_a_build_that_ends_keeps_its_exit_code_and_runs_without_a_daemon(tmp_path):
    ok = gradlew(tmp_path, 'echo "BUILD SUCCESSFUL in 1s"')
    got, _ = watched(tmp_path, ok)
    assert got.returncode == 0 and "BUILD SUCCESSFUL in 1s" in got.stdout and not got.stderr
    assert (tmp_path / "args").read_text().split() == ["--no-daemon", "assembleRelease", "-x", "lint"]
    (tmp_path / "b").mkdir()
    bad = gradlew(tmp_path / "b", 'echo "BUILD FAILED in 2s"; exit 3')
    got, _ = watched(tmp_path / "b", bad)
    assert got.returncode == 3 and "BUILD FAILED in 2s" in got.stdout and "stopped" not in got.stderr


def test_a_machine_wide_jvmargs_is_said(tmp_path):
    """~/.gradle/gradle.properties overrides the project's jvmargs (plugins/with-gradle-memory.js): said, not hidden."""
    gh = tmp_path / "gh"
    gh.mkdir()
    (gh / "gradle.properties").write_text("# note\norg.gradle.jvmargs=-Xmx2048m -XX:MaxMetaspaceSize=1024m\n")
    got, _ = watched(tmp_path, gradlew(tmp_path, "true"), GRADLE_USER_HOME=str(gh))
    assert got.returncode == 0
    assert (f"gradle-watch: {gh}/gradle.properties sets org.gradle.jvmargs=-Xmx2048m -XX:MaxMetaspaceSize=1024m — it "
            "OVERRIDES the project's") in got.stdout
    (gh / "gradle.properties").write_text("org.gradle.parallel=true\n")
    got, _ = watched(tmp_path / "gh", gradlew(tmp_path / "gh", "true"), GRADLE_USER_HOME=str(gh))
    assert "OVERRIDES" not in got.stdout


def test_both_build_apk_gradle_builds_are_watched():
    sh = (ROOT / "deploy/staging/build-apk.sh").read_text()
    assert "source deploy/lib/gradle-watch.sh" in sh
    runs = [ln for ln in sh.splitlines() if "./gradlew" in ln and not ln.lstrip().startswith(("#", "echo"))]
    assert runs == [], runs                                                 # no bare ./gradlew left
    assert sh.count("gradle_watched android bundleRelease") == 1 and sh.count("gradle_watched android assembleRelease") == 1
