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
FAST = {"GRADLE_DONE_GRACE_S": "2", "GRADLE_POLL_S": "1", "GRADLE_STOP_GRACE_S": "2", "GRADLE_MAX_S": "20",
        "GRADLE_DRAIN_S": "2"}


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
    for form in ("org.gradle.jvmargs: -Xmx1g", "org.gradle.jvmargs -Xmx1g", "  org.gradle.jvmargs = -Xmx1g"):
        (gh / "gradle.properties").write_text(form + "\n")       # (review) a properties file's three separators
        got, _ = watched(tmp_path, tmp_path / "android", GRADLE_USER_HOME=str(gh))
        assert f"sets {form.strip()} — it OVERRIDES" in got.stdout, (form, got.stdout)
    (gh / "gradle.properties").write_text("org.gradle.parallel=true\norg.gradle.jvmargsX=1\n")
    got, _ = watched(tmp_path / "gh", gradlew(tmp_path / "gh", "true"), GRADLE_USER_HOME=str(gh))
    assert "OVERRIDES" not in got.stdout


def test_both_build_apk_gradle_builds_are_watched():
    sh = (ROOT / "deploy/staging/build-apk.sh").read_text()
    assert "source deploy/lib/gradle-watch.sh" in sh
    runs = [ln for ln in sh.splitlines() if "./gradlew" in ln and not ln.lstrip().startswith(("#", "echo"))]
    assert runs == [], runs                                                 # no bare ./gradlew left
    assert sh.count("gradle_watched android bundleRelease") == 1 and sh.count("gradle_watched android assembleRelease") == 1


def test_a_failing_build_is_shown_whole_to_a_slow_reader_and_its_log_kept(tmp_path):
    """(review of #155) a `tail -f` stopped after a fixed second lost a failing build's last lines ("What went wrong",
    BUILD FAILED) to a slow reader, and the log was deleted: every line reaches the reader, and the log stays."""
    n = 200_000
    d = gradlew(tmp_path, f'seq 1 {n}; echo "* What went wrong:"; echo "BUILD FAILED in 1s"; exit 1')
    e = {**{k: v for k, v in os.environ.items() if not k.startswith("GRADLE_")}, **FAST,
         "GRADLE_USER_HOME": str(tmp_path / "gh")}
    p = subprocess.Popen(["bash", "-c", 'set -euo pipefail; source "$0"; gradle_watched "$1" assembleRelease',
                          str(LIB), str(d)], env=e, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out = b""
    while chunk := p.stdout.read1(16384):                                # a slow reader: ~5 s for the whole output
        out += chunk
        time.sleep(0.01)
    err = p.stderr.read().decode()
    assert p.wait(timeout=60) == 1
    lines = out.decode().splitlines()
    assert lines[-2:] == ["* What went wrong:", "BUILD FAILED in 1s"] and len(lines) == n + 2, len(lines)
    m = re.search(r"the build FAILED \(exit 1\); its whole output: (\S+)", err)
    assert m and Path(m.group(1)).read_text().splitlines()[-1] == "BUILD FAILED in 1s", err
    Path(m.group(1)).unlink()


def test_the_grace_counts_from_the_build_line_not_the_start(tmp_path):
    """(review) a BUILD line printed later than the grace after the start, the client exiting soon after: not stopped."""
    d = gradlew(tmp_path, 'sleep 6; echo "BUILD SUCCESSFUL in 6s"; sleep 1')
    got, _ = watched(tmp_path, d, GRADLE_DONE_GRACE_S="4")
    assert got.returncode == 0, got.stderr


def test_a_kotlin_daemon_under_the_client_is_spared(tmp_path):
    """(review) a Kotlin compile daemon may serve another build; it stops itself once its Gradle is gone."""
    d = gradlew(tmp_path, f'(exec -a KotlinCompileDaemon sleep 300) >/dev/null 2>&1 & echo $! > "{tmp_path}/kotlin"\n'
                          f'sleep 300 & echo $! > "{tmp_path}/child"\necho "BUILD SUCCESSFUL in 1s"\nsleep 300')
    got, _ = watched(tmp_path, d)
    kotlin = int((tmp_path / "kotlin").read_text())
    try:
        assert got.returncode == 124, got.stderr
        assert "KotlinCompileDaemon" in subprocess.run(["ps", "-o", "command=", "-p", str(kotlin)],
                                                      capture_output=True, text=True).stdout
        assert alive(kotlin) and not alive(int((tmp_path / "child").read_text()))
    finally:
        subprocess.run(["kill", str(kotlin)], capture_output=True)       # this test's own process, by its pid


def test_output_still_held_after_gradle_exits_is_said_and_fails_the_build(tmp_path):
    """(W46) a process left holding Gradle's output would keep the watcher waiting: GRADLE_DRAIN_S after Gradle exited
    with the output not growing, it stops waiting, says so, and the build fails (125) with its log kept."""
    d = gradlew(tmp_path, f'sleep 300 & echo $! > "{tmp_path}/child"\necho "BUILD SUCCESSFUL in 1s"')
    got, took = watched(tmp_path, d)
    try:
        assert got.returncode == 125 and took < 30, got.stderr
        assert "the output did not end 2 s after Gradle exited (a process still holds it)" in got.stderr
        assert re.search(r"the build FAILED \(exit 125\); its whole output: \S+", got.stderr), got.stderr
    finally:
        subprocess.run(["kill", (tmp_path / "child").read_text().strip()], capture_output=True)   # its own pid
