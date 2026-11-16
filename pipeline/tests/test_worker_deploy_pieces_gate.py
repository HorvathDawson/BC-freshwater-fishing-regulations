"""T757 item 7: THE WORKER-DEPLOY MIRROR OF THE PIECES GATE (`pipeline.deliver.release.pieces_gate --worker-deploy`).

A feeds worker whose code does not serve `dfo.pieces >= 1` (its `DFO_SHAPE`, read by `deploy/lib/worker-shape.mjs`)
is refused while the environment's live release binds a closed-area piece; an unreadable live release or worker shape
is refused too, never passed. Both feeds deploy scripts ask it before `wrangler deploy`. No network: fake releases and
fake worker modules; nothing is deployed.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from pipeline.deliver.release import pieces_gate as G

ROOT = Path(__file__).resolve().parents[2]
PIECE = "3:x-river#closed:abc123"
BINDS = {"p": {"3:x-river:whole": {"r": "3"}, PIECE: {"r": "3", "piece": 1, "bound": 1}}}
CARRIED = {"p": {"3:x-river:whole": {"r": "3"}, PIECE: {"r": "3", "piece": 1}}}
OLD = {"p": {"3:x-river:whole": {"r": "3"}}}


@pytest.mark.parametrize("payload,shape,refused,why", [
    (BINDS, None, True, "does not emit them"),                 # today's worker (no DFO_SHAPE) beside a binding release
    (BINDS, {}, True, "does not emit them"),
    (BINDS, {"pieces": 0}, True, "does not emit them"),
    (BINDS, {"pieces": "1"}, True, "does not emit them"),
    (BINDS, {"pieces": 1}, False, ""),
    (BINDS, {"pieces": 2}, False, ""),
    (CARRIED, None, False, ""),                               # a carried piece binds nothing
    (OLD, None, False, ""),                                   # today's truth: no piece bound, no shape: passes
    # (2026-11-16) a worker that serves the shape passes with NO release read: the first production deploy has none yet
    (None, {"pieces": 1}, False, ""),
    (None, {"pieces": 2}, False, ""),
    (None, None, True, "could not be read"),                   # unreadable live release, a worker without the shape
    (None, {"pieces": 0}, True, "could not be read"),
    (None, {"pieces": True}, True, "could not be read"),       # true is not a shape number
    (None, "unread", True, "the feeds worker's feed shape (DFO_SHAPE) could not be evaluated"),
    (OLD, "unread", True, "the feeds worker's feed shape (DFO_SHAPE) could not be evaluated from app/workers/feeds/src/status.ts"),
])
def test_a_worker_that_does_not_emit_pieces_is_refused_beside_a_release_that_binds_one(payload, shape, refused, why):
    """Mutation: pass a worker without `dfo.pieces` beside a binding release, or an unreadable one — after a rollback
    every bound piece's feed location is gone and the app shows the release's own file (T757 item 6) without anyone
    having decided it."""
    got = G.worker_problems(payload, shape, "the live release r-test")
    assert bool(got) is refused, got
    if refused:
        assert why in got[0]
    if refused and payload is BINDS:
        assert PIECE in got[0] and "T757" in got[0] and "r-test" in got[0]


def _release(tmp: Path, rel: str = "r-test") -> Path:
    (tmp / "releases" / rel).mkdir(parents=True)
    (tmp / "releases" / rel / "manifest.json").write_text("{}")
    live = tmp / "live.json"
    live.write_text(json.dumps({"data_release": rel, "minAppVersion": "0.5.0"}))
    return live


def test_the_cli_reads_the_live_release_named_by_the_bucket_manifest(tmp_path, monkeypatch, capsys):
    from pipeline.deliver import salmon as S
    live = _release(tmp_path)
    seen = []
    monkeypatch.setattr(S, "read_release", lambda d: seen.append(Path(d).name) or BINDS)
    args = ["--worker-deploy", "--live-manifest", str(live), "--releases", str(tmp_path / "releases")]
    assert G.main([*args, "--worker-shape", "null"]) == 1
    assert "feeds deploy: refused" in capsys.readouterr().err and seen == ["r-test"]
    assert G.main([*args, "--worker-shape", '{"pieces":1}']) == 0
    assert G.main([*args, "--worker-shape", ""]) == 1                      # worker-shape.mjs failed: nothing printed
    assert G.main([*args, "--worker-shape", "not json"]) == 1
    monkeypatch.setattr(S, "read_release", lambda d: OLD)
    assert G.main([*args, "--worker-shape", "null"]) == 0
    assert G.main([*args, "--worker-shape", ""]) == 1                      # unreadable shape: refused, pieces or not


def _unreadable(tmp_path):
    """the four ways the live release cannot be read: [args]"""
    live = _release(tmp_path)
    rel = tmp_path / "releases"
    empty = tmp_path / "empty.json"
    empty.write_text("")                                                     # rclone cat failed, or no release yet
    other = tmp_path / "other.json"
    other.write_text(json.dumps({"data_release": "r-not-here"}))
    return [["--worker-deploy", "--live-manifest", str(empty), "--releases", str(rel)],
            ["--worker-deploy", "--live-manifest", str(other), "--releases", str(rel)],
            # the folder is there but holds no salmon file the reader can open
            ["--worker-deploy", "--live-manifest", str(live), "--releases", str(rel)],
            ["--worker-deploy", "--releases", str(rel)]]


@pytest.mark.parametrize("shape", ["null", "{}", '{"pieces":0}'])
def test_the_cli_refuses_a_live_release_it_cannot_read_for_a_worker_without_the_shape(tmp_path, capsys, shape):
    for args in _unreadable(tmp_path):
        assert G.main([*args, "--worker-shape", shape]) == 1
    assert capsys.readouterr().err.count("feeds deploy: refused") == 4


def test_a_worker_that_serves_pieces_is_deployed_without_the_live_release_being_read(tmp_path, monkeypatch, capsys):
    """(2026-11-16) Mutation: read the live release first — production's FIRST worker deploy (no release in the bucket
    yet, `rclone cat` writes nothing) is refused, and the upload gate refuses the release until that worker is up."""
    from pipeline.deliver import salmon as S

    def never(*a, **k):
        raise AssertionError("the live release was read for a worker that serves pieces")
    monkeypatch.setattr(S, "read_release", never)
    monkeypatch.setattr(G, "read_live", never)
    for args in _unreadable(tmp_path):
        assert G.main([*args, "--worker-shape", '{"pieces":1}']) == 0
    missing = ["--worker-deploy", "--live-manifest", str(tmp_path / "no.json"), "--releases", str(tmp_path / "no-such")]
    assert G.main([*missing, "--worker-shape", '{"pieces":1}']) == 0
    assert capsys.readouterr().err == ""
    # an unreadable worker shape is refused always, and needs no release either
    assert G.main([*missing, "--worker-shape", ""]) == 1
    assert "DFO_SHAPE" in capsys.readouterr().err


NAME_IT = ("A deploy from another checkout or a scratch worktree names the main checkout's folder: "
           "RELEASES_DIR=/path/to/main/checkout/data/release (deploy/staging/feeds.sh, deploy/prod/feeds.sh) "
           "or --releases <path> (the gate)")


def test_a_live_release_not_under_the_folder_is_refused_saying_how_to_name_it(tmp_path, monkeypatch, capsys):
    """Mutation: fall back to data/release, or refuse without saying where it looked and RELEASES_DIR — a deploy from a
    scratch worktree (no data/release) is stuck with no way out but a workaround."""
    from pipeline.deliver import salmon as S
    monkeypatch.setattr(S, "read_release", lambda d: OLD)
    live = tmp_path / "live.json"
    live.write_text(json.dumps({"data_release": "r-live"}))
    here, elsewhere = tmp_path / "scratch" / "data" / "release", tmp_path / "main" / "data" / "release"
    here.mkdir(parents=True)
    (elsewhere / "r-live").mkdir(parents=True)
    (elsewhere / "r-live" / "manifest.json").write_text("{}")
    args = ["--worker-deploy", "--live-manifest", str(live), "--worker-shape", "null"]
    assert G.main([*args, "--releases", str(here)]) == 1
    assert capsys.readouterr().err.strip() == (
        f"feeds deploy: refused — the live release r-live was looked for under {here} and is not there "
        f"({here}/r-live/manifest.json), so whether it binds closed-area pieces (T573) is not known. {NAME_IT}")
    assert G.main([*args, "--releases", str(elsewhere)]) == 0                 # the main checkout's folder, named
    missing = tmp_path / "no-such" / "release"
    assert G.main([*args, "--releases", str(missing)]) == 1
    assert capsys.readouterr().err.strip() == (
        f"feeds deploy: refused — the release folder {missing} does not exist, so the live release cannot be read and "
        f"whether it binds closed-area pieces (T573) is not known. {NAME_IT}")


def test_the_upload_mode_is_unchanged(tmp_path, monkeypatch):
    from pipeline.deliver import salmon as S
    monkeypatch.setattr(S, "read_release", lambda d: OLD)
    assert G.main([str(tmp_path / "r-x" / "manifest.json")]) == 0


def _shape(entry: Path) -> subprocess.CompletedProcess:
    node = shutil.which("node")
    assert node, "node is needed to read the worker's DFO_SHAPE (deploy/lib/worker-shape.mjs)"
    env = {"PATH": os.path.dirname(node), "HOME": str(entry.parent)}           # built per case, never the caller's
    return subprocess.run([node, str(ROOT / "deploy/lib/worker-shape.mjs"), str(entry)], capture_output=True, text=True,
                          env=env, timeout=60)


@pytest.mark.parametrize("src,rc,out", [
    ('import { N } from "./n";\nexport const DFO_SHAPE = { pieces: N } as const;\n', 0, '{"pieces":1}'),
    ("export const OTHER: number = 1;\n", 0, "null"),
    ('throw new Error("boom");\nexport const DFO_SHAPE = { pieces: 1 };\n', 1, ""),
    ("export const DFO_SHAPE = { pieces: ;\n", 1, ""),
    ("export const DFO_SHAPE = 1;\n", 1, ""),
])
def test_the_worker_shape_is_read_by_evaluating_the_module(tmp_path, src, rc, out):
    """Mutation: read the text (a grep) — a commented-out or computed DFO_SHAPE reads wrong; swallow a failure — an
    unreadable worker passes."""
    (tmp_path / "n.ts").write_text("export const N: number = 1;\n")
    entry = tmp_path / "status.ts"
    entry.write_text(src)
    got = _shape(entry)
    assert (got.returncode, got.stdout.strip()) == (rc, out), got.stderr
    if rc:
        assert "could not be read" in got.stderr


def test_the_worker_shape_reads_the_real_status_module():
    got = _shape(ROOT / "app/workers/feeds/src/status.ts")
    assert got.returncode == 0, got.stderr
    shape = json.loads(got.stdout)
    assert shape is None or isinstance(shape, dict)


@pytest.mark.parametrize("script,deploy", [
    ("deploy/staging/feeds.sh", "npx --prefix app wrangler deploy -c app/workers/feeds/wrangler.toml"),
    ("deploy/prod/feeds.sh", 'npx --prefix app wrangler deploy -c "$C"\n'),
])
def test_both_feeds_deploys_ask_the_gate_before_wrangler_deploy(script, deploy):
    body = (ROOT / script).read_text()
    run = "\n".join(ln for ln in body.splitlines() if not ln.lstrip().startswith("#")) + "\n"
    at = run.index(deploy)
    gate = run.index("pipeline.deliver.release.pieces_gate --worker-deploy")
    shape = run.index("node deploy/lib/worker-shape.mjs")
    live = run.index("/release/manifest.json")
    assert shape < gate < at and live < gate
    assert len(re.findall(r"wrangler deploy -c (?!.*--dry-run)", run)) == 1      # no other real deploy to dodge it
    # (T757) the folder the gate reads the live release from is the caller's to name, one variable
    call = run[gate:run.index("\n", run.index("--worker-shape", gate))]
    assert '--releases "${RELEASES_DIR:-data/release}"' in call
    assert subprocess.run(["bash", "-n", str(ROOT / script)], env={"PATH": "/bin:/usr/bin"}).returncode == 0
