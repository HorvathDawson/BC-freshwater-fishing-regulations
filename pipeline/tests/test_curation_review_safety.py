"""The curation-review tool is SAFE to sit a reviewer in front of (T96, curation-review/PLAN.md Phase 0).

  no process    no endpoint starts a subprocess: the in-app "Rebuild graph" / "Promote" started an
                18-minute, 9 GB atlas build outside `scripts/heavy.sh`, beside the review buttons
  not exposed   Vite binds 127.0.0.1 only and the backend's CORS admits only the Vite origin (it
                was `host: true` and CORS `*`, with no authentication, on an editor that writes the
                curated catalogue)
  split notes   each split's curator note (`_note`) and auto-proposal grade reach the frontend: 151
                cut-points are still auto-proposals and the tool hid every word of that

Runs the backend against TEMP COPIES of the files it could write; reads no generated data.
"""
from __future__ import annotations

import ast
import json
import re
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED

REPO = Path(__file__).resolve().parents[2]
TOOL = REPO / "curation-review"
BACKEND = TOOL / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes")


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("curation")
    cat = tmp / "catalogue"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    splits = tmp / "splits.json"
    shutil.copy2(CURATED.waters.splits, splits)
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_ENTRIES_DIR", str(cat))
    (tmp / "marks.jsonl").write_text("")
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_SPLITS_JSON", str(splits))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp / "backups"))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    assert reuse.SPLITS_JSON_PATH == splits
    from fastapi.testclient import TestClient
    yield {"app": app_mod, "client": TestClient(app_mod.app), "reuse": reuse}
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


# --------------------------------------------------------------------------- no process

_SPAWN_MODULES = ("subprocess", "multiprocessing", "pty")
_SPAWN_CALLS = ("system", "popen", "spawnv", "spawnl", "spawnlp", "spawnvp", "execv", "execvp",
                "execl", "execlp", "fork", "posix_spawn", "posix_spawnp",
                "create_subprocess_exec", "create_subprocess_shell")


def _spawners(src: str, name: str) -> list[str]:
    """Every way `src` could start a process: a spawning module imported, an `os`/`asyncio`
    spawn called by attribute, or one imported by name (`from os import system`)."""
    out = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            out += [f"{name}: import {a.name}" for a in node.names
                    if a.name.split(".")[0] in _SPAWN_MODULES]
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.split(".")[0] in _SPAWN_MODULES:
                out.append(f"{name}: import {mod}")
            elif mod in ("os", "asyncio"):
                out += [f"{name}: from {mod} import {a.name}" for a in node.names
                        if a.name in _SPAWN_CALLS]
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                and node.value.id in ("os", "asyncio") and node.attr in _SPAWN_CALLS:
            out.append(f"{name}: {node.value.id}.{node.attr}")
    return out


@pytest.mark.parametrize("src,found", [
    ("import subprocess", ["m.py: import subprocess"]),
    ("from multiprocessing import Process", ["m.py: import multiprocessing"]),
    ("import os\nos.system('x')", ["m.py: os.system"]),
    ("from os import system", ["m.py: from os import system"]),
    ("import asyncio\nasyncio.create_subprocess_exec('x')", ["m.py: asyncio.create_subprocess_exec"]),
    ("from asyncio import create_subprocess_shell", ["m.py: from asyncio import create_subprocess_shell"]),
    ("import os\nos.path.join('a')\nfrom os import environ", []),
])
def test_the_process_detector_sees_every_spawn(src, found):
    """The detector `test_no_endpoint_starts_a_process` relies on is not a tautology: it names each
    spawning form, and nothing harmless."""
    assert _spawners(src, "m.py") == found


def test_no_endpoint_starts_a_process(env):
    """No route rebuilds or promotes, and no backend module can spawn a process."""
    paths = sorted(r.path for r in env["app"].app.routes if r.path.startswith("/api/"))
    assert [p for p in paths if "rebuild" in p or "promote" in p] == []
    spawners = [x for py in sorted(BACKEND.glob("*.py"))
                for x in _spawners(py.read_text(encoding="utf-8"), py.name)]
    assert spawners == []
    assert not (BACKEND / "rebuild.py").exists()
    assert not (TOOL / "frontend" / "src" / "components" / "RebuildButton.tsx").exists()


# --------------------------------------------------------------------------- not exposed

def test_vite_binds_loopback_only():
    cfg = (TOOL / "frontend" / "vite.config.ts").read_text(encoding="utf-8")
    hosts = re.findall(r"^\s*host:\s*([^,\n]+),", cfg, re.MULTILINE)
    assert hosts == ['"127.0.0.1"']


def test_the_backend_binds_loopback_only():
    for sh in (TOOL / "run.sh", BACKEND / "run.sh"):
        line = next(x for x in sh.read_text(encoding="utf-8").splitlines() if "uvicorn\" app:app" in x)
        assert "--host 127.0.0.1 " in line, sh


@pytest.mark.parametrize("origin,allowed", [
    ("http://127.0.0.1:5173", "http://127.0.0.1:5173"),
    ("http://localhost:5173", None),
    ("http://192.168.1.20:5173", None),
    ("https://evil.example", None),
])
def test_cors_admits_only_the_vite_origin(env, origin, allowed):
    r = env["client"].options("/api/splits/x", headers={
        "Origin": origin, "Access-Control-Request-Method": "PUT"})
    assert r.headers.get("access-control-allow-origin") == allowed
    got = env["client"].get("/api/splits/adam_river__hwy_19_bridge", headers={"Origin": origin})
    assert got.headers.get("access-control-allow-origin") == allowed


# --------------------------------------------------------------------------- split notes

def test_a_split_carries_its_curator_note_and_grade(env):
    """GET /api/splits/{id} sends `review`: the `_note` the curator wrote and the auto-proposal
    grade read from it (`[auto-proposal L]`), so `SplitEditor` can show what still needs checking."""
    sid = "adam_river__hwy_19_bridge"
    raw = next(sp for wb in json.loads(CURATED.waters.splits.read_text())["waterbodies"]
               for sp in wb["splits"] if sp["id"] == sid)
    got = env["client"].get(f"/api/splits/{sid}").json()
    assert got["review"] == {"note": raw["_note"], "grade": "L"}


@pytest.mark.parametrize("note,grade", [
    ("[auto-proposal L] Hwy 19 bridge …", "L"),
    ("[auto-proposal M] …", "M"),
    ("[auto-proposal H] …", "H"),
    ("[structural-split 2026-08-05] boundary=… [auto-proposal L] later text", None),
    ("[human-review 2026-08-05 FWA-confluence] mouth of Eve River", None),
    ("", None),
])
def test_the_grade_is_the_note_s_leading_auto_proposal_tag(env, note, grade):
    assert env["reuse"].split_review({"_note": note}) == {"note": note, "grade": grade}


def test_the_hwy_19_bridge_note_is_on_the_bridge_not_the_eve_river_confluence():
    """The Eve River confluence carried the Hwy 19 bridge's auto-proposal text (and its L grade)
    word for word; the bridge split already holds it. The confluence keeps its own history."""
    by_id = {sp["id"]: sp for wb in json.loads(CURATED.waters.splits.read_text())["waterbodies"]
             for sp in wb["splits"]}
    eve, bridge = by_id["adam_river__eve_river_into_adam_river"], by_id["adam_river__hwy_19_bridge"]
    assert "Hwy 19 bridge over Adam River" in bridge["_note"]
    assert bridge["_note"].startswith("[auto-proposal L] Hwy 19 bridge over Adam River")
    assert eve["_note"] == (
        "[review: two-boundary] 'upstream of Eve River, to Hwy 19 bridge' = TWO boundaries (Eve "
        "River confluence + Hwy 19 bridge); may need -a/-b rows per the two-boundary writing rule. "
        "[structural-split 2026-08-05 from “upstream of Eve River, to Hwy 19 bridge”] "
        "boundary=upstream of Eve River [human-review 2026-08-05 FWA-confluence] mouth of Eve "
        "River on mainstem (0 m validated); target blk/wsc set. [T96 2026-10-22] the Hwy 19 "
        "bridge auto-proposal text that sat here is the bridge's (adam_river__hwy_19_bridge).")
