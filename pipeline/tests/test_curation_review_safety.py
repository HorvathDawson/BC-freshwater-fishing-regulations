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
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes",
                "review", "evidence")


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
    mp.setenv("CURATION_FIX", "0")                       # review mode, the default (T134)
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


# --------------------------------------------------------------------------- two modes (T134)

#: every route fix mode adds — the editors (PLAN §4 phase 3)
FIX_ROUTES = {("POST", "/api/check"), ("PUT", "/api/entries/{entry_id}"),
              ("PUT", "/api/splits/{split_id}"), ("POST", "/api/splits"),
              ("DELETE", "/api/splits/{split_id}"), ("POST", "/api/splits/{split_id}/rename")}
MARK_ROUTE = {("POST", "/api/marks")}


def _writes_of(fastapi_app) -> set[tuple[str, str]]:
    """Every (method, path) the app answers that is not a read."""
    return {(m, r.path) for r in fastapi_app.routes for m in getattr(r, "methods", None) or ()
            if m not in ("GET", "HEAD")}


def _load_app(monkeypatch, fix: str):
    import importlib.util
    monkeypatch.setenv("CURATION_FIX", fix)
    spec = importlib.util.spec_from_file_location(f"app_mode_{fix}", BACKEND / "app.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_review_mode_serves_one_write_a_mark(env):
    """Review mode (the default) exposes only POST /api/marks and reads (PLAN §4 phase 3)."""
    assert env["app"].FIX is False
    assert _writes_of(env["app"].app) == MARK_ROUTE


def test_fix_mode_adds_exactly_the_editors(env, monkeypatch):
    assert _writes_of(_load_app(monkeypatch, "1").app) == MARK_ROUTE | FIX_ROUTES
    # anything but "1" is review mode: a typo never opens the editors
    for value in ("0", "", "true", "yes"):
        assert _writes_of(_load_app(monkeypatch, value).app) == MARK_ROUTE, value


def test_the_one_write_check_is_pinned_by_mutation(env, monkeypatch):
    """MUTATION: an app that registers the fix router whatever the flag (the `if FIX:` dropped)
    fails `test_review_mode_serves_one_write_a_mark`'s check."""
    mod = _load_app(monkeypatch, "0")
    mod.register_fix_routes(mod.app)
    assert _writes_of(mod.app) != MARK_ROUTE


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_a_fix_request_in_review_mode_is_refused_and_writes_nothing(env):
    """Every editor's request, sent to a review-mode server, is refused by the router (404/405) and
    leaves every byte of the catalogue, splits.json, the marks store and the backups as it was."""
    c = env["client"]
    root = Path(env["reuse"].SPLITS_JSON_PATH).parent
    before = _snapshot(root)
    eid, sid = "r1:adam_river_except_eve_river@1-10", "adam_river__hwy_19_bridge"
    sent = [c.post("/api/check", json={"entry": {}}),
            c.put(f"/api/entries/{eid}", json={"region": "1", "entry": {"entry_id": eid}}),
            c.put(f"/api/splits/{sid}", json={"patch": {"label": "x"}}),
            c.post("/api/splits", json={"split": {"id": "x"}, "waterbody": "x"}),
            c.delete(f"/api/splits/{sid}"),
            c.post(f"/api/splits/{sid}/rename", json={"new_id": "y"})]
    assert [r.status_code in (404, 405) for r in sent] == [True] * len(FIX_ROUTES)
    assert _snapshot(root) == before


def test_a_mark_without_a_verdict_takes_the_answers_verdict():
    """The verdict follows from the answers (`marks.verdict_from_answers`, PLAN §2) — the review screen sends
    none; the model refuses any other."""
    from pipeline.regs.review.marks import verdict_from_answers
    got = {k: verdict_from_answers("entry", a) for k, a in {
        "all y": {"T": "y", "R": "y", "P": "y", "A": "y"},
        "an n": {"T": "y", "R": "n", "P": "y", "A": "y"},
        "n beats ?": {"T": "?", "R": "n"},
        "a ?": {"T": "y", "R": "?", "P": "y", "A": "y"},
        "unfinished": {"T": "y", "R": "y"},
        "none": {}}.items()}
    assert got == {"all y": "verified", "an n": "wrong", "n beats ?": "wrong", "a ?": "unsure",
                   "unfinished": None, "none": None}
    assert verdict_from_answers("split", {"P": "y"}) == "verified"
    assert verdict_from_answers("split", {"P": "?"}) == "unsure"


def _verdict_agrees_with_the_model(verdict_from_answers) -> list:
    """Every answer combination: the model accepts `verdict_from_answers`'s verdict and no other derived one.
    Returns the disagreements (empty when the two are one rule)."""
    import itertools
    from pydantic import ValidationError
    from pipeline.regs.review.marks import CHECKS, Mark
    prints = {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}
    bad = []
    for combo in itertools.product([None, "y", "n", "?"], repeat=4):
        answers = {k: v for k, v in zip(CHECKS["entry"], combo) if v}
        want = verdict_from_answers("entry", answers)
        for verdict in ("verified", "wrong", "unsure"):
            try:
                Mark.model_validate({"scope": "entry", "target": "e", "verdict": verdict,
                                     "answers": answers, "problem": "p", "note": "n",
                                     "reviewer": "r", "date": "2026-10-23",
                                     "pdf_sha256": "0" * 64, "prints": prints})
                ok = True
            except ValidationError:
                ok = False
            if ok != (verdict == want):
                bad.append((answers, verdict, want, ok))
    return bad


def test_verdict_from_answers_and_the_mark_model_are_one_rule():
    from pipeline.regs.review.marks import verdict_from_answers
    assert _verdict_agrees_with_the_model(verdict_from_answers) == []


def test_the_verdict_rule_is_pinned_by_mutation():
    """MUTATION: a `verdict_from_answers` that lets a ? through as verified (or an n as unsure) disagrees with
    the model — `test_verdict_from_answers_and_the_mark_model_are_one_rule` fails on it."""
    from pipeline.regs.review.marks import CHECKS

    def lenient(scope, answers):
        got = [answers.get(k) for k in CHECKS[scope]]
        return "verified" if all(got) and "n" not in got else ("wrong" if "n" in got else None)

    def soft(scope, answers):
        got = [answers.get(k) for k in CHECKS[scope]]
        return "unsure" if "n" in got or "?" in got else ("verified" if all(got) else None)

    assert _verdict_agrees_with_the_model(lenient) != []
    assert _verdict_agrees_with_the_model(soft) != []
