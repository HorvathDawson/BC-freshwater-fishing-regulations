"""The curation-review tool is SAFE to sit a reviewer in front of (T96, curation-review/PLAN.md Phase 0).

  no process    no endpoint starts a subprocess: the in-app "Rebuild graph" / "Promote" started an
                18-minute, 9 GB atlas build outside `scripts/heavy.sh`, beside the review buttons
  not exposed   Vite binds 127.0.0.1 only and the backend's CORS admits only the Vite origin (it
                was `host: true` and CORS `*`, with no authentication, on an editor that writes the
                curated catalogue)
  split notes   each split's curator note (`_note`) and auto-proposal grade reach the frontend: 151
                cut-points are still auto-proposals and the tool hid every word of that
  one write     the app serves reads and one write, a mark; no backend module but `writes.py`
                writes a file, and its one caller is the mark (T185: the fix-mode writers are
                retired) — and ONE exception (T230, owner 2026-10-28): `POST /api/inseason/matches`,
                the in-season matches file (Claude/owner tooling, not a mark), through the same
                `writes.commit`

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
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo",
                "review", "evidence", "inseason")


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
    r = env["client"].options("/api/marks", headers={
        "Origin": origin, "Access-Control-Request-Method": "POST"})
    assert r.headers.get("access-control-allow-origin") == allowed
    got = env["client"].get("/api/synopsis/pages", headers={"Origin": origin})
    assert got.headers.get("access-control-allow-origin") == allowed


def test_cors_offers_no_write_method_but_the_mark_s():
    """The browser is told GET and POST (the mark) only — PUT and DELETE were the editors'."""
    src = (BACKEND / "app.py").read_text(encoding="utf-8")
    assert 'allow_methods=["GET", "POST"]' in src


# --------------------------------------------------------------------------- split notes

def test_a_split_carries_its_curator_note_and_grade(env):
    """`reuse.split_review` — what an entry's boundaries carry as `live.review` (`bindable`): the
    `_note` the curator wrote and the auto-proposal grade read from it (`[auto-proposal L]`), so
    the review screen shows what still needs checking on a cut-point an entry's rules name."""
    sid = "adam_river__hwy_19_bridge"
    raw = next(sp for wb in json.loads(CURATED.waters.splits.read_text())["waterbodies"]
               for sp in wb["splits"] if sp["id"] == sid)
    assert env["reuse"].split_review(raw) == {"note": raw["_note"], "grade": "L"}


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


# --------------------------------------------------------------------------- one write (T134, T185)

#: THE ONE WRITE the app serves: a mark. The editors' routes (entry save and the draft check,
#: split add / move / delete / rename — the retired fix mode) are gone, not hidden behind a flag.
MARK_ROUTE = {("POST", "/api/marks")}
#: ... and THE ONE EXCEPTION (T230, owner 2026-10-28: "this file is Claude/owner tooling, not marks; keep a
#: history note per edit"): the in-season matches file, data/curated/inseason/matches.json
INSEASON_ROUTE = {("POST", "/api/inseason/matches")}
WRITE_ROUTES = MARK_ROUTE | INSEASON_ROUTE
#: what the retired editors were asked, so a request to one is shown refused
RETIRED = (("POST", "/api/check"), ("PUT", "/api/entries/{entry_id}"),
           ("PUT", "/api/splits/{split_id}"), ("POST", "/api/splits"),
           ("DELETE", "/api/splits/{split_id}"), ("POST", "/api/splits/{split_id}/rename"))


def _writes_of(fastapi_app) -> set[tuple[str, str]]:
    """Every (method, path) the app answers that is not a read."""
    return {(m, r.path) for r in fastapi_app.routes for m in getattr(r, "methods", None) or ()
            if m not in ("GET", "HEAD")}


def _load_app(monkeypatch, env_value: str | None):
    import importlib.util
    if env_value is None:
        monkeypatch.delenv("CURATION_FIX", raising=False)
    else:
        monkeypatch.setenv("CURATION_FIX", env_value)
    spec = importlib.util.spec_from_file_location("app_mode_probe", BACKEND / "app.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_the_app_serves_one_write_a_mark(env):
    """Reads and POST /api/marks — nothing else writes (PLAN §4 phase 3, T185) — but the T230 in-season
    matches route."""
    assert _writes_of(env["app"].app) == WRITE_ROUTES


def test_no_switch_opens_a_writer(env, monkeypatch):
    """The old fix-mode switch (`CURATION_FIX=1`, `run.sh --fix`) opens nothing: there is no
    second mode, and neither run script offers one."""
    for value in (None, "0", "1", "true"):
        mod = _load_app(monkeypatch, value)
        assert _writes_of(mod.app) == WRITE_ROUTES, value
        assert not hasattr(mod, "FIX") and not hasattr(mod, "register_fix_routes")
    for sh in (TOOL / "run.sh", BACKEND / "run.sh"):
        assert "--fix" not in sh.read_text(encoding="utf-8") and \
            "CURATION_FIX" not in sh.read_text(encoding="utf-8"), sh


def test_the_one_write_check_is_pinned_by_mutation(env, monkeypatch):
    """MUTATION: an app with any further write route fails `test_the_app_serves_one_write_a_mark`'s
    check."""
    mod = _load_app(monkeypatch, None)

    @mod.app.put("/api/entries/{entry_id}")
    def _a_writer(entry_id: str):                  # pragma: no cover — never called
        return {}
    assert _writes_of(mod.app) != WRITE_ROUTES


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_a_retired_editor_request_is_refused_and_writes_nothing(env):
    """Every retired editor's request is refused by the router (404/405) and leaves every byte of
    the catalogue, splits.json, the marks store and the backups as it was."""
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
    assert [r.status_code in (404, 405) for r in sent] == [True] * len(RETIRED)
    assert _snapshot(root) == before


#: the calls that write a file — `writes.py` is the one module that may make them. By name
#: wherever they are called (a Path's, a module's) ...
_FILE_WRITERS = {"write_text", "write_bytes", "atomic_write", "copy2", "copyfile", "copytree",
                 "rmtree", "mkdir", "makedirs", "touch", "unlink", "write_entryfile", "mkstemp"}
#: ... and these only on a module that writes with them (`str.replace`, `list.append` are not)
_MODULE_WRITERS = {"os": {"replace", "rename", "remove"}, "shutil": {"copy", "move"},
                   "writes": {"commit", "backup"}, "M": {"append"}, "marks": {"append"},
                   "io": {"write_entryfile", "atomic_write"}}


def _write_calls(src: str) -> list[tuple[str, str]]:
    """(enclosing top-level function, called name) for each call of a file writer in `src`, and
    for each `open(..., "w"/"a"/"x")`."""
    out = []
    for fn in ast.parse(src).body:
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for n in ast.walk(fn):
            if not isinstance(n, ast.Call):
                continue
            f = n.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            owner = f.value.id if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) \
                else None
            if name in _FILE_WRITERS or name in _MODULE_WRITERS.get(owner, ()):
                out.append((fn.name, name))
            elif name == "open" and any(isinstance(a, ast.Constant) and isinstance(a.value, str)
                                        and set(a.value) & set("wax") for a in n.args[1:]):
                out.append((fn.name, "open"))
    return out


def test_the_only_file_write_is_the_mark_s_commit():
    """THE PIN (T185): no backend module but `writes.py` writes a file, and the one call into it
    is `reuse.post_mark`'s `writes.commit` of the marks store. Every editor's writer — entry save,
    split save / add / delete / rename — called `writes.commit` on the catalogue or splits.json."""
    got = {py.name: _write_calls(py.read_text(encoding="utf-8"))
           for py in sorted(BACKEND.glob("*.py")) if py.name != "writes.py"}
    assert {k: v for k, v in got.items() if v} == {"reuse.py": [("post_mark", "commit")],
                                                    "inseason.py": [("save_match", "commit")]}
    src = (BACKEND / "reuse.py").read_text(encoding="utf-8")
    # T55: a slough batch ("accept all visible") is several marks in that same one commit
    assert "writes.commit([(MARKS_PATH, M.appended(MARKS_PATH, marks))], \"mark\")" in src
    assert "marks = batch or [mark]" in src
    # T230: the in-season write names exactly the matches file, as one commit
    ins = (BACKEND / "inseason.py").read_text(encoding="utf-8")
    assert ins.count("writes.commit(") == 1 and 'writes.commit([(path, text)], "inseason-match")' in ins
    assert "path = matches_path()" in ins and "with writes.LOCK:" in ins


def test_the_write_pin_is_pinned_by_mutation():
    """MUTATION: a module that writes the catalogue — the retired `save_entry` shape — is seen."""
    src = ("def save_entry(region, e):\n"
           "    writes.commit([(ENTRIES_DIR / 'region-1.json', text)], 'save-entry')\n"
           "def note(p):\n"
           "    with open(p, 'w') as f:\n"
           "        f.write('x')\n")
    assert _write_calls(src) == [("save_entry", "commit"), ("note", "open")]


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
