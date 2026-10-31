"""T325 / T329: one apply command for every open review domain; lock = close a FINISHED domain (marks never edited).

  apply     a new DFO extent mark reaches its curated DFO file through `python -m pipeline.regs.review.apply dfo`
            (export, then the domain's writer — apply_proposals, one commit, in a temp git repo)
  refuse    `lock` refuses while the domain has an unsure mark, an unmarked queue item or a waiting proposal
  closed    a closed domain is not exported (its decided file is removed), not applied, and the review app answers
            423 "locked" on its queue and refuses a mark on its scopes
  reopen    removing the entry restores it: exported again, served again, marks taken again
  ponds     T329: the curated closed list closes ponds; decided/ponds.json is gone

Every store, decided dir and closed list is a temp copy (a closed list lives beside its marks store).
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.review import apply as AP
from pipeline.regs.review import closed as C
from pipeline.regs.review import export as E
from pipeline.regs.review import lock as L
from pipeline.regs.review import marks as M
from pipeline.regs.review import proposals as P

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
SHA = "0" * 64
COWICHAN = "1:cowichan-river:from-66-mile-trestle-downstream-to"


def _pond(pid: str, verdict="verified", call="pool") -> M.Mark:
    return M.Mark.model_validate({
        "scope": "q154", "target": pid, "verdict": verdict, "call": call if verdict == "verified" else None,
        "answers": {"P": "y" if verdict == "verified" else "?"}, "reviewer": "dawson", "date": "2026-10-29",
        "pdf_sha256": SHA, "prints": {"text": None, "records": [{"key": f"q154:{pid}", "meaning": "m", "binding": "b"}]}})


def _store(tmp: Path, marks) -> Path:
    p = tmp / "marks.jsonl"
    p.write_text("".join(M.line(m) for m in marks))
    return p


def _lock(store: Path, decided: Path, *, left=0, waiting=()):
    return L.lock("ponds", marks=store, decided_dir=decided, queue_left=lambda _m: left,
                  waiting=lambda _d, _dd: list(waiting), today="2026-10-30")


# --------------------------------------------------------------------------- lock refuses while open

def test_lock_refuses_while_the_domain_is_open(tmp_path, capsys):
    store = _store(tmp_path, [_pond("lake:1"), _pond("lake:2", verdict="unsure")])
    decided = tmp_path / "decided"
    E.main(["--marks", str(store), "--out", str(decided)])
    assert _lock(store, decided) == 1                                    # an unsure mark in force
    assert "unsure / wrong" in capsys.readouterr().out
    store = _store(tmp_path, [_pond("lake:1"), _pond("lake:2")])
    E.main(["--marks", str(store), "--out", str(decided)])
    assert _lock(store, decided, left=3) == 1                            # queue items not marked yet
    assert "3 queue item(s) not marked" in capsys.readouterr().out
    assert _lock(store, decided, waiting=["mark-q154-x"]) == 1          # a proposal still waiting
    assert not C.path(store).exists() and (decided / "ponds.json").exists()
    # tidal has no final-file writer yet (T326): refused whatever the marks say
    assert L.lock("tidal", marks=store, decided_dir=decided, queue_left=lambda _m: 0,
                  waiting=lambda _d, _dd: []) == 1


# --------------------------------------------------------------------------- closed: skipped; reopen restores

@pytest.fixture()
def app_env(tmp_path):
    """The review app over a temp marks store (its closed list beside it) and a 2-pond sample."""
    sample = tmp_path / "sample.json"
    row = {"set": "example", "n": 1, "stratum": "caught", "why": "w", "region": "7", "river": None, "order": 3,
           "ha": 0.2, "avg_width_m": 10.0, "length_m": 50, "elongation": 2.0, "river_width_m": 8.0, "ratio": 1.25,
           "man_made": False, "caught": True, "md_read": None, "lat": 55.0, "lon": -125.0,
           "rings": [[[-125, 55], [-125.001, 55.001], [-125, 55]]], "lines": []}
    sample.write_text(json.dumps({"cutoff": {"max_ha": 1.0}, "rows": [{**row, "id": "lake:1"}, {**row, "id": "lake:2"}]}))
    store = _store(tmp_path, [_pond("lake:1"), _pond("lake:2")])
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_Q154_SAMPLE", str(sample))
    mp.setenv("CURATION_MARKS", str(store))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "ponds", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import q154 as Q
    Q._load.cache_clear()
    import app as app_mod                                                  # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "store": store, "decided": tmp_path / "decided"}
    mp.undo()
    for mod in ("app", "reuse", "ponds", "writes", "review"):
        sys.modules.pop(mod, None)


def test_a_closed_domain_is_skipped_and_reopening_restores_it(app_env):
    c, store, decided = app_env["client"], app_env["store"], app_env["decided"]
    E.main(["--marks", str(store), "--out", str(decided)])
    assert (decided / "ponds.json").exists()
    assert c.get("/api/q154/queue").status_code == 200 and c.get("/api/review/closed").json() == {"closed": {}}
    # lock: finished -> closed; marks.jsonl untouched; decided/ponds.json removed
    before = store.read_bytes()
    assert _lock(store, decided) == 0
    assert store.read_bytes() == before
    assert C.domains(C.path(store)) == {"ponds"} and not (decided / "ponds.json").exists()
    # the exporter skips it (nothing written, --check clean, load does not ask for it)
    E.main(["--marks", str(store), "--out", str(decided)])
    assert not (decided / "ponds.json").exists() and E.stale(decided, store) == []
    assert not E.load(decided, closed={"ponds"}).scope("q154")
    # the apply command skips it
    assert AP.main(["ponds", "--marks", str(store), "--decided", str(decided)]) == 0
    # the app: 423 locked on its queue / tally / pond, a mark refused, the closed list served
    for url in ("/api/q154/queue", "/api/q154/tally", "/api/q154/ponds/lake:1"):
        r = c.get(url)
        assert r.status_code == 423 and r.json()["locked"] == "ponds", url
    got = c.post("/api/marks", json={"scope": "q154", "target": "lake:1", "call": "real lake",
                                     "answers": {"P": "y"}, "reviewer": "test"})
    assert got.status_code == 423 and "lock --reopen ponds" in json.dumps(got.json())
    assert c.get("/api/review/closed").json()["closed"]["ponds"]["scopes"] == ["q154"]
    assert store.read_bytes() == before
    # a closed file left behind is reported by --check
    (decided / "ponds.json").write_text("{}")
    assert E.stale(decided, store) == ["ponds.json (closed: remove it)"]
    (decided / "ponds.json").unlink()
    # reopen: remove the entry -> served, exported and markable again
    assert L.reopen("ponds", C.path(store)) == 0
    assert C.domains(C.path(store)) == frozenset()
    assert c.get("/api/q154/queue").status_code == 200
    E.main(["--marks", str(store), "--out", str(decided)])
    assert len(json.loads((decided / "ponds.json").read_text())["decisions"]) == 2


def test_closing_one_domain_moves_no_byte_of_another(tmp_path):
    store = _store(tmp_path, [_pond("lake:1")])
    d = E.decide(M.load(store), tidal_decisions=None)
    a, b = E.documents(d), E.documents(d, {"ponds"})
    assert set(a) - set(b) == {"ponds.json"} and all(a[n] == b[n] for n in b)


# --------------------------------------------------------------------------- T329: ponds closed for real

def test_ponds_are_closed_in_the_curated_list_and_not_exported():
    got = C.entries()
    assert got["ponds"]["scopes"] == ["q154"] and got["ponds"]["final"] is None
    assert not (Path(CURATED.decided) / "ponds.json").exists()
    assert C.path() == Path(CURATED.regulations.review.marks).parent / "closed.json"


# --------------------------------------------------------------------------- apply: a new DFO extent mark lands

def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, capture_output=True, text=True, check=True).stdout


def test_a_new_dfo_extent_mark_reaches_its_curated_file_through_apply(tmp_path, monkeypatch):
    dfo, splits = tmp_path / "data/curated/dfo_salmon", tmp_path / "data/curated/splits.json"
    dfo.mkdir(parents=True)
    (tmp_path / "data/curated/catalogue").mkdir(parents=True)
    region1 = Path(CURATED.regulations.entries.dfo_salmon) / "region-1.json"
    (dfo / "region-1.json").write_bytes(region1.read_bytes())
    splits.write_bytes(Path(CURATED.waters.splits).read_bytes())
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "t")
    _git(tmp_path, "add", "data")
    _git(tmp_path, "commit", "-qm", "base")
    # the owner's one click: a DFO mark whose extent is "downstream of the CNR trestle" (an existing cut)
    fix = [{"scope": "entry", "op": "downstream_of", "items": ["gnis:12227"],
            "points": [{"kind": "split", "split_id": "cowichan_river__cnr_trestle"}]}]
    mark = M.Mark.model_validate({
        "scope": "dfo", "target": COWICHAN, "verdict": "verified", "answers": {"T": "y", "R": "y", "P": "y"},
        "reviewer": "dawson", "date": "2026-10-30", "pdf_sha256": SHA, "extent_fix": fix,
        "prints": {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}})
    store = tmp_path / "review" / "marks.jsonl"
    store.parent.mkdir()
    store.write_text(M.line(mark))
    def live(cls, build=None):                                              # the files as they are NOW
        return P.Sources(entries_dir=tmp_path / "data/curated/catalogue", dfo_dir=dfo, splits_path=splits,
                         build=tmp_path / "build")
    monkeypatch.setattr(P, "REPO", tmp_path)
    monkeypatch.setattr(P, "check", lambda ps, _s: [p.update(check={"cheap": True, "rules": {}}) for p in ps])
    monkeypatch.setattr(P, "side_ready", lambda p: False)
    monkeypatch.setattr(P.Sources, "live", classmethod(live))
    monkeypatch.setattr(P, "TIDAL_DECISIONS", tmp_path / "none.json")     # no pre-Tab tidal calls in a test
    decided = tmp_path / "review" / "decided"
    assert AP.main(["dfo", "--marks", str(store), "--decided", str(decided)]) == 0
    loc = next(x for x in json.loads((dfo / "region-1.json").read_text())["locations"]
               if x["location_id"] == COWICHAN)
    assert loc["binding"]["extents"] == [{"op": "downstream_of", "splits": ["cowichan_river__cnr_trestle"],
                                          "item_id": "gnis:12227"}]
    log = _git(tmp_path, "log", "--format=%s").splitlines()
    assert len(log) == 2 and log[0].startswith("T185 apply mark-dfo-")
    assert _git(tmp_path, "status", "--porcelain", "--", "data") == ""
    assert json.loads((decided / "dfo.json").read_text())["decisions"][0]["key"][1] == COWICHAN
    # again: nothing left to land
    assert AP.main(["dfo", "--marks", str(store), "--decided", str(decided)]) == 0
    assert len(_git(tmp_path, "log", "--format=%s").splitlines()) == 2
