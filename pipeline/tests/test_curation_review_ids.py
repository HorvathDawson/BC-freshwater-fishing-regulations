"""T414 (in T239): the review app's "only these entries" list. `synopsis_diff --ids-out FILE` writes the
yearly review list; `GET /api/review/ids?path=FILE` reads it, says the progress over the list, and names
every id no entry has — shown, never dropped. A file it cannot read is refused with the reason, never an
empty list. Read only (the safety pins hold: a GET)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from pipeline.tests.conftest import curated_root

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "review", "evidence", "idlist")

ROWS = [{"entry_id": "r1:a@1-1", "verify": "verified"}, {"entry_id": "r1:b@1-2", "verify": "wrong"},
        {"entry_id": "r2:c@2-1", "verify": "stale"}, {"entry_id": "r2:d@2-2", "verify": "unmarked"},
        {"entry_id": "r3:e@3-1", "verify": "verified"}]
DONE = ("verified", "wrong", "unsure", "deferred")


@pytest.fixture()
def idlist(monkeypatch):
    monkeypatch.syspath_prepend(str(BACKEND))
    sys.modules.pop("idlist", None)
    import idlist as mod
    yield mod
    sys.modules.pop("idlist", None)


def test_the_list_s_progress_and_its_unknown_ids(idlist, tmp_path):
    f = tmp_path / "review_ids.txt"
    f.write_text("# the 2027 book\nr1:a@1-1\n\nr1:b@1-2\nr9:gone@9-9\n r2:c@2-1 \nr2:d@2-2\nr1:a@1-1\nr0:new@0-0\n")
    v = idlist.view("review_ids.txt", tmp_path, ROWS, DONE)
    assert v["ids"] == ["r1:a@1-1", "r1:b@1-2", "r9:gone@9-9", "r2:c@2-1", "r2:d@2-2", "r0:new@0-0"]
    assert (v["total"], v["found"], v["reviewed"], v["verified"], v["repeats"]) == (6, 4, 2, 1, 1)
    assert v["missing"] == ["r9:gone@9-9", "r0:new@0-0"]          # shown, in the file's order
    assert v["path"] == "review_ids.txt"
    # a mark gone stale is to do again; an entry outside the list never counts
    assert "r3:e@3-1" not in v["ids"]


def test_an_unreadable_list_is_refused_never_empty(idlist, tmp_path):
    (tmp_path / "repo").mkdir()
    (tmp_path / "outside.txt").write_text("r1:a@1-1\n")
    root = tmp_path / "repo"
    for path, why in (("", "no ids file"), ("nope.txt", "no such file"),
                      ("../outside.txt", "inside the repo"), (str(tmp_path / "outside.txt"), "inside the repo")):
        with pytest.raises(idlist.Refused, match=why):
            idlist.view(path, root, ROWS, DONE)
    (root / "empty.txt").write_text("\n# nothing\n")
    assert idlist.view(str(root / "empty.txt"), root, ROWS, DONE)["total"] == 0


def test_the_route_reads_the_list_and_says_a_refusal(monkeypatch, tmp_path):
    curated_root(monkeypatch, tmp_path / "curated")
    monkeypatch.setenv("CURATION_BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    try:
        import app as app_mod
        import reuse
        from fastapi.testclient import TestClient
        monkeypatch.setattr(reuse, "_ROOT", tmp_path)
        monkeypatch.setattr(reuse, "queue", lambda **kw: [dict(r) for r in ROWS])
        (tmp_path / "ids.txt").write_text("r2:d@2-2\nr1:b@1-2\nr7:x@7-1\n")
        c = TestClient(app_mod.app)
        got = c.get("/api/review/ids", params={"path": "ids.txt"})
        assert got.status_code == 200, got.text
        b = got.json()
        assert (b["total"], b["reviewed"], b["verified"], b["missing"]) == (3, 1, 0, ["r7:x@7-1"])
        bad = c.get("/api/review/ids", params={"path": "missing.txt"})
        assert bad.status_code == 422 and "no such file" in bad.json()["detail"][0]["msg"]
        assert {m for r in app_mod.app.routes if r.path == "/api/review/ids" for m in r.methods} == {"GET"}
    finally:
        for mod in _MODS:
            sys.modules.pop(mod, None)
