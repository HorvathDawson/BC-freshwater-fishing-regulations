"""T230: the review app's In-season tab — the feed's rows with their match state, and the ONE non-mark write
(owner 2026-10-28, AGENTS 66): POST /api/inseason/matches edits the in-season matches file through
`writes.commit`, with a history note per edit. Runs against a TEMP feed + TEMP matches file + TEMP backups;
nothing under data/curated changes."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "dfo", "review", "evidence",
         "inseason")

COWICHAN = "gnis:12227"
TRESTLE = "cowichan_river__greendale_trestle"
LIVE, OPEN, GONE, STALE = "is-aaaaaaaaaaa1", "is-aaaaaaaaaaa2", "is-aaaaaaaaaaa3", "is-aaaaaaaaaaa9"


def _row(i, water, frm, to, status="unchanged"):
    return {"id": i, "region": "1", "section": "Region 1", "water": water, "effective": f"{frm} to {to}",
            "from": frm, "to": to, "change": "No fishing", "parsed": {"kinds": ["closure"]}, "status": status,
            "firstSeen": "2026-10-28T12:00:00.000Z", "changedAt": "2026-10-28T12:00:00.000Z"}


FEED = {"format": "inseason-feed/1", "source": "https://example.invalid/inseason",
        "fetchedAt": "2026-10-28T12:00:00.000Z",
        "rows": [_row(LIVE, "Cowichan River", "2025-07-04", "2025-08-31"),
                 _row(OPEN, "Cowichan River", "2020-01-01", None, status="new")],
        "removed": [{"id": GONE, "region": "1", "water": "Koksilah River", "change": "No fishing",
                     "effective": "x", "removedAt": "2026-10-20T00:00:00.000Z"}]}
MATCHES = {"$comment": "test", "format": "inseason-matches/1", "history": [],
           "matches": {STALE: {"region": "1", "water": "Gone Creek", "note": "old", "by": "t", "at": "x",
                               "extents": [{"op": "whole", "item_id": COWICHAN, "splits": []}]}}}


def _stamps(root: Path) -> dict[str, str]:
    """Content digests (not mtimes: the owner's live review app may re-save the real marks store unchanged)."""
    import hashlib
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("inseason")
    (tmp / "feed.json").write_text(json.dumps(FEED))
    (tmp / "matches.json").write_text(json.dumps(MATCHES))
    (tmp / "marks.jsonl").write_text("")
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_INSEASON_FEED", str(tmp / "feed.json"))
    mp.setenv("CURATION_INSEASON_MATCHES", str(tmp / "matches.json"))
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    mp.setenv("CURATION_BACKUP_DIR", str(tmp / "backups"))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    curated = _stamps(CURATED.base)
    yield {"client": TestClient(app_mod.app), "dir": tmp}
    assert _stamps(CURATED.base) == curated, "an in-season route wrote under data/curated"
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _q(env):
    got = env["client"].get("/api/inseason/queue")
    assert got.status_code == 200, got.text
    return got.json()


def _post(env, **body):
    return env["client"].post("/api/inseason/matches", json={"note": "n", "by": "tester", **body})


def test_1_the_queue_lists_every_row_with_its_state(env):
    q = _q(env)
    by = {r["id"]: r for r in q["rows"]}
    assert set(by) == {LIVE, OPEN, GONE, STALE}
    assert (by[LIVE]["state"], by[OPEN]["state"], by[GONE]["state"], by[STALE]["state"]) == \
        ("unmatched", "unmatched", "unmatched", "stale")
    assert by[GONE]["removed"] and by[GONE]["status"] == "removed"
    assert by[LIVE]["in_effect"] is False and by[OPEN]["in_effect"] is True    # `to` null = open-ended
    assert by[STALE]["match"]["extents"][0]["item_name"] == "Cowichan River"
    assert q["progress"] == {"matched": 0, "auto": 0, "unmatched": 2, "total": 2, "removed": 1, "stale": 1}
    assert q["source"] == FEED["source"]


def test_2_a_good_save_writes_the_match_and_its_history(env):
    r = _post(env, id=LIVE, extents=[{"op": "upstream_of", "item_id": COWICHAN, "splits": [TRESTLE]}],
              effective={"from": None, "to": "2099-12-31"}, note="the notice names the trestle")
    assert r.status_code == 200, r.text
    got = r.json()
    # the curated dates override the feed's field by field: the feed's `from`, the curated `to`
    assert got["state"] == "matched" and got["in_effect"] is True and got["dates"] == \
        {"from": "2025-07-04", "to": "2099-12-31", "source": "curated"}
    doc = json.loads((env["dir"] / "matches.json").read_text())
    m = doc["matches"][LIVE]
    assert m["extents"] == [{"op": "upstream_of", "item_id": COWICHAN, "splits": [TRESTLE]}]
    assert (m["water"], m["note"], m["by"], m["effective"]) == \
        ("Cowichan River", "the notice names the trestle", "tester", {"to": "2099-12-31"})
    assert [(h["id"], h["action"], h["note"]) for h in doc["history"]] == \
        [(LIVE, "set", "the notice names the trestle")]
    snaps = sorted((env["dir"] / "backups").iterdir())
    assert len(snaps) == 1 and snaps[0].name.endswith("-inseason-match")
    assert json.loads((snaps[0] / env["dir"].name / "matches.json").read_text()) == MATCHES   # the old bytes
    assert _q(env)["progress"]["matched"] == 1


@pytest.mark.parametrize("body,words", [
    ({"id": "is-ffffffffffff", "extents": [{"op": "whole", "item_id": COWICHAN}]}, "no such in-season row"),
    ({"id": STALE, "extents": [{"op": "whole", "item_id": COWICHAN}]}, "no such in-season row"),
    ({"id": OPEN, "extents": [{"op": "upstream_of", "item_id": COWICHAN, "splits": ["no_such_split"]}]},
     "unknown split"),
    ({"id": OPEN, "extents": [{"op": "between", "item_id": COWICHAN, "splits": [TRESTLE]}]}, "names 2 split"),
    ({"id": OPEN, "extents": [{"op": "whole", "item_id": "gnis:999999999"}]}, "no such water"),
    # a split on another water: the extent binds nothing on the Cowichan
    ({"id": OPEN, "extents": [{"op": "upstream_of", "item_id": COWICHAN, "splits": ["adam_river__hwy_19_bridge"]}]},
     "binds no section"),
    ({"id": OPEN, "extents": [{"op": "whole", "item_id": COWICHAN}], "note": "  "}, "note is required"),
    ({"id": OPEN, "extents": None}, "not matched"),
])
def test_3_a_bad_save_is_refused_and_writes_nothing(env, body, words):
    before = (env["dir"] / "matches.json").read_bytes()
    n = len(list((env["dir"] / "backups").iterdir()))
    r = _post(env, **body)
    assert r.status_code == 400 and words in r.json()["detail"], r.text
    assert (env["dir"] / "matches.json").read_bytes() == before
    assert len(list((env["dir"] / "backups").iterdir())) == n


def test_4_clear_removes_the_match_and_appends_history(env):
    r = _post(env, id=LIVE, extents=[], note="wrong water")
    assert r.status_code == 200 and r.json()["state"] == "unmatched", r.text
    doc = json.loads((env["dir"] / "matches.json").read_text())
    assert LIVE not in doc["matches"] and STALE in doc["matches"]
    assert [(h["action"], h["note"]) for h in doc["history"]] == [("set", "the notice names the trestle"),
                                                                  ("clear", "wrong water")]
    row = next(x for x in _q(env)["rows"] if x["id"] == LIVE)
    assert [h["action"] for h in row["history"]] == ["set", "clear"]


def test_5_the_preview_and_the_picker_read_only(env):
    c = env["client"]
    before = (env["dir"] / "matches.json").read_bytes()
    p = c.get("/api/inseason/extent-preview",
              params={"extent": json.dumps({"op": "whole", "item_id": COWICHAN})}).json()
    assert p["resolvable"] and p["n_sections"] > 0 and p["geojson"]["features"]
    assert c.get("/api/inseason/extent-preview", params={"extent": "[{\"op\": \"nope\"}]"}).status_code == 422
    s = c.get(f"/api/inseason/items/{COWICHAN}/splits").json()
    assert TRESTLE in {f["properties"]["split_id"] for f in s["splits"]["features"]}
    assert (env["dir"] / "matches.json").read_bytes() == before


def test_6_no_feed_says_so(env, monkeypatch):
    monkeypatch.setenv("CURATION_INSEASON_FEED", str(env["dir"] / "absent.json"))
    q = _q(env)
    assert q["available"] is False and "no in-season feed" in q["message"]
    assert _post(env, id=OPEN, extents=[{"op": "whole", "item_id": COWICHAN}]).status_code == 400
