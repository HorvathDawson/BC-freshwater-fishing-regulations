"""A tidal mark on a water the atlas registry does not carry (Nelson Creek, Port Coquitlam: its FWA line is
replaced by the municipal added-streams network) — the review app accepts it AT THE MOUTH only (a mouth means
no cut and nothing tidal, so no registry item is needed), stamped at the FWA mouth with no `on_item`; any other
point keeps the refusal "not in the atlas registry: nothing to cut". Temp candidates + marks store."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from pipeline.tests import under_test
from pipeline.tests.conftest import ATLAS_HINT, curated_root, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
CANDIDATES = under_test.generated().review / "tidal" / "tidal-candidates.json"
NELSON = "tidal:356013125"

pytestmark = pytest.mark.needs_atlas


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", under_test.atlas(request) / "registry.json", ATLAS_HINT)
    tmp = tmp_path_factory.mktemp("tidal_unreg")
    rows = [r for r in json.loads(CANDIDATES.read_text())["candidates"] if r["id"] == NELSON]
    assert rows and rows[0]["items"] == [], f"{NELSON} is in {CANDIDATES} with no registry item"
    (tmp / "tidal-candidates.json").write_text(json.dumps({"coverage": {}, "candidates": rows}))
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_TIDAL_CANDIDATES", str(tmp / "tidal-candidates.json"))
    curated_root(mp, tmp / "curated")                         # T422: the whole curated tree is a temp copy
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "dfo", "tidal", "writes", "review"):
        sys.modules.pop(mod, None)
    from pipeline.regs.review import tidal as T
    T._load.cache_clear()
    import app as app_mod                                     # noqa: E402
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp, "row": rows[0], "marks": Path(under_test.curated().review.marks),
           "splits": Path(under_test.curated().waters.cuts)}
    mp.undo()
    T._load.cache_clear()


def _body(row, coord, note):
    return {"scope": "tidal", "target": NELSON, "answers": {"P": "y"}, "reviewer": "test", "note": "",
            "new_splits": [{"ref": f"tidal-{row['blk']}", "anchor": "coord", "on_item": None,
                            "name": "Nelson Creek: mouth (tidal boundary)", "coord": coord, "note": note}]}


def test_a_point_above_the_mouth_is_still_refused(env):
    c, row = env["client"], env["row"]
    r = c.post("/api/marks", json=_body(row, row["proposed"]["coord"], "chosen point: polygon"))
    assert r.status_code == 422 and "not in the atlas registry: nothing to cut" in r.text
    assert env["marks"].read_text() == ""


def test_the_mouth_is_recorded_as_the_mouth_with_no_cut(env):
    c, row = env["client"], env["row"]
    r = c.post("/api/marks", json=_body(row, row["mouth"]["coord"], "chosen point: mouth — FWA mouth"))
    assert r.status_code == 200, r.text
    [ns] = r.json()["new_splits"]
    assert ns["blk"] == row["blk"] and ns["measure"] == row["mouth"]["measure"] and ns["on_item"] is None
    assert "not in the atlas registry — no cut, nothing tidal" in ns["note"]
    assert len(env["marks"].read_text().splitlines()) == 1
    # T326: the tidal writer makes it the water's `mouth` row — no cut, never a split proposal
    from pipeline.regs.review import export as E, marks as M, proposals as P
    from pipeline.tools import tidal_limits as TL
    src = P.Sources(entries_dir=env["dir"], dfo_dir=env["dir"], splits_path=env["splits"])
    assert P.build(M.load(env["marks"]), src) == []
    [w], _ = TL.build(E.decide(M.load(env["marks"])), src, existing=[])
    assert w.blk == row["blk"] and w.limit == "mouth"
