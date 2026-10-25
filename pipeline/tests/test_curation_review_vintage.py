"""Curation review phase 2 (T133) THROUGH THE REVIEW APP: one vintage on screen, with no restart.

The FastAPI app (curation-review/backend) runs with `TestClient` over a TEMP COPY of the catalogue,
of splits.json and of the marks store, the real atlas's registry and section handles, and a reach
run, bundle and release written into a temp dir the way `pipeline build` would leave them (the
reach report's and the bundle's digests, a release manifest cut by `release.build.ship`). Every
"build" below writes those files and the SAME client asks again — the app re-reads them by their
stamps, as it does when the owner's rebuild lands in a terminal.

  * an entry edit (made in its region file — a reviewed change, T185; the app writes none): that
    entry reads "awaiting build" (the pending-build panel lists it, alone, with
    the command), a verified mark is refused and a T/R-only mark taken; the reach run alone does not
    clear it; deliver does;
  * a split added mid-review (owner 2026-10-23): the entries on its water await the atlas and the
    pending-build panel lists the split, the old mark on that water reads "binding moved"; once
    the atlas holds the split (promote) the entries clear and review continues on them — the split
    itself is checked through them (T184: no split is a review target).
"""
from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.regs.parsing import io
from pipeline.regs.review import vintage as V
from pipeline.tests.conftest import ATLAS_HINT, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
SCHEMA = REPO / "pipeline" / "deliver" / "bundle" / "schema.sql"
BULL = "r4:bull_river@4-22"
ALL_Y = {"T": "y", "R": "y", "P": "y", "A": "y"}

pytestmark = pytest.mark.needs_atlas


@pytest.fixture(scope="module")
def app_env(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    tmp = tmp_path_factory.mktemp("vintage")
    cat = tmp / "catalogue"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    shutil.copy2(CURATED.waters.splits, tmp / "splits.json")
    (tmp / "marks.jsonl").write_text("")
    # the served atlas as the vintage reads it: the real registry and handles, and the split
    # record a build of the temp splits.json would write
    real = GENERATED.build()
    atlas = tmp / "atlas"
    atlas.mkdir()
    for name in ("registry.json", "section_handles.txt"):
        os.symlink(real / name, atlas / name)
    shutil.copy2(real / "splits.resolved.json", atlas / "splits.resolved.json")
    mp = pytest.MonkeyPatch()
    for k, v in {"CURATION_ENTRIES_DIR": cat,
                 "CURATION_SPLITS_JSON": tmp / "splits.json",
                 "CURATION_MARKS": tmp / "marks.jsonl", "CURATION_BACKUP_DIR": tmp / "backups",
                 "CURATION_REACH_DIR": tmp / "reach", "CURATION_RELEASE_ROOT": tmp / "release",
                 "CURATION_TILES": tmp / "tiles"}.items():
        mp.setenv(k, str(v))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes",
                "review", "evidence"):
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    from pipeline.deliver.bundle import read as R
    mp.setattr(reuse, "_BUILD", atlas)
    mp.setattr(R, "BUNDLE", str(tmp / "bundle.sqlite"))
    assert reuse.ENTRIES_DIR == cat and reuse.REACH_DIR == tmp / "reach"
    from fastapi.testclient import TestClient
    from pipeline.common.section_handles import digest_for, registry_digest_for
    env = {"client": TestClient(app_mod.app), "reuse": reuse, "tmp": tmp, "atlas": atlas,
           "cat": cat, "handles": digest_for(atlas), "registry": registry_digest_for(atlas)}
    record_atlas(env)
    run_reach(env)
    deliver(env)
    yield env
    mp.undo()


# -- the owner's rebuild, as files ------------------------------------------------------------

def _corpus(env) -> dict:
    """The entries as the reach run and the bundle read them (the corpus' one read point)."""
    return {e["entry_id"]: e for _, e in env["reuse"]._all_entries()}


def record_atlas(env) -> None:
    """The atlas holds splits.json as it is now (an atlas build + promote)."""
    from pipeline.atlas.splits import splits as S
    doc = json.loads((env["tmp"] / "splits.json").read_text())
    S.write_anchor_record(doc, env["atlas"])
    rows = json.loads((env["atlas"] / "splits.resolved.json").read_text())
    have = {r["split_id"] for r in rows}
    rows += [{"split_id": sid, "source": "curated"} for sid in S.split_index(doc) if sid not in have]
    (env["atlas"] / "splits.resolved.json").write_text(json.dumps(rows))


def run_reach(env) -> None:
    es = _corpus(env)
    d = env["tmp"] / "reach"
    d.mkdir(exist_ok=True)
    (d / "report.json").write_text(json.dumps({
        "handles": env["handles"], "registry_digest": env["registry"],
        "digest": f"run{time.monotonic_ns()}"[-16:], "entries_digest": io.corpus_digest(es),
        "entry_digests": io.entry_digests(es)}))


def deliver(env) -> None:
    from pipeline.deliver.release.build import ship
    rep = json.loads((env["tmp"] / "reach" / "report.json").read_text())
    b = env["tmp"] / "bundle.sqlite"
    b.unlink(missing_ok=True)
    db = sqlite3.connect(b)
    db.executescript(SCHEMA.read_text())
    meta = {"section_handles": rep["handles"], "registry_digest": rep["registry_digest"],
            "reach_digest": rep["digest"], "entries_digest": rep["entries_digest"],
            "entry_digests": json.dumps(rep["entry_digests"])}
    db.executemany("INSERT INTO meta (k, v) VALUES (?, ?)", sorted(meta.items()))
    db.commit()
    db.close()
    work = env["tmp"] / f"work{time.monotonic_ns()}"
    work.mkdir()
    time.sleep(0.02)
    ship(work, {}, rep["handles"], env["tmp"] / "release", bundle=meta)


# -- the app ----------------------------------------------------------------------------------

def _get(env, eid) -> dict:
    r = env["client"].get(f"/api/entries/{eid}")
    assert r.status_code == 200, r.text
    return r.json()


def _vintage(env, eid) -> tuple:
    v = _get(env, eid)["vintage"]
    return v["state"], v["reasons"]


def _mark(env, eid, **over):
    body = {"scope": "entry", "target": eid, "verdict": "verified", "answers": ALL_Y,
            "reviewer": "tester", **over}
    return env["client"].post("/api/marks", json=body)


def _entry_mark(env, eid):
    return next((m for m in _get(env, eid)["marks"] if m["scope"] == "entry"), None)


def _edit_in_file(env, eid, change):
    """Change entry `eid` IN ITS REGION FILE (`change(entry)`), as a reviewed change to the curated
    files lands (T185) — the app has no writer. Returns the file's bytes before, to put back."""
    path = next(p for p in sorted(env["cat"].glob("region-*.json"))
                if f'"{eid}"' in p.read_text(encoding="utf-8"))
    before = path.read_bytes()
    doc = json.loads(before)
    change(next(e for e in doc["entries"] if e["entry_id"] == eid))
    path.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return path, before


def _flip_tributaries(e):
    e["rules"][2]["includes_tributaries"] = e["rules"][2].get("includes_tributaries") is False


CUR = (V.CURRENT, [])


def test_an_entry_edit_awaits_reach_and_deliver_then_clears_without_a_restart(app_env):
    env = app_env
    assert _vintage(env, BULL) == CUR
    assert env["client"].get("/api/build").json()["entries"] == []
    path, before = _edit_in_file(env, BULL, _flip_tributaries)
    try:
        want = (V.AWAITING, [V.Waiting.reach_entry.value, V.Waiting.bundle_entry.value])
        assert _vintage(env, BULL) == want
        pend = env["client"].get("/api/build").json()
        assert [x["entry_id"] for x in pend["entries"]] == [BULL]
        # the tool serves COPIES, so the rebuild it prints is of the copies (T136), never the live tree
        assert pend["commands"] == V.rebuild_commands([V.Waiting.reach_entry], env["reuse"].rebuild_target())
        assert pend["commands"][0].endswith(f"--entries {env['cat']} --out {env['tmp'] / 'reach'}")
        row = next(x for x in env["client"].get("/api/entries", params={"region": "4"}).json()
                   if x["entry_id"] == BULL)
        assert row["vintage"] == V.AWAITING and row["vintage_reasons"] == want[1]
        # the binding and the answer are not this entry's yet: not shown, not signed off
        assert env["client"].get(f"/api/entries/{BULL}/bundle").json()["awaiting"] is True
        assert env["client"].get(f"/api/entries/{BULL}/answer/waters").status_code == 409
        r = _mark(env, BULL)
        assert r.status_code == 422 and "awaiting build" in r.text, r.text
        r = _mark(env, BULL, verdict="unsure", answers={"T": "y", "R": "?"})     # T and R: taken
        assert r.status_code == 200, r.text

        run_reach(env)                                    # the reach run alone: not yet
        assert _vintage(env, BULL) == (V.AWAITING, [V.Waiting.bundle_entry.value])
        assert _mark(env, BULL).status_code == 422
        deliver(env)                                      # deliver: built, same client
        assert _vintage(env, BULL) == CUR
        assert env["client"].get("/api/build").json()["entries"] == []
        r = _mark(env, BULL)
        assert r.status_code == 200 and r.json()["status"] == "verified", r.text
    finally:
        path.write_bytes(before)
        run_reach(env)
        deliver(env)


def test_a_split_added_mid_review_clears_after_the_rebuild_without_a_restart(app_env):
    """Owner 2026-10-23: "add splits and rebuild then continue review with the new splits"."""
    from pipeline.regs.review import fingerprint as F
    env = app_env
    reuse = env["reuse"]
    splits_path = env["tmp"] / "splits.json"
    original = splits_path.read_text()
    doc = json.loads(original)
    # a water carrying a point split, and the entries whose binding reads it
    wb = next(w for w in doc["waterbodies"] if w.get("applies_to")
              and any(s.get("anchor", {}).get("type") == "point" for s in w["splits"]))
    base = next(s for s in wb["splits"] if s.get("anchor", {}).get("type") == "point")
    ctx = reuse._review_context()
    corpus = _corpus(env)
    on_water = sorted(e for e, x in corpus.items() if base["id"] in F.entry_split_ids(x, ctx))
    assert on_water, f"no entry reads {base['id']}"
    other = next(e for e in sorted(corpus) if e not in on_water and not e.startswith("z")
                 and _vintage(env, e) == CUR)
    eid = on_water[0]
    assert _vintage(env, eid) == CUR
    assert _mark(env, eid).status_code == 200                   # signed off before the split

    new = copy.deepcopy(base)
    new["id"] = base["id"] + "__t133"
    new["anchor"]["coord"] = [new["anchor"]["coord"][0] + 0.001, new["anchor"]["coord"][1]]
    wb["splits"].append(new)
    splits_path.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n")
    try:
        v = _get(env, eid)["vintage"]
        assert (v["state"], v["reasons"], v["splits"]) == (
            V.AWAITING, [V.Waiting.split_unbuilt.value], [new["id"]])
        assert v["commands"][0].startswith("scripts/heavy.sh atlas 30 -- python -m pipeline.atlas.build "
                                           f"--full --splits {splits_path} --out ")
        assert _vintage(env, other) == CUR                       # another water: untouched
        assert _entry_mark(env, eid)["reasons"] == ["binding moved"]
        states = env["client"].get("/api/marks/splits").json()
        assert states[new["id"]]["vintage"]["state"] == V.AWAITING
        r = _mark(env, eid)                                      # P and A wait for the build
        assert r.status_code == 422 and "awaiting build" in r.text, r.text
        pend = env["client"].get("/api/build").json()
        assert set(on_water) <= {x["entry_id"] for x in pend["entries"]}
        assert [x["split_id"] for x in pend["splits"]] == [new["id"]]

        record_atlas(env)                 # atlas → binding diff → promote (the same sid space)
        assert _vintage(env, eid) == CUR
        assert env["client"].get("/api/build").json() | {"built": None} == {
            "built": None, "entries": [], "splits": [], "splits_checked": "anchors", "commands": []}
        assert _mark(env, eid).status_code == 200                # review continues
    finally:
        splits_path.write_text(original)
        record_atlas(env)


def test_the_app_gate_is_pinned_by_mutation(app_env, monkeypatch):
    """The refusal is `vintage.refuse_unbuilt`, asked by the app: without it the same verified
    mark on an entry awaiting its build is taken."""
    env = app_env
    path, before = _edit_in_file(env, BULL, _flip_tributaries)
    try:
        assert _vintage(env, BULL)[0] == V.AWAITING
        assert _mark(env, BULL).status_code == 422
        monkeypatch.setattr(V, "refuse_unbuilt", lambda scope, answers, state: None)
        assert _mark(env, BULL).status_code == 200, "the mutation must let the mark through"
    finally:
        path.write_bytes(before)
