"""Curation review phase 3 (T134) THROUGH THE REVIEW APP, in REVIEW MODE (the only mode, T185).

The FastAPI app (curation-review/backend) runs with `TestClient` over TEMP COPIES of the catalogue,
splits.json and the marks store, and the real atlas, bundle and synopsis extraction (read only).
The row-box sidecar is made here by the one-off helper itself, for page 16 (no credits).

  * no request writes anything but a mark — every read route is called, then a mark is posted,
    and only the marks store (and its backup) changed; NOTHING under data/curated changed (T185);
  * the in-season Re-resolve (T520, AGENTS 66's exception) writes only notice_waters.json and never
    changes what was matched;
  * Adam River shows "ADAM RIVER (except Eve River)" and a readable crop: its row, the water column
    above the regulation column, rendered at 2.6 px/pt (the old crop was the 530 pt row at 24 %);
  * a zone entry is found on its chapter page by text, and each rule carries its impact line;
  * progress counts every entry (entries only — T184: no S0 split tier);
  * a split mark is refused; the split marks in the store stay readable.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import load_repo
from pipeline.tests import under_test
from pipeline.tests.conftest import ATLAS_HINT, curated_root, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
ADAM = "r1:adam_river_except_eve_river@1-10"
ZONE = "z4:daily_quotas"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "review", "evidence",
         "dfo", "inseason", "checks")
#: T164: a DFO location with no extent yet, and the tributary whose confluence it names
CAMPBELL = "1:campbell-river:downstream-of-the-confluence-with"
_QUINSAM = {"ref": "n1", "anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
            "side": "downstream"}
EXTRACTION_HINT = "python -m pipeline.regs.extraction.extract_synopsis"

pytestmark = [pytest.mark.needs_atlas, pytest.mark.needs_source]


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", under_test.atlas(request) / "registry.json", ATLAS_HINT)
    raw = need(request, "source", under_test.generated().regs.extraction / "synopsis_raw_data.json",
               EXTRACTION_HINT)
    root = tmp_path_factory.mktemp("screen")
    # the sidecar, made by the helper itself for page 16 (Adam River's page)
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.regs.extraction.row_boxes import VERSION, _page_boxes
    rows = {b["image"]: {k: v for k, v in b.items() if k not in ("image", "raw_regs")}
            for b in _page_boxes((str(default_pdf()), 16))}
    sidecar = tmp_path_factory.mktemp("sidecar") / "row_boxes.json"
    sidecar.write_text(json.dumps({"version": VERSION, "rows": rows,
                                   "pdf_sha256": hashlib.sha256(default_pdf().read_bytes()).hexdigest(),
                                   "raw_data_sha256": hashlib.sha256(raw.read_bytes()).hexdigest()}))
    mp = pytest.MonkeyPatch()
    curated_root(mp, root / "curated")                        # T422: the whole curated tree is a temp copy
    cat = Path(under_test.curated().regulations.synopsis)
    for k, v in {"CURATION_BACKUP_DIR": root / "backups", "CURATION_ROW_BOXES": sidecar}.items():
        mp.setenv(k, str(v))
    mp.syspath_prepend(str(BACKEND))
    for mod in _MODS:
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    assert reuse.ENTRIES_DIR == cat
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "app": app_mod, "root": root, "reuse": reuse}
    mp.undo()
    for mod in _MODS:
        sys.modules.pop(mod, None)


def _png_size(data: bytes) -> tuple[int, int]:
    from PIL import Image
    return Image.open(io.BytesIO(data)).size


# --------------------------------------------------------------------------- Adam River

def test_adam_river_shows_its_raw_name_and_a_readable_crop(env):
    c = env["client"]
    v = c.get(f"/api/entries/{ADAM}/review").json()
    assert (v["name"], v["display_name"], v["kind"], v["tier"], v["risks"]) == (
        "ADAM RIVER (except Eve River)", "Adam River", "water", "P3", ["cut"])
    ev = v["evidence"]
    assert {k: ev[k] for k in ("kind", "printed", "pdf_page", "boxes", "crop_url", "page_url",
                               "row_image")} == {
        "kind": "row", "printed": 14, "pdf_page": 16, "boxes": [[36.14, 86.88, 539.94, 96.48]],
        "crop_url": f"/api/entries/{ADAM}/evidence/crop.png",
        "page_url": f"/api/entries/{ADAM}/evidence/page.png",
        "row_image": "/api/row-image/page_016_row_000.png"}
    assert v["coverage"] == {"segments": [{"text": "Artificial fly only upstream of Eve River, to "
                                                   "Hwy 19 bridge", "kind": "quoted"}],
                             "unquoted_chars": 0, "missing": []}
    crop = c.get(ev["crop_url"])
    assert crop.status_code == 200 and crop.headers["content-type"] == "image/png"
    # READABLE: two strips, the water column (36.14–173.16 pt) above the regulation column
    # (173.16–539.94 pt), padded 4 pt left and right and 1 pt above and below (the next row starts
    # right there), at 2.6 px/pt, 7.8 px apart. The book's ~6.5 pt row text is ~17 px tall; the
    # old crop (the whole row, 2855 px wide at 400 dpi) was shown ~680 px wide — 1.3 px/pt, ~9 px.
    s, pad, pad_y = 2.6, 4.0, 1.0
    strip_w = int((539.94 + pad) * s) - int((173.16 - pad) * s)
    strip_h = int((96.48 + pad_y) * s) - int((86.88 - pad_y) * s)
    assert _png_size(crop.content) == (strip_w, 2 * strip_h + int(3 * s)) == (975, 67)
    page = c.get(ev["page_url"])
    assert page.status_code == 200 and _png_size(page.content) == (922, 1210)


def test_the_crop_is_refused_by_name_when_the_sidecar_is_missing(env, monkeypatch):
    """No silent fallback: no sidecar is a message with the command that makes it, never another
    picture."""
    import evidence
    monkeypatch.setattr(evidence, "ROW_BOXES_PATH", env["root"] / "nope.json")
    v = env["client"].get(f"/api/entries/{ADAM}/review").json()
    assert v["evidence"]["kind"] == "missing"
    assert v["evidence"]["why"] == (f"{env['root'] / 'nope.json'} does not exist — make it with "
                                    "`python -m pipeline.regs.extraction.row_boxes` (no credits; a "
                                    "few minutes)")
    r = env["client"].get(f"/api/entries/{ADAM}/evidence/crop.png")
    assert r.status_code == 404 and "pipeline.regs.extraction.row_boxes" in r.text


# --------------------------------------------------------------------------- a zone entry

def test_a_zone_entry_is_found_by_text_and_each_rule_carries_its_impact(env):
    c = env["client"]
    v = c.get(f"/api/entries/{ZONE}/review").json()
    ev = v["evidence"]
    assert (v["kind"], v["tier"], ev["kind"], ev["printed"], ev["found"], ev["total"]) == (
        "zone", "Z", "passage", 34, 87, 87)
    assert c.get(ev["crop_url"]).status_code == 200
    import answer
    import review
    from pipeline.regs.review.fingerprint import rule_section_counts
    if v["vintage"]["state"] == "current":
        db = answer._db()
        try:
            n = rule_section_counts(db, ZONE)
        finally:
            db.close()
        want = [review.impact_line(n.get(r, 0), "4") for r in
                ("trout_char_quota.r1", "trout_char_quota.r2", "trout_char_quota.r3",
                 "trout_char_quota.r4")]
        assert all(re.fullmatch(r"binds [\d,]+ sections? in region 4", w) for w in want), want
    else:
        want = ["awaiting build — the bundle's count is another vintage's"] * 4
    # the zone-quota merge (6335f73d) added the zone's daily_quotas rules: pin the four named ones
    got = [r["impact"] for r in v["rules"] if r["rule_id"].startswith("trout_char_quota.")]
    assert got[:4] == want


def test_the_impact_line_whole():
    sys.path.insert(0, str(BACKEND))
    try:
        import review
    finally:
        sys.path.remove(str(BACKEND))
    assert [review.impact_line(n, r) for n, r in ((160_342, "4"), (1, "7a"), (0, "2"),
                                                   (12, "provincial"), (None, "1"))] == [
        "binds 160,342 sections in region 4", "binds 1 section in region 7a",
        "binds no section in region 2", "binds 12 sections province-wide",
        "awaiting build — the bundle's count is another vintage's"]


# --------------------------------------------------------------------------- the one write

#: read routes the sweep below does not call, and why — each is a pure read of the build
_HEAVY_READS = {"/api/items/{item_id}/geojson", "/api/items/{item_id}/tributaries/geojson",
                "/api/entries/{entry_id}/rules/{rule_id}/resolved", "/basemap/bc.pmtiles",
                "/api/synopsis.pdf",
                # T229: a whole watershed's chains + geometry (the real reach run, not the fixture)
                "/api/dfo/cascade/{name}"}
_PARAMS = {"entry_id": ADAM, "rule_id": "adam_river.r1",
           "item_id": "gnis:28136", "filename": "page_016_row_000.png", "printed": "14",
           "location_id": CAMPBELL, "cid": "0", "pid": "lake:0", "key": "0", "slug": "2"}
#: T191: the extent-fix editor's reads — a split search, a water search, a preview that resolves
_FIX = {"scope": "adam_river.r1", "op": "upstream_of",
        "points": [{"kind": "split", "split_id": "adam_river__hwy_19_bridge"}]}
_QUERY = {"/api/answer": {"sid": 1, "date": "2026-07-01",
                                                               "fish": "rainbow_trout"},
          "/api/splits/search": {"q": "adam"}, "/api/items/search": {"q": "adam"},
          "/api/entries/{entry_id}/extent-preview": {"fix": json.dumps(_FIX)},
          "/api/dfo/locations/{location_id}/anchor": {"split": json.dumps(_QUINSAM)},
          "/api/dfo/locations/{location_id}/extent-preview": {"fix": json.dumps(
              {"scope": "entry", "op": "whole", "points": []})}}


def test_the_extent_fix_reads_search_and_preview_without_writing(env):
    """T191: the editor's reads. Ops are the catalogue's own names; a split and a water are found
    by name; a fix on an existing split previews through the builder (sections + geometry); a NEW
    point says it cannot be previewed until it is built; a malformed fix is a 422."""
    c = env["client"]
    ops = {o["op"]: (o["points"], o["words"]) for o in c.get("/api/extent-fix/ops").json()["ops"]}
    assert ops["between"] == (2, "between") and ops["upstream_of"] == (1, "upstream")
    assert any(h["id"] == "adam_river__hwy_19_bridge"
               for h in c.get("/api/splits/search", params={"q": "hwy 19"}).json())
    assert any(h["name"] == "Adam River" for h in c.get("/api/items/search", params={"q": "adam"}).json())
    got = c.get(f"/api/entries/{ADAM}/extent-preview", params={"fix": json.dumps(_FIX)}).json()
    assert got["resolvable"] and got["n_sections"] > 0 and got["geojson"]["features"], got
    new = {**_FIX, "points": [{"kind": "confluence", "name": "Eve River"}]}
    got = c.get(f"/api/entries/{ADAM}/extent-preview", params={"fix": json.dumps(new)}).json()
    assert got["resolvable"] is False and "NEW point" in got["why"]
    bad = {**_FIX, "op": "between"}                                      # 1 point of 2
    assert c.get(f"/api/entries/{ADAM}/extent-preview",
                 params={"fix": json.dumps(bad)}).status_code == 422


def test_the_within_op_lists_the_atlas_areas_and_an_area_row_is_matched_by_area(env):
    """Owner 2026-10-26: the "within an area" op showed no area list. The areas are listed (an
    empty search lists some; a name narrows it) and the editor picks from that list. An area row
    with no registry item (Chilkoot) says "matched by area", never no_registry."""
    c = env["client"]
    assert len(c.get("/api/areas/search").json()) > 0
    chil = "area:chilkoot_trail:chilkoot_trail_national_historic_site"
    hits = c.get("/api/areas/search", params={"q": "chilkoot"}).json()
    assert hits and hits[0]["id"] == chil and "Chilkoot" in hits[0]["name"]
    eid = "r6:chilkoot_trail_national_historic_park_waters@6-28"
    d = c.get(f"/api/entries/{eid}").json()
    assert d["item"] is None and [a["id"] for a in d["matched_areas"]] == [chil]
    row, = [r for r in c.get("/api/entries", params={"region": "6"}).json() if r["entry_id"] == eid]
    assert row["status"] != "no_registry" and row["severity"] != "high", row
    src = (REPO / "curation-review/frontend/src/components/ExtentFixEditor.tsx").read_text()
    assert "<AreaSearch" in src and "api.searchAreas" in src


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def _stamps(root: Path) -> dict[str, tuple[int, int]]:
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, p.stat().st_size)
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_no_request_writes_anything_but_a_mark(env):
    """PLAN §4 phase 3 accept: every read route answers, and then a mark; the only bytes that
    changed are the marks store's — and the backup the store's write took of it first.
    THE T185 PIN: the one write route is the mark's (and AGENTS 66's one exception, the in-season matches file:
    its re-resolve is pinned by the next test), and NOTHING under data/curated (the real
    catalogue, splits.json, overrides, marks store) changes while every route is called."""
    c, app = env["client"], env["app"].app
    writes = {(m, r.path) for r in app.routes for m in getattr(r, "methods", None) or ()
              if m not in ("GET", "HEAD")}
    # THE ONE EXCEPTION (AGENTS 66): the in-season matches file, written by two routes — T230 (owner 2026-10-28)
    # POST /api/inseason/matches, an edit (not called here), and T520 POST /api/inseason/resolve, which re-derives the
    # saved matches' ranges on the shipped release and never changes a match (proven below)
    assert writes == {("POST", "/api/marks"), ("POST", "/api/inseason/matches"),
                      ("POST", "/api/inseason/resolve")}, writes
    curated = _stamps(load_repo().base)
    before = _snapshot(env["root"])
    called = []
    for r in app.routes:
        if "GET" not in (getattr(r, "methods", None) or ()) or r.path in _HEAVY_READS:
            continue
        url = re.sub(r"\{(\w+)\}", lambda m: _PARAMS[m.group(1)], r.path)
        got = c.get(url, params=_QUERY.get(r.path))
        assert got.status_code < 500, (url, got.text[:300])
        called.append(r.path)
    assert len(called) >= 20, called
    assert _snapshot(env["root"]) == before                   # reads: nothing written
    r = c.post("/api/marks", json={"scope": "entry", "target": ADAM, "answers": {"T": "y", "R": "?"},
                                   "reviewer": "tester"})
    assert r.status_code == 200 and r.json()["verdict"] == "unsure", r.text
    after = _snapshot(env["root"])
    changed = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
    store = "curated/review/marks.jsonl"                       # the sandbox's store (T422)
    backups = [k for k in changed if k != store]
    assert store in changed and len(backups) == 1, changed
    assert re.fullmatch(r"backups/\d{8}T\d{6}\.\d{6}Z-mark/[^/]+/marks\.jsonl", backups[0]), changed
    assert json.loads(after[store].decode().strip())["target"] == ADAM
    assert _stamps(load_repo().base) == curated, "a route wrote under data/curated"


#: the mark-free fields of an in-season match: what was matched. Re-resolve may rewrite only the derived rest
_DERIVED = {"waters", "stamp", "unresolved"}


def _what_was_matched(doc: dict) -> dict:
    return {rid: {k: v for k, v in m.items() if k not in _DERIVED} for rid, m in doc["matches"].items()}


def _changed(before: dict[str, bytes], after: dict[str, bytes]) -> list[str]:
    return sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))


def test_re_resolve_writes_only_the_matches_file_and_never_changes_a_match(env):
    """T520 (AGENTS 66): POST /api/inseason/resolve, the in-season matches file's second writer. Made stale the way a
    release promote leaves it (the file names an older release; a match's ranges are gone), a Re-resolve writes ONLY
    notice_waters.json (and the backup `writes.commit` took of it first), puts back exactly the ranges the shipped
    release gives, changes nothing that was matched, and starts no sync (the sandbox is not the live tree). An
    up-to-date file is not rewritten at all. Nothing under the real data/curated changes."""
    c = env["client"]
    curated = _stamps(load_repo().base)
    nw = Path(under_test.curated().inseason.notice_waters)
    key = str(nw.relative_to(env["root"]))
    matched = _what_was_matched(json.loads(nw.read_text(encoding="utf-8")))
    assert matched, "the sandbox copy holds no in-season match: nothing to re-resolve"

    def resolve() -> dict:
        r = c.post("/api/inseason/resolve")
        assert r.status_code == 200, r.text
        assert r.json()["sync"]["state"] == "off", r.json()["sync"]
        return r.json()

    base = resolve()                                          # the copy, on the shipped release
    resolved = nw.read_bytes()
    assert json.loads(resolved)["release"] == base["release"]
    assert _what_was_matched(json.loads(resolved)) == matched, "re-resolve changed what was matched"

    before = _snapshot(env["root"])
    resolve()
    assert _snapshot(env["root"]) == before, "an up-to-date matches file was rewritten"

    stale = json.loads(resolved)
    stale["release"] = "r-000000000000"
    rid = next(r for r, m in sorted(stale["matches"].items()) if m["waters"])
    stale["matches"][rid].update(waters={}, stamp=None)
    nw.write_text(json.dumps(stale, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    before = _snapshot(env["root"])
    resolve()
    after = _snapshot(env["root"])
    changed = _changed(before, after)
    backups = [k for k in changed if k != key]
    assert key in changed and len(backups) == 1, changed
    assert re.fullmatch(r"backups/\d{8}T\d{6}\.\d{6}Z-inseason-resolve/(?:[^/]+/)*notice_waters\.json",
                        backups[0]), changed
    assert after[backups[0]] == before[key], "the backup is not the file as it was"
    assert json.loads(after[key]) == json.loads(resolved), f"{rid}: re-resolve did not put back the ranges the shipped release gives"
    assert _stamps(load_repo().base) == curated, "re-resolve wrote under the real data/curated"


# --------------------------------------------------------------------------- T164: DFO mode

def test_the_dfo_queue_counts_and_filters_every_location(env):
    """T164: one row per scraped DFO location, with progress counts and the filters."""
    c = env["client"]
    rows = c.get("/api/dfo/queue", params={"status": ""}).json()
    prog = c.get("/api/dfo/progress").json()
    assert prog["all"]["total"] == len(rows) > 100
    assert sum(prog["all"]["by_state"].values()) == len(rows)
    need = c.get("/api/dfo/queue", params={"need": "needs_cut"}).json()
    assert need and all(r["state"] == "needs_cut" and r["need"]["missing"] > 0 for r in need)
    assert any(r["id"] == CAMPBELL for r in c.get("/api/dfo/queue", params={"q": "quinsam"}).json())
    assert c.get("/api/dfo/queue", params={"need": "nope"}).status_code == 422


NANAIMO_TIDAL = "1:nanaimo-river:in-subarea-17-14-tidal-portions-of"


@pytest.fixture
def undecided_nanaimo(env):
    """T491: the Nanaimo place as it was before the owner decided it (2026-11-06: their mark wrote `tidal_water` onto the
    live place) — the flag taken off in THIS module's sandbox copy only, put back after, so the test never depends on what
    the live curated data has decided."""
    f = Path(under_test.curated().dfo_salmon.places) / "region-1.json"
    was = f.read_text(encoding="utf-8")

    def strip(x):
        if isinstance(x, dict):
            if str(x.get("location_id", "")).split("~")[0] == NANAIMO_TIDAL:
                x.pop("tidal_water", None)
            for v in x.values():
                strip(v)
        elif isinstance(x, list):
            for v in x:
                strip(v)
    doc = json.loads(was)
    strip(doc)
    f.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    yield
    f.write_text(was, encoding="utf-8")


def _seen(c, target: str) -> dict:
    """T525: the extent the owner is shown on a place — what a DFO mark sends back as `extent_before`."""
    return c.get(f"/api/dfo/locations/{target}").json()["extent_now"]["seen"]


def test_a_dfo_place_wholly_in_tidal_water_is_proposed_and_the_owners_mark_decides_it(env, undecided_nanaimo):
    """T449: the Nanaimo's "tidal portions … from Cedar Bridge downstream" sits below its tidal limit (the Cedar Road
    bridge cut): the row is PROPOSED tidal water (still "needs a cut" — a proposal is never pre-accepted); the owner's
    accept (call "tidal water", every check y) makes it `tidal` — last in the queue, a done mark, no write under
    data/curated from the app; a reject ("not tidal", deferred) leaves it needing its cut, the proposal rejected."""
    c = env["client"]
    row = lambda: next(r for r in c.get("/api/dfo/queue", params={"q": "nanaimo", "status": ""}).json()
                       if r["id"] == NANAIMO_TIDAL)
    got = row()
    assert got["state"] == "needs_cut" and got["tidal"]["status"] == "proposed", got
    assert got["tidal"]["split"] == "nanaimo_river__cedar_road_bridge"
    curated = _stamps(load_repo().base)
    base = {"scope": "dfo", "target": NANAIMO_TIDAL, "extent_before": _seen(c, NANAIMO_TIDAL), "reviewer": "tester"}
    assert c.post("/api/marks", json={**base, "answers": {"P": "?"}, "call": "stream"}).status_code == 422
    r = c.post("/api/marks", json={**base, "answers": {"T": "y", "R": "y", "P": "y"}, "call": "tidal water"})
    assert r.status_code == 200, r.text
    got = row()
    assert got["state"] == "tidal" and got["tidal"]["status"] == "accepted" and got["verify"] == "verified"
    rows = c.get("/api/dfo/queue", params={"status": ""}).json()
    tail = [x["state"] for x in rows if x["status"] == "active"]
    assert tail[tail.index("tidal"):] == ["tidal"] * tail.count("tidal")     # missing nothing: last
    assert c.get("/api/dfo/queue", params={"need": "tidal", "status": ""}).json()[0]["id"] == NANAIMO_TIDAL
    assert c.get("/api/dfo/progress").json()["all"]["by_state"]["tidal"] >= 1
    r = c.post("/api/marks", json={**base, "answers": {}, "verdict": "deferred", "note": "not wholly tidal",
                                   "call": "not tidal"})
    assert r.status_code == 200, r.text
    got = row()
    assert got["state"] == "needs_cut" and got["tidal"]["status"] == "rejected"
    assert _stamps(load_repo().base) == curated, "a DFO mark wrote under data/curated"


def test_a_dfo_anchor_is_checked_and_a_new_split_rides_the_mark_only(env):
    """T164: a new split's anchor resolves on the water (a tributary's mouth snaps onto it; a
    stream that is not its tributary is refused, so is a map point on another water); a DFO mark
    carries the new split with its resolved point and an extent fix naming it — and the only file
    that changes is the marks store (never splits.json or the DFO entry files)."""
    c = env["client"]
    url = f"/api/dfo/locations/{CAMPBELL}/anchor"
    got = c.get(url, params={"split": json.dumps(_QUINSAM)}).json()
    assert got["ok"] and got["on_item"] == "gnis:39532" and got["distance_m"] < 50, got
    far = c.get(url, params={"split": json.dumps({"ref": "x", "anchor": "confluence",
                                                  "name": "Fraser River"})}).json()
    assert far["ok"] is False and "not its tributary" in far["why"]
    off = c.get(url, params={"split": json.dumps({"ref": "x", "anchor": "bridge",
                                                  "coord": [-123.1, 49.3]})}).json()
    assert off["ok"] is False and "click on the water" in off["why"]
    assert c.get(url, params={"split": json.dumps({"ref": "x", "anchor": "bridge"})}).status_code == 422
    detail = c.get(f"/api/dfo/locations/{CAMPBELL}").json()
    assert detail["verbatim"]["specific_area"].startswith("downstream of the confluence")
    assert any(h["name"] == "Quinsam River" for h in detail["hints"])
    curated = _stamps(load_repo().base)
    body = {"scope": "dfo", "target": CAMPBELL, "extent_before": _seen(c, CAMPBELL),
            "answers": {"T": "y", "R": "y", "P": "n"},
            "problem": "split missing", "note": "no Quinsam cut", "reviewer": "tester",
            "new_splits": [_QUINSAM],
            "extent_fix": [{"scope": "entry", "op": "downstream_of",
                            "points": [{"kind": "new", "split_id": "n1"}]}]}
    r = c.post("/api/marks", json=body)
    assert r.status_code == 200, r.text
    ns = r.json()["new_splits"][0]
    assert ns["blk"] and ns["measure"] and ns["coord"] and ns["on_item"] == "gnis:39532"
    bad = {**body, "new_splits": [{"ref": "n1", "anchor": "confluence", "name": "Fraser River"}]}
    assert c.post("/api/marks", json=bad).status_code == 422
    dangling = {**body, "new_splits": []}
    assert c.post("/api/marks", json=dangling).status_code == 422
    assert _stamps(load_repo().base) == curated, "a DFO mark wrote under data/curated"
    row = next(x for x in c.get("/api/dfo/queue", params={"q": "quinsam"}).json() if x["id"] == CAMPBELL)
    assert row["verify"] == "wrong" and row["new_splits"] == 1
    assert c.get("/api/dfo/progress").json()["all"]["new_splits"] >= 1


def test_a_dfo_suggestion_is_served_read_only_and_its_new_splits_land(env):
    """The SUGGESTED extents (`dfo-suggestions.json`): a location serves its suggestion and the
    queue says one waits; every NEW split a suggestion proposes passes the server's own anchor
    check on the real atlas (so accepting it and saving is not refused); reading writes nothing."""
    c = env["client"]
    doc = json.loads((under_test.generated().review / "dfo-suggestions.json").read_text())
    curated = _stamps(load_repo().base)
    # a "between" suggestion with new splits; campbell, then nanaimo (the earlier pins) were placed by the owner
    lid = "1:puntledge-river:from-the-weir-at-the-lower-hatcher"
    got = c.get(f"/api/dfo/locations/{lid}").json()["suggestion"]
    assert got == doc["suggestions"][lid] and got["suggestion"]["op"] == "between"
    row = next(x for x in c.get("/api/dfo/queue", params={"q": "puntledge"}).json() if x["id"] == lid)
    assert row["suggested"] == doc["suggestions"][lid]["confidence"]
    news = [(k, n) for k, r in doc["suggestions"].items() for n in (r.get("suggestion") or {}).get("new_splits", [])]
    assert news
    for k, n in news:
        res = c.get(f"/api/dfo/locations/{k}/anchor", params={"split": json.dumps(n)}).json()
        assert res["ok"], (k, n, res)
    assert _stamps(load_repo().base) == curated


# --------------------------------------------------------------------------- progress

def test_progress_counts_every_entry(env):
    p = env["client"].get("/api/review/progress").json()
    tiers = {t["tier"]: t for t in p["tiers"]}
    assert list(tiers) == ["P1", "Z", "P2", "P3", "P4", "sweep"]
    assert tiers["Z"]["total"] == 122  # 132 before the zone merge (6335f73d)
    # the progress bar counts the queue as served: "lake split later" entries (aa286a24, owner 2026-10-28)
    # are hidden from it by default, so it counts the corpus (1515) less them. Their number moves with
    # the reach run (a lake's only open findings decide it: 50 on the run of 2026-10-28), so the pin
    # is the corpus, not the shown total
    later = len(env["reuse"].queue(severity="later"))
    assert later > 0 and p["overall"]["total"] + later == 1515
    assert p["overall"]["total"] == sum(t["total"] for t in p["tiers"])
    assert all(t["todo"] == t["total"] - t["done"] for t in p["tiers"])


# --------------------------------------------------------------------------- adversarial review

def test_a_sidecar_cut_from_another_pdf_is_refused_by_name(env, monkeypatch, tmp_path):
    """The boxes are points on ONE book. A new PDF dropped in before the extraction is re-run keeps
    the raw data's sha — so the sidecar must name the PDF's bytes too, or every crop is cut from
    the new book at the old one's places."""
    import evidence
    doc = json.loads(evidence.ROW_BOXES_PATH.read_text())
    other = tmp_path / "row_boxes.json"
    other.write_text(json.dumps({**doc, "pdf_sha256": "0" * 64}))
    monkeypatch.setattr(evidence, "ROW_BOXES_PATH", other)
    v = env["client"].get(f"/api/entries/{ADAM}/review").json()
    import synopsis_pages
    assert (v["evidence"]["kind"], v["evidence"]["why"]) == (
        "missing", f"{other} was cut from another PDF than {synopsis_pages.PDF_PATH} — re-run the "
                   "extraction on it, then `python -m pipeline.regs.extraction.row_boxes`")
    assert env["client"].get(f"/api/entries/{ADAM}/evidence/crop.png").status_code == 404


#: a confirmed point whose note asks for no check
PLAIN_SPLIT = "adams_river__signs_200_m_downstream_of_salmon_viewing_platforms"


def test_a_split_is_not_marked_but_its_old_marks_stay_readable(env):
    """T184 (owner 2026-10-24): no split is a review target — a split mark is refused, in the
    reviewer's words (check it on an entry's map; a wrong cut is that entry's Place: wrong). The
    split marks already in the store are history, kept: still served, still read stale when the
    split moves (and a stale one is a split to verify again, `tiers.split_needs_verify`)."""
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review import marks as M
    reuse, c = env["reuse"], env["client"]
    r = c.post("/api/marks", json={"scope": "split", "target": PLAIN_SPLIT, "answers": {"P": "y"},
                                   "reviewer": "tester"})
    assert r.status_code == 422 and "is not marked on its own" in r.text, r.text
    assert "split" not in c.get("/api/mode").json()["checks"]
    path, store = Path(reuse.SPLITS_JSON_PATH), Path(reuse.MARKS_PATH)
    original, marks_before = path.read_text(), store.read_text()
    old = M.Mark(scope="split", target=PLAIN_SPLIT, verdict="verified", answers={"P": "y"},
                 reviewer="earlier", date="2026-10-23", pdf_sha256="0" * 64,
                 prints=M.Prints.model_validate(F.split_fingerprints(PLAIN_SPLIT,
                                                                     reuse._review_context())))
    try:
        store.write_text(marks_before + M.line(old))
        got = c.get("/api/marks", params={"scope": "split"}).json()
        assert [(m["target"], m["status"]) for m in got] == [(PLAIN_SPLIT, "verified")]
        assert c.get("/api/marks/splits").json()[PLAIN_SPLIT]["status"] == "verified"
        assert PLAIN_SPLIT not in reuse.splits_to_verify()
        path.write_text(_shift(original))
        assert c.get("/api/marks/splits").json()[PLAIN_SPLIT]["status"] == "stale"
        assert PLAIN_SPLIT in reuse.splits_to_verify()
    finally:
        path.write_text(original)
        store.write_text(marks_before)


def _shift(text: str) -> str:
    """splits.json with PLAIN_SPLIT's offset moved 200 -> 250 m (whatever the file's layout)."""
    data = json.loads(text)
    for wb in data["waterbodies"]:
        for sp in wb["splits"]:
            if sp["id"] == PLAIN_SPLIT:
                sp["anchor"]["offset_m"] = 250
    return json.dumps(data, indent=1, ensure_ascii=False) + "\n"


def test_a_proposed_split_is_shared_on_the_river_and_never_entered_twice(env):
    """T210: a split proposed on one DFO location shows (once) on every location on the same river;
    a second mark proposing the same anchor reuses the first's id and says so; Place y rides an
    extent fix made of existing cuts; and the only file that changes is the marks store."""
    c = env["client"]
    other = CAMPBELL + "~2"
    assert c.get(f"/api/dfo/locations/{other}").status_code == 200
    curated = _stamps(load_repo().base)
    first = {"scope": "dfo", "target": CAMPBELL, "extent_before": _seen(c, CAMPBELL),
             "answers": {"T": "y", "R": "y", "P": "n"},
             "problem": "split missing", "note": "no Quinsam cut", "reviewer": "tester",
             "new_splits": [{**_QUINSAM, "ref": "n1"}]}
    r = c.post("/api/marks", json=first)
    assert r.status_code == 200, r.text
    seen = c.get(f"/api/dfo/locations/{other}").json()["proposed"]
    # T355: shown, stored and landed under its REAL splits.json id, never the local "n1"
    real = "campbell_river__quinsam_river"
    assert [p["id"] for p in seen].count(real) == 1 and seen[0]["coord"] and seen[0]["n_marks"] >= 1
    assert r.json()["new_splits"][0]["ref"] == real
    # another location proposes the same confluence under another ref and name: reused, not added
    again = {**first, "target": other, "extent_before": _seen(c, other), "new_splits": [{**_QUINSAM, "ref": "n7", "name": "Quinsam"}],
             "extent_fix": [{"scope": "entry", "op": "downstream_of",
                             "points": [{"kind": "new", "split_id": "n7"}]}]}
    r = c.post("/api/marks", json=again)
    assert r.status_code == 200, r.text
    got = r.json()
    assert [n["ref"] for n in got["new_splits"]] == [real]                  # the first one's id
    assert got["extent_fix"][0]["points"][0]["split_id"] == real            # the fix follows it
    assert got["reused"] == [{"ref": "n7", "id": real, "name": "Quinsam River"}]
    once = [p for p in c.get(f"/api/dfo/locations/{CAMPBELL}").json()["proposed"]
            if p["anchor"] == "confluence" and p["item_id"] == "gnis:27883"]
    assert len(once) == 1 and once[0]["n_marks"] == 2 and once[0]["name"] == "Quinsam River"
    # the progress bar counts the cut once
    assert c.get("/api/dfo/progress").json()["all"]["new_splits"] == len(
        c.get(f"/api/dfo/locations/{CAMPBELL}").json()["proposed"])
    # Place y with an extent made of existing cuts only is recorded as it is (no new split)
    fix = {"scope": "entry", "op": "downstream_of", "points": [{"kind": "split", "split_id": "x"}]}
    yes = {"scope": "dfo", "target": other, "extent_before": _seen(c, other),
           "answers": {"T": "y", "R": "y", "P": "y"},
           "reviewer": "tester", "extent_fix": [fix]}
    r = c.post("/api/marks", json=yes)
    assert r.status_code == 200 and r.json()["verdict"] == "verified", r.text
    assert c.post("/api/marks", json={**yes, "new_splits": [_QUINSAM]}).status_code == 422
    assert _stamps(load_repo().base) == curated, "a DFO mark wrote under data/curated"


def test_a_dfo_reach_skips_the_tributary_walk_unless_asked(env):
    """Owner 2026-10-28: the Extents map draws only the place's own water(s) by default; the
    tributary walk (Bella Coola `whole`: 13 MB) comes only with `tributaries=true`."""
    c = env["client"]
    bc = "5b:bella-coola-river-not-including-at:whole"
    own = c.get(f"/api/dfo/locations/{bc}/reach").json()
    full = c.get(f"/api/dfo/locations/{bc}/reach", params={"tributaries": "true"}).json()
    assert own["resolvable"] and full["resolvable"] and own["walks_tributaries"] is True
    assert full["walks_tributaries"] is True      # the binding's own call, whether or not it was walked
    assert 0 < own["n_sections"] < full["n_sections"]


def test_a_dfo_extent_fix_is_saved_and_the_location_serves_it_back(env):
    """Owner 2026-10-27: a saved extent was not shown when the location reopened. POST a DFO mark
    carrying an extent fix; GET the location -> its `mark.extent_fix` is that fix (what the editor
    pre-fills from); a later mark replaces it; a refused save says why."""
    c = env["client"]
    fix = {"scope": "entry", "op": "whole", "points": [], "items": ["gnis:39532"], "tributaries": False}
    body = {"scope": "dfo", "target": CAMPBELL, "extent_before": _seen(c, CAMPBELL),
            "answers": {"T": "y", "R": "y", "P": "?"},
            "note": "", "reviewer": "tester", "extent_fix": [fix]}
    assert c.post("/api/marks", json=body).status_code == 200
    got = c.get(f"/api/dfo/locations/{CAMPBELL}").json()["mark"]
    assert got["extent_fix"][0]["op"] == "whole" and got["extent_fix"][0]["items"] == ["gnis:39532"]
    assert got["date"] and got["reviewer"] == "tester"
    in_force = [m for m in c.get("/api/marks", params={"scope": "dfo", "target": CAMPBELL}).json()]
    assert in_force and in_force[0]["extent_fix"][0]["op"] == "whole"
    later = {**body, "extent_fix": [{**fix, "tributaries": True}]}
    assert c.post("/api/marks", json=later).status_code == 200
    held = c.get(f"/api/dfo/locations/{CAMPBELL}").json()["mark"]
    assert held["extent_fix"][0]["tributaries"] is True
    refused = c.post("/api/marks", json={**body, "answers": {}})        # nothing answered
    assert refused.status_code == 422 and refused.json()["detail"][0]["msg"]
    # T525: a mark names the extent it was made on — one made on an extent that has moved since is refused, and so is
    # one that names none; neither is saved
    stale = {**later, "extent_before": {**body["extent_before"], "extents": [{"op": "whole"}]}}
    r = c.post("/api/marks", json=stale)
    assert r.status_code == 422 and "changed since you opened it" in r.json()["detail"][0]["msg"], r.text
    r = c.post("/api/marks", json={k: v for k, v in later.items() if k != "extent_before"})
    assert r.status_code == 422 and "extent_before" in r.json()["detail"][0]["msg"], r.text
    assert c.get(f"/api/dfo/locations/{CAMPBELL}").json()["mark"] == held


def test_an_offset_cut_resolves_off_its_reference_and_the_mark_stamps_the_reference(env):
    """Owner 2026-11-02: a cut N m up / down of a reference is the reference + the split's offset. The
    anchor check answers the cut's point AND its reference; the mark stamps the reference (the split
    adds the offset itself — stamping the offset point would offset it twice)."""
    c = env["client"]
    url = f"/api/dfo/locations/{CAMPBELL}/anchor"
    at = c.get(url, params={"split": json.dumps(_QUINSAM)}).json()
    ref = {"ref": "r1", "anchor": "bridge", "coord": at["coord"], "offset_m": -100}
    got = c.get(url, params={"split": json.dumps(ref)}).json()
    assert got["ok"] and got["ref"]["coord"] == at["coord"], got
    assert abs((got["ref"]["measure"] - got["measure"]) - 100) < 0.5        # 100 m downstream, along it
    body = {"scope": "dfo", "target": CAMPBELL, "extent_before": _seen(c, CAMPBELL),
            "answers": {"T": "y", "R": "y", "P": "?"},
            "reviewer": "tester", "new_splits": [ref],
            "extent_fix": [{"scope": "entry", "op": "downstream_of", "points": [{"kind": "new", "split_id": "r1"}]}]}
    r = c.post("/api/marks", json=body)
    assert r.status_code == 200, r.text
    ns = r.json()["new_splits"][0]
    assert ns["offset_m"] == -100 and ns["measure"] == got["ref"]["measure"] and ns["coord"] == at["coord"]


def test_a_checklist_is_saved_only_on_the_rows_the_place_has_now(env):
    """T532: the checklist screen's two saves are marks with `checklist` — refused on a stale digest, refused as "all
    correct" while a ? / ✗ row is open, kept as a P-only DFO mark with its rows; the place reads checked in the queue
    (grouped by water) and nothing under data/curated is written by the app."""
    c = env["client"]
    base = {"scope": "dfo", "extent_before": _seen(c, CAMPBELL), "reviewer": "tester", "answers": {"P": "y"},
            "verdict": "verified"}
    pc = c.get(f"/api/checks/dfo/{CAMPBELL}").json()
    if pc.get("held"):                  # a save of the owner's still waits to land: a check would replace it
        r = c.post("/api/marks", json={**base, "target": CAMPBELL, "checklist": {"digest": pc["digest"], "rows": [
            {"n": x["n"], "kind": x["kind"], "state": x["state"], "ticked": True} for x in pc["rows"]]}})
        assert r.status_code == 422 and "has not landed yet" in r.text, r.text
    target = next(p["id"] for p in c.get("/api/checks/queue", params={"source": "dfo"}).json()
                  if not c.get(f"/api/checks/dfo/{p['id']}").json().get("held"))
    pc = c.get(f"/api/checks/dfo/{target}").json()
    assert pc["check"] in ("unchecked", "stale", "checked", "wrong") and pc["rows"][0]["kind"] == "water"
    rows = [{"n": r["n"], "kind": r["kind"], "state": r["state"]} for r in pc["rows"]]
    base = {**base, "target": target, "extent_before": _seen(c, target)}
    curated = _stamps(load_repo().base)
    stale = c.post("/api/marks", json={**base, "checklist": {"digest": "0" * 16, "rows": rows}})
    assert stale.status_code == 422 and "changed since you opened it" in stale.text
    if any(r["state"] != "ok" for r in rows):
        held = c.post("/api/marks", json={**base, "checklist": {"digest": pc["digest"], "rows": rows}})
        assert held.status_code == 422 and "every row" in held.text, held.text
    ticked = [{**r, "ticked": r["state"] != "ok"} for r in rows]
    r = c.post("/api/marks", json={**base, "checklist": {"digest": pc["digest"], "rows": ticked}})
    assert r.status_code == 200, r.text
    assert c.get(f"/api/checks/dfo/{target}").json()["check"] == "checked"
    q = c.get("/api/checks/queue", params={"source": "dfo"}).json()
    keys = [p["water_key"] for p in q]
    assert all(k not in keys[:i] or keys[i - 1] == k for i, k in enumerate(keys)), "the queue left a water and came back"
    assert next(p for p in q if p["id"] == target)["check"] == "checked"
    assert c.get("/api/checks/progress", params={"source": "dfo"}).json()["done"] >= 1
    assert _stamps(load_repo().base) == curated, "a checklist mark wrote under data/curated"


def test_a_regulation_extent_is_checked_with_the_same_rows_and_a_rule_mark(env):
    """T532 phase 3: a regulation rule opens in the same checklist (its rows, its reach drawn), the check is a rule mark
    (P only) refused on a stale digest; the queue holds both sources, a water's places together."""
    c = env["client"]
    pc = c.get(f"/api/checks/rule/{ADAM}/adam_river.r1").json()
    assert pc["rows"][0]["kind"] == "water" and pc["source"]["rule_id"] == "adam_river.r1"
    assert pc["reach"]["geojson"]["type"] == "FeatureCollection"
    rows = [{"n": r["n"], "kind": r["kind"], "state": r["state"], "ticked": r["state"] != "ok"} for r in pc["rows"]]
    base = {"scope": "rule", "target": ADAM, "rule_id": "adam_river.r1", "reviewer": "tester", "answers": {"P": "y"},
            "verdict": "verified"}
    stale = c.post("/api/marks", json={**base, "checklist": {"digest": "0" * 16, "rows": rows}})
    assert stale.status_code == 422 and "changed since you opened it" in stale.text
    r = c.post("/api/marks", json={**base, "checklist": {"digest": pc["digest"], "rows": rows}})
    if r.status_code == 200:
        assert c.get(f"/api/checks/rule/{ADAM}/adam_river.r1").json()["check"] == "checked"
    else:                                   # an entry awaiting its build takes only T and R (AGENTS 65)
        assert r.status_code == 422 and "build" in r.text, r.text
    q = c.get("/api/checks/queue", params={"source": "all"}).json()
    assert {p["source"] for p in q} == {"dfo", "regs"}
    keys = [p["water_key"] for p in q]
    assert all(k not in keys[:i] or keys[i - 1] == k for i, k in enumerate(keys)), "the queue left a water and came back"
