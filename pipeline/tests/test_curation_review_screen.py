"""Curation review phase 3 (T134) THROUGH THE REVIEW APP, in REVIEW MODE (the only mode, T185).

The FastAPI app (curation-review/backend) runs with `TestClient` over TEMP COPIES of the catalogue,
splits.json and the marks store, and the real atlas, bundle and synopsis extraction (read only).
The row-box sidecar is made here by the one-off helper itself, for page 16 (no credits).

  * no request writes anything but a mark — every read route is called, then a mark is posted,
    and only the marks store (and its backup) changed; NOTHING under data/curated changed (T185);
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
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.tests.conftest import ATLAS_HINT, need

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
ADAM = "r1:adam_river_except_eve_river@1-10"
ZONE = "z4:daily_quotas"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "review", "evidence",
         "dfo")
#: T164: a DFO location with no extent yet, and the tributary whose confluence it names
CAMPBELL = "1:campbell-river:downstream-of-the-confluence-with"
_QUINSAM = {"ref": "n1", "anchor": "confluence", "name": "Quinsam River", "item_id": "gnis:27883",
            "side": "downstream"}
EXTRACTION_HINT = "python -m pipeline.regs.extraction.extract_synopsis"

pytestmark = [pytest.mark.needs_atlas, pytest.mark.needs_source]


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    raw = need(request, "source", GENERATED.regs.extraction / "synopsis_raw_data.json",
               EXTRACTION_HINT)
    root = tmp_path_factory.mktemp("screen")
    cat = root / "catalogue"
    cat.mkdir()
    for p in sorted(CURATED.regulations.entries.catalogue.glob("region-*.json")):
        shutil.copy2(p, cat / p.name)
    shutil.copy2(CURATED.waters.splits, root / "splits.json")
    (root / "marks.jsonl").write_text("")
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
    for k, v in {"CURATION_ENTRIES_DIR": cat,
                 "CURATION_SPLITS_JSON": root / "splits.json", "CURATION_MARKS": root / "marks.jsonl",
                 "CURATION_BACKUP_DIR": root / "backups", "CURATION_ROW_BOXES": sidecar}.items():
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
                "/api/synopsis.pdf"}
_PARAMS = {"entry_id": ADAM, "rule_id": "adam_river.r1",
           "item_id": "gnis:28136", "filename": "page_016_row_000.png", "printed": "14",
           "location_id": CAMPBELL}
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


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def _stamps(root: Path) -> dict[str, tuple[int, int]]:
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, p.stat().st_size)
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_no_request_writes_anything_but_a_mark(env):
    """PLAN §4 phase 3 accept: every read route answers, and then a mark; the only bytes that
    changed are the marks store's — and the backup the store's write took of it first.
    THE T185 PIN: the one write route is the mark's, and NOTHING under data/curated (the real
    catalogue, splits.json, overrides, marks store) changes while every route is called."""
    c, app = env["client"], env["app"].app
    writes = {(m, r.path) for r in app.routes for m in getattr(r, "methods", None) or ()
              if m not in ("GET", "HEAD")}
    assert writes == {("POST", "/api/marks")}, writes
    curated = _stamps(CURATED.base)
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
    backups = [k for k in changed if k != "marks.jsonl"]
    assert "marks.jsonl" in changed and len(backups) == 1, changed
    assert re.fullmatch(r"backups/\d{8}T\d{6}\.\d{6}Z-mark/[^/]+/marks\.jsonl", backups[0]), changed
    assert json.loads(after["marks.jsonl"].decode().strip())["target"] == ADAM
    assert _stamps(CURATED.base) == curated, "a route wrote under data/curated"


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
    curated = _stamps(CURATED.base)
    body = {"scope": "dfo", "target": CAMPBELL, "answers": {"T": "y", "R": "y", "P": "n"},
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
    assert _stamps(CURATED.base) == curated, "a DFO mark wrote under data/curated"
    row = next(x for x in c.get("/api/dfo/queue", params={"q": "quinsam"}).json() if x["id"] == CAMPBELL)
    assert row["verify"] == "wrong" and row["new_splits"] == 1
    assert c.get("/api/dfo/progress").json()["all"]["new_splits"] >= 1


# --------------------------------------------------------------------------- progress

def test_progress_counts_every_entry(env):
    p = env["client"].get("/api/review/progress").json()
    tiers = {t["tier"]: t for t in p["tiers"]}
    assert list(tiers) == ["P1", "Z", "P2", "P3", "P4", "sweep"]
    assert tiers["Z"]["total"] == 122  # 132 before the zone merge (6335f73d)
    assert p["overall"]["total"] == 1515
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
    curated = _stamps(CURATED.base)
    first = {"scope": "dfo", "target": CAMPBELL, "answers": {"T": "y", "R": "y", "P": "n"},
             "problem": "split missing", "note": "no Quinsam cut", "reviewer": "tester",
             "new_splits": [{**_QUINSAM, "ref": "n1"}]}
    r = c.post("/api/marks", json=first)
    assert r.status_code == 200, r.text
    seen = c.get(f"/api/dfo/locations/{other}").json()["proposed"]
    assert [p["id"] for p in seen].count("n1") == 1 and seen[0]["coord"] and seen[0]["n_marks"] >= 1
    # another location proposes the same confluence under another ref and name: reused, not added
    again = {**first, "target": other, "new_splits": [{**_QUINSAM, "ref": "n7", "name": "Quinsam"}],
             "extent_fix": [{"scope": "entry", "op": "downstream_of",
                             "points": [{"kind": "new", "split_id": "n7"}]}]}
    r = c.post("/api/marks", json=again)
    assert r.status_code == 200, r.text
    got = r.json()
    assert [n["ref"] for n in got["new_splits"]] == ["n1"]                  # the first one's id
    assert got["extent_fix"][0]["points"][0]["split_id"] == "n1"            # the fix follows it
    assert got["reused"] == [{"ref": "n7", "id": "n1", "name": "Quinsam River"}]
    once = [p for p in c.get(f"/api/dfo/locations/{CAMPBELL}").json()["proposed"]
            if p["anchor"] == "confluence" and p["item_id"] == "gnis:27883"]
    assert len(once) == 1 and once[0]["n_marks"] == 2 and once[0]["name"] == "Quinsam River"
    # the progress bar counts the cut once
    assert c.get("/api/dfo/progress").json()["all"]["new_splits"] == len(
        c.get(f"/api/dfo/locations/{CAMPBELL}").json()["proposed"])
    # Place y with an extent made of existing cuts only is recorded as it is (no new split)
    fix = {"scope": "entry", "op": "downstream_of", "points": [{"kind": "split", "split_id": "x"}]}
    yes = {"scope": "dfo", "target": other, "answers": {"T": "y", "R": "y", "P": "y"},
           "reviewer": "tester", "extent_fix": [fix]}
    r = c.post("/api/marks", json=yes)
    assert r.status_code == 200 and r.json()["verdict"] == "verified", r.text
    assert c.post("/api/marks", json={**yes, "new_splits": [_QUINSAM]}).status_code == 422
    assert _stamps(CURATED.base) == curated, "a DFO mark wrote under data/curated"
