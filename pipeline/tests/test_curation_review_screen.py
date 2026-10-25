"""Curation review phase 3 (T134) THROUGH THE REVIEW APP, in REVIEW MODE (the only mode, T185).

The FastAPI app (curation-review/backend) runs with `TestClient` over TEMP COPIES of the catalogue,
splits.json and the marks store, and the real atlas, bundle and synopsis extraction (read only).
The row-box sidecar is made here by the one-off helper itself, for page 16 (no credits).

  * no request writes anything but a mark — every read route is called, then a mark is posted,
    and only the marks store (and its backup) changed; NOTHING under data/curated changed (T185);
  * Adam River shows "ADAM RIVER (except Eve River)" and a readable crop: its row, the water column
    above the regulation column, rendered at 2.6 px/pt (the old crop was the 530 pt row at 24 %);
  * a zone entry is found on its chapter page by text, and each rule carries its impact line;
  * progress counts every entry and every S0 split point.
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
ZONE = "z4:trout_char_quota"
_MODS = ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes", "review", "evidence")
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
        "zone", "Z", "passage", 34, 31, 31)
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
    assert [r["impact"] for r in v["rules"]] == want


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
           "item_id": "gnis:28136", "filename": "page_016_row_000.png", "printed": "14"}
_QUERY = {"/api/answer": {"sid": 1, "date": "2026-07-01",
                                                               "fish": "rainbow_trout"}}


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


# --------------------------------------------------------------------------- progress

def test_progress_counts_every_entry_and_every_s0_split_point(env):
    p = env["client"].get("/api/review/progress").json()
    tiers = {t["tier"]: t for t in p["tiers"]}
    assert list(tiers) == ["S0", "P1", "Z", "P2", "P3", "P4", "sweep"]
    assert tiers["Z"]["total"] == 132 and tiers["S0"]["total"] == 150
    assert p["overall"]["total"] == 1525 + 150
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


#: a confirmed point whose note asks for no check — not in S0 until something changes it
PLAIN_SPLIT = "adams_river__signs_200_m_downstream_of_salmon_viewing_platforms"


def _card(env, split_id):
    import review
    return next((c for c in review.split_queue() if c["split_id"] == split_id), None)


def test_a_split_mark_gone_stale_is_listed_in_s0(env):
    """A split confirmed, then changed by hand (not through the tool, so no stamp): its mark is
    stale and S0 lists it again — the queue never stops showing a point to confirm."""
    reuse = env["reuse"]
    path = Path(reuse.SPLITS_JSON_PATH)
    original = path.read_text()
    try:
        r = env["client"].post("/api/marks", json={"scope": "split", "target": PLAIN_SPLIT,
                                                   "answers": {"P": "y"}, "reviewer": "tester"})
        assert r.status_code == 200 and r.json()["status"] == "verified", r.text
        assert _card(env, PLAIN_SPLIT) is None                        # confirmed, not S0
        path.write_text(_shift(original))
        card = _card(env, PLAIN_SPLIT)
        assert card is not None and (card["s0"], card["status"]) == (True, "stale"), card
    finally:
        path.write_text(original)


def _shift(text: str) -> str:
    """splits.json with PLAIN_SPLIT's offset moved 200 -> 250 m (whatever the file's layout)."""
    data = json.loads(text)
    for wb in data["waterbodies"]:
        for sp in wb["splits"]:
            if sp["id"] == PLAIN_SPLIT:
                sp["anchor"]["offset_m"] = 250
    return json.dumps(data, indent=1, ensure_ascii=False) + "\n"
