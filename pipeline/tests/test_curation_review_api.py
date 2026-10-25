"""The curation-review app reviews the CURRENT catalogue model, end to end, through its HTTP API.

Runs the FastAPI app (curation-review/backend) with `TestClient` against a TEMP COPY of the
catalogue region files (`CURATION_ENTRIES_DIR`), splits.json and the marks store — never the real
ones. It proves:

  GET        the entry is served with a generated `label` on every rule and record, the same
             string `catalogue.label` / `catalogue.licensing_label` produce (one entry of every
             rule type and every licensing kind);
  marks      a mark is held against the fingerprints, refused without what it needs or on an entry
             that fails the gate; `matched` is the only source of what an entry covers.

That no route writes under data/curated (T185) is `test_curation_review_screen.py`'s sweep and
`test_curation_review_safety.py`'s pin.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED, GENERATED
from pipeline.regs.parsing import catalogue as C
from pipeline.tests.conftest import need, ATLAS_HINT, BUNDLE_HINT

REPO = Path(__file__).resolve().parents[2]
BACKEND = REPO / "curation-review" / "backend"
REAL = CURATED.regulations.entries.catalogue

#: the review app serves the built registry (data/generated/atlas/full)
pytestmark = pytest.mark.needs_atlas


@pytest.fixture(scope="module")
def env(request, tmp_path_factory):
    need(request, "atlas", GENERATED.build() / "registry.json", ATLAS_HINT)
    tmp = tmp_path_factory.mktemp("catalogue")
    for p in sorted(REAL.glob("region-*.json")):
        shutil.copy2(p, tmp / p.name)
    mp = pytest.MonkeyPatch()
    mp.setenv("CURATION_ENTRIES_DIR", str(tmp))
    shutil.copy2(CURATED.waters.splits, tmp / "splits.json")
    mp.setenv("CURATION_SPLITS_JSON", str(tmp / "splits.json"))
    # the reviewer's marks go to a temp store, never the real one
    (tmp / "marks.jsonl").write_text("")
    mp.setenv("CURATION_MARKS", str(tmp / "marks.jsonl"))
    # a mark backs the store up first (`writes.py`) — into a temp dir, never data/generated
    mp.setenv("CURATION_BACKUP_DIR", str(tmp_path_factory.mktemp("backups")))
    mp.syspath_prepend(str(BACKEND))
    for mod in ("app", "reuse", "model_api", "answer", "synopsis_pages", "writes",
                "review", "evidence"):
        sys.modules.pop(mod, None)
    import app as app_mod                                     # noqa: E402
    import reuse                                              # noqa: E402
    assert reuse.ENTRIES_DIR == tmp, "the app must be pointed at the temp copy, not the real files"
    from fastapi.testclient import TestClient
    yield {"client": TestClient(app_mod.app), "dir": tmp, "reuse": reuse}
    mp.undo()


def _entries(d: Path) -> dict[str, tuple[str, dict]]:
    out = {}
    for p in sorted(d.glob("region-*.json")):
        region = p.stem.split("region-", 1)[1]
        for e in json.loads(p.read_text(encoding="utf-8"))["entries"]:
            out[e["entry_id"]] = (region, e)
    return out


def _smallest(d: Path) -> dict[str, str]:
    """The smallest entry carrying each rule type and each licensing kind."""
    best: dict[str, tuple[int, str]] = {}
    for eid, (_, e) in _entries(d).items():
        size = len(json.dumps(e))
        keys = [f"rule:{r['type']}" for r in e.get("rules") or []]
        keys += [f"licensing:{x['kind']}" for x in e.get("licensing") or []]
        for k in keys:
            if k not in best or size < best[k][0]:
                best[k] = (size, eid)
    return {k: v[1] for k, v in best.items()}


ALL_PARTS = [f"rule:{t.value}" for t in C.RuleType] + [
    f"licensing:{k}" for k in ("designation", "not_classified", "requirement", "licence_terms",
                               "exemption", "alternative")]


@pytest.fixture(scope="module")
def picks(env):
    got = _smallest(env["dir"])
    missing = [k for k in ALL_PARTS if k not in got]
    assert not missing, f"the corpus carries no entry of {missing} — nothing to curate them on"
    # the smallest entry whose FIRST angler_closure names a residency in its own sentence
    best = None
    for eid, (_, e) in _entries(env["dir"]).items():
        first = next((r for r in e.get("rules") or [] if r["type"] == "angler_closure"), None)
        if first and C.residency_said(first["verbatim"]):
            size = len(json.dumps(e))
            if best is None or size < best[0]:
                best = (size, eid)
    assert best, "no angler_closure names a residency"
    got["rule:angler_closure:residency"] = best[1]
    return got


def _file_of(env, eid) -> Path:
    region, _ = _entries(env["dir"])[eid]
    return env["dir"] / f"region-{region}.json"


def _get(env, eid) -> dict:
    r = env["client"].get(f"/api/entries/{eid}")
    assert r.status_code == 200, r.text
    return r.json()


# --------------------------------------------------------------------------- #
# GET: every rule and record carries the generated label
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("part", ALL_PARTS)
def test_get_serves_the_generated_label(env, picks, part):
    eid = picks[part]
    served = _get(env, eid)["entry"]
    stored = _entries(env["dir"])[eid][1]
    model = C.CatalogueEntry.model_validate(stored)
    siblings = {r.rule_id: r for r in model.rules}
    # THE BUNDLE'S LABEL: the same function, handed the same place-namer the bundle uses.
    place_of = env["reuse"]._place_namer().for_entry(model.matched)
    # and the corpus, so a lift of another entry's rule names it in words, as the bundle's does
    entries = sys.modules["model_api"].corpus_entries(env["dir"])
    for got, r in zip(served.get("rules") or [], model.rules):
        assert got["label"] == C.label(r, siblings, place_of, entries)
    for got, x in zip(served.get("licensing") or [], model.licensing):
        assert got["label"], f"{x.kind} {x.id} was served with no label"
        assert got["label"] == C.licensing_label(
            x, siblings, units=_units(env), refs=_refs(env, eid, model))


def _units(env) -> dict:
    out = {}
    for _eid, (_, e) in _entries(env["dir"]).items():
        for x in C.CatalogueEntry.model_validate(e).licensing:
            if isinstance(x, C.Designation):
                out.setdefault(x.unit, x.unit_name)
    return out


def _refs(env, eid, model) -> dict:
    out = {}
    for other, (_, e) in _entries(env["dir"]).items():
        for x in C.CatalogueEntry.model_validate(e).licensing:
            out[(other, x.id)] = x
    for x in model.licensing:
        out[(eid, x.id)] = x
    return out


# --------------------------------------------------------------------------- #
# `matched` is authoritative; empty binds nothing
# --------------------------------------------------------------------------- #

def test_covered_ids_read_matched_only(env, picks):
    reuse = env["reuse"]
    _, e = _entries(env["dir"])[picks["rule:tackle_restriction"]]
    assert e["matched"] and reuse._covered_ids(e) == e["matched"]
    # the same row with `matched` emptied: its name still names a registry water, and the app
    # must NOT fall back to it
    empty = dict(e, matched=[])
    assert reuse._covered_ids(empty) == []
    assert reuse._item_for_entry(empty) is None
    assert reuse.entry_status(empty, None) == "no_registry"


# --------------------------------------------------------------------------- #
# The review pass (T97): a mark in the append-only store, held against fingerprints
# --------------------------------------------------------------------------- #

BULL = "r4:bull_river@4-22"
ALL_Y = {"T": "y", "R": "y", "P": "y", "A": "y"}


def _mark(env, eid, **over):
    body = {"scope": "entry", "target": eid, "verdict": "verified", "answers": ALL_Y,
            "reviewer": "tester", **over}
    return env["client"].post("/api/marks", json=body)


def _entry_mark(env, eid):
    ms = [m for m in _get(env, eid)["marks"] if m["scope"] == "entry"]
    return ms[0] if ms else None


@pytest.fixture
def as_built(env, monkeypatch):
    """The real bundle, read as THIS entry's vintage. These tests are about marks and the bundle's
    answers; whether the reach run, bundle and release on disk were made from the temp catalogue
    (T133's one-vintage gate) is `test_curation_review_vintage.py`'s, over a vintage it writes. The
    generated data on a machine may predate the per-entry digests, which reads "awaiting build"."""
    from pipeline.regs.review import vintage as V
    now = {"state": V.CURRENT, "reasons": [], "splits": [], "splits_checked": "anchors",
           "commands": []}
    monkeypatch.setattr(env["reuse"], "entry_vintage", lambda e, on=None: dict(now))
    monkeypatch.setattr(env["reuse"], "split_vintage", lambda sid, on=None: dict(now))


@pytest.fixture
def fresh_marks(env):
    store = env["dir"] / "marks.jsonl"
    store.write_text("")
    yield store
    store.write_text("")


@pytest.mark.needs_bundle
def test_a_mark_is_held_against_the_fingerprints_not_the_bytes(env, fresh_marks, as_built):
    _the_bundle()
    reuse = env["reuse"]
    assert reuse.MARKS_PATH == fresh_marks, "marks must go to the temp store"
    path = _file_of(env, BULL)
    before = path.read_bytes()
    r = _mark(env, BULL)
    assert r.status_code == 200, r.text
    assert (r.json()["status"], r.json()["reasons"]) == ("verified", [])
    assert path.read_bytes() == before, "marking an entry wrote to its region file"
    rec, = [json.loads(x) for x in fresh_marks.read_text().splitlines()]
    assert rec["reviewer"] == "tester" and len(rec["pdf_sha256"]) == 64
    assert rec["pages"] and rec["prints"]["text"] and rec["prints"]["records"]
    assert all("section" not in json.dumps(x) for x in rec["prints"]["records"])
    assert _entry_mark(env, BULL)["status"] == "verified"
    rows = env["client"].get("/api/entries", params={"region": "4", "verify": "verified"}).json()
    assert [x["entry_id"] for x in rows] == [BULL]
    todo = env["client"].get("/api/entries", params={"region": "4", "verify": "todo"}).json()
    assert BULL not in {x["entry_id"] for x in todo}

    # a placement edit (made in the file, as a reviewed change makes it — T185) stales it, saying
    # so; putting the file back makes it hold again
    doc = json.loads(before)
    e = next(x for x in doc["entries"] if x["entry_id"] == BULL)
    e["rules"][2]["includes_tributaries"] = e["rules"][2].get("includes_tributaries") is False
    path.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    try:
        got = _entry_mark(env, BULL)
        assert (got["status"], got["reasons"]) == ("stale", ["binding moved"])
        row, = [x for x in env["client"].get("/api/entries", params={"region": "4",
                                                                       "verify": "stale"}).json()]
        assert row["entry_id"] == BULL and row["mark_reasons"] == ["binding moved"]
    finally:
        path.write_bytes(before)
    assert _entry_mark(env, BULL)["status"] == "verified"


@pytest.mark.needs_bundle
def test_a_mark_is_refused_without_what_it_needs(env, fresh_marks, as_built):
    _the_bundle()
    assert _mark(env, BULL, reviewer="").status_code == 422
    assert _mark(env, BULL, verdict="wrong", answers={**ALL_Y, "R": "n"}).status_code == 422
    assert _mark(env, BULL, surprise=1).status_code == 422
    assert _mark(env, "r9:nope").status_code == 404
    assert _mark(env, BULL, scope="rule", rule_id="no_such_rule").status_code == 404
    r = _mark(env, BULL, verdict="wrong", answers={**ALL_Y, "R": "n"}, problem="quota",
              note="Galbraith cut")
    assert r.status_code == 200 and r.json()["status"] == "wrong"
    row = next(x for x in env["client"].get(
        "/api/entries", params={"region": "4", "verify": "wrong"}).json()
        if x["entry_id"] == BULL)
    assert row["verify_note"] == "Galbraith cut"


@pytest.mark.needs_bundle
def test_a_mark_is_refused_on_an_entry_that_fails_the_gate(env, fresh_marks):
    _the_bundle()
    path = _file_of(env, BULL)
    before = path.read_bytes()
    doc = json.loads(before)
    e = next(x for x in doc["entries"] if x["entry_id"] == BULL)
    r0 = next(r for r in e["rules"] if r.get("take") is not None)
    r0["take"] = 97                                   # a number its sentence does not print
    path.write_text(json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    try:
        r = _mark(env, BULL)
        assert r.status_code == 422 and "fails the gate" in r.text, r.text
        assert fresh_marks.read_text() == ""
    finally:
        path.write_bytes(before)


@pytest.mark.needs_bundle
def test_a_split_is_no_review_target(env, fresh_marks, as_built):
    """T184: a split is checked by proxy on its entries' maps — a split mark is refused and the
    store is untouched."""
    _the_bundle()
    states = env["client"].get("/api/marks/splits").json()
    sid = sorted(states)[0]
    assert states[sid]["status"] == "unmarked"
    r = env["client"].post("/api/marks", json={"scope": "split", "target": sid,
                                               "verdict": "verified", "answers": {"P": "y"},
                                               "reviewer": "tester"})
    assert r.status_code == 422 and "is not marked on its own" in r.text, r.text
    assert fresh_marks.read_text() == ""
    assert env["client"].get("/api/marks/splits").json()[sid]["status"] == "unmarked"


def test_book_order_is_region_then_page(env):
    rows = env["client"].get("/api/entries", params={"region": "5", "order": "book"}).json()
    pages = [x["page"] for x in rows if x["page"] is not None]
    assert pages == sorted(pages), "book order must run by printed page"
    first_water = next(i for i, x in enumerate(rows) if x["kind"] == "water")
    assert all(x["kind"] == "zone" for x in rows[:first_water])   # the chapter precedes its tables
    assert env["client"].get("/api/entries", params={"order": "nope"}).status_code == 422


def test_a_pointer_row_has_no_unused_splits(env):
    rows = env["client"].get("/api/entries", params={"region": "5"}).json()
    bc = next(x for x in rows if x["entry_id"] == "r5:bella_coola_river@5-8")
    assert bc["pointer"] is True
    assert bc["status"] != "unused_splits" and bc["unused_curated_splits"] == 0


def test_a_cut_bound_only_by_a_licensing_record_is_used(env):
    """T119 review: `unused_curated_splits` walked the scope and the rules' extents only, so the
    two Skeena cuts that only its classified-water record binds were listed as unused."""
    d = _get(env, "r6:skeena_river_mainstem_only@6-10")
    unused = {u["id"] for u in d["unused_curated_splits"]}
    assert unused, "the Skeena has curated cuts its rules do not bind"
    assert not unused & {"skeena_river__kitsumkalum_river_into_skeena_river_u1500m",
                         "area:SKEENA RIVER ECOLOGICAL RESERVE"}


def test_the_app_spells_cut_points_as_the_parse_menu_does(env):
    """T119 review: ONE gate needs one menu spelling. The app's cut-points read the picker's
    display aliases (every prefix stripped: `329069968`), the parse menu's keep `lake:329069968`
    — so each accepted an alias the other refused. Every built boundary of the Campbell River
    (12 `lake:` aliases corpus-wide, two of them here) now reads the same in both."""
    from pipeline.atlas.registry import load_registry
    from pipeline.regs.parsing.validate_catalogue import registry_cut_points
    reuse = env["reuse"]
    pipe = dict(registry_cut_points(load_registry(str(GENERATED.registry())))("gnis:39532"))
    app = dict(reuse._cut_points("gnis:39532"))
    assert {k: app[k] for k in pipe} == pipe
    assert "lake:329069968" in pipe["campbell_river__ladore_dam"]


def test_minted_cuts_are_not_orphans_or_unused(env):
    d = _get(env, "r5:dean_river@5-9")
    minted = [b for b in d["item"]["boundaries"] if b.get("minted")]
    assert minted, "Dean River has gauge and length cuts the build minted"
    assert all(b["id"].startswith(("gauge__", "length:", "area:", "border:")) for b in minted)
    assert not any(u["id"].startswith(("gauge__", "length:")) for u in d["unused_curated_splits"])


def test_a_split_carrying_its_own_applies_to_is_in_splits_json(env):
    """Atnarko/Bella Coola's waterbody has `applies_to: null`; each split carries its own."""
    d = _get(env, "r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8")
    authored = [b for b in d["item"]["boundaries"] if b["curated"] and not b.get("minted")]
    assert authored and all(b["in_splits"] for b in authored), \
        [b["id"] for b in authored if not b["in_splits"]]


def test_loaders_keep_cache_clear(env):
    reuse = env["reuse"]
    for fn in (reuse._registry, reuse._graph, reuse._geoms):
        assert callable(fn.cache_clear)


# --------------------------------------------------------------------------- #
# What an angler is told: the live bundle, through read.effective_rules
# --------------------------------------------------------------------------- #

def _the_bundle():
    from pipeline.deliver.bundle import read as R
    need(None, "bundle", R.BUNDLE, BUNDLE_HINT)


DEAN = "r5:dean_river@5-9"


@pytest.mark.needs_bundle
def test_bundle_copy_of_an_entry(env, as_built):
    _the_bundle()
    b = env["client"].get(f"/api/entries/{DEAN}/bundle").json()
    assert b["in_bundle"] is True
    served = {r["rule_id"]: r["label"] for r in _get(env, DEAN)["entry"]["rules"]}
    assert {r["rule_id"]: r["label_now"] for r in b["rules"]} == served
    assert {x["id"] for x in b["licensing"]} >= {"dean_river_class_i_main"}
    assert all(x["n_sections"] >= 0 and "waters" in x for x in b["licensing"])
    # T133: "bundle matches" compares each rule's sections too — the bundle's beside the builder's
    # now on the served atlas — and says which rules differ and why
    assert all(isinstance(r["n_sections_now"], int) for r in b["rules"])
    assert b["bundle_matches"] == (not b["mismatches"]), b["mismatches"]
    assert b["vintage"]["state"] == "current" and b["awaiting"] is False


@pytest.mark.needs_bundle
def test_answer_for_one_piece_one_day_one_fish(env, as_built):
    _the_bundle()
    from pipeline.deliver.bundle import read as R
    waters = env["client"].get(f"/api/entries/{DEAN}/answer/waters").json()
    dean = next(w for w in waters if w["name"] == "Dean River")
    piece = dean["pieces"][0]
    assert piece["rules"] and all(r["rule_id"].startswith("dean_river.") for r in piece["rules"])
    got = env["client"].get("/api/answer", params={"sid": piece["sid"], "date": "2026-07-01",
                                                    "fish": "ST"}).json()
    want = R.effective_rules(piece["sid"], __import__("datetime").date(2026, 7, 1), "ST")
    assert [(x["entry_id"], x["rule_id"], x["state"]) for x in got["rules"]] == \
        [(x["entry"], x["rule"], x["state"]) for x in want], "the app must answer as read.py does"
    assert "Dean River" in got["waters"]
    # a group is not one fish, and a date must be a date — refused, readably
    r = env["client"].get("/api/answer", params={"sid": piece["sid"], "date": "2026-07-01",
                                                  "fish": "TROUT_CHAR"})
    assert r.status_code == 422 and "group" in r.text
    r = env["client"].get("/api/answer", params={"sid": piece["sid"], "date": "July",
                                                  "fish": "ST"})
    assert r.status_code == 422 and "YYYY-MM-DD" in r.text


@pytest.mark.needs_bundle
def test_tidal_water_says_so(env, as_built):
    _the_bundle()
    waters = env["client"].get("/api/entries/r1:nitinat_lake@1-3/answer/waters").json()
    sid = waters[0]["pieces"][0]["sid"]
    got = env["client"].get("/api/answer", params={"sid": sid, "date": "2026-07-01",
                                                    "fish": "CT"}).json()
    assert got["tidal"] == "r1:nitinat_lake@1-3"


# --------------------------------------------------------------------------- #
# The book: source_pages are PRINTED numbers; the PDF index is not
# --------------------------------------------------------------------------- #

def test_printed_pages_map_to_pdf_pages(env):
    sp = sys.modules.get("synopsis_pages") or __import__("synopsis_pages")
    assert sp.PDF_PATH.exists(), f"{sp.PDF_PATH} is tracked in git and missing"
    m = {int(k): v for k, v in env["client"].get("/api/synopsis/pages").json().items()}
    # printed = PDF - 2 up to printed 40, PDF - 6 after the unnumbered centre gloss
    assert (m[14], m[35], m[40]) == (16, 37, 42)
    assert (m[41], m[42], m[44]) == (47, 48, 50)
    r = env["client"].get("/api/synopsis/page/44.png")
    assert r.status_code == 200 and r.content[:8] == b"\x89PNG\r\n\x1a\n"
    assert env["client"].get("/api/synopsis/page/999.png").status_code == 404
