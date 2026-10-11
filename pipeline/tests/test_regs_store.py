"""THE REGS STORE (S0, S1, S1b): `regs.sqlite` decodes back to the answers file BYTE FOR BYTE and to
the export subset it carries by value; every section handle a named part covers holds that part's
akey, every other section in B.C. its unnamed part's (S1: never 0 inside B.C.; S1b: the key of its
CONTENT, a named part's where the same), and every section in B.C. a place (its region set,
`section_place`; MUs are not shipped); an altered row is refused.

The synthetic tests need no data (a hand-made answers wire + export subset). The data tests build a
store from the live set (`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point them at a side build) into
a temporary directory — never over the canonical one.
"""
from __future__ import annotations

import copy
import json
import os
import shutil
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import GENERATED
from pipeline.deliver.store.build import (FAMILIES, SECTIONS, bundle_problems, build, load_inputs,
                                          write_store)
from pipeline.deliver.store.common import (BLOCK, CODECS, StoreError, answers_file_bytes, dumps,
                                           export_subset, pack, store_digest, train_zdict, unpack)
from pipeline.deliver.store.decode import Store
from pipeline.tests.conftest import BUNDLE_HINT, EXPORT_HINT, need

# --------------------------------------------------------------------------------------------
# synthetic: no data
# --------------------------------------------------------------------------------------------


def _wire() -> dict:
    return {
        "about": {"what": "w", "bundle": {"section_handles": "h", "reach_digest": "r"},
                  "format": "answers/2", "export": {"rule_ids_sha256": "x", "rules": 2},
                  "sections": {}, "reserved": {}, "counts": {}},
        "spec": {"a": "b"}, "schemas": {"s": {}}, "fish": ["RB", "ST"],
        "keys": [[1, None, 0, "known", 1, [], ["1"], "stream", 0, 0, None],
                 [2, 3, 0, None, 0, ["tidal"], [], "lake", 1, 1, 0],
                 [1, None, 0, None, 0, [], [], "wetland", 0, 0, None]],          # unnamed only
        "segments": [[1], [1, 1, 200]],
        "moments": [{"weekdays": ["Monday"], "hours": None},
                    {"weekdays": ["Tuesday"], "hours": None}],
        "segment_moments": [[0, 1, 0]],
        "parts": {"w:a": [0, None], "w:b": [1], "w:c": []},
        "places": [{"regions": ["2"], "label": "Region 2",
                    "names": {"lake": "Unnamed lake · Region 2", "stream": "Unnamed stream · Region 2",
                              "wetland": "Unnamed wetland · Region 2"}},
                   {"regions": ["3", "8"], "label": "Regions 3 and 8",
                    "names": {"lake": "Unnamed lake · Regions 3 and 8",
                              "stream": "Unnamed stream · Regions 3 and 8",
                              "wetland": "Unnamed wetland · Regions 3 and 8"}}],
        "unnamed": {"first": 2, "parts": [2, 0]},
        "glossary": {"version": 1, "terms": [{"id": "t", "says": "ü — “quoted”"}]},
        "sections": {
            "ladder": {"version": 1, "at": [[1], [0, 1, 0], [0]], "reasons": ["moot"],
                       "reason_state": ["moot"], "verdicts": [[[0], [], [], [], [], []],
                                                              [[], [1], [], [], [], [[0, 0, 1]]]],
                       "frames": [[0, [[0, 1]]], [1, [[0, 0, 1, 0], [1, 0]]]]},
            "rows": {"version": 5, "at": [[0], [0, 1, 1], [0]], "decided": [{"status": "keep", "daily": 2}],
                     "rows": [{"kind": "keep", "km": 0.1}],
                     "frames": [[["RB"], {"RB": [0, None]}, [0], None], [[], {}, [], "known_no_rules"]]},
            "gear": {"version": 5, "at": [[0], [1, 1, 1], [0]], "frames": [{"counts": {}}, {"tidal": True}],
                     "province_methods": ["angling"], "parent": {}, "methods": [], "moments": [],
                     "conduct_means": {}, "count_nouns": {"lines_per_angler": ["line (rod)", "lines (rods)"]}},
            "licence": {"version": 4, "at": [[0], [0, 0, 0], [0]], "holds": [{"holds": [0]}],
                        "answers": [{"documents": []}], "documents": [[0, 0]], "frames": [[0, 0]],
                        "profiles": ["a", "b"], "profile_dims": [["age", ["a", "b"]]]},
            "display": {"version": 5, "at": [[0], [0, 0, 1], [0]],
                        "frames": [{"status": "base", "closing": [], "strip": 16},
                                   {"status": "closed", "closing": [[0, ["RB"]]], "strip": 1}],
                        "strip": [{"state": "open", "bit": 0, "label": "open", "says": "Open."}],
                        "reasons": {"ladder": {"lost": "Replaced by {by}.", "won": "Replaces {rule}."}},
                        "rules": [{"kind": "gear"}, {"kind": "quota", "plain": "Keep 2"}],
                        "waters": {"w:a": {"parts": [{"order": 0, "label": "From the mouth",
                                                      "runs": "From the mouth", "place": None,
                                                      "hint": "Whole water", "km": 1.5,
                                                      "closed_all_year": False,
                                                      "paper_licence": [0]}, None],
                                           "picker": {"choices": [], "headed": False},
                                           "unresolved_licensing": []},
                                   "w:b": {"parts": [{"order": 0, "label": "L", "runs": "R",
                                                      "place": "P", "hint": None, "km": None,
                                                      "closed_all_year": True, "paper_licence": []}],
                                           "picker": {"choices": [], "headed": False},
                                           "unresolved_licensing": [0]}}},
        },
    }


def _subset() -> dict:
    r = lambda i: {"entry_id": "e1", "rule_id": f"r{i}", "label": f"rule {i}", "prov": {"rank": i}}
    return {
        "rule_ids": ["e1::r1", "e1::r2"], "licensing_ids": ["e1#l1"],
        "rules": {"e1::r1": r(1), "e1::r2": r(2)},
        "licensing": {"e1#l1": {"entry_id": "e1", "record_id": "l1", "prov": {}}},
        "entries": {"e1": {"name": "E", "pages": [3]}, "e2": {"name": "F"}},
        "licences": {"basic": {"name": "basic"}}, "species": {"families": {}},
        "conduct": {"act": "Do it"},
        "rulesets": {"1": {"sections": 3, "reach": ["e1::r1", "e1::r2"]},
                     "2": {"sections": 1, "reach": [], "trib": ["e1::r2"]}},
        "licensing_sets": {"3": {"sections": 1, "reach": ["e1#l1"]}},
        "waters": {
            "w:a": {"name": "A", "kind": "stream", "sections": 3, "outside_bc": 1,
                    "entries": ["e1"], "steelhead": "known", "steelhead_source": ["regulations"],
                    "parts": [{"ruleset": "1", "licensing_set": None, "sections": 2,
                               "runs": [{"from": "mouth", "to": "source", "km_from": 0.0}]},
                              {"ruleset": None, "licensing_set": None, "sections": 1, "runs": []}]},
            "w:b": {"name": "B", "kind": "lake", "sections": 1, "outside_bc": 0, "entries": [],
                    "tidal": {"guide": "g"},
                    "parts": [{"ruleset": "2", "licensing_set": "3", "sections": 1, "runs": [],
                               "anadromous_rainbow": True}]},
            "w:c": {"name": "C", "kind": "lake", "sections": 0, "outside_bc": 0, "entries": ["e2"],
                    "divided_into": ["w:b"], "parts": []},
        },
    }


#: (bundle item ord, sid, part_ix): w:a part 0 two sections (one in the next block), part 1 outside
#: B.C. (no key), w:b one section. Unnamed (S1): sections 13 and 2*BLOCK + 5 in unnamed part 0
#: (answers key 2, a content no named part has), 14 in unnamed part 1 (S1b: the content of w:a's
#: key 0, so key 0); every section in B.C.: all but 11. WHERE (S1b): (sid, place).
ITEM_ORD = {"w:a": 5, "w:b": 9, "w:c": 11}
PART_SECTION = [(5, 10, 0), (5, BLOCK + 1, 0), (5, 11, 1), (9, 12, 0)]
UNNAMED_SECTION = [(13, 0), (14, 1), (2 * BLOCK + 5, 0)]
IN_BC = sorted([10, BLOCK + 1, 12, 13, 14, 2 * BLOCK + 5])
WHERE = [(10, 0), (12, 1), (13, 1), (14, 0), (BLOCK + 1, 0), (2 * BLOCK + 5, 0)]
STORE_KW = dict(unnamed_section=UNNAMED_SECTION, in_bc=IN_BC, where=WHERE)


@pytest.fixture(params=CODECS)
def synthetic(tmp_path, request):
    A, sub = _wire(), _subset()
    raw = answers_file_bytes(A)
    p = tmp_path / f"regs.{request.param}.sqlite"
    write_store(p, raw, A, sub, ITEM_ORD, PART_SECTION, request.param, **STORE_KW)
    return p, raw, A, sub


def test_a_synthetic_store_round_trips_exactly(synthetic):
    p, raw, A, sub = synthetic
    s = Store(p)
    try:
        assert s.answers() == A
        assert s.answers_bytes() == raw
        assert s.export_subset() == sub
        assert s.section_akeys() == {10: 1, BLOCK + 1: 1, 12: 2, 13: 3, 14: 1, 2 * BLOCK + 5: 3}
        assert s.akey_of(11) == 0 and s.akey_of(BLOCK * 7) == 0
        # where every section in B.C. is, apart from its key (S1b)
        assert s.section_places() == {10: 0, BLOCK + 1: 0, 12: 1, 13: 1, 14: 0, 2 * BLOCK + 5: 0}
        assert s.place_of(11) is None and s.place_of(13) == 1
        # MUs are not shipped (the bundle keeps them; the app reads the tiles' MU layer)
        tables = {r[0] for r in s.con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert not tables & {"section_mu", "mu_set"}
        # an unnamed section's name: its key's kind and its place's regions, whole
        assert s.unnamed_name(13) == "Unnamed wetland · Regions 3 and 8"
        assert s.unnamed_name(2 * BLOCK + 5) == "Unnamed wetland · Region 2"
        assert s.unnamed_name(14) == "Unnamed stream · Region 2"           # shares w:a's key
        assert s.unnamed_name(11) is None
    finally:
        s.close()


def test_the_store_refuses_a_section_in_bc_with_no_akey(tmp_path):
    """S1: every section in B.C. has an answer — a 0 there is refused (here: section 15 carries a
    rule set, but no part, named or unnamed, covers it)."""
    A = _wire()
    with pytest.raises(StoreError, match="in B.C. have no akey"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "in_bc": IN_BC + [15]})


def test_the_store_refuses_an_akey_outside_bc(tmp_path):
    A = _wire()
    with pytest.raises(StoreError, match="outside B.C. carry an akey"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "in_bc": [x for x in IN_BC if x != 13]})


def test_the_store_refuses_an_unnamed_section_a_named_part_claims(tmp_path):
    A = _wire()
    with pytest.raises(StoreError, match="claimed by akeys"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "unnamed_section": UNNAMED_SECTION + [(12, 0)]})


def test_the_store_refuses_an_unnamed_part_with_no_key(tmp_path):
    A = _wire()
    with pytest.raises(StoreError, match="which has no key"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "unnamed_section": [(13, 2)]})


def test_the_store_refuses_a_section_in_bc_with_no_place(tmp_path):
    """S1b: every section in B.C. has a place, named or not."""
    A = _wire()
    with pytest.raises(StoreError, match="have no place"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "where": WHERE[1:]})
    with pytest.raises(StoreError, match="not one of the 2 places"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION, **{**STORE_KW, "where": WHERE[:-1] + [(2 * BLOCK + 5, 2)]})


def test_a_synthetic_store_has_no_dangling_reference(synthetic):
    con = sqlite3.connect(synthetic[0])
    try:
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
        assert con.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    finally:
        con.close()


def test_an_altered_row_is_refused_by_the_digest(synthetic, tmp_path):
    p = synthetic[0]
    con = sqlite3.connect(p)
    con.execute("UPDATE gear_frame SET j = ? WHERE id = 0", (dumps({"counts": {"x": 1}}),))
    con.commit()
    con.close()
    with pytest.raises(StoreError, match="store_digest"):
        Store(p)


def test_an_altered_row_restamped_decodes_differently_and_is_refused(synthetic):
    p, raw, _A, _sub = synthetic
    con = sqlite3.connect(p)
    con.execute("UPDATE rows_decided SET j = ? WHERE id = 0", (dumps({"status": "keep", "daily": 3}),))
    con.execute("UPDATE meta SET v = ? WHERE k = 'store_digest'", (store_digest(con),))
    con.commit()
    con.close()
    s = Store(p)
    try:
        assert s.answers_bytes(check=False) != raw
        with pytest.raises(StoreError, match="answers file it was built from"):
            s.answers_bytes()
    finally:
        s.close()


def test_the_store_refuses_an_untaught_section(tmp_path):
    A = _wire()
    A["sections"]["new"] = {"version": 1, "at": [[0], [0, 0, 0]], "frames": [{}]}
    with pytest.raises(StoreError, match="teach it"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(A), A, _subset(), ITEM_ORD,
                    PART_SECTION)


def test_the_store_refuses_a_section_two_akeys_claim(tmp_path):
    with pytest.raises(StoreError, match="claimed by akeys"):
        write_store(tmp_path / "x.sqlite", answers_file_bytes(_wire()), _wire(), _subset(),
                    ITEM_ORD, PART_SECTION + [(9, 10, 0)])


def test_blob_codecs_round_trip():
    blobs = [dumps({"k": i, "label": "Release — wild trout"}) for i in range(200)] + [b"1", b"[]"]
    zd = train_zdict(blobs)
    assert 0 < len(zd) <= 32768
    for codec in CODECS:
        for b in blobs:
            packed = pack(b, codec, zd if codec == "zdict" else None)
            assert unpack(packed, zd) == b
            assert len(packed) <= len(b)


# --------------------------------------------------------------------------------------------
# the live set
# --------------------------------------------------------------------------------------------

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or GENERATED.bundle / "bundle.sqlite")


def _export_dir() -> Path:
    from pipeline.tools.export_ui_rules import OUT
    return Path(os.environ.get("ANSWERS_EXPORT_DIR") or OUT.parent)


@pytest.fixture(scope="module")
def live(request, tmp_path_factory):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    exp = need(request, "bundle", _export_dir() / "ui-rules-answers.json", EXPORT_HINT).parent
    out = tmp_path_factory.mktemp("store") / "regs.sqlite"
    build(exp / "ui-rules-answers.json", exp, BUNDLE, out)          # the deliver step's codec
    raw, A, E, G = load_inputs(exp / "ui-rules-answers.json", exp)
    return out, raw, A, E, G


@pytest.mark.needs_bundle
def test_the_live_store_decodes_to_the_answers_bytes_and_the_export_subset(live):
    from pipeline.tools.export_codec import expand
    out, raw, _A, E, G = live
    s = Store(out)
    try:
        assert s.answers_bytes(check=False) == raw
        assert s.export_subset(check=False) == export_subset(expand(E, G))
    finally:
        s.close()


@pytest.mark.needs_bundle
def test_every_named_section_holds_the_akey_of_the_part_covering_it(live):
    out, _raw, A, _E, _G = live
    b = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    s = Store(out)
    try:
        assert bundle_problems(A, b) == []
        ords = dict(b.execute("SELECT ord, item_id FROM item"))
        want = {}
        for o, sid, pix in b.execute("SELECT ord, sid, part_ix FROM part_section"):
            k = A["parts"][ords[o]][pix]
            want[sid] = 0 if k is None else k + 1
        got = s.section_akeys()
        assert {h: a for h, a in want.items() if a} == {h: got[h] for h in want if h in got}
        assert not [h for h, a in want.items() if not a and h in got]     # outside B.C.: 0
        assert all(s.akey_of(h) == a for h, a in list(want.items())[::97])
        # the store's own part rows say the same
        part_akey = {(o, i): a or 0 for o, i, a in s.con.execute("SELECT item_ord, part_ix, akey FROM part")}
        for o, sid, pix in b.execute("SELECT ord, sid, part_ix FROM part_section"):
            assert got.get(sid, 0) == part_akey[(o, pix)]
    finally:
        s.close()
        b.close()


@pytest.mark.needs_bundle
def test_every_section_in_bc_has_a_nonzero_akey_and_none_outside(live):
    """S1: a tapped section anywhere in B.C. has an answer — its named part's akey, or its unnamed
    part's (named by its place); outside B.C. 0."""
    out, _raw, A, *_ = live
    b = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    s = Store(out)
    try:
        got = s.section_akeys()
        in_bc = {sid for (sid,) in b.execute("SELECT sid FROM section_ruleset")}
        outside = {sid for (sid,) in b.execute("SELECT sid FROM outside_bc")}
        assert set(got) == in_bc and not (set(got) & outside) and len(in_bc) > 1_000_000
        key_of = A["unnamed"]["parts"]
        up_of = dict(b.execute("SELECT sid, up FROM section_ruleset WHERE up IS NOT NULL"))
        assert all(got[sid] == key_of[up] + 1 for sid, up in up_of.items())
        # and a place, the bundle's (S1b); the name is the kind and the regions only
        places = s.section_places()
        assert set(places) == in_bc
        assert all(places[sid] == pl for sid, pl in b.execute("SELECT sid, place FROM section_ruleset"))
        for sid in list(up_of)[::50021]:
            name = s.unnamed_name(sid)
            assert name.startswith("Unnamed ") and " MU" not in name
    finally:
        s.close()
        b.close()


@pytest.mark.needs_bundle
def test_every_keyed_section_reads_its_akeys_rule_set(live):
    """Per handle, the bundle's own section facts are the akey's: its rule set."""
    out, *_ = live
    b = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    s = Store(out)
    try:
        rs = dict(s.con.execute("SELECT akey, ruleset FROM akey"))
        got = s.section_akeys()
        bad = [(sid, rs[got[sid]], set_id) for sid, set_id in
               b.execute("SELECT sid, set_id FROM section_ruleset") if sid in got
               and rs[got[sid]] != set_id]
        assert bad == [] and len(got) > 0
    finally:
        s.close()
        b.close()


@pytest.mark.needs_bundle
def test_every_keyed_section_reads_its_akeys_licensing_set(live):
    out, *_ = live
    b = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    s = Store(out)
    try:
        ls = dict(s.con.execute("SELECT akey, licensing_set FROM akey"))
        lic = dict(b.execute("SELECT sid, set_id FROM section_licensing"))
        bad = [sid for sid, a in s.section_akeys().items() if lic.get(sid) != ls[a]]
        assert bad == []
    finally:
        s.close()
        b.close()


@pytest.mark.needs_bundle
def test_the_live_store_schema_invariants(live):
    out, raw, A, *_ = live
    s = Store(out)
    con = s.con
    try:
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
        # one akey per answers key, every one with exactly its segments' cells, every one a part's
        assert con.execute("SELECT count(*) FROM akey").fetchone()[0] == len(A["keys"])
        assert con.execute("SELECT count(*) FROM akey a WHERE nseg <> (SELECT count(*) FROM cell c "
                           "WHERE c.akey = a.akey)").fetchone()[0] == 0
        # every akey before `unnamed.first` is a named part's; every one from it an unnamed
        # part's (S1b: a content no named part has)
        first = A["unnamed"]["first"]
        assert con.execute("SELECT count(*) FROM akey WHERE akey <= ? AND akey NOT IN "
                           "(SELECT akey FROM part WHERE akey IS NOT NULL)", (first,)
                           ).fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM akey WHERE akey > ? AND akey IN "
                           "(SELECT akey FROM part WHERE akey IS NOT NULL)", (first,)
                           ).fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM place").fetchone()[0] == len(A["places"])
        # every family holds exactly the answers' interned table, ids 0..n-1
        for (name, key), tbl in FAMILIES.items():
            n, lo, hi = con.execute(f"SELECT count(*), min(id), max(id) FROM {tbl}").fetchone()
            assert (n, lo, hi) == (len(A["sections"][name][key]), 0, len(A["sections"][name][key]) - 1)
        for name in SECTIONS:
            assert con.execute(f"SELECT max({name}) FROM cell").fetchone()[0] < \
                len(A["sections"][name]["frames"])
        # keyed tables with a composite key are WITHOUT ROWID
        for t in ("cell", "part", "meta", "static", "zdict"):
            sql = con.execute("SELECT sql FROM sqlite_master WHERE name = ?", (t,)).fetchone()[0]
            assert "WITHOUT ROWID" in sql, t
        # the digests pair the store with its bundle and its answers
        bm = dict(sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True).execute("SELECT k, v FROM meta"))
        for k in ("section_handles", "reach_digest", "rule_ids_sha256"):
            assert s.meta[k] == bm[k]
        import hashlib
        assert s.meta["answers_sha256"] == hashlib.sha256(raw).hexdigest()
    finally:
        s.close()


@pytest.mark.needs_bundle
def test_one_altered_live_frame_is_refused(live, tmp_path):
    out, raw, *_ = live
    p = tmp_path / "mutant.sqlite"
    shutil.copy(out, p)
    con = sqlite3.connect(p)
    zd = dict(con.execute("SELECT tbl, d FROM zdict")).get("display_frame")
    j = json.loads(unpack(con.execute("SELECT j FROM display_frame WHERE id = 0").fetchone()[0], zd))
    mutated = copy.deepcopy(j)
    mutated["status"] = "closed" if j.get("status") != "closed" else "base"
    con.execute("UPDATE display_frame SET j = ? WHERE id = 0", (dumps(mutated),))
    con.commit()
    con.close()
    with pytest.raises(StoreError, match="store_digest"):
        Store(p)
    s = Store(p, verify=False)
    try:
        assert s.answers_bytes(check=False) != raw
        with pytest.raises(StoreError):
            s.answers_bytes()
    finally:
        s.close()
