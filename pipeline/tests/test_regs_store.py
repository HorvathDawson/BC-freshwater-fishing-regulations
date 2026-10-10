"""THE REGS STORE (S0): `regs.sqlite` decodes back to the answers file BYTE FOR BYTE and to the export
subset it carries by value; every section handle a named part covers holds that part's akey; an
altered row is refused.

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
                 [2, 3, 0, None, 0, ["tidal"], [], "lake", 1, 1, 0]],
        "segments": [[1], [1, 1, 200]],
        "moments": [{"weekdays": ["Monday"], "hours": None},
                    {"weekdays": ["Tuesday"], "hours": None}],
        "segment_moments": [[0, 1, 0]],
        "parts": {"w:a": [0, None], "w:b": [1], "w:c": []},
        "glossary": {"version": 1, "terms": [{"id": "t", "says": "ü — “quoted”"}]},
        "sections": {
            "ladder": {"version": 1, "at": [[1], [0, 1, 0]], "reasons": ["moot"],
                       "reason_state": ["moot"], "verdicts": [[[0], [], [], [], [], []],
                                                              [[], [1], [], [], [], [[0, 0, 1]]]],
                       "frames": [[0, [[0, 1]]], [1, [[0, 0, 1, 0], [1, 0]]]]},
            "rows": {"version": 5, "at": [[0], [0, 1, 1]], "decided": [{"status": "keep", "daily": 2}],
                     "rows": [{"kind": "keep", "km": 0.1}],
                     "frames": [[["RB"], {"RB": [0, None]}, [0], None], [[], {}, [], "known_no_rules"]]},
            "gear": {"version": 4, "at": [[0], [1, 1, 1]], "frames": [{"counts": {}}, {"tidal": True}],
                     "province_methods": ["angling"], "parent": {}, "methods": [], "moments": [],
                     "conduct_means": {}},
            "licence": {"version": 4, "at": [[0], [0, 0, 0]], "holds": [{"holds": [0]}],
                        "answers": [{"documents": []}], "documents": [[0, 0]], "frames": [[0, 0]],
                        "profiles": ["a", "b"], "profile_dims": [["age", ["a", "b"]]]},
            "display": {"version": 4, "at": [[0], [0, 0, 1]],
                        "frames": [{"status": "base", "closing": []}, {"status": "closed", "closing": [[0, ["RB"]]]}],
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
#: B.C. (no key), w:b one section.
ITEM_ORD = {"w:a": 5, "w:b": 9, "w:c": 11}
PART_SECTION = [(5, 10, 0), (5, BLOCK + 1, 0), (5, 11, 1), (9, 12, 0)]


@pytest.fixture(params=CODECS)
def synthetic(tmp_path, request):
    A, sub = _wire(), _subset()
    raw = answers_file_bytes(A)
    p = tmp_path / f"regs.{request.param}.sqlite"
    write_store(p, raw, A, sub, ITEM_ORD, PART_SECTION, request.param)
    return p, raw, A, sub


def test_a_synthetic_store_round_trips_exactly(synthetic):
    p, raw, A, sub = synthetic
    s = Store(p)
    try:
        assert s.answers() == A
        assert s.answers_bytes() == raw
        assert s.export_subset() == sub
        assert s.section_akeys() == {10: 1, BLOCK + 1: 1, 12: 2}
        assert s.akey_of(11) == 0 and s.akey_of(BLOCK * 7) == 0
    finally:
        s.close()


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
        assert {h: a for h, a in want.items() if a} == got
        assert all(s.akey_of(h) == a for h, a in list(want.items())[::97])
        # the store's own part rows say the same
        part_akey = {(o, i): a or 0 for o, i, a in s.con.execute("SELECT item_ord, part_ix, akey FROM part")}
        for o, sid, pix in b.execute("SELECT ord, sid, part_ix FROM part_section"):
            assert got.get(sid, 0) == part_akey[(o, pix)]
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
        assert con.execute("SELECT count(*) FROM akey WHERE akey NOT IN (SELECT akey FROM part "
                           "WHERE akey IS NOT NULL)").fetchone()[0] == 0
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
