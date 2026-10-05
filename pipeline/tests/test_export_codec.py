"""The UI export's wire format (`export_codec`, Phase 4): the encoded pair decodes to EXACTLY the
model `build()` makes, its integer references are checked, and nothing a section handle names
ships.

LOSSLESS is proved three ways:
  · a hand-made model with every shape the codec special-cases (a trivial run, a polygon run, a
    branch, part flags, an interned base, a non-default `who`) round-trips — no bundle needed;
  · the real model round-trips through JSON, as a reader receives it;
  · a second decoder written from the words of `field_dictionary.encoding` alone agrees;
  · the files on disk are, byte for byte, the shipped bundle's pair (a stale pair fails).
The comparison with the PRE-COMPRESSION builder (59cd509e) for the same bundle was made once,
outside the suite: every table equal; the differences are the 21,568 unreferenced lake edges, the
`about` closure lists and the guide's closure notes naming a rule set (or an `example` key) instead
of a section, and the dictionary/guide texts this phase rewrote (pipeline/docs/06-ui-data-contract.md Part 6).
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from pipeline.tools import export_codec as K
from pipeline.tools import export_ui_rules as X

BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or X.BUNDLE)


def _through_json(o):
    return json.loads(K.dumps(o))


# ---------------------------------------------------------------------------------------------
# A hand-made model: every shape the codec special-cases
# ---------------------------------------------------------------------------------------------
def _tiny() -> dict:
    def rule(e, r, typ, fam, auth, to, who, rank, **kw):
        return {"id": f"{e}::{r}", "entry_id": e, "rule_id": r, "type": typ, "family": fam,
                "dimension": "daily", "label": "L", "parts": {"what": "L"}, "verbatim": "V",
                "binds": "sections", "fields": {"extents": [{"op": "whole"}]},
                "provenance": {"entry_name": NAMES[e], "authority": auth, "binds_to": to,
                               "rank": rank, "who": who, "scope": "section",
                               "uncertain": False, "why": None}, **kw}
    NAMES = {"r1:a": "A Lake", "z1:t": "Region 1", "zp:p": ""}
    rules = {
        "r1:a::a.r1": rule("r1:a", "a.r1", "retention_limit", "retention", "region", "water",
                           "Region 1 · for this water (A Lake)", 0),
        # a `who` the default cannot say ships as is
        "r1:a::a.r2": rule("r1:a", "a.r2", "advisory", "information", "region", "water",
                           "Region 1 · for this water (the north arm)", 0),
        "z1:t::t.r1": rule("z1:t", "t.r1", "retention_limit", "retention", "region", "region",
                           "Region 1 · region-wide", 3),
        "zp:p::p.r1": rule("zp:p", "p.r1", "bait_restriction", "gear_and_method", "province",
                           "region", "Provincial · province-wide", 4),
    }
    rules["r1:a::a.r2"]["provenance"].update(uncertain=True, why="no_extents: x")
    lic = {"r1:a#d": {"id": "r1:a#d", "entry_id": "r1:a", "record_id": "d", "kind": "designation",
                      "label": "C", "parts": {}, "verbatim": "C", "fields": {},
                      "placement": "sections",
                      "provenance": {"entry_name": "A Lake", "uncertain": False, "why": None}}}

    def entry(eid, kind, name, rs, ls, **kw):
        return {"kind": kind, "chapter": eid.split(":")[0], "name": name, "full_name": None,
                "item_id": None, "matched": [], "mus": [], "pages": [], "symbols": [],
                "scope_note": None, "extents": [], "printed": None, "rules": rs,
                "licensing": ls, **kw}
    entries = {
        "r1:a": entry("r1:a", "water", "A Lake", ["r1:a::a.r1", "r1:a::a.r2"], ["r1:a#d"],
                      item_id="wbk:1", matched=["wbk:1"], pages=[3], see=[]),
        "z1:t": entry("z1:t", "zone", "Region 1", ["z1:t::t.r1"], []),
        "zp:p": entry("zp:p", "province", None, ["zp:p::p.r1"], []),
    }
    base = ["z1:t::t.r1", "zp:p::p.r1"]
    rulesets = {"0": {"sections": 5, "reach": base},
                "1": {"sections": 2, "reach": ["r1:a::a.r1"] + base, "trib": ["r1:a::a.r2"]},
                "2": {"sections": 0, "reach": ["r1:a::a.r1"]}}
    lsets = {"0": {"sections": 2, "trib": ["r1:a#d"]}}
    run = lambda f, t, a, b, **kw: {"from": f, "to": t, "km_from": a, "km_to": b, **kw}  # noqa
    waters = {
        "gnis:1": {"name": "A Creek", "kind": "stream", "sections": 7, "entries": ["r1:a"],
                   "outside_bc": 1, "steelhead": "known", "steelhead_rows": ["r1:a"],
                   "steelhead_source": ["regulations"], "parts": [
                       {"ruleset": "1", "licensing_set": "0", "sections": 4,
                        "anadromous_rainbow": True, "steelhead": "known", "touches": [1],
                        "home_region": ["1"],
                        "runs": [run("source", "mouth", 3.25, 0.0)]},
                       {"ruleset": "0", "licensing_set": None, "sections": 2,
                        "steelhead": "possible", "touches": [0],
                        "runs": [run("x__y", "mouth", 9.5, 3.25),
                                 run("source", "mouth", 4.0, 4.0, branch=True)]},
                       {"ruleset": None, "licensing_set": None, "sections": 1, "touches": [],
                        "runs": [run("bc_border", "x__y", None, None)]}]},
        "wbk:1": {"name": "A Lake", "kind": "lake", "sections": 3, "entries": [], "outside_bc": 0,
                  "steelhead": "possible", "steelhead_source": ["curated list"], "parts": [
                      {"ruleset": "0", "licensing_set": None, "sections": 3, "steelhead":
                       "possible", "touches": [], "runs": [run(None, None, None, None,
                                                               polygon="whole")]}]},
        "wbk:2": {"name": "A Lake — North Arm", "kind": "lake", "sections": 0, "entries": [],
                  "outside_bc": 0, "part_of": "wbk:1", "parts": []},
        "wbk:3": {"name": "B Lake — Part", "kind": "lake", "sections": 1, "entries": [],
                  "outside_bc": 0, "part_of": "wbk:1", "parts": [
                      {"ruleset": "2", "licensing_set": None, "sections": 1, "touches": [],
                       "runs": [run(None, None, None, None, polygon="B Lake — Part")]}]},
    }
    splits = {"x__y": {"name": "X", "kind": "point", "water_id": "gnis:1", "km": 9.5},
              "region_line:1": {"name": "Region 1 boundary", "kind": "region_line",
                                "water_id": None, "km": None}}
    about = {"what": "w", "bundle": {"section_handles": "abc", "reach_digest": "def"}}
    return {"about": about, "guide": {"g": 1}, "field_dictionary": {"encoding": K.ENCODING_TEXT},
            "species": {}, "licences": {}, "entries": entries, "rules": rules, "licensing": lic,
            "rulesets": rulesets, "licensing_sets": lsets, "waters": waters, "splits": splits,
            "index": K.index_of(rules, lic)}


def test_a_hand_made_model_round_trips_through_the_wire():
    doc = _tiny()
    data, guide = X.encode(doc)
    assert K.expand(_through_json(data), _through_json(guide)) == doc
    assert K.wire_problems(data, guide) == []
    # the shapes are the compact ones
    p = data["waters"]["gnis:1"]["parts"]
    assert p[0][3] == 3.25 and p[1][3][1][4] == {"branch": True}
    assert data["waters"]["wbk:1"]["parts"][0][3] == K.POLYGON
    assert data["waters"]["wbk:3"]["parts"][0][3] == K.POLYGON
    assert "sections" not in data["waters"]["gnis:1"] and "steelhead_source" not in \
        data["waters"]["gnis:1"] and data["waters"]["wbk:1"]["steelhead_source"] == ["curated list"]
    assert data["bases"] == [[2, 3]] and data["rulesets"][1] == {"sections": 2, "base": 0,
                                                                 "reach": [0], "trib": [1]}
    r = data["rules"]
    assert "who" not in r[0]["provenance"] and r[1]["provenance"]["who"].endswith("north arm)")
    assert r[1]["provenance"]["uncertain"] is True
    assert "id" not in r[0] and "family" not in r[0] and "rank" not in r[0]["provenance"]
    assert data["entries"]["r1:a"] == {"kind": "water", "name": "A Lake", "item_id": "wbk:1",
                                       "matched": ["wbk:1"], "pages": [3], "see": []}
    assert "index" not in data and "guide" not in data and "guide" in guide


@pytest.mark.parametrize("break_it", [
    "rule family", "entry chapter", "water sections", "unsorted reach", "entry id with ::"])
def test_the_encoder_refuses_what_it_could_not_decode(break_it):
    doc = _tiny()
    if break_it == "rule family":
        doc["rules"]["r1:a::a.r1"]["family"] = "vessel"
    elif break_it == "entry chapter":
        doc["entries"]["r1:a"]["chapter"] = "r2"
    elif break_it == "water sections":
        doc["waters"]["gnis:1"]["sections"] = 8
    elif break_it == "unsorted reach":
        doc["rulesets"]["0"]["reach"] = list(reversed(doc["rulesets"]["0"]["reach"]))
    else:
        doc["entries"]["r1:a::x"] = doc["entries"].pop("r1:a")
    with pytest.raises(K.EncodeError):
        X.encode(doc)


# ---------------------------------------------------------------------------------------------
# A second decoder, written from the WORDS of `field_dictionary.encoding` (never from the codec's
# constants or functions): if the prose and the encoder ever disagree, this one decodes wrong.
# ---------------------------------------------------------------------------------------------
def _decode_from_the_dictionary(data: dict, guide: dict) -> dict:
    rule_ids, lic_ids = data["rule_ids"], data["licensing_ids"]
    fam, rank = data["codec"]["family_of_type"], data["codec"]["rank"]
    # entries: chapter = id before its first ':'; rules/licensing = ids starting `<id>::`/`<id>#`
    empty = {"full_name": None, "item_id": None, "matched": [], "mus": [], "pages": [],
             "symbols": [], "scope_note": None, "extents": [], "printed": None}
    entries = {}
    for eid, e in data["entries"].items():
        d = {**json.loads(json.dumps(empty)), **e, "chapter": eid.split(":", 1)[0]}
        d["rules"] = [r for r in rule_ids if r.startswith(eid + "::")]
        d["licensing"] = [r for r in lic_ids if r.startswith(eid + "#")]
        entries[eid] = d

    def who(eid, a, b, name):
        head = eid.split(":", 1)[0]
        wrote = ("Federal or Parks" if a == "superior" else "Provincial" if a == "province"
                 else "Region " + (head[1:] if head[:1] in "zr" else "").upper())
        if b == "region":
            return f"{wrote} · " + ("province-wide" if a == "province" else "region-wide")
        if b == "water":
            return f"{wrote} · for this water" + (f" ({name})" if name else "")
        assert b == "area" and eid.startswith("r") and name, (eid, b)
        return f"{wrote} · {name}"

    rules = {}
    for rid, r in zip(rule_ids, data["rules"]):
        eid, sub = rid.split("::", 1)
        name = entries[eid]["name"] or ""
        p = {"uncertain": False, "why": None, **r["provenance"], "entry_name": name,
             "rank": rank[f"{r['provenance']['authority']}/{r['provenance']['binds_to']}"]}
        if "who" not in p:              # the default applies only where `who` is absent
            p["who"] = who(eid, p["authority"], p["binds_to"], name)
        rules[rid] = {**r, "id": rid, "entry_id": eid, "rule_id": sub,
                      "family": fam[r["type"]], "provenance": p}
    lic = {}
    for lid, r in zip(lic_ids, data["licensing"]):
        eid, sub = lid.split("#", 1)
        lic[lid] = {**r, "id": lid, "entry_id": eid, "record_id": sub,
                    "provenance": {"uncertain": False, "why": None, **r["provenance"],
                                   "entry_name": entries[eid]["name"] or ""}}
    rulesets = {}
    for i, s in enumerate(data["rulesets"]):
        d = {"sections": s["sections"]}
        if "base" in s or "reach" in s:
            own = (data["bases"][s["base"]] if "base" in s else []) + s.get("reach", [])
            d["reach"] = [rule_ids[j] for j in sorted(own)]
        for via, v in s.items():
            if via not in ("sections", "base", "reach"):
                d[via] = [rule_ids[j] for j in v]
        rulesets[str(i)] = d
    lsets = {str(i): {via: (v if via == "sections" else [lic_ids[j] for j in v])
                      for via, v in s.items()} for i, s in enumerate(data["licensing_sets"])}
    waters = {}
    for wid, w in data["waters"].items():
        parts = []
        for arr in w["parts"]:
            rs, ls, n, runs = arr[:4]
            p = {"ruleset": None if rs is None else str(rs),
                 "licensing_set": None if ls is None else str(ls), "sections": n,
                 "touches": [], **(arr[4] if len(arr) > 4 else {})}
            if runs == "polygon":
                p["runs"] = [{"from": None, "to": None, "km_from": None, "km_to": None,
                              "polygon": w["name"] if w.get("part_of") else "whole"}]
            elif isinstance(runs, (int, float)):
                p["runs"] = [{"from": "source", "to": "mouth", "km_from": runs, "km_to": 0.0}]
            else:
                p["runs"] = [{"from": r[0], "to": r[1], "km_from": r[2], "km_to": r[3],
                              **(r[4] if len(r) > 4 else {})} for r in runs]
            parts.append(p)
        d = {"entries": [], "outside_bc": 0, **{k: v for k, v in w.items() if k != "parts"},
             "parts": parts, "sections": sum(p["sections"] for p in parts)}
        st = {p.get("steelhead") for p in parts}
        if st & {"known", "possible"}:
            d["steelhead"] = "known" if "known" in st else "possible"
        if d.get("steelhead_rows") and "steelhead_source" not in d:
            d["steelhead_source"] = ["regulations"]
        waters[wid] = d
    index = {"rules_by_type": {}, "rules_by_family": {}, "licensing_by_kind": {}}
    for rid in rule_ids:
        index["rules_by_type"].setdefault(rules[rid]["type"], []).append(rid)
        index["rules_by_family"].setdefault(rules[rid]["family"], []).append(rid)
    for lid in lic_ids:
        index["licensing_by_kind"].setdefault(lic[lid]["kind"], []).append(lid)
    return {"about": {k: v for k, v in data["about"].items() if k != "format"},
            "guide": guide["guide"], "field_dictionary": guide["field_dictionary"],
            "species": guide["species"], "licences": data["licences"], "entries": entries,
            "rules": rules, "licensing": lic, "rulesets": rulesets, "licensing_sets": lsets,
            "waters": waters,
            "splits": {k: {"water_id": None, "km": None, **v} for k, v in data["splits"].items()},
            "index": {k: dict(sorted(v.items())) for k, v in index.items()}}


def test_the_dictionary_words_decode_the_hand_made_model():
    data, guide = X.encode(_tiny())
    assert _decode_from_the_dictionary(_through_json(data), _through_json(guide)) == _tiny()


# ---------------------------------------------------------------------------------------------
# The real model
# ---------------------------------------------------------------------------------------------
@pytest.fixture(scope="module")
def doc() -> dict:
    return X.build(BUNDLE)


@pytest.fixture(scope="module")
def pair(doc):
    data, guide = X.encode(doc)
    return _through_json(data), _through_json(guide)


def test_the_shipped_pair_decodes_to_the_model(doc, pair):
    """LOSSLESS: what a reader parses decodes to exactly the model every check proves."""
    assert K.expand(*pair) == doc
    assert X.problems(K.expand(*pair)) == []


def test_the_dictionary_words_decode_the_real_pair(doc, pair):
    """F2: the prose in `field_dictionary.encoding` is enough to decode — a decoder written from
    it alone gives exactly what `export_codec.expand` gives, and the model."""
    assert _decode_from_the_dictionary(*pair) == K.expand(*pair) == doc


def test_the_wire_checks_pass_and_cover_every_key(pair):
    data, guide = pair
    assert K.wire_problems(data, guide) == []
    assert set(data) == set(K.ENCODING_TEXT["data file keys"])
    assert set(guide) == set(K.ENCODING_TEXT["guide file keys"])
    assert guide["field_dictionary"]["encoding"] == K.ENCODING_TEXT


def _mutated(pair, how):
    data, guide = copy.deepcopy(pair)
    if how == "ruleset member out of range":
        data["rulesets"][1].setdefault("reach", []).append(len(data["rules"]))
    elif how == "base out of range":
        s = next(s for s in data["rulesets"] if "base" in s)
        s["base"] = len(data["bases"])
    elif how == "base member out of range":
        data["bases"][0].append(len(data["rules"]) + 7)
    elif how == "licensing member out of range":
        s = next(s for s in data["licensing_sets"] if s.get("reach"))
        s["reach"][0] = -1
    elif how == "part ruleset out of range":
        w = next(w for w in data["waters"].values() if w["parts"])
        w["parts"][0][0] = len(data["rulesets"])
    elif how == "part licensing set is a string":
        w = next(w for w in data["waters"].values() if w["parts"] and w["parts"][0][1] is not None)
        w["parts"][0][1] = str(w["parts"][0][1])
    elif how == "a bool for an index":
        s = next(s for s in data["rulesets"] if s.get("reach"))
        s["reach"][0] = True
    elif how == "parallel ids out of step":
        data["rule_ids"].pop()
    elif how == "guide from another bundle":
        guide["about"]["bundle"] = {**guide["about"]["bundle"], "section_handles": "0000"}
    elif how == "undocumented key":
        data["extra"] = {}
    return data, guide


@pytest.mark.parametrize("how", [
    "ruleset member out of range", "base out of range", "base member out of range",
    "licensing member out of range", "part ruleset out of range",
    "part licensing set is a string", "a bool for an index", "parallel ids out of step",
    "guide from another bundle", "undocumented key"])
def test_an_integer_reference_that_does_not_resolve_is_refused(pair, how):
    """MUTATION: every integer reference the wire carries is checked (the string references are
    checked on the decoded model by `problems`)."""
    assert K.wire_problems(*_mutated(pair, how)), how


def _keys(o):
    if isinstance(o, dict):
        for k, v in o.items():
            yield k
            yield from _keys(v)
    elif isinstance(o, list):
        for v in o:
            yield from _keys(v)


HANDLE_KEYS = {"sid", "section", "section_id", "dense_id", "example_section"}


def test_no_section_handle_ships_in_either_file(pair):
    """AGENTS 5: neither file names a section — not as a key, not in the `about` lists (which
    name the rule set a finding was made on), not in the guide's closure notes (which name a
    `ruleset` or an `example` KEY that `example_sid` resolves inside the bundle)."""
    data, guide = pair
    assert not HANDLE_KEYS & set(_keys(data))
    assert not HANDLE_KEYS & set(_keys(guide))
    for k in ("release_under_closure", "quota_under_closure"):
        assert all(x["ruleset"].isdigit() for x in data["about"][k])
    notes = [n for e in guide["guide"]["gotchas"]["closures_combine"]["entries"].values()
             for n in e["notes"]]
    assert [n for n in notes if n["kind"] == "row_rule"] and all(
        n["ruleset"].isdigit() for n in notes if n["kind"] == "row_rule")
    assert all(set(n["example"]) == {"ruleset", "anadromous_rainbow", "steelhead_rules"}
               for n in notes if n["kind"] == "row_closure")


def test_only_named_lake_edges_ship(doc):
    """21,636 lake edges are in the bundle's `split` table; only the ones an extent, a run or
    `same_place_as` names ship, and every one an extent names does (E2's check, `dangling`)."""
    named = set(X._split_refs([x["fields"] for x in doc["rules"].values()]))
    named |= set(X._split_refs([x["fields"] for x in doc["licensing"].values()]))
    named |= set(X._split_refs([e.get("extents") for e in doc["entries"].values()]))
    edges = {k for k, x in doc["splits"].items() if x["kind"] == "lake_edge"}
    assert edges and edges <= named | {x.get("same_place_as") for x in doc["splits"].values()}
    assert not [k for k in named if k not in doc["splits"]]


def test_the_files_on_disk_are_the_bundles_pair(doc):
    """The canonical files are DERIVED from the shipped bundle and must be its pair, byte for
    byte (`python -m pipeline.deliver export` rewrites them). Skipped only where there is nothing
    to compare (no export written, or the suite pointed at a side bundle); a stale pair fails."""
    if BUNDLE != X.BUNDLE or not X.OUT.is_file():
        pytest.skip("no shipped export, or a side bundle")
    g = X.guide_path(X.OUT)
    assert g.is_file(), f"{X.OUT} has no {g.name} beside it"
    shipped = json.loads(X.OUT.read_text(encoding="utf-8"))
    assert shipped["about"].get("bundle") == doc["about"]["bundle"], \
        "the shipped export is another bundle's — rerun `python -m pipeline.deliver export`"
    data, guide = X.encode(doc)
    assert X.OUT.read_text(encoding="utf-8") == K.dumps(data)
    assert g.read_text(encoding="utf-8") == K.dumps(guide)
    assert X.load(X.OUT) == doc
    assert _decode_from_the_dictionary(json.loads(X.OUT.read_text(encoding="utf-8")),
                                       json.loads(g.read_text(encoding="utf-8"))) == doc


def test_a_refused_export_fails_the_deliver_command(tmp_path, monkeypatch):
    """F5: `python -m pipeline.deliver export` exits non-zero when the export refuses, so the old
    pair left on disk is never taken for a fresh one."""
    from pipeline import deliver
    from pipeline.deliver import __main__ as D
    assert deliver is not None
    monkeypatch.setattr(X, "main", lambda argv=None: 1)
    with pytest.raises(SystemExit) as e:
        D.main(["export", "--out", str(tmp_path / "x.json")])
    assert e.value.code not in (0, None)
