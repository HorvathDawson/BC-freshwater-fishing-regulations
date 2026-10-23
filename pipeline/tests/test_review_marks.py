"""Curation review phase 1 (T97, curation-review PLAN §4): marks that last and mean something.

A mark is held against four fingerprints (`pipeline.regs.review.fingerprint`); `marks.stale_reasons`
is the one staleness decision; `carry_marks` re-stamps what a migration declares unchanged.

The acceptance of PLAN phase 1, each against an OUTPUT, never a copy of the input:
  * a no-op normalisation over the WHOLE corpus (the pipeline's own writer) stales 0 marks — and
    one real edit in the same run stales exactly 1;
  * a split move, an added split (owner 2026-10-23: "add splits and rebuild then continue review"),
    an override edit, a steelhead edit, a label wording change and a verbatim edit each stale
    exactly the affected marks, with the right reason;
  * `carry_marks` is pinned by mutation: three lying versions of it are each caught.

The fixture bundle is written with the bundle's own schema (`pipeline/deliver/bundle/schema.sql`)
and read by the same `BundleFacts.read` the app uses; a "rebuild" here is writing that fixture
again with what a build would change. No generated data is read except in the one `needs_bundle`
test, which reads the real bundle.
"""
from __future__ import annotations

import copy
import json
import random
import re
import sqlite3
from pathlib import Path

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.parsing import io
from pipeline.regs.parsing.catalogue import CatalogueEntry
from pipeline.regs.review import carry, carry_marks
from pipeline.regs.review import fingerprint as F
from pipeline.regs.review import marks as M

REPO = Path(__file__).resolve().parents[2]
SCHEMA = REPO / "pipeline" / "deliver" / "bundle" / "schema.sql"
SHA = "0" * 64

# --------------------------------------------------------------------------------------------
# Fixtures: three waters, their splits, their overrides, a bundle
# --------------------------------------------------------------------------------------------

ADAM, AMOR, BIG = "r1:adam_river@1-10", "r1:amor_de_cosmos_creek@1-10", "r1:big_lake@1-10"


def _rule(rid: str, verbatim: str, extents: list | None = None) -> dict:
    return {"rule_id": rid, "type": "tackle_restriction", "verbatim": verbatim,
            "gear": [{"slot": "lure", "only": ["artificial_fly"]}],
            "extents": extents or [{"op": "whole"}]}


def _entry(eid: str, name: str, gnis: str, rules: list[dict]) -> dict:
    return {"entry_id": eid, "name": name, "region": "1",
            "regs_verbatim": "; ".join(r["verbatim"] for r in rules), "source_pages": [14],
            "matched": [gnis], "extents": [{"op": "whole"}], "rules": rules}


def corpus() -> dict[str, dict]:
    return {
        ADAM: _entry(ADAM, "ADAM RIVER", "gnis:28136", [
            _rule("adam_river.r1", "Artificial fly only upstream of Eve River, to Hwy 19 bridge",
                  [{"op": "between", "splits": ["adam__eve", "adam__bridge"]}]),
            _rule("adam_river.r2", "Artificial fly only")]),
        AMOR: _entry(AMOR, "AMOR DE COSMOS CREEK", "gnis:5480", [
            _rule("amor_de_cosmos_creek.r1", "Artificial fly only")]),
        BIG: _entry(BIG, "BIG LAKE", "gnis:99", [_rule("big_lake.r1", "Artificial fly only")]),
    }


def splits_doc() -> dict:
    return {"waterbodies": [
        {"name": "ADAM RIVER", "applies_to": {"gnis_id": "28136"}, "splits": [
            {"id": "adam__eve", "label": "Eve River", "kind": "confluence",
             "anchor": {"type": "confluence", "tributary_wsc": "920-757566-086842",
                        "_name": "Eve River"}, "note": "", "_note": "x"},
            {"id": "adam__bridge", "label": "Hwy 19 bridge", "kind": "point",
             "anchor": {"type": "point", "coord": [-126.13643, 50.36229], "is_lonlat": True},
             "note": ""}]},
        {"name": "AMOR DE COSMOS CREEK", "applies_to": {"gnis_id": "5480"}, "splits": [
            {"id": "amor__falls", "label": "upper falls", "kind": "point",
             "anchor": {"type": "point", "coord": [-125.66021, 50.33685], "is_lonlat": True},
             "note": ""}]},
    ]}


def overrides() -> tuple:
    return tuple({"type": "override", "gnis_ids": [g], "note": "pin",
                  "criteria": {"name_verbatim": n, "region": "REGION 1 - Vancouver Island",
                               "mus": ["1-10"]}}
                 for n, g in (("ADAM RIVER", "28136"), ("AMOR DE COSMOS CREEK", "5480")))


#: what the fixture build binds: rule -> [(sid, item ord, lo_m, hi_m)]
BINDS = {(ADAM, "adam_river.r1"): [(1, 1, 0, 4000)],
         (ADAM, "adam_river.r2"): [(1, 1, 0, 4000), (2, 1, 4000, 9000)],
         (AMOR, "amor_de_cosmos_creek.r1"): [(3, 2, 0, 3000), (4, 2, 3000, 5000)],
         (BIG, "big_lake.r1"): [(5, 3, None, None)]}
ITEMS = {1: "gnis:28136", 2: "gnis:5480", 3: "gnis:99"}


def make_bundle(path: Path, entries: dict[str, dict], *, labels: dict | None = None,
                steelhead: dict | None = None, binds: dict | None = None) -> Path:
    """A bundle in the real schema holding what a build of `entries` would: each rule's label and
    parts, its sections, each water's parts. `labels` / `steelhead` / `binds` override what the
    build would write (a wording change, a steelhead list edit, a new cut)."""
    path.unlink(missing_ok=True)
    db = sqlite3.connect(path)
    db.executescript(SCHEMA.read_text())
    labels, steelhead, binds = labels or {}, steelhead or {}, binds or BINDS
    for o, iid in ITEMS.items():
        db.execute("INSERT INTO item (ord, item_id, name, kind) VALUES (?,?,?,?)",
                   (o, iid, iid, "lake" if o == 3 else "stream"))
        db.execute("INSERT INTO part (ord, part_ix, province_except, steelhead_water, steelhead, "
                   "steelhead_rules, home_regions, tidal, sections, rep_sid) "
                   "VALUES (?,0,'',0,?,0,'1',0,1,1)", (o, steelhead.get(iid)))
    set_id = 0
    for eid, e in entries.items():
        db.execute("INSERT INTO entry (entry_id, matched) VALUES (?,?)",
                   (eid, json.dumps(e["matched"])))
        for r in e["rules"]:
            key = (eid, r["rule_id"])
            db.execute("INSERT INTO rule (entry_id, rule_id, type, family, dimension, label, "
                       "parts) VALUES (?,?,?,?,?,?,?)",
                       (eid, r["rule_id"], r["type"], "gear_and_method", "lure",
                        labels.get(key, r["verbatim"]), json.dumps({"what": r["verbatim"]})))
            for sid, o, lo, hi in binds.get(key, []):
                set_id += 1
                db.execute("INSERT INTO ruleset VALUES (?,?,?,?)", (set_id, eid, r["rule_id"],
                                                                     "reach"))
                db.execute("INSERT INTO section_ruleset (sid, set_id, key_ix) VALUES (?,?,0)",
                           (sid * 1000 + set_id, set_id))
                db.execute("INSERT INTO item_section VALUES (?,?)", (o, sid * 1000 + set_id))
                if lo is not None:
                    db.execute("INSERT INTO section_span (sid, lo_m, hi_m, lo, hi) "
                               "VALUES (?,?,?,0,0)", (sid * 1000 + set_id, lo, hi))
    db.commit()
    db.close()
    return path


def ctx_of(entries=None, *, splits=None, ovr=None, bundle=None) -> F.Context:
    return F.Context(splits=splits if splits is not None else splits_doc(),
                     overrides=ovr if ovr is not None else overrides(),
                     bundle=F.BundleFacts.read(bundle) if bundle else None)


def mark(entry: dict | None, ctx: F.Context, *, scope="entry", rule_id=None, target=None,
         verdict="verified") -> M.Mark:
    if scope == "split":
        prints = F.split_fingerprints(target, ctx)
        answers = {"P": "y"}
    else:
        prints = F.entry_fingerprints(entry, ctx, rule_id=rule_id)
        answers = {k: "y" for k in "TRPA"}
    return M.Mark(scope=scope, target=target or entry["entry_id"], rule_id=rule_id,
                  verdict=verdict, answers=answers, reviewer="tester", date="2026-10-23",
                  pdf_sha256=SHA, pages=[14], prints=prints)


def states(ms: list[M.Mark], entries: dict, ctx: F.Context) -> dict:
    """target(/rule) -> stale reasons NOW (empty = holds)."""
    out = {}
    for m in M.latest(ms).values():
        if m.scope == "split":
            try:
                now = F.split_fingerprints(m.target, ctx)
            except KeyError:
                now = None
        else:
            now = F.entry_fingerprints(entries[m.target], ctx) if m.target in entries else None
        key = m.target + (f"/{m.rule_id}" if m.rule_id else "") + \
            ("#split" if m.scope == "split" else "")
        out[key] = [r.value for r in M.stale_reasons(m, now)]
    return out


@pytest.fixture
def world(tmp_path):
    entries = corpus()
    for e in entries.values():                       # the fixture is the model's, not a sketch
        CatalogueEntry.model_validate(e)
    bundle = make_bundle(tmp_path / "bundle.sqlite", entries)
    ctx = ctx_of(bundle=bundle)
    ms = [mark(e, ctx) for e in entries.values()]
    ms.append(mark(entries[ADAM], ctx, scope="rule", rule_id="adam_river.r1"))
    ms.append(mark(None, ctx, scope="split", target="adam__bridge"))
    ms.append(mark(None, ctx, scope="split", target="amor__falls"))
    return {"entries": entries, "bundle": bundle, "ctx": ctx, "marks": ms, "tmp": tmp_path}


FRESH = {ADAM: [], AMOR: [], BIG: [], f"{ADAM}/adam_river.r1": [], "adam__bridge#split": [],
         "amor__falls#split": []}


def test_every_mark_holds_on_the_vintage_it_was_made_on(world):
    assert states(world["marks"], world["entries"], world["ctx"]) == FRESH


# --------------------------------------------------------------------------------------------
# Splits mid-review (owner 2026-10-23)
# --------------------------------------------------------------------------------------------

def test_adding_a_split_stales_only_that_waters_entries_binding_moved_and_queues_the_split(world):
    """Add a split on Amor de Cosmos Creek, then "rebuild" (the new cut makes one section two):
    only the Amor entry goes stale, "binding moved"; Adam, Big Lake, every rule mark and the old
    split marks hold; the new split reads `unmarked` — queued for its own split mark."""
    sp = splits_doc()
    sp["waterbodies"][1]["splits"].append(
        {"id": "amor__logging_bridge", "label": "logging road bridge", "kind": "point",
         "anchor": {"type": "point", "coord": [-125.66758, 50.33945], "is_lonlat": True}})
    binds = dict(BINDS)
    binds[(AMOR, "amor_de_cosmos_creek.r1")] = [(3, 2, 0, 1500), (6, 2, 1500, 3000),
                                                (4, 2, 3000, 5000)]
    for bundle in (None, make_bundle(world["tmp"] / "b2.sqlite", world["entries"], binds=binds)):
        ctx = ctx_of(splits=sp, bundle=bundle or world["bundle"])
        got = states(world["marks"], world["entries"], ctx)
        assert got == {**FRESH, AMOR: ["binding moved"]}, ("before build" if bundle is None
                                                          else "after build")
    queued = M.split_states(world["marks"], ctx)
    assert queued["amor__logging_bridge"]["status"] == "unmarked"
    assert {k: v["status"] for k, v in queued.items() if k != "amor__logging_bridge"} == \
        {"adam__eve": "unmarked", "adam__bridge": "verified", "amor__falls": "verified"}


def test_moving_a_split_stales_its_water_and_its_own_mark_binding_moved(world):
    sp = splits_doc()
    sp["waterbodies"][0]["splits"][1]["anchor"]["coord"] = [-126.2, 50.4]
    got = states(world["marks"], world["entries"], ctx_of(splits=sp, bundle=world["bundle"]))
    assert got == {**FRESH, ADAM: ["binding moved"], f"{ADAM}/adam_river.r1": ["binding moved"],
                   "adam__bridge#split": ["binding moved"]}


def test_a_split_label_or_note_edit_moves_nothing(world):
    sp = splits_doc()
    sp["waterbodies"][0]["splits"][1].update(label="Highway 19 bridge", note="checked", _note="y",
                                             _offset_m=40, _coord=[-126.1, 50.3])
    sp["waterbodies"][0]["splits"][0]["anchor"]["_name"] = "Eve R."
    assert states(world["marks"], world["entries"], ctx_of(splits=sp, bundle=world["bundle"])) \
        == FRESH


def test_a_rebuild_that_moves_a_rules_reach_stales_it_binding_moved(world):
    binds = dict(BINDS)
    binds[(ADAM, "adam_river.r2")] = [(1, 1, 0, 4000), (2, 1, 4000, 7000)]   # 9 km -> 7 km
    b = make_bundle(world["tmp"] / "b3.sqlite", world["entries"], binds=binds)
    assert states(world["marks"], world["entries"], ctx_of(bundle=b)) == \
        {**FRESH, ADAM: ["binding moved"]}


# --------------------------------------------------------------------------------------------
# Override, steelhead, label, verbatim: exactly the affected marks, the right reason
# --------------------------------------------------------------------------------------------

def test_an_override_edit_stales_the_rows_it_decides_binding_moved(world):
    ovr = list(copy.deepcopy(overrides()))
    ovr[0]["gnis_ids"] = ["28137"]
    got = states(world["marks"], world["entries"], ctx_of(ovr=tuple(ovr), bundle=world["bundle"]))
    assert got == {**FRESH, ADAM: ["binding moved"], f"{ADAM}/adam_river.r1": ["binding moved"]}
    ovr = list(copy.deepcopy(overrides()))
    ovr[1]["note"] = "a curator's note says nothing about the water"
    assert states(world["marks"], world["entries"],
                  ctx_of(ovr=tuple(ovr), bundle=world["bundle"])) == FRESH


def test_a_steelhead_edit_stales_the_waters_entries_answer_changed(world):
    """The steelhead list binds no rule (AGENTS 54): it shows in the bundle's water parts, and
    only on the entries of the water it names."""
    b = make_bundle(world["tmp"] / "b4.sqlite", world["entries"], steelhead={"gnis:5480": 2})
    assert states(world["marks"], world["entries"], ctx_of(bundle=b)) == \
        {**FRESH, AMOR: ["answer changed"]}


def test_a_label_wording_change_stales_that_rule_answer_changed(world):
    b = make_bundle(world["tmp"] / "b5.sqlite", world["entries"],
                    labels={(ADAM, "adam_river.r2"): "Fly fishing only"})
    # the entry mark answered for r2 too; the rule mark on r1 did not
    assert states(world["marks"], world["entries"], ctx_of(bundle=b)) == \
        {**FRESH, ADAM: ["answer changed"]}


def test_a_verbatim_edit_is_book_text_and_a_quote_edit_is_the_rule(world):
    e = copy.deepcopy(world["entries"])
    e[AMOR]["regs_verbatim"] = "Artificial fly only (see p.14)"
    assert states(world["marks"], e, world["ctx"]) == {**FRESH, AMOR: ["book text changed"]}
    e = copy.deepcopy(world["entries"])
    e[ADAM]["rules"][1]["verbatim"] = "Artificial fly only, all year"
    assert states(world["marks"], e, world["ctx"]) == {**FRESH, ADAM: ["rule changed"]}


def test_a_rule_edit_a_new_rule_and_awaiting_build(world):
    e = copy.deepcopy(world["entries"])
    e[ADAM]["rules"][1]["gear"] = [{"slot": "lure", "only": ["artificial_lure"]}]
    assert states(world["marks"], e, world["ctx"]) == {**FRESH, ADAM: ["rule changed"]}
    e = copy.deepcopy(world["entries"])
    e[BIG]["rules"].append(_rule("big_lake.r2", "Artificial fly only"))
    e[BIG]["regs_verbatim"] = world["entries"][BIG]["regs_verbatim"]
    # a new rule the bundle has not seen: the entry mark never answered for it
    assert states(world["marks"], e, world["ctx"]) == {**FRESH, BIG: ["rule changed"]}
    e = copy.deepcopy(world["entries"])
    e[BIG]["rules"][0]["rule_id"] = "big_lake.r9"         # same meaning, a new id: follows it,
    assert states(world["marks"], e, world["ctx"]) == {    # but the bundle has no copy of it yet
        **FRESH, BIG: ["awaiting build"]}


def test_a_renumbered_rule_keeps_its_mark_and_a_mark_never_lands_on_another_rule(world):
    e = copy.deepcopy(world["entries"])
    r1, r2 = e[ADAM]["rules"]
    r1["rule_id"], r2["rule_id"] = "adam_river.r2", "adam_river.r1"
    ctx = ctx_of()                                          # before a rebuild
    ms = [mark(world["entries"][ADAM], ctx),
          mark(world["entries"][ADAM], ctx, scope="rule", rule_id="adam_river.r1")]
    assert states(ms, e, ctx) == {ADAM: [], f"{ADAM}/adam_river.r1": []}
    e[ADAM]["rules"][0]["verbatim"] = "Artificial fly only upstream of the Eve, to the bridge"
    e[ADAM]["regs_verbatim"] = world["entries"][ADAM]["regs_verbatim"]
    assert states(ms, e, ctx) == {ADAM: ["rule changed"], f"{ADAM}/adam_river.r1": ["rule changed"]}


def test_a_folded_id_is_the_same_water():
    """Item ids are folded through the registry's absorbed-id map before they are hashed."""
    class Item:
        ref_ids = ("gnis:5480",)
    e = corpus()[AMOR]
    folded = copy.deepcopy(e)
    folded["matched"] = ["wbk:1"]
    reg = {"gnis:5480": Item()}

    import pipeline.atlas.registry.flowing as flowing
    orig = flowing.alias_map
    flowing.alias_map = lambda registry: {"wbk:1": "gnis:5480"}
    try:
        ctx = F.Context(splits=splits_doc(), overrides=overrides(), registry=reg)
        assert F.entry_fingerprints(folded, ctx) == F.entry_fingerprints(e, ctx)
    finally:
        flowing.alias_map = orig


# --------------------------------------------------------------------------------------------
# Adversarial review (T97): twin rules, rules naming rules, a carry across a rebuild, the lock
# --------------------------------------------------------------------------------------------

TWIN = "r1:twin_creek@1-10"


def _twins() -> dict:
    """Two rules that say the SAME thing on two stretches (38 rules in 16 entries do this)."""
    rules = [_rule("twin_creek.r1", "No fishing", [{"op": "between",
                                                    "splits": ["adam__eve", "adam__bridge"]}]),
             _rule("twin_creek.r2", "No fishing")]
    return {TWIN: _entry(TWIN, "TWIN CREEK", "gnis:28136", rules)}


def test_twin_rules_a_mark_follows_its_own_twin_never_the_other(world):
    entries, ctx = _twins(), ctx_of()
    ms = [mark(entries[TWIN], ctx), mark(entries[TWIN], ctx, scope="rule", rule_id="twin_creek.r1"),
          mark(entries[TWIN], ctx, scope="rule", rule_id="twin_creek.r2")]
    holds = {TWIN: [], f"{TWIN}/twin_creek.r1": [], f"{TWIN}/twin_creek.r2": []}
    swapped = copy.deepcopy(entries)                    # a re-parse numbers them the other way
    a, b = swapped[TWIN]["rules"]
    a["rule_id"], b["rule_id"] = "twin_creek.r2", "twin_creek.r1"
    assert states(ms, swapped, ctx) == holds
    now = F.entry_fingerprints(swapped[TWIN], ctx)
    assert M.held_rule(ms[1], now) == "twin_creek.r2" and M.held_rule(ms[2], now) == "twin_creek.r1"
    edited = copy.deepcopy(entries)                     # r1's rule changes; r2 still says "No fishing"
    edited[TWIN]["rules"][0]["gear"] = [{"slot": "lure", "only": ["artificial_lure"]}]
    assert states(ms, edited, ctx) == {**holds, TWIN: ["rule changed"],
                                       f"{TWIN}/twin_creek.r1": ["rule changed"]}
    assert M.held_rule(ms[1], F.entry_fingerprints(edited[TWIN], ctx)) is None


ZONE = "z1:quota_zone"


def _zone() -> dict:
    """A zone entry whose rules NAME each other by id (`within`, `siblings`), as 130 zone rules do."""
    def r(rid, verbatim, **kw):
        return {"rule_id": rid, "type": "tackle_restriction", "verbatim": verbatim,
                "gear": [{"slot": "lure", "only": ["artificial_fly"]}], "extents": [{"op": "whole"}],
                **kw}
    rules = [r("quota_zone.r1", "Artificial fly only"),
             r("quota_zone.r2", "Artificial fly only, in streams", within="quota_zone.r1"),
             r("quota_zone.r3", "Artificial fly only, elsewhere",
               extents=[{"op": "rest", "siblings": ["quota_zone.r2"]}])]
    return {ZONE: {"entry_id": ZONE, "name": "QUOTA ZONE", "region": "1", "regs_verbatim": "x",
                   "source_pages": [14], "matched": ["gnis:28136"], "rules": rules}}


def _renumber(entry: dict, mapping: dict) -> dict:
    """`entry` with its rules renumbered by `mapping`, every reference to them rewritten too."""
    out = copy.deepcopy(entry)
    for rule in out.get("rules") or []:
        rule["rule_id"] = mapping.get(rule["rule_id"], rule["rule_id"])
        for k in F.RULE_REF_FIELDS:
            if rule.get(k) in mapping:
                rule[k] = mapping[rule[k]]
        for k in ("extents", "tributary_excludes"):
            for x in rule.get(k) or []:
                if isinstance(x, dict) and x.get("siblings"):
                    x["siblings"] = [mapping.get(s, s) for s in x["siblings"]]
    return out


def check_renumber(entries: dict, ctx: F.Context) -> dict:
    """The stale marks after renumbering every entry's rules backwards (refs rewritten): every
    entry mark and every rule mark, which should all hold."""
    ms = [mark(e, ctx) for e in entries.values()]
    ms += [mark(e, ctx, scope="rule", rule_id=r["rule_id"]) for e in entries.values()
           for r in e.get("rules") or []]
    after = {}
    for eid, e in entries.items():
        ids = [r["rule_id"] for r in e.get("rules") or []]
        after[eid] = _renumber(e, dict(zip(ids, [f"{i}.renumbered" for i in reversed(ids)])))
    return {k: v for k, v in states(ms, after, ctx).items() if v}


def test_rules_naming_rules_survive_a_renumber_and_see_what_they_name_change(world):
    entries, ctx = _zone(), ctx_of()
    assert check_renumber(entries, ctx) == {}
    ms = [mark(entries[ZONE], ctx, scope="rule", rule_id=f"quota_zone.r{i}") for i in (1, 2, 3)]
    edited = copy.deepcopy(entries)                     # the rule r2 is `within` changes
    edited[ZONE]["rules"][0]["gear"] = [{"slot": "lure", "only": ["artificial_lure"]}]
    assert states(ms, edited, ctx) == {f"{ZONE}/quota_zone.r1": ["rule changed"],
                                       f"{ZONE}/quota_zone.r2": ["rule changed"],
                                       f"{ZONE}/quota_zone.r3": []}


def test_the_renumber_check_is_pinned_by_mutation(monkeypatch):
    monkeypatch.setattr(F, "_deref", lambda record, named: dict(record))   # ids hashed as written
    assert check_renumber(_zone(), ctx_of()), "an id-bound reference went unseen"


def test_renumbering_every_rule_of_the_real_corpus_stales_no_mark():
    """Every entry of the catalogue, its rules numbered backwards and every reference rewritten:
    no entry mark and no rule mark goes stale."""
    entries = io.read_entries_dir(CURATED.regulations.entries.catalogue)
    ctx = F.Context(splits=json.loads(CURATED.waters.splits.read_text(encoding="utf-8")),
                    overrides=tuple(json.loads(CURATED.regulations.overrides.read_text(
                        encoding="utf-8"))))
    stale = check_renumber(entries, ctx)
    assert stale == {}, f"{len(stale)} marks stale after a renumber: {list(stale.items())[:5]}"


def check_carry_across_a_rebuild(carry_fn, world) -> list[str]:
    """A split rename (declares ADAM unchanged) whose after-context reads a REBUILT bundle in which
    ADAM r2's label changed: the copy must still read "answer changed"."""
    b = make_bundle(world["tmp"] / "b-carry.sqlite", world["entries"],
                    labels={(ADAM, "adam_river.r2"): "Fly fishing only"})
    after = _rename(world["entries"], "adam__bridge", "adam__hwy19")
    ctx_a = ctx_of(splits=_renamed_splits("adam__bridge", "adam__hwy19"), bundle=b)
    new = carry_fn(world["entries"], after, marks=world["marks"], unchanged={ADAM}, by="rename",
                   ctx_before=world["ctx"], ctx_after=ctx_a,
                   renamed_splits={"adam__bridge": "adam__hwy19"})
    got = states(world["marks"] + list(new), after, ctx_a)
    problems = []
    if got.get(ADAM) != ["answer changed"]:
        problems.append(f"ADAM {got.get(ADAM)}: the rebuild's label change was laundered")
    if got.get(f"{ADAM}/adam_river.r1") != []:
        problems.append(f"ADAM r1 {got.get(f'{ADAM}/adam_river.r1')}: the rename was not carried")
    return problems


def test_carry_marks_never_launders_a_rebuild(world, monkeypatch):
    assert check_carry_across_a_rebuild(carry_marks, world) == []
    monkeypatch.setattr(carry, "_keep_bundle_half", lambda was, now: now)
    assert check_carry_across_a_rebuild(carry_marks, world), "a laundered answer went unseen"


_APPENDER = """
import sys
from pathlib import Path
from pipeline.regs.review import marks as M
store, n = Path(sys.argv[1]), int(sys.argv[2])
one = M.load(store.parent / "one.jsonl")
for _ in range(n):
    M.append(store, one)
"""


def _appenders(store: Path, k: int, n: int):
    import subprocess
    import sys
    return [subprocess.Popen([sys.executable, "-c", _APPENDER, str(store), str(n)], cwd=REPO)
            for _ in range(k)]


def test_concurrent_appends_from_several_processes_lose_no_line(world, tmp_path):
    store = tmp_path / "marks.jsonl"
    store.write_text("")
    (tmp_path / "one.jsonl").write_text(M.line(world["marks"][0]))
    procs = _appenders(store, 4, 25)
    assert all(p.wait(timeout=240) == 0 for p in procs)
    assert len(M.load(store)) == 100


def test_the_store_lock_holds_off_another_process(world, tmp_path):
    import time
    store = tmp_path / "marks.jsonl"
    store.write_text("")
    (tmp_path / "one.jsonl").write_text(M.line(world["marks"][0]))
    with M.store_lock(store):
        (p,) = _appenders(store, 1, 1)
        time.sleep(3)
        assert p.poll() is None and store.read_text() == "", "it wrote while the lock was held"
    assert p.wait(timeout=120) == 0 and len(M.load(store)) == 1


# --------------------------------------------------------------------------------------------
# A no-op normalisation over the WHOLE corpus: 0 stale marks
# --------------------------------------------------------------------------------------------

def _shuffled(d: dict, rnd: random.Random) -> dict:
    ks = list(d)
    rnd.shuffle(ks)
    return {k: d[k] for k in ks}


def test_a_noop_normalisation_of_the_whole_corpus_stales_no_mark(tmp_path):
    src = CURATED.regulations.entries.catalogue
    ctx = F.Context(splits=json.loads(CURATED.waters.splits.read_text(encoding="utf-8")),
                    overrides=tuple(json.loads(CURATED.regulations.overrides.read_text(
                        encoding="utf-8"))))
    before = io.read_entries_dir(src)
    ms = [mark(e, ctx) for e in before.values()]
    rnd = random.Random(7)
    for p in sorted(src.glob("region-*.json")):           # the pipeline's own writer, every file
        region = p.stem.split("region-", 1)[1]
        out = tmp_path / p.name
        out.write_bytes(p.read_bytes())
        io.write_entryfile(out, region, [CatalogueEntry.model_validate(_shuffled(e, rnd))
                                         for e in io.read_entryfile(p).values()])
    after = io.read_entries_dir(tmp_path)
    assert set(after) == set(before) and len(after) > 1000
    # and spelled differently still: every rule's keys shuffled, its defaults spelled out
    for e in after.values():
        e["rules"] = [_shuffled({"obligation": "must", **r}, rnd) for r in e.get("rules") or []]
    stale = {k: v for k, v in states(ms, after, ctx).items() if v}
    assert stale == {}, f"{len(stale)} marks stale after a no-op: {list(stale.items())[:5]}"
    # the check has teeth: one real edit in the same corpus stales exactly that mark
    eid = next(k for k, e in after.items() if e.get("rules"))
    after[eid]["rules"][0]["verbatim"] += " (edited)"
    assert {k: v for k, v in states(ms, after, ctx).items() if v} == {eid: ["rule changed"]}


# --------------------------------------------------------------------------------------------
# carry_marks, pinned by mutation
# --------------------------------------------------------------------------------------------

def _rename(entries: dict, old: str, new: str) -> dict:
    out = copy.deepcopy(entries)
    for e in out.values():
        for r in e["rules"]:
            for x in r.get("extents") or []:
                x["splits"] = [new if s == old else s for s in x.get("splits") or []]
    return out


def _renamed_splits(old: str, new: str) -> dict:
    sp = splits_doc()
    for wb in sp["waterbodies"]:
        for s in wb["splits"]:
            if s["id"] == old:
                s["id"] = new
    return sp


def check_carry(carry_fn, world) -> list[str]:
    """What `carry_fn` (a `carry_marks`) must do, as problems. Scenario: rename `adam__bridge`
    (a migration that moves nothing, declaring ADAM and the split unchanged), while BIG LAKE's
    rule is edited by the same pass but NOT declared, and AMOR's mark was already stale before."""
    before = copy.deepcopy(world["entries"])
    ms = list(world["marks"])
    stale_amor = copy.deepcopy(before)
    stale_amor[AMOR]["regs_verbatim"] = "edited before the migration"
    before = stale_amor                                 # AMOR's mark is stale going in
    after = _rename(before, "adam__bridge", "adam__hwy19")
    after[BIG]["rules"][0]["verbatim"] = "Artificial fly only (changed by the pass)"
    ctx_b = ctx_of(bundle=world["bundle"])
    ctx_a = ctx_of(splits=_renamed_splits("adam__bridge", "adam__hwy19"), bundle=world["bundle"])
    new = carry_fn(before, after, marks=ms, unchanged={ADAM, AMOR}, by="test-rename",
                   ctx_before=ctx_b, ctx_after=ctx_a,
                   renamed_splits={"adam__bridge": "adam__hwy19"})
    got = states(ms + list(new), after, ctx_a)
    want = {ADAM: [], f"{ADAM}/adam_river.r1": [], AMOR: ["book text changed"],
            BIG: ["rule changed"], "adam__bridge#split": [], "adam__hwy19#split": [],
            "amor__falls#split": []}
    problems = []
    if got.pop("adam__bridge#split", None) not in (["rule changed"], [], None):
        problems.append("the old split id's mark did something odd")
    want.pop("adam__bridge#split")
    for k, v in want.items():
        if got.get(k) != v:
            problems.append(f"{k}: {got.get(k)} != {v}")
    if any(m.carried_by != "test-rename" for m in new):
        problems.append("a carried mark does not name its migration")
    if any(m.reviewer != "tester" or m.verdict != "verified" for m in new):
        problems.append("a carried mark changed who or what")
    return problems


def test_carry_marks_does_what_it_says(world):
    assert check_carry(carry_marks, world) == []


def test_carry_marks_is_pinned_by_mutation(world, monkeypatch):
    def carries_everything(before, after, **kw):         # ignores what was declared
        kw["unchanged"] = set(after)
        return carry_marks(before, after, **kw)

    def carries_nothing(before, after, **kw):
        return []

    assert check_carry(carries_everything, world), "re-stamping an undeclared edit went unseen"
    assert check_carry(carries_nothing, world), "dropping the declared carries went unseen"
    monkeypatch.setattr(carry, "stale_reasons", lambda m, now: [])   # carries a stale mark
    assert check_carry(carry_marks, world), "carrying an already-stale mark went unseen"


def test_a_carried_mark_is_appended_never_edited(world, tmp_path):
    store = tmp_path / "marks.jsonl"
    store.write_text("")
    M.append(store, world["marks"])
    first = store.read_text()
    new = carry_marks(world["entries"], _rename(world["entries"], "adam__bridge", "adam__hwy19"),
                      marks=M.load(store), unchanged={ADAM}, by="rename",
                      ctx_before=world["ctx"],
                      ctx_after=ctx_of(splits=_renamed_splits("adam__bridge", "adam__hwy19"),
                                       bundle=world["bundle"]),
                      renamed_splits={"adam__bridge": "adam__hwy19"})
    assert new
    M.append(store, new)
    assert store.read_text().startswith(first)
    assert len(M.load(store)) == len(world["marks"]) + len(new)


# --------------------------------------------------------------------------------------------
# The Mark, the store, the gate
# --------------------------------------------------------------------------------------------

def _m(world, **over):
    base = json.loads(world["marks"][0].model_dump_json())
    return M.Mark.model_validate({**base, **over})


@pytest.mark.parametrize("over, why", [
    ({"reviewer": ""}, "reviewer"), ({"reviewer": "   "}, "reviewer"),
    ({"surprise": 1}, "extra"), ({"pdf_sha256": "abc"}, "sha"),
    ({"verdict": "verified", "answers": {"T": "y", "R": "n", "P": "y", "A": "y"}}, "verified"),
    ({"verdict": "wrong", "answers": {"T": "y", "R": "n", "P": "y", "A": "y"}}, "problem+note"),
    ({"verdict": "unsure", "answers": {"T": "y", "R": "y", "P": "y", "A": "y"}}, "unsure"),
    ({"verdict": "deferred"}, "deferred note"), ({"date": "23/10/2026"}, "date"),
    ({"scope": "rule"}, "rule mark without rule_id"),
])
def test_the_mark_refuses(world, over, why):
    with pytest.raises(ValueError):
        _m(world, **over)


def test_the_mark_accepts_wrong_unsure_deferred_with_what_they_need(world):
    _m(world, verdict="wrong", answers={"T": "y", "R": "n", "P": "y", "A": "y"},
       problem="quota", note="the book says 2")
    _m(world, verdict="unsure", answers={"T": "y", "R": "?", "P": "y", "A": "y"})
    _m(world, verdict="deferred", answers={}, note="lake split deferred (K5)")


def test_the_store_raises_when_missing_and_names_a_bad_line(tmp_path, world):
    with pytest.raises(FileNotFoundError):
        M.load(tmp_path / "nope.jsonl")
    p = tmp_path / "marks.jsonl"
    p.write_text(M.line(world["marks"][0]) + '{"scope": "entry"}\n')
    with pytest.raises(ValueError, match=r"marks.jsonl:2"):
        M.load(p)


def test_the_last_mark_on_a_target_is_the_one_in_force(world):
    a = world["marks"][0]
    b = a.model_copy(update={"verdict": "deferred", "note": "later"})
    assert M.latest([a, b])[a.key] is b and M.latest([b, a])[a.key] is a


def test_a_mark_is_refused_on_an_entry_that_fails_the_gate():
    with pytest.raises(ValueError, match="fails the gate"):
        M.refuse_on_gate(ADAM, [{"path": "rules.0.take", "msg": "15 is not in its own verbatim"}])
    M.refuse_on_gate(ADAM, [])


def test_the_checked_in_store_loads():
    assert isinstance(M.load(CURATED.regulations.review.marks), list)


# --------------------------------------------------------------------------------------------
# One decision, one function: no second copy
# --------------------------------------------------------------------------------------------

def _py_sources():
    for p in sorted(list((REPO / "pipeline").rglob("*.py")) +
                    list((REPO / "curation-review" / "backend").rglob("*.py"))):
        if "tests" not in p.parts:
            yield p, p.read_text(encoding="utf-8")


def test_no_second_copy_of_the_fingerprint_the_split_targets_or_the_override_pick():
    assert not (REPO / "curation-review" / "backend" / "verification.py").exists()
    hits = {"entry_hash": [], "def applies_keys": [], "def _applies_keys": [],
            "_pick_override(": [], "def stale_reasons": [], "def entry_fingerprints(": [],
            "def match_records(": [], "flock(": []}
    for p, text in _py_sources():
        for k in hits:
            if k in text:
                hits[k].append(str(p.relative_to(REPO)))
    assert hits["entry_hash"] == [] and hits["def _applies_keys"] == []
    assert hits["def applies_keys"] == ["pipeline/atlas/splits/splits.py"]
    assert hits["_pick_override("] == ["pipeline/regs/matching/matcher.py"]
    assert hits["def stale_reasons"] == ["pipeline/regs/review/marks.py"]
    assert hits["def entry_fingerprints("] == ["pipeline/regs/review/fingerprint.py"]
    assert hits["def match_records("] == ["pipeline/regs/review/marks.py"]   # rule identity
    assert hits["flock("] == ["pipeline/regs/review/marks.py"]               # the store's lock
    matcher = (REPO / "pipeline/regs/matching/matcher.py").read_text(encoding="utf-8")
    assert len(re.findall(r"_pick_override\(", matcher)) == 2     # its def, and override_for


def test_the_app_has_no_verification_endpoint_left():
    app = (REPO / "curation-review" / "backend" / "app.py").read_text(encoding="utf-8")
    assert "/api/verification" not in app and "/verify" not in app
    assert '@app.post("/api/marks")' in app


# --------------------------------------------------------------------------------------------
# The real bundle
# --------------------------------------------------------------------------------------------

@pytest.mark.needs_bundle
def test_the_real_bundle_reads_every_rule_and_twice_the_same():
    from pipeline.deliver.bundle import read as R
    from pipeline.tests.conftest import BUNDLE_HINT, need
    need(None, "bundle", R.BUNDLE, BUNDLE_HINT)
    a, b = F.BundleFacts.read(R.BUNDLE), F.BundleFacts.read(R.BUNDLE)
    db = sqlite3.connect(f"file:{R.BUNDLE}?mode=ro", uri=True)
    n = db.execute("SELECT COUNT(*) FROM rule").fetchone()[0]
    db.close()
    assert len(a.rule_answer) == n
    assert a == b
    assert all(not str(k).startswith("sid") for v in a.rule_reach.values() for k, *_ in v)
