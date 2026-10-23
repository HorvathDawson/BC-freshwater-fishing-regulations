"""T109: ONE entry-id grammar (`pipeline/common/entry_id.py`) — what an id says (kind, region, zones,
book region, MUs, slug), one region wording, and how a water row's id is minted.

Pinned three ways: the parse of real ids, whole; the predicates against the inline tests they
replaced, over every id of the curated corpus (a pure refactor); and a source scan that fails on an
inline `startswith("z")` / `startswith("r")` / `startswith("zp:")` or an entry-id regex anywhere
in the pipeline outside `entry_id.py` (and, in the app, outside level.ts) — with a mutation proving
the scan sees one.
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path


from pipeline.common import entry_id as EID
from pipeline.common.entry_id import EntryId

ROOT = Path(__file__).resolve().parents[2]
CATALOGUE = ROOT / "data/curated/regulations/entries/catalogue"


def _corpus_ids() -> list[str]:
    out = []
    for f in sorted(CATALOGUE.glob("region-*.json")):
        out += [e["entry_id"] for e in json.loads(f.read_text())["entries"]]
    return out


def test_parse_whole():
    assert EID.parse("r7:williston_lake_in_zone_a@7-30+7-37+7-38") == EntryId(
        raw="r7:williston_lake_in_zone_a@7-30+7-37+7-38", head="r7", kind="water", region="7",
        slug="williston_lake_in_zone_a", mus=("7-30", "7-37", "7-38"), regions=("7",))
    assert EID.parse("z7a:bait_ban_streams") == EntryId(
        raw="z7a:bait_ban_streams", head="z7a", kind="zone", region="7a", slug="bait_ban_streams", mus=(),
        regions=None)
    assert EID.parse("zp:steelhead") == EntryId(
        raw="zp:steelhead", head="zp", kind="province", region=None, slug="steelhead", mus=(), regions=None)
    assert EID.parse("r5:fraser_river@5-2+3-17").regions == ("3", "5")
    assert [EID.parse(x).book_region for x in ("z7a:x", "z7b:x", "z4:x", "r7:x", "zp:x")] == ["7", "7", "4", "7", None]


def test_zones_from_units_with_a_registry():
    """A Region 7 row's MUs name its zone (user ruling 2026-09-24) — `parse(eid, registry)`."""
    from pipeline.common.models.registry import RegistryItem
    reg = {"area:region:7a": RegistryItem(id="area:region:7a", name="7A", kind="area", section_ids=("a",), mus=("7-30",)),
           "area:region:7b": RegistryItem(id="area:region:7b", name="7B", kind="area", section_ids=("b",), mus=("7-31",))}
    assert EID.parse("r7:w@7-30", reg).regions == ("7a",)
    assert EID.parse("r7:w@7-31", reg).regions == ("7b",)
    assert EID.parse("r7:w@7-30+7-99", reg).regions == ("7",)       # one MU unresolved: the whole region
    assert EID.parse("r7:w", reg).regions == ("7",)


def test_region_words_is_the_one_wording():
    assert [EID.region_words(r) for r in (["5"], ["7a"], ["8", "3"], ["7b", "2", "7a", "10"])] == [
        "Region 5", "Region 7A", "Regions 3 and 8", "Regions 2, 7A, 7B and 10"]
    from pipeline.deliver.answers import display
    from pipeline.deliver.bundle import read
    assert read.region_words is EID.region_words
    assert display.regions_label(["8", "3"]) == "Regions 3 and 8"


def test_predicates_equal_the_inline_tests_they_replaced_on_every_corpus_id():
    ids = _corpus_ids()
    assert len(ids) > 1400 and {EID.kind_of(i) for i in ids} == {"province", "zone", "water"}
    for i in ids:
        assert EID.is_province(i) == i.startswith("zp:"), i               # T109: the oracle IS the old inline test
        assert EID.is_table(i) == i.startswith("z"), i                    # T109
        assert EID.is_zone(i) == (i.startswith("z") and not i.startswith("zp:")), i   # T109
        assert EID.is_water_row(i) == i.startswith("r"), i                # T109
        head = i.split(":", 1)[0]                                         # T109
        assert EID.region_of(i) == (head[1:] if head != "zp" else None), i
        assert EID.zone_region(i) == (head[1:] if head[0] == "z" and head != "zp" else None), i


def test_minting_is_unchanged_every_water_row_round_trips():
    """`batch_exporter` mints through `mint_water_row`; every corpus water row reads back to itself, so
    the format did not move (changing it needs the registry → entry resync — T109 follow-up)."""
    rows = [i for i in _corpus_ids() if EID.is_water_row(i)]
    assert len(rows) > 1300
    for i in rows:
        p = EID.parse(i)
        assert EID.mint_water_row(p.region, p.slug, p.mus) == i
    assert EID.mint_water_row("", "x", []) == "r?:x"
    assert EID.mint_water_row("7", "w", ["7-38", "7-30"]) == "r7:w@7-30+7-38"


# ---- the pin: no second copy -----------------------------------------------------------------

_HEAD = re.compile(r"^(z|r|zp|zp:|z\d+[a-z]?:?|r\d+[a-z]?:?)$")
#: an entry-id regex: `^r(\d…`, `r(\d)`, `z\d`, `^z(\d…` at the start of a pattern
_RX = re.compile(r"^\^?[rz](\(\\d|\\d)")


def inline_entry_id_tests(src: str) -> list[int]:
    """Lines that test an entry id's prefix inline or match it with a regex."""
    out = []
    for n in ast.walk(ast.parse(src)):
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "startswith" and n.args:
            a = n.args[0]
            vals = [a.value] if isinstance(a, ast.Constant) else [e.value for e in getattr(a, "elts", [])
                                                                  if isinstance(e, ast.Constant)]
            if any(isinstance(v, str) and _HEAD.match(v) for v in vals):
                out.append(n.lineno)
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and _RX.match(n.value):
            out.append(n.lineno)
        # T109 review: a zone letter read off a sliced region (`zr[-1:] in ("a", "b")`) is `book_region` re-inlined
        if isinstance(n, ast.Compare) and isinstance(n.left, ast.Subscript) and isinstance(n.left.slice, ast.Slice):
            for c in n.comparators:
                vals = [e.value for e in getattr(c, "elts", []) if isinstance(e, ast.Constant)]
                if sorted(vals) == ["a", "b"]:
                    out.append(n.lineno)
    return out


def test_the_scan_sees_a_reinlined_test():
    """Mutation: each spelling the refactor removed is caught."""
    for bad in ('x.startswith("z")', 'eid.startswith("zp:")', 'str(k[0]).startswith("r")',
                'e.startswith(("z", "zp:"))', 're.match(r"^r(\\d+[ab]?)$", ch)', 're.match(r"z\\d", eid)',
                're.compile(r"^r(\\d+[a-z]?):")', 'zr[:-1] == region and zr[-1:] in ("a", "b")',
                'r[-1:] in ("a", "b")'):
        assert inline_entry_id_tests(bad), bad
    for fine in ('name.startswith("regs-")', 'i.startswith("r6:kitimat")', 'k.startswith("zp:classified_waters_licence#x")',
                 'kk in ("a", "b")'):
        assert not inline_entry_id_tests(fine), fine


def test_no_entry_id_is_parsed_outside_entry_id_py():
    hits = []
    for p in sorted((ROOT / "pipeline").rglob("*.py")):
        rel = p.relative_to(ROOT).as_posix()
        if "/tests/" in rel or rel == "pipeline/common/entry_id.py":
            continue
        hits += [f"{rel}:{ln}" for ln in inline_entry_id_tests(p.read_text())]
    assert hits == [], "parse entry ids with pipeline.common.entry_id, not inline: " + ", ".join(hits)


def test_no_ts_reader_of_an_entry_id_region():
    """T109 (left item, done): the export ships the parsed region (`provenance.region`, `entry_id.region_of`) and the
    store carries it (`store.common.RULE_PROV_KEEP`), so the app reads NO entry id — level.ts, which held the last
    regex, reads `prov.region`. No second copy: nothing in the app regexes or prefix-tests an entry id (mutation: put
    level.ts's `/^z(\\d+[a-z]?):/.exec(r.export.entry_id …)` back and this fails)."""
    app = ROOT / "app/packages"
    assert app.is_dir()
    pat = re.compile(r"/\^z\(|/\^r\(|/\^zp|entry_id[^;\n]{0,40}\.(startsWith|match|exec|test)\(|\.exec\([^)]*entry_id")
    hits = sorted(p.relative_to(ROOT).as_posix() for p in app.rglob("*.ts")
                  if "node_modules" not in p.parts and not p.name.endswith(".test.ts") and pat.search(p.read_text()))
    assert hits == []
    level = (app / "core/src/regs/level.ts").read_text()
    assert "prov?.region" in level and "entry_id" not in level.split("export function levelTag", 1)[1].split("\n}", 1)[0]
    assert pat.search('const m = /^z(\\d+[a-z]?):/.exec(r.export.entry_id || "");')


def test_the_export_and_the_store_ship_the_parsed_region():
    """The region the app shows is the pipeline's parse, carried end to end: the export's rule provenance
    (`export_ui_rules`), the codec (dropped on the wire as derivable, refilled by `expand` from the same function) and
    the store's rule blob."""
    from pipeline.deliver.store import common as SC
    from pipeline.tools import export_codec as K
    from pipeline.tools import export_ui_rules as X
    import inspect
    assert "region" in SC.RULE_PROV_KEEP and "region" in X.PROVENANCE_TEXT
    assert '"region": EID.region_of(r["entry_id"])' in inspect.getsource(X)
    assert [EID.region_of(e) for e in ("z7a:x", "z7b:x", "z5:x", "r6:x@6-1", "r7:w@7-30", "zp:x")] == \
        ["7a", "7b", "5", "6", "7", None]
    src = inspect.getsource(K)
    assert 'p.pop("region") != EID.region_of(eid)' in src and '"region": EID.region_of(eid)' in src
    rule = {"id": "z7a:q::q.r1", "entry_id": "z7a:q", "rule_id": "q.r1"}
    assert SC.rule_words({**rule, "provenance": {"rank": 3, "region": "7a", "entry_name": "Zone A"}})["prov"] == \
        {"rank": 3, "entry_name": "Zone A", "region": "7a"}
