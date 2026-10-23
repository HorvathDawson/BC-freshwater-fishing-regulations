"""THE KEEP TAB'S LAST CHOICES (rows 31, display 18) on the REAL waters, against the GOLDEN ORACLE: the answers build
(this code, the bundle on this machine) for the 42 waters the 134 golden states name, read at each state's own key and
segment (taps.json), against what the PROTOTYPE drew (app/design/regs-v11/port/golden/golden.json — never this code's
output). The no-data twin is `test_answers_keep_answers.py`; the app's parity (banner, group head, tree, group lines)
proves the rest end to end on a release built from this code.

`UI_EXPORT_BUNDLE` / `ANSWERS_EXPORT_DIR` point the suite at a side bundle and its export pair.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import common as C
from pipeline.deliver.answers.display import closers_of
from pipeline.deliver.bundle import read as R
from pipeline.tests.conftest import need, BUNDLE_HINT, EXPORT_HINT

BUNDLE = os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE
EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or Path(R.BUNDLE).parent.parent / "regs")
GOLDEN = Path(__file__).resolve().parents[2] / "app" / "design" / "regs-v11" / "port" / "golden"
G = json.loads((GOLDEN / "golden.json").read_text())["states"]
TAPS = json.loads((GOLDEN / "taps.json").read_text())["states"]
IDS = sorted(TAPS)
#: OWNER RULING 2026-10-21 (a): the Kitimat's stored tree is not page v36's by design (exclusions.ts KITIMAT_RULED_IDS)
KITIMAT_RULED = {"x14p0_1010", "x14p1_1010", "x14p2_1010"}


@pytest.fixture(scope="module")
def built(request):
    need(request, "bundle", BUNDLE, BUNDLE_HINT)
    need(request, "bundle", EXPORT_DIR / "ui-rules-export.json", EXPORT_HINT)
    data, guide = C.load_export(EXPORT_DIR)
    items = sorted({TAPS[i]["item"] for i in IDS})
    m = A.build(BUNDLE, EXPORT_DIR, workers=1, items=items, export=(data, guide), log=lambda *_: None)
    rid = {ix: k for k, ix in C.check_export(C.load(BUNDLE), data, guide).items()}
    return m, rid


def _at(built, sid, section):
    m, _ = built
    tap = TAPS[sid]
    k = m.parts[tap["item"]][tap["partIx"]]
    assert m.segments[k] == tap["starts"]                       # the oracle's own key year
    return m.sections[section][k][tap["s"]]


def _walk_kinds(nodes):
    for n in nodes or []:
        if n.get("node") == "tier":
            yield from _walk_kinds(n.get("children"))
        elif n.get("node") == "kind":
            yield n


def _stamp_lines(rows, licence) -> list:
    """The stamp lines the stored groups draw, in draw order (each group: its tree's kinds, then its not-kept
    lines), as the app words them: "You need <name> › <when, first letter lower-cased>" — a kind whose duty line
    already links the steelhead stamp draws no second one (the prototype's stampST skip)."""
    st_doc = any(d["doc"] == "steelhead_stamp" for p in licence.get("profiles") or []
                 for d in p.get("documents") or [])
    line = lambda x: f"You need {x['name']} › {x['when'][:1].lower()}{x['when'][1:]}"
    out = []
    for r in rows["rows"]:
        g = r.get("group")
        if not g or not g.get("headline") or r.get("parent") is not None:
            continue
        for k in _walk_kinds(g.get("tree")):
            skip = bool(k.get("duties")) and st_doc and "ST" in k["species"]
            out += [line(x) for x in k.get("stamps") or [] if not (skip and x["doc"] == "steelhead_stamp")]
        for x in g.get("not_kept") or []:
            out += [line(y) for y in x.get("stamps") or []]
    return out


#: D19 (a), T80 3: klmb_1010's rainbow row, drawn by page v36's legacy card path (golden `plainRows`), is a group now
D19_LEGACY_ROW = {"klmb_1010"}


def _golden_stamp_lines(sid, legacy: bool = False) -> list:
    keep = G[sid].get("keep") or {}
    lines = [l for g in keep.get("groups") or [] for l in g.get("lines") or []]
    if legacy and sid in D19_LEGACY_ROW:
        lines += [l for row in keep.get("plainRows") or [] for l in row]
    return [l for l in lines if l.startswith("You need ")]


@pytest.mark.needs_bundle
# The 3 Kitimat states are left out, not skipped (no silent skips): owner ruling 2026-10-21 (a) makes their stored tree
# differ from page v36's by design (GOTCHAS.md); their trees are pinned in test_answers_kitimat_steelhead_lift.py.
@pytest.mark.parametrize("sid", [s for s in IDS if s not in KITIMAT_RULED])
def test_5_every_stamp_line_the_groups_carry_is_the_prototypes(built, sid):
    assert _stamp_lines(_at(built, sid, "rows"), _at(built, sid, "licence")) == _golden_stamp_lines(sid, legacy=True)


@pytest.mark.needs_bundle
def test_5_the_stamp_states_are_the_eight_the_prototype_drew(built):
    assert sorted(s for s in IDS if _golden_stamp_lines(s)) == [
        "anderson_1010", "x13p4_1010", "x15p16_1010", "x15p17_1010", "x15p1_1010", "x20p1_1010", "x23p0_1010",
        "x4p_1010"]
    assert _golden_stamp_lines("x4p_1010") == [
        "You need Conservation Surcharge Stamp for Shuswap Lake char › only if you keep lake trout and Dolly "
        "Varden/bull trout over 60 cm",
        "You need Conservation Surcharge Stamp for Shuswap rainbow trout › only if you keep rainbow trout over 50 cm"]


@pytest.mark.needs_bundle
def test_1_cow0_the_white_sturgeon_closure_closes_nothing_more(built):
    _, rid = built
    f = _at(built, "cow0_1010", "display")
    assert f["status"] == "closed"
    shut = {rid[r]: fs for r, fs in f["closing"]}
    assert [rid[r] for r in f["closers"]] == [k for k, fs in shut.items() if len(fs) > 1]
    assert any(fs == ["WSG"] for fs in shut.values())           # the Province's white sturgeon closure is closing
    assert all(shut[rid[r]] != ["WSG"] for r in f["closers"])   # … and is not a closer


@pytest.mark.needs_bundle
@pytest.mark.parametrize("sid", IDS)
def test_1_closers_are_set_exactly_on_a_closed_frame(built, sid):
    f = _at(built, sid, "display")
    if f["status"] == "tidal":
        return
    assert ("closers" in f) == (f["status"] == "closed")
    if f["status"] == "closed":
        assert f["closers"] == closers_of(f["closing"])


@pytest.mark.needs_bundle
def test_2_vedder_all_region_2_streams(built):
    # golden vedder_1010: the badge reads "All Region 2 streams" (page v36 nar0: a stream share binding every fish)
    rows = _at(built, "vedder_1010", "rows")["rows"]
    keep = [r for r in rows if r["kind"] == "keep" and r["scope"]["share"]]
    assert keep and all(r["scope"]["narrow_all"] for r in keep)


@pytest.mark.needs_bundle
def test_4_klmb_the_kootenay_record_rule_is_a_duty(built):
    # golden klmb_1010: "Write each rainbow trout over 50 cm (20 in) on your licence right away." sits under "Other
    # rules" with its "Record it" tag (page v36 ladderHtml moved the deciding record rule to `duty`)
    _, rid = built
    rb = _at(built, "klmb_1010", "rows")["fish"]["RB"]
    roles = {rid[r]: role for o in ("hatchery", "wild") if rb[o] for r, role, _by in rb[o]["roles"]}
    assert roles["zp:kootenay_rainbow_stamp::kootenay_rainbow_stamp.r1"] == "duty"
    other = (G["klmb_1010"]["keep"]["plainRows"][0])
    assert other[other.index("Other rules"):].count(
        "Write each rainbow trout over 50 cm (20 in) on your licence right away. Record it Rule book p.7 ↗") == 1
