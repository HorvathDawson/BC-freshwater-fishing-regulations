"""T810 V-py (trout table contract 2.8): THE SHARED TROUT FIXTURE, Python half.

(a) the committed vectors are the committed trout fixture's cells (`trout_vectors --check`), one per case of
`trout_vectors.CASES`, in order, each a valid `TroutFrame` (or null exactly where Q-D3 says), the `{day}` slot filled
once; a changed vector fails the check. (b) every hand case of `trout-hand.json` is refused by the model, for its own
reason (the app's reader refuses the same file). (c) the chain on the mini chain's release (`mini_chain`: every
release carries the section since the cutover, T810 L): the fixture cut twice is the same bytes, the vectors read off it twice are the same, and they are
the side release's own cells. (d) the refresh script refuses a release without the section; the staging `side`
scenario's pure half (`deploy/staging/test-updates.test.mjs`).
"""
from __future__ import annotations

import copy
import filecmp
import functools
import json
import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.common.curated import REPO_ROOT
from pipeline.deliver.answers.model.trout import TroutFrame
from pipeline.tools.emit import trout_vectors as TV
from pipeline.tests import under_test

HAND = json.loads(TV.HAND.read_text(encoding="utf-8"))


@functools.lru_cache(maxsize=1)
def _vectors() -> dict:
    return json.loads(TV.OUT.read_text(encoding="utf-8"))


def _valid(frame) -> TroutFrame:
    return TroutFrame.model_validate_json(json.dumps(frame, ensure_ascii=False))


# ---------------------------------------------------------------------------------------- (a) the committed vectors

def test_the_committed_vectors_are_the_committed_fixtures_cells():
    assert TV.main(["--check"]) == 0


def test_the_vectors_hold_every_case_in_order():
    assert _vectors()["format"] == TV.FORMAT
    assert [{k: v[k] for k in ("name", "water", "item", "anchor", "day", "weekday", "pins")} for v in _vectors()["cases"]] \
        == [c.json() for c in TV.CASES]
    assert len(TV.CASES) >= 45 and _vectors()["drafts"] == []


def test_no_table_exactly_where_the_water_is_closed_or_tidal():
    """Q-D3 on the cases: Elk River's closed Apr 15 and tidal Nitinat Lake are null, every other case has a table."""
    assert {v["name"] for v in _vectors()["cases"] if v["frame"] is None} == {"elk-apr15", "nitinat-jul15"}


@pytest.mark.parametrize("name", [c.name for c in TV.CASES])
def test_each_vector_is_a_valid_frame_and_composes_its_day_once(name):
    v = next(x for x in _vectors()["cases"] if x["name"] == name)
    if v["frame"] is None:
        assert v["composed"] is None
        return
    _valid(v["frame"])
    lead = v["frame"]["table"]["top"]["lead"]
    mo, d = map(int, v["day"].split("-"))
    assert v["composed"] == {"lead_today": lead.replace("{day}", "today"),
                             "lead_on": lead.replace("{day}", f"on {TV.MONTHS[mo - 1]} {d}")}


def test_the_vectors_carry_the_fixed_bands():
    """J's last faults (T810 2026-11-17) as the vectors show them: Kaslo's Dolly Varden inside the 2 from streams; the
    Kitimat's wild rainbow inside Region 6's "1 trout from streams"."""
    by = {v["name"]: v["frame"]["table"] for v in _vectors()["cases"] if v["frame"]}
    assert by["kaslo-jul15"]["top"]["lead"] == "Up to 2 {day}"
    # G2 (owner 2026-11-17, "'counts toward the 5'"): a row inside the day's shared number says "counts toward the N" on its own band
    kaslo = by["kaslo-jul15"]["boxes"][0]
    assert [r["key"] for r in kaslo["rows"]][0] == "DV"
    assert [(b["text"], b["from"], b["to"]) for b in kaslo["bands"]] == [("counts toward the 2", i, i) for i in range(6)]
    kit = by["kitimat-jul15"]["boxes"][0]
    band = next(b for b in kit["bands"] if b["text"] == "only 1 of these 3")
    assert {r["key"] for r in kit["rows"][band["from"]:band["to"] + 1]} == {"GB", "CT", "RB/wild"}


def test_the_fixture_keeps_exactly_the_case_waters():
    """`regs_fixture --trout-cases`: the case waters, and the waters they flow into (kept whole for the map, T459)."""
    fx = TV.Fixture(TV.FIXTURE, TV.LINES)
    try:
        with_parts = {i for i, o in fx.ord_of.items() if fx.parts_of.get(o)}
        mine = {fx.ord_of[i] for i in TV.case_items()}
        receiving = {i for i, o in fx.ord_of.items() if any(o in fx.down.get(m, ()) for m in mine)}
    finally:
        fx.close()
    assert set(TV.case_items()) <= with_parts
    assert with_parts - set(TV.case_items()) <= receiving


def test_a_changed_vector_fails_the_check(tmp_path):
    bad = copy.deepcopy(_vectors())
    v = next(x for x in bad["cases"] if x["frame"])
    v["frame"]["table"]["top"]["lead"] = v["frame"]["table"]["top"]["lead"].replace("Up to", "Keep up to")
    out = tmp_path / "v.json"
    out.write_text(json.dumps(bad, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    assert TV.main(["--check", "--out", str(out)]) == 1


# ------------------------------------------------------------------------------------------------ (b) the hand cases

#: each hand case -> the model's own reason (a refusal for another reason would not pin the case)
REASON = {
    "band gap": "the bands cover rows 0..0 of 2",
    "band overlap": "the bands leave a gap or overlap",
    "a date key to a missing box": "a date names box 9, which does not exist",
    "a date key to a missing row": "a date names row 0:7, which does not exist",
    "a row with dates at two levels": "carries one date at two levels",
    "a key used twice": "a row key twice",
    "a label on a box and its rows": "a label on a box and on its rows",
    "{day} twice": "the top line holds {day} more than once",
    "{day} outside the top line": "{day} is the top line's slot only",
    "covers out of order": "covers: rows-frame row indexes, ascending, each once",
    "a top date on a keep table": "a top-line date only on a closed or release-every-one table",
    "a year bar on a keep table": "the year bar only on a table whose fish all share one state",
    "a reopening date in a keep box": "a reopening date in a keep box",
}


def test_the_hand_base_is_a_valid_frame():
    assert HAND["format"] == "trout-hand/1"
    _valid(HAND["base"])
    assert max(i for i in HAND["base"]["table"]["src"]) < HAND["rule_count"]


def test_the_hand_file_holds_every_case_the_contract_lists():
    assert {c["name"] for c in HAND["cases"]} == set(REASON) | {"a dangling rule index"}


@pytest.mark.parametrize("name", sorted(REASON))
def test_the_model_refuses_each_hand_case_for_its_reason(name):
    c = next(x for x in HAND["cases"] if x["name"] == name)
    with pytest.raises(ValidationError) as e:
        _valid(c["frame"])
    assert any(REASON[name] in x["msg"] for x in e.value.errors()), e.value.errors()


def test_the_dangling_rule_index_is_refused_by_the_rule_table():
    """The shape cannot know the rule table: the index past `rule_count` is the reader's refusal (both sides)."""
    c = next(x for x in HAND["cases"] if x["name"] == "a dangling rule index")
    _valid(c["frame"])
    cited = set(c["frame"]["table"]["src"]) | {i for b in c["frame"]["table"]["boxes"] for r in b["rows"]
                                               for i in r["src"]}
    assert any(i >= HAND["rule_count"] for i in cited)


# ------------------------------------------------------------------------------- (c) the chain on a side release

@pytest.fixture(scope="module")
def side(tmp_path_factory):
    """The mini chain's release (`mini_chain`) and, per one-part water with a table on a day that reads the same every
    weekday, (item, its day): two lakes and two streams at most."""
    from pipeline.deliver.store.release import province_store
    from pipeline.tests import mini_chain
    rel = mini_chain.release(tmp_path_factory)
    st = province_store(rel)
    try:
        w = st.answers()
    finally:
        st.close()
    trout = w["sections"]["trout"]["at"]
    lakes, streams = [], []
    for item, ps in sorted(w["parts"].items()):
        for ix, k in enumerate(ps):
            j = None if k is None else next((j for j, ref in enumerate(trout[k]) if ref is not None), None)
            if j is None or (item.startswith("wbk:") and len(ps) != 1):
                continue
            (lakes if item.startswith("wbk:") else streams).append((item, ix, w["segments"][w["keys"][k][9]][j]))
            break
    return rel, lakes[:2], streams[:2]


def _cases(side, root: Path, lines: Path):
    """Cases on the mini cut: a lake by its one part, a stream by a section's line and start metre."""
    from pipeline.deliver import calendar as CAL
    _rel, lakes, streams = side
    fx = TV.Fixture(root, lines)
    try:
        out = [TV.Case(f"lake-{n}", item, item, ("lake",), tuple(CAL.month_day(day)), "mini")
               for n, (item, _ix, day) in enumerate(lakes)]
        for n, (item, ix, day) in enumerate(streams):
            mine = set(fx.part_sids[(fx.ord_of[item], ix)])
            h, blk, lo, _hi = min(r for r in fx.lines if r[0] in mine)
            out.append(TV.Case(f"stream-{n}", item, item, ("stream", blk, lo), tuple(CAL.month_day(day)), "mini"))
    finally:
        fx.close()
    return out


def test_the_chain_twice_is_a_no_op_and_the_vectors_are_the_side_releases_cells(side, tmp_path):
    from pipeline.deliver.release import regs_fixture as RF
    from pipeline.tools.audit import answers_diff as AD
    from pipeline.deliver.answers.encode import CODECS
    rel, lakes, streams = side
    assert lakes and streams, "the mini world has no one-part lake and stream with a trout table"
    items = sorted({x[0] for x in lakes + streams})
    got = []
    for n in ("a", "b"):
        RF.build(rel, items, [], tmp_path / n / "regs-release", lines_out=tmp_path / n / "section-lines.json",
                 unnamed=None)
        got.append(tmp_path / n)
    cmp = filecmp.dircmp(got[0] / "regs-release", got[1] / "regs-release")
    assert not cmp.diff_files and not cmp.left_only and not cmp.right_only
    for a, b in zip(sorted((got[0] / "regs-release").rglob("*")), sorted((got[1] / "regs-release").rglob("*"))):
        assert a.name == b.name and (a.is_dir() or a.read_bytes() == b.read_bytes())
    assert (got[0] / "section-lines.json").read_bytes() == (got[1] / "section-lines.json").read_bytes()
    cases = _cases(side, got[0] / "regs-release", got[0] / "section-lines.json")
    one = TV.render(got[0] / "regs-release", got[0] / "section-lines.json", cases)
    assert one == TV.render(got[1] / "regs-release", got[1] / "section-lines.json", cases)
    # the cells read off the cut are the side release's own (the cut copies, it never re-derives)
    R = AD.Release(rel)
    sec = R.sections["trout"]
    where = {f"lake-{n}": (x[0], 0) for n, x in enumerate(lakes)} | {f"stream-{n}": x[:2] for n, x in enumerate(streams)}
    for v in json.loads(one)["cases"]:
        item, ix = where[v["name"]]
        k = R.wire["parts"][item][ix]
        assert v["frame"] == CODECS["trout"].decode_frame(sec, sec["at"][k][v["s"]], None, None), v["name"]


# --------------------------------------------------------------------------- (d) the refresh script, the scenario

def test_the_refresh_script_refuses_a_release_without_the_trout_section(tmp_path):
    """A release cut before the cutover (T810 L: every release since holds the section): the committed mini release's
    manifest with its trout version taken out, which is all the script reads before it refuses."""
    rel = tmp_path / under_test.mini().name
    rel.mkdir()
    m = json.loads((under_test.mini() / "manifest.json").read_text())
    m["regulations"]["versions"].pop("trout")
    (rel / "manifest.json").write_text(json.dumps(m))
    r = subprocess.run(["bash", str(REPO_ROOT / "scripts" / "trout-refresh.sh"), "--release", str(rel)],
                       capture_output=True, text=True, cwd=REPO_ROOT)
    assert r.returncode != 0
    assert "holds no trout section" in r.stderr + r.stdout


def test_the_staging_side_scenario_reaches_test_release_and_writes_test_only():
    r = subprocess.run(["node", "--test", "deploy/staging/test-updates.test.mjs"], capture_output=True, text=True,
                       cwd=REPO_ROOT)
    assert r.returncode == 0, r.stdout[-2000:] + r.stderr[-2000:]
