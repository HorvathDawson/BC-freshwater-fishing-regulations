"""A NO-DATA card (answers 2.8): Region 2's trout/char table and one water's lift, run through the REAL
reader and the REAL rows — no bundle, no verdicts store, no export.

`fixtures/answers28_region2.json` holds 15 rule records cut from the bundle: Region 2's trout and char
table (p.21: 4 a day, 2 from streams, 1 Dolly Varden/lake trout, the Q16 over-50 pair, the 30 cm and
60 cm floors, wild released on streams), the provincial steelhead rules (p.8: 10 a year, record it) and
Region 2's "stop after your hatchery steelhead", and ONE water's lift — the Vedder's r9 (p.22: hatchery
rainbow trout 50 cm or less, 4 a day, Jul 1-Apr 30, lifting the 2-from-streams share and the 30 cm
floor for them). Three waters are bound from it:

  VEDDER  a Region 2 steelhead stream with the lift (every rule)
  STREAM  a plain Region 2 steelhead stream (every rule but the lift)
  LAKE    a plain Region 2 lake (the rules not limited to streams)
  TWO     the Vedder with a SECOND lift from its own row (ROUND10 W1, the Duncan's shape: one row lifting
          two kinds out of the share with two numbers) — a made-up r90, hatchery cutthroat trout 3 a day
          lifting the 2-from-streams share; never bound on the other three

`card(water, md)` asks the reader (`read.effective_rules_bound`, trace, per fish and origin — what the
verdicts stage stores) over the rule set, builds the `rows.Part` and returns `rows.produce` plus the
Part: what the answers build would write for that key and day.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from pipeline.deliver.answers import answers as A
from pipeline.deliver.answers import rows as RW
from pipeline.deliver.bundle import read

FIXTURE = Path(__file__).parent / "fixtures" / "answers28_region2.json"
#: the reader's rule table for the fixture is seeded under this path (never a file)
PATH = "fixture:answers28_region2"
LIFT = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r9"
SHARE = "z2:trout_char_quota::trout_char_quota.r4"
TOTAL = "z2:trout_char_quota::trout_char_quota.r1"
FISH = ("RB", "CT", "EB", "GB", "DV", "LT", "ST")
ORIGINS = ("none", "hatchery", "wild")

LIFT2 = "r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r90"
VEDDER, STREAM, LAKE, TWO = "vedder", "stream", "lake", "two_lifts"
#: water -> (set id, kind, steelhead water, steelhead rules here, presence)
WATERS = {VEDDER: (1, "stream", True, True, "known"),
          STREAM: (2, "stream", True, True, "known"),
          LAKE: (3, "lake", False, True, None),
          TWO: (4, "stream", True, True, "known")}


@dataclass
class _B:
    """The two things `rows.Part` reads of a bundle."""
    rules: Dict[Tuple[str, str], dict]
    sets: Dict[int, List[Tuple[str, str, str]]]


def _load() -> _B:
    got = read._RULES_BY_PATH.get(PATH)
    if got is None:
        rules = json.loads(FIXTURE.read_text(encoding="utf-8"))["rules"]
        got = {(x["entry"], x["rule"]): x for x in rules}
        got[tuple(LIFT2.split("::"))] = _second_lift(got[tuple(LIFT.split("::"))])
        for x in got.values():
            x["_rank"] = read.source_of(x).rank
        read._RULES_BY_PATH[PATH] = got
    lift, lift2 = tuple(LIFT.split("::")), tuple(LIFT2.split("::"))
    every = sorted(k for k in got if k != lift2)
    sets = {1: [(e, r, "reach") for e, r in every],
            4: [(e, r, "reach") for e, r in sorted(got)],
            2: [(e, r, "reach") for e, r in every if (e, r) != lift],
            3: [(e, r, "reach") for e, r in every
                if (e, r) != lift and got[(e, r)].get("water") != "stream"]}
    return _B(rules=got, sets=sets)


def _second_lift(r9: dict) -> dict:
    """A made-up second rule of the Vedder's row: hatchery cutthroat trout, 3 a day, all year, lifting
    Region 2's 2-from-streams share for them (only the share — the 30 cm floor stays)."""
    x = json.loads(json.dumps(r9))
    x.update(rule="chilliwack_vedder_rivers.r90", rule_id="chilliwack_vedder_rivers.r90", species=["CT"],
             take=3, lengths=None, when=None, verbatim="hatchery cutthroat trout: daily quota = 3",
             label="Cutthroat trout — 3 per day, hatchery only",
             parts=json.dumps({"what": "Cutthroat trout — 3 per day", "conditions": "hatchery only"}),
             exempts=[{"entry_id": "z2:trout_char_quota", "rule_id": "trout_char_quota.r4",
                       "species": ["CT"], "note": "hatchery cutthroat trout quota = 3 replaces '2 from streams'"}])
    return {k: v for k, v in x.items() if v is not None or k in r9 and r9[k] is None}


def ladder(water: str, md: Tuple[int, int]) -> dict:
    """Every asked fish's traced verdict per origin, as the verdicts stage stores it."""
    B = _load()
    set_id, _kind, sw, sr, _pres = WATERS[water]
    bound = B.sets[set_id]
    return {f: {o: A.ladder_verdict(read.effective_rules_bound(
        bound, sw, md, f, PATH, steelhead_rules_here=sr, origin=None if o == "none" else o,
        trace=True)) for o in ORIGINS} for f in FISH}


def part(water: str, md: Tuple[int, int], lad: dict | None = None) -> RW.Part:
    B = _load()
    set_id, kind, sw, sr, pres = WATERS[water]
    return RW.Part(B, set_id, sw, sr, kind, pres, lad if lad is not None else ladder(water, md), md, {})


def card(water: str, md: Tuple[int, int], lad: dict | None = None):
    """(the rows `produce` writes for the water on the day, its Part)."""
    P = part(water, md, lad)
    return RW.produce(P), P


def trout_group(out: dict) -> dict:
    """The trout and char total's group (on one row of the card)."""
    got = [r["group"] for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL]
    assert len(got) == 1, [r.get("group") for r in out["rows"]]
    return got[0]


def trout_row(out: dict) -> dict:
    return next(r for r in out["rows"] if r.get("group") and r["group"]["pool"] == TOTAL)
