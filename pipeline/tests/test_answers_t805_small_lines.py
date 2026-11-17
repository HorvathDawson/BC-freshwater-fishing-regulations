"""T805 part D (adversarial review of the trout redesign, 2026-11-16, "data gaps"): the small lines.

  SHAMROCK CREEK (blk:356196726, Region 2, steelhead possible) takes Lonzo (Marshall) Creek's row by the walk, p.24:
    "Trout/char 20-30 cm in lenth Jun 16-Jan 31". The card read "Up to 2 hatchery steelhead" kept at 20-30 cm; p.80:
    "steelhead: a rainbow trout longer than 50 cm" — no steelhead is 20-30 cm, so none may be kept (as on Lonzo Creek).
  ECHOES LAKES (Region 4), p.36: "trout/char daily quota = 1 in the aggregate from both lakes (none under 50 cm)": the
    lake's own line says "from both lakes together", not only the kept-elsewhere sentence.
The live twins (the real waters) are in `test_answers_t805_live.py`.
"""
from __future__ import annotations

import math
from types import SimpleNamespace

from pipeline.deliver.answers import tree as TR
from pipeline.deliver.answers.rows import evaluate as EV


def test_a_steelhead_is_over_50_cm_wherever_it_is_answered(monkeypatch):
    possible = SimpleNamespace(steelhead_water=False)
    known = SimpleNamespace(steelhead_water=True)
    assert EV.fish_range(possible, "ST") == (50, math.inf)
    assert EV.fish_range(known, "ST") == (50, math.inf)
    assert EV.fish_range(possible, "RB") == (0, math.inf)      # a big rainbow is not known to be a steelhead here
    assert EV.fish_range(known, "RB") == (0, 50)
    monkeypatch.setattr(EV, "A_STEELHEAD_IS_OVER_50_EVERYWHERE", False)
    assert EV.fish_range(possible, "ST") == (0, math.inf)       # the mutation: Shamrock Creek's 20-30 cm steelhead


def test_only_echoes_lakes_counts_over_both_lakes():
    assert TR._BOTH_LAKES.search("trout/char daily quota = 1 in the aggregate from both lakes (none under 50 cm)")
    for w in ("Trout/char daily quota = 1", "Rainbow trout/hatchery steelhead quota = 6 in the aggregate",
              "kokanee 5 from both arms"):
        assert not TR._BOTH_LAKES.search(w), w
