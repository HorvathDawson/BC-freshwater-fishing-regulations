"""T789 K8 (owner-approved for 0.8.0): Shuswap Lake, p.32. The Salmon Arm Bay closure prints "(anglers fishing from the
community pier in the city of Salmon Arm are exempt from the closure)" and the bait ban "(… exempt from the bait ban)".
Both exemptions are lifts held on an undrawn part (`shuswap_lake.r5x`, `r11x`) with no verdict, so no reader drew them;
the exempted rule's own line now says the book's sentence in brackets (`labels.exempted_on_an_undrawn_part`), and so
reaches the water's caveat line (the app draws the rule's `parts.what`)."""
from __future__ import annotations

import json

from pipeline.regs.parsing.catalogue import CatalogueRule
from pipeline.regs.parsing.catalogue_parts import labels as L
from pipeline.tests import under_test

ENTRY = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26"


def _shuswap() -> dict:
    rows = json.loads((under_test.curated().regulations.synopsis / "region-3.json").read_text(encoding="utf-8"))
    e = next(x for x in rows["entries"] if x["entry_id"] == ENTRY)
    return {r["rule_id"]: CatalogueRule.model_validate(r) for r in e["rules"]}


def test_the_bay_closure_and_the_bait_ban_say_the_pier_exemption():
    sib = _shuswap()
    what = lambda k: L.label_parts(sib[k], sib)["what"]          # noqa: E731
    assert what("shuswap_lake.r5") == ("No fishing (anglers fishing from the community pier in the city of Salmon Arm are "
                                       "exempt from the closure)")
    assert what("shuswap_lake.r11") == ("Bait ban (anglers fishing from the community pier in the city of Salmon Arm are "
                                        "exempt from the bait ban)")
    assert L.compose(L.label_parts(sib["shuswap_lake.r5"], sib)) == (
        "No fishing (anglers fishing from the community pier in the city of Salmon Arm are exempt from the closure), "
        "Sep 1-Dec 31 — in part: Salmon Arm Bay, west of line between Engineer's Point and Sunnybrae Point")


def test_a_lift_never_quotes_itself_back_and_other_rules_are_untouched():
    """The lift names the closure as before; a rule no undrawn lift exempts keeps its line (corpus-wide, 2 of 3,404
    labels moved: these two). MUTATION: naming the lifted rule with the lifter among its siblings quotes the note
    inside the lift's own line."""
    sib = _shuswap()
    assert L.compose(L.label_parts(sib["shuswap_lake.r5x"], sib)) == (
        "“No fishing, Sep 1-Dec 31” lifted, Sep 1-Dec 31 — in part: from the community pier in the city of Salmon Arm")
    assert L.label_parts(sib["shuswap_lake.r12"], sib)["what"] == "Single barbless hook"
    assert L.exempted_on_an_undrawn_part(sib["shuswap_lake.r1"], sib) == ""
