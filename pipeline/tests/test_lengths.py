"""`lengths` — the size limit said once, plainly.

The old `over_cm`/`under_cm`/`band` trio is gone from the model and refused on load. What these
pin is the corpus: each trap the migration had to get past, on the rule it was found on, read
the way a consumer reads it — first matching range wins.
"""
import os

import pytest

from pipeline.regs.parsing.catalogue import CatalogueRule, LengthBand
from pipeline.deliver.bundle import read as corpus

#: `UI_EXPORT_BUNDLE` points the corpus checks at a side bundle, as it does the export's.
_BUNDLE = str(os.environ.get("UI_EXPORT_BUNDLE") or corpus.BUNDLE)


def rules():
    return corpus.rules(_BUNDLE)


def keep(bands, cm, take=None):
    """How many of a fish THIS long the bands allow; None = not spoken about."""
    for b in bands or []:
        if b.holds(cm):
            return b.take if b.take is not None else take
    return None


def _rule(rule_id: str, entry_prefix: str) -> dict:
    got = [x for x in rules() if x.get("rule") == rule_id
           and (x.get("entry") or "").startswith(entry_prefix)]
    assert len(got) == 1, f"{entry_prefix}::{rule_id}: {len(got)} matches"
    return got[0]


def _bands(x: dict):
    return [LengthBand(**b) for b in x["lengths"]]


# ---------------------------------------------------------------------------------------
# The old fields are refused, not converted
# ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("old", [{"over_cm": 50}, {"under_cm": 30}, {"band": True},
                                 {"needs_review": True, "review_reason": "a real reason here"}])
def test_the_fields_lengths_replaced_are_refused(old):
    with pytest.raises(ValueError):
        CatalogueRule.model_validate({"rule_id": "x.r1", "type": "retention_limit",
                                      "verbatim": "v", "species": ["RB"], "take": 2, **old})


# ---------------------------------------------------------------------------------------
# Each reading of "over", on the rule that proved it
# ---------------------------------------------------------------------------------------
def test_a_ceiling_on_the_whole_allowance_grants_below_and_denies_above():
    """"Trout/char daily and possession quotas = 2 (none over 50 cm)". Lumped in with `annual`
    as "not daily" it once granted the 2 to exactly the fish the sentence forbids."""
    x = _rule("buckinghorse_lake.r3", "r6:buckinghorse_lake")
    assert keep(_bands(x), 40, x["take"]) == 2
    assert keep(_bands(x), 60, x["take"]) == 0


def test_a_floor_is_absolute_even_inside_a_clause():
    """"no more than 1 char (none under 60 cm)" forbids a 50 cm char outright rather than
    handing it back to the parent quota."""
    x = _rule("alta_lake.r4", "r2:alta_lake")
    assert x.get("within")
    assert keep(_bands(x), 50, x["take"]) == 0
    assert keep(_bands(x), 70, x["take"]) == 1


def test_the_shared_endpoint_is_granted_not_denied():
    """"only 1 bull trout - none under 60 cm": a fish of exactly 60 cm is the granted one."""
    x = _rule("spruce_lake.r2", "r3:spruce_lake")
    assert keep(_bands(x), 60, x["take"]) == 1


def test_a_hole_carrying_a_number_gives_it_to_the_piece_above_only():
    """"only 1 over 100 cm, none between 70 cm and 100 cm" under a parent quota: the 1 is for the
    big ones, the hole keeps none, and a 50 cm pike is the parent's business — capping it at 1
    was the defect the migration fixed."""
    x = _rule("bennett_lake.r10", "r6:bennett_lake")
    b = _bands(x)
    assert keep(b, 80, x["take"]) == 0
    assert keep(b, 110, x["take"]) == 1
    assert keep(b, 50, x["take"]) is None


def test_a_size_on_a_licence_names_the_fish_and_sets_no_number():
    """"Conservation Surcharge Stamp required to catch and keep rainbow trout over 50 cm" says
    the stamp is needed for the big ones. Read as retention it became "keep zero rainbow trout
    over 50 cm" — a paperwork rule turned into a ban. It is a licensing REQUIREMENT now, not a
    rule, and its band names which fish — a band on a requirement cannot carry a number."""
    import json
    from pipeline.common.curated import CURATED
    from pipeline.regs.parsing.catalogue import CatalogueFile, Requirement
    path = CURATED.regulations.entries.catalogue / "region-3.json"
    cf = CatalogueFile.model_validate(json.loads(path.read_text(encoding="utf-8")))
    e = next(e for e in cf.entries if e.entry_id.startswith("r3:shuswap_lake"))
    assert not [r for r in e.rules if r.lengths and r.take is None and "Stamp" in r.verbatim]
    x = next(x for x in e.licensing if x.id == "shuswap_rainbow_stamp")
    assert isinstance(x, Requirement) and x.doing.act == "retaining"
    assert x.doing.species == ["RB"]
    assert [b.model_dump(exclude_none=True) for b in x.doing.lengths] == [{"min_cm": 50}]


def test_an_annual_quota_counts_a_size_class_and_forbids_nothing_beneath_it():
    """"Rainbow trout: 5 over 50 cm" is five big ones per licence year and says NOTHING about a
    30 cm rainbow, which the daily quota governs. A floor here is an invented annual ban."""
    x = _rule("shuswap_annual.r1", "z3:shuswap_annual")
    assert keep(_bands(x), 60, x["take"]) == 5
    assert keep(_bands(x), 30, x["take"]) is None


# ---------------------------------------------------------------------------------------
# The model refuses a range that says nothing
# ---------------------------------------------------------------------------------------
@pytest.mark.parametrize("kw", [{}, {"min_cm": 60, "max_cm": 40}, {"min_cm": 50, "max_cm": 50}])
def test_a_range_that_holds_no_fish_or_every_fish_is_refused(kw):
    with pytest.raises(ValueError):
        LengthBand(**kw)


# ---------------------------------------------------------------------------------------
# And it is actually on the data
# ---------------------------------------------------------------------------------------
def test_the_corpus_carries_no_trace_of_the_fields_lengths_replaced():
    """`over_cm`, `under_cm` and `band` are gone from the model, the entries AND the bundle —
    checked on what a reader actually gets, not on what the model would accept."""
    rs = rules()
    left = [x for x in rs if any(k in x for k in ("over_cm", "under_cm", "band"))]
    assert not left, f"{len(left)} rules still carry a field lengths replaced"


def test_only_a_rule_about_fish_of_a_size_carries_lengths_and_every_band_is_real():
    rs = [x for x in rules() if x.get("lengths")]
    assert rs, "no rule carries lengths — the join or the bundle has dropped them"
    for x in rs:
        # A size on a licence is on the licensing record (see above), never a rule row.
        assert x.get("type") == "retention_limit", \
            f"{x.get('rule')}: a size on a {x.get('type')} rule"
        for d in x["lengths"]:
            b = LengthBand(**d)                       # refuses empty and open-both-ends ranges
            assert b.min_cm is not None or b.max_cm is not None


def test_the_four_rules_whose_band_was_backwards_read_as_windows():
    """Coquitlam, Gwillim, Lower Blue and Williston Zone B were flagged as holes when the
    sentence is a window, which permits exactly the fish the rule protects."""
    want = {"coquitlam_river.r3", "gwillim_lake.r1"}
    seen = 0
    for x in rules():
        if x.get("rule") not in want or not x.get("lengths"):
            continue
        seen += 1
        bands = [LengthBand(**b) for b in x["lengths"]]
        span = next(b for b in bands if b.min_cm is not None and b.max_cm is not None)
        mid = (span.min_cm + span.max_cm) // 2
        assert keep(bands, mid, x.get("take")) not in (0, None), \
            f"{x['rule']}: {mid} cm is inside the window and must be keepable"
    assert seen == len(want)
