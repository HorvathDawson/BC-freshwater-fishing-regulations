"""T189: the review screen's extent in plain words (`scope_words`) and one pre-check per fact.

Pure: a fake `Names` stands in for the bundle's place-namer, so these need no atlas.
"""
from __future__ import annotations

from pipeline.regs.review import precheck as P
from pipeline.regs.review import scope_words as S

NAMES = S.Names(split=lambda sid, scope: {"a__eve": "the Eve River confluence"}.get(sid),
                item=lambda iid: {"gnis:1": "Adam River", "gnis:2": "Eve River",
                                  "wbk:9": "Hatzic Lake"}.get(iid),
                area=lambda aid: {"area:park:p": "Strathcona Park"}.get(aid),
                labels={"a__bridge": "Hwy 19 bridge"})


def _entry(rules, **kw):
    return {"entry_id": "r1:adam_river@1-10", "name": "ADAM RIVER (except Eve River)",
            "matched": ["gnis:1"], "extents": [{"op": "whole"}], "rules": rules, **kw}


def test_a_rules_extent_reads_in_words_with_each_split_by_its_name():
    r = {"rule_id": "x.r1", "extents": [{"op": "between", "splits": ["a__eve", "a__bridge"]}]}
    got = S.rule_scope(r, _entry([r]), NAMES)
    assert got["where"] == "between the Eve River confluence and Hwy 19 bridge"
    assert got["splits"] == [{"id": "a__eve", "name": "the Eve River confluence"},
                             {"id": "a__bridge", "name": "Hwy 19 bridge"}]
    assert got["items"] == [{"id": "gnis:1", "name": "Adam River"}]
    assert S.rule_scope({"rule_id": "x.r2", "extents": [{"op": "whole"}]}, _entry([]),
                        NAMES)["where"] == "the whole water"


def test_tributaries_are_the_reach_builders_answer_and_say_why():
    own = {"rule_id": "x.r1", "includes_tributaries": True, "extents": [{"op": "whole"}]}
    inherit = {"rule_id": "x.r2", "extents": [{"op": "whole"}]}
    undrawn = {"rule_id": "x.r3", "undrawn_part": "on parts", "extents": [{"op": "whole"}]}
    e = _entry([own, inherit, undrawn], includes_tributaries=True)
    assert S.rule_scope(own, e, NAMES)["tributaries"] == {
        "yes": True, "only": False, "why": "the rule says so"}
    assert S.rule_scope(inherit, e, NAMES)["tributaries"]["why"] == "inherited from the entry"
    u = S.rule_scope(undrawn, e, NAMES)
    assert u["tributaries"]["yes"] is False and u["in_part"] == "on parts"
    assert S.rule_scope(inherit, _entry([inherit]), NAMES)["tributaries"] == {
        "yes": False, "only": False, "why": "not stated, so no"}
    whole = S.entry_scope(e, NAMES)
    assert whole["tributaries"] == {"yes": True, "stated": True}
    assert whole["rules_walking"] == ["x.r1", "x.r2"]


def test_carve_outs_and_exceptions_are_named():
    r = {"rule_id": "x.r1", "tributaries_only": True, "extents": [
            {"op": "within", "area_id": "area:park:p", "outside_items": ["wbk:9"]}],
         "tributary_excludes": [{"op": "whole", "item_id": "gnis:2", "walk_past": True}],
         "exempts": [{"default_id": "summer_stream_closure"}]}
    got = S.rule_scope(r, _entry([r]), NAMES)
    assert got["where"] == "all waters within Strathcona Park"
    assert got["carve_outs"] == ["not Hatzic Lake", "tributaries not walked into Eve River"]
    assert got["exceptions"] == ["not subject to summer stream closure"]
    assert got["tributaries"]["only"] is True
    assert S.entry_scope(_entry([r]), NAMES)["name_except"] == "except Eve River"


def test_one_of_several_waters_is_named():
    r = {"rule_id": "x.r1", "extents": [{"op": "whole", "item_id": "wbk:9"}]}
    e = _entry([r], matched=["wbk:9", "gnis:1"])
    assert S.rule_scope(r, e, NAMES)["where"] == "the whole of Hatzic Lake"


def test_an_undrawn_part_is_one_finding_not_two():
    """The normaliser that held a rule as an undrawn part also wrote it into `review_reason`:
    `parser_uncertain` said the same fact again (126 rules; the pilot's "pre-checks always have a
    duplicate"). One `undrawn_part` finding now, carrying the parser's why; a review_reason of its
    own is still `parser_uncertain`."""
    held = {"rule_id": "x.r1", "undrawn_part": "on parts", "extents": [{"op": "whole"}],
            "review_reason": P.UNDRAWN_REASON + ": no cut-point to bind"}
    other = {"rule_id": "x.r2", "extents": [{"op": "whole"}], "review_reason": "two speeds"}
    got = P.prechecks(_entry([held, other]), binds=True)
    assert [(f["code"], f["rule_id"]) for f in got] == [("parser_uncertain", "x.r2"),
                                                      ("undrawn_part", "x.r1")]
    assert got[1]["what"].endswith("on parts — the parser: no cut-point to bind")
