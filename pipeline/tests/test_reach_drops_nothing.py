"""T546: THE REACH RUN DROPS NOTHING SILENTLY — a curated id, a scope or a closure piece that cannot be
placed stops the run, or is counted where the run and the release check read it.

Synthetic river (`test_registry_flowing._river`), no FWA. Each test names the mutation that turns it red."""
from __future__ import annotations

import pytest

from pipeline.atlas.reach.build import build_reaches, gone_refs
from pipeline.tests.test_registry_flowing import _river


def _entry(**kw):
    return {"entry_id": "r1:x_river@1-1", "name": "X RIVER", "region": "1", "matched": ["gnis:1"],
            "rules": [{"rule_id": "r1", "type": "closure", "extents": [{"op": "whole"}]}], **kw}


# ------------------------------------------------------------------ items 1 + 6: a gone id stops the run

def test_a_matched_id_the_registry_lacks_stops_the_run():
    """Mutation: drop the `gone_refs` refusal in `build_reaches` — `covered_ids` filters the id out and the
    row binds the rest of its waters with no word said."""
    g, reg = _river()
    assert build_reaches([("1", _entry())], reg, g).report.n_rules == 1         # clean: runs
    with pytest.raises(SystemExit, match=r"r1:x_river@1-1 matched: gnis:404"):
        build_reaches([("1", _entry(matched=["gnis:1", "gnis:404"]))], reg, g)


def test_an_extent_or_carve_out_id_the_registry_lacks_is_named_too():
    g, reg = _river()
    e = _entry(rules=[{"rule_id": "r1", "type": "closure",
                       "extents": [{"op": "whole", "item_ids": ["gnis:1", "wbk:404"],
                                    "outside_items": ["wbk:405"]}]}])
    assert gone_refs([e], reg) == ["r1:x_river@1-1 item_ids: wbk:404",
                                   "r1:x_river@1-1 outside_items: wbk:405"]
    with pytest.raises(SystemExit, match="2 record id"):
        build_reaches([("1", e)], reg, g)


def test_an_absorbed_id_is_not_counted_as_gone():
    """An absorbed id has its own refusal (`absorbed_refs`, naming the absorber): not reported twice."""
    from pipeline.atlas.registry.flowing import merge_flowing_polygons
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    assert "wbk:2" not in out
    assert gone_refs([_entry(matched=["wbk:2"])], out) == []
