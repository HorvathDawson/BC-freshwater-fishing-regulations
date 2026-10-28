"""The low-confidence cuts queue (owner 2026-10-28): every entry whose rules name a cut splits.json
marks `confidence: low` carries it on its queue row, so the owner checks it on that entry's map."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "curation-review" / "backend"))
import reuse  # noqa: E402


def test_the_guessed_cuts_reach_their_entries():
    assert reuse.low_cuts_of("r2:chapman_creek@2-5") == [
        "chapman_creek__lower_falls_below_power_line", "chapman_creek__100_m_below_lower_falls"]
    assert reuse.low_cuts_of("r1:deena_creek@6-12") == ["deena_creek__second_bridge_5_km_above_tidal"]
    assert reuse.low_cuts_of("r1:mamin_river@6-13") == ["mamin_river__third_bridge_mamin_main"]
    assert reuse.low_cuts_of("r6:cranberry_river@6-15") == ["cranberry_river__canyon_bottom_estimate"]


def test_every_low_cut_named_by_an_entry_is_listed_and_nothing_else():
    low = {sid for sid, (_w, sp) in reuse._split_records().items() if sp.get("confidence") == "low"}
    listed = {sid for _r, e in reuse._all_entries() for sid in reuse.low_cuts_of(e["entry_id"])}
    assert listed <= low and {"chapman_creek__100_m_below_lower_falls"} <= listed
    assert reuse.low_cuts_of("r1:no_such_entry@0-0") == []
