"""The steelhead presence line, in angler words, placed inside the steelhead section (`place_presence`)."""
from __future__ import annotations

from typing import List, Optional
from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError


#: ROUND14 4 (owner): the presence line, in angler words, said INSIDE the steelhead section
PRESENCE_WORDS = {"possible_with_rules": "Steelhead rules apply here; steelhead may not be present in this water.",
                  "known_with_rules": "Steelhead are known to be in this water."}


def presence_words(line: str, anadromous: bool) -> str:
    """The presence line's words. UR2 4 (user panel: "Steelhead are known …" beside a harvest limit read as a warning
    with no point): where a big rainbow IS a steelhead (`steelhead_water`: known, a stream, steelhead rules apply —
    AGENTS 54) the line says what follows from it, in the book's p.80 definition ("steelhead: a rainbow trout longer
    than 50 cm in waters where anadromous rainbow trout are found"): "Steelhead are known to be in this water, so a
    rainbow trout over 50 cm (20 in) counts as a steelhead." Elsewhere (a lake whose own row prints steelhead) the
    words as they were."""
    if line == "known_with_rules" and anadromous:
        from pipeline.deliver.bundle import read
        # ROUND20g 2 (owner: fewer words; the ⓘ beside it says why — steelhead are known here)
        return f"A rainbow trout over {common.cm_in(read._steelhead_min_cm())} here counts as a steelhead."
    return PRESENCE_WORDS[line]


def place_presence(rows: List[dict], line: Optional[str], anadromous: bool = False) -> None:
    """ROUND14 4 (owner): where steelhead are known (or steelhead rules apply), the presence line
    (`presence` {line, says}) sits IN THE STEELHEAD SECTION, exactly once: the card's steelhead kind row
    (kept), its not-kept steelhead line (closed or released), else the row holding steelhead (a release
    or closed row, or a keep row with no card). A line with no steelhead section is refused — never the
    note without the section. `known_no_rules` is about rainbow trout and stays the frame's own."""
    if line not in PRESENCE_WORDS:
        return
    note = {"line": line, "says": presence_words(line, anadromous)}
    for r in rows:
        if "ST" not in (r.get("all_members") or []):
            continue
        g = r.get("group") or {}
        for b in g.get("buckets") or []:
            for k in list(b["kinds"]) + [k for s in b.get("groups") or [] for k in s["kinds"]]:
                if "ST" in k["species"]:
                    k["presence"] = note
                    # ROUND18 1: the same kind's node in the card's tree carries it too
                    from pipeline.deliver.answers.tree import walk
                    for x, _t in walk(g.get("tree") or []):
                        if x["node"] == "kind" and "ST" in x["species"]:
                            x["presence"] = note
                    return
        for x in g.get("not_kept") or []:
            if "ST" in x["species"]:
                x["presence"] = note
                return
        r["presence"] = note
        return
    raise AnswersError(f"rows: steelhead line {line!r} with no steelhead section on the card")
