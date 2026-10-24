"""The open-days strip (answers 2.6): what the water strip draws per segment (`strip_code`, `strip_legend`)."""
from __future__ import annotations

from typing import Dict, List


#: The strip's vocabulary: (state, bit, label, says). A segment's `strip` code is the OR of its
#: states' bits; 0 is open with none of them. CLOSED is drawn alone (the gear of a closed water is
#: moot); RELEASE, FLY_ONLY, ARTIFICIAL_FLY_ONLY and BAIT_BAN combine (Vedder, May: fly fishing
#: only + bait ban). ONE SOURCE: closed is the display frame's own status (the verdicts'),
#: release the card's decided answers (`rows`), the gear states the gear answer (`gear.fly`,
#: `gear.bait_ban`) at the same part key and segment.
STRIP = (("open", 0, "open", "Open."),
         ("closed", 1, "closed", "Closed: no fishing for any game fish."),
         ("release", 2, "release only", "Open, but every game fish caught must be released."),
         ("fly_only", 4, "fly fishing only", "Fly fishing only."),
         ("artificial_fly_only", 8, "artificial flies only",
          "Artificial flies only: no bait or other lures."),
         ("bait_ban", 16, "bait ban", "Bait ban: no bait of any kind."))
STRIP_BIT = {st: bit for st, bit, _, _ in STRIP}


def strip_legend() -> List[dict]:
    """The strip's legend (`display.strip`): every state with its bit, label and sentence."""
    return [{"state": st, "bit": bit, "label": lab, "says": says} for st, bit, lab, says in STRIP]


def strip_code(display_v: dict, rows_v: dict, gear_v: dict) -> int:
    """One segment's strip code from the three answers at that part key and segment: CLOSED where
    the display status is closed (alone); else RELEASE where no game fish the card asks about may
    be kept (every decided answer, hatchery and wild, is release or closed, and one is release),
    FLY_ONLY / ARTIFICIAL_FLY_ONLY by the gear answer's `fly`, BAIT_BAN by its `bait_ban`."""
    from pipeline.deliver import types as T
    if display_v["status"] == "closed":
        return STRIP_BIT["closed"]
    code = 0
    game = set(T.GAME_FISH)
    got = [d["status"] for S, v in rows_v["fish"].items() if S in game
           for d in (v["hatchery"], v["wild"]) if d is not None]
    if got and "release" in got and all(g in ("release", "closed") for g in got):
        code |= STRIP_BIT["release"]
    if gear_v.get("fly") == "fly_fishing_only":
        code |= STRIP_BIT["fly_only"]
    elif gear_v.get("fly") == "artificial_fly_only":
        code |= STRIP_BIT["artificial_fly_only"]
    if gear_v.get("bait_ban"):
        code |= STRIP_BIT["bait_ban"]
    return code


def compose(value: dict, others: Dict[str, dict]) -> dict:
    """The display frame with its strip code (`strip_code`), from the rows and gear frames at the
    same part key and segment (`answers.Section.compose`). A tidal frame has no strip. RULINGS P12: an
    inherited lift the card already names (a quota lift walked per T3a: "On the Duncan River and its
    tributaries, …") is not said again."""
    if value.get("status") == "tidal":
        return value
    out = {**value, "strip": strip_code(value, others["rows"], others["gear"])}
    if value.get("inherited"):
        carded = {e["by"] for r in others["rows"].get("rows") or [] for L in (r.get("group") or {}).get("lifts") or []
                  for e in L.get("each") or []}
        keep = [x for x in value["inherited"] if x["by"] not in carded]
        if keep:
            out["inherited"] = keep
        else:
            out.pop("inherited")
    return out
