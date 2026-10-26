"""WHERE A RULE HOLDS, IN PLAIN WORDS (T189): what the review screen shows for each rule's extent
and for the entry's own scope, so a reviewer can read a rule's reach without decoding JSON.

    rule_scope(rule, entry, names)    one rule: where (the place in words), which item(s), the
                                      splits it names, an undrawn "in part" note, tributaries yes
                                      or no and why, and every carve-out or exception
    entry_scope(entry, names)         the entry: its items, where (its own extents), tributaries,
                                      and its carve-outs

Read only and pure: `names` (`Names`) says how a split, an item and an area are called — the
review app passes the bundle's own place-namer, so the words are the ones the app prints.
Tributaries are the reach builder's own answer (`classify.wants_tributaries`), never a second
reading of the fields.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Mapping, Optional

from pipeline.regs.review.tiers import unidentified_part

#: op -> how the screen says it when the place-namer cannot name the place itself
_OP_WORDS = {"upstream_of": "upstream of", "downstream_of": "downstream of"}


@dataclass
class Names:
    """How the screen calls things. `split(sid, scope)` -> words or None; `item(iid)` and
    `area(aid)` -> words or None; `place(extent, matched)` -> the bundle namer's words, False for
    "nothing to name" (the entry's own whole water), None when it cannot name it."""
    split: Callable[[str, list], Optional[str]] = lambda sid, scope: None
    item: Callable[[str], Optional[str]] = lambda iid: None
    area: Callable[[str], Optional[str]] = lambda aid: None
    place: Callable[[dict, tuple], object] = lambda x, matched: None
    #: split id -> its label (splits.json / the built atlas): the fallback for an unnamed cut
    labels: Mapping[str, str] = field(default_factory=dict)


def _split_words(sid: str, scope: list, names: Names) -> str:
    return names.split(sid, scope) or names.labels.get(sid) or sid


def _item_words(iid: str, names: Names) -> str:
    return names.item(iid) or iid


def _area_words(aid: str, names: Names) -> str:
    return names.area(aid) or aid


def _items_of(x: dict) -> list[str]:
    return list(x.get("item_ids") or ([x["item_id"]] if x.get("item_id") else []))


def extent_words(x: dict, matched: tuple, names: Names) -> str:
    """One extent as a reviewer reads it: the bundle namer's words when it has them, else the
    op spelled out with each split by its name — never the raw JSON."""
    scope = _items_of(x) or list(matched)
    got = names.place(x, tuple(matched))
    if isinstance(got, str) and got:
        kinds = x.get("feature_types") or []
        return got + (f" ({' and '.join(kinds)} only)" if kinds else "")
    op = x.get("op")
    on = [i for i in _items_of(x) if i not in matched]
    pre = (" and ".join(_item_words(i, names) for i in on) + ", ") if on else ""
    sp = [_split_words(s, scope, names) for s in x.get("splits") or []]
    if op == "whole":
        own = [i for i in _items_of(x) if i in matched]
        if own and not on and len(matched) > 1:      # one of the entry's several waters
            pre = "the whole of " + " and ".join(_item_words(i, names) for i in own) + ", "
        return (pre.rstrip(", ") or "the whole water") + (
            f", within {_area_words(x['within_area'], names)}" if x.get("within_area") else "")
    if op in _OP_WORDS:
        return f"{pre}{_OP_WORDS[op]} {sp[0] if sp else '?'}"
    if op == "between":
        return f"{pre}between {sp[0] if sp else '?'} and {sp[1] if len(sp) > 1 else '?'}"
    if op == "within":
        aid = str(x.get("area_id") or "")
        if aid.startswith(("area:region:", "region:")) or x.get("area_kind") == "region":
            what = "the whole region" if not aid else f"region {aid.rsplit(':', 1)[-1]}"
        else:
            what = (_area_words(aid, names) if aid
                    else str(x.get("area_kind") or "?").replace("_", " "))
        kinds = x.get("feature_types") or []
        return f"{pre}{'every ' + ' and '.join(kinds) + ' ' if kinds else 'all waters '}within {what}"
    if op == "rest":
        return "the rest of the water, after " + (", ".join(x.get("siblings") or []) or "?")
    if op == "steelhead_waters":
        return "known steelhead waters"
    return str(op)


def _carve_outs_of_extents(extents, matched: tuple, names: Names) -> list[str]:
    """The parts an extent takes OUT of itself: other waters, areas, an area family."""
    out: list[str] = []
    for x in extents or []:
        if not isinstance(x, dict):
            continue
        for iid in x.get("outside_items") or []:
            out.append(f"not {_item_words(iid, names)}")
        for aid in ([x["outside_area"]] if x.get("outside_area") else []) + list(
                x.get("outside_areas") or []):
            out.append(f"not within {_area_words(aid, names)}")
        if x.get("outside_area_kind"):
            out.append(f"not in any {str(x['outside_area_kind']).replace('_', ' ')}")
    return out


def _name_except(name: str) -> Optional[str]:
    """The heading's own exception ("ADAM RIVER (except Eve River)") — the book's words, shown so a
    reviewer checks the binding leaves that water out."""
    m = re.search(r"\(([^()]*\bexcept\b[^()]*)\)", name or "", flags=re.I)
    return m.group(1).strip() if m else None


def tributaries(rule: dict, entry: dict) -> dict:
    """{yes, only, why}: whether the rule walks its tributaries — the reach builder's own answer
    (`classify.wants_tributaries`) — and where that comes from."""
    from pipeline.atlas.reach.classify import UNDRAWN_PART_DOES_NOT_WALK, wants_tributaries
    yes = wants_tributaries(rule, entry)
    own = rule.get("includes_tributaries")
    if UNDRAWN_PART_DOES_NOT_WALK and str(rule.get("undrawn_part") or "").strip():
        why = "an undrawn part never walks its tributaries"
    elif rule.get("tributaries_only"):
        why = "the rule is on the tributaries only"
    elif own is not None:
        why = "the rule says so"
    elif entry.get("includes_tributaries") is not None:
        why = "inherited from the entry"
    else:
        why = "not stated, so no"
    return {"yes": bool(yes), "only": bool(rule.get("tributaries_only")), "why": why}


def rule_scope(rule: Mapping, entry: Mapping, names: Names) -> dict:
    """One rule's extent in words (`extent_words` per extent, `; `-joined), with its items, its
    splits by name, the undrawn part it holds in, the side, any unresolved place, tributaries
    (`tributaries`), its carve-outs (an extent's `outside_*`, `tributary_excludes`) and its
    exceptions (`exempts`: the zone defaults it is not subject to)."""
    rule, entry = dict(rule), dict(entry)
    matched = tuple(entry.get("matched") or ())
    extents = [x for x in rule.get("extents") or [] if isinstance(x, dict)]
    items: list[str] = []
    for x in extents:
        for i in _items_of(x) or list(matched):
            if i not in items:
                items.append(i)
    splits = []
    for x in extents:
        for s in x.get("splits") or []:
            if s not in [p["id"] for p in splits]:
                splits.append({"id": s, "name": _split_words(s, _items_of(x) or list(matched), names)})
    carve = _carve_outs_of_extents(extents, matched, names)
    for x in rule.get("tributary_excludes") or []:
        if isinstance(x, dict):
            carve.append("tributaries not walked into "
                         + extent_words({k: v for k, v in x.items() if k != "walk_past"},
                                        (), names))
    exc = []
    for x in rule.get("exempts") or []:
        if isinstance(x, dict):
            what = x.get("default_id") or x.get("target") or x.get("entry_id") or "?"
            exc.append(f"not subject to {str(what).replace('_', ' ')}"
                       + (f" ({x['note']})" if x.get("note") else ""))
    return {
        "where": "; ".join(dict.fromkeys(extent_words(x, matched, names) for x in extents))
                 or "no extent",
        "book_says": rule.get("extent_text") or None,
        "items": [{"id": i, "name": _item_words(i, names)} for i in items],
        "splits": splits,
        "in_part": str(rule.get("undrawn_part") or "").strip() or None,
        # T207: "on parts" names no reach — the screen shows it as a note, never a warning
        "in_part_identified": not unidentified_part(rule),
        "side": rule.get("side") or None,
        "unresolved": list(rule.get("unresolved_locators") or []),
        "tributaries": tributaries(rule, entry),
        "carve_outs": carve,
        "exceptions": exc,
    }


def entry_scope(entry: Mapping, names: Names) -> dict:
    """The entry's own scope (which CLIPS every rule): its items, where (its extents in words),
    whether it includes tributaries ("not stated" when the row says nothing: then no rule
    inherits a walk), its carve-outs, the heading's own exception, and how many of its rules walk
    their tributaries."""
    entry = dict(entry)
    matched = tuple(entry.get("matched") or ())
    extents = [x for x in entry.get("extents") or [] if isinstance(x, dict)]
    t = entry.get("includes_tributaries")
    rules = [r for r in entry.get("rules") or [] if isinstance(r, dict)]
    walking = [r.get("rule_id") for r in rules if tributaries(r, entry)["yes"]]
    return {
        "items": [{"id": i, "name": _item_words(i, names)} for i in matched],
        "where": "; ".join(dict.fromkeys(extent_words(x, matched, names) for x in extents))
                 or ("the whole water" if matched else "no water of its own: each rule's extent"),
        "tributaries": {"yes": bool(t), "stated": t is not None},
        "carve_outs": _carve_outs_of_extents(extents, matched, names),
        "name_except": _name_except(str(entry.get("name") or "")),
        "rules_walking": walking,
    }


def namer_names(namer, labels: Mapping[str, str]) -> Names:
    """`Names` over the bundle's `PlaceNamer` (`place_names.PlaceNamer`): the app's own words."""
    return Names(split=lambda sid, scope: namer.point(sid, scope)[0],
                 item=namer._item, area=namer._area,
                 place=lambda x, matched: namer.extent(x, matched), labels=dict(labels))
