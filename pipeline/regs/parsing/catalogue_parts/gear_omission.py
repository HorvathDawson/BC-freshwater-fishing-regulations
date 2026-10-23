"""EVERY GEAR PHRASE A RULE PRINTS LANDS IN A STORED FIELD (T92).

The gear model (`gear.py`) makes an INVERSION unwritable — `barb` is `only: ["barbless"]`, there is
no polarity flag to flip. It does nothing about OMISSION. Eight lakes printed "single barbless
hook" and stored only `{slot: barb}`: a barbless treble was legal there per the data, and every
check stayed green, because every check ran field → text (`count_in_words`, "is this number in
its own sentence") and a field that was never written has nothing to check.

This runs the other way: PHRASE → FIELD. Each printed gear phrase names the field that must hold
it, and some rule quoting the same words must store that field. "Some rule", because a compound
sentence is split into sibling rules that share one verbatim ("Bait ban and single barbless hook"
is a bait rule and a tackle rule) — but only a sibling quoting the SAME span of the row: the same
three words printed in two clauses are two conjuncts, and storing one does not account for the
other.

A phrase printed as what an exemption LIFTS ("EXEMPT from single barbless hooks", "Exempt from
spring closure and from bait ban") is held by the lift (`exempts`), not by a gear field — but only
inside the clause that says "exempt"; a `;` ends it.

What the corpus cannot store is listed in `KEPT_GEAR_GAPS` with the reason, and the corpus test
fails on an unexplained miss AND on a kept gap that no longer fires.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from .text import squash

if TYPE_CHECKING:  # annotations only
    from .rule import CatalogueRule
    from .entry import CatalogueEntry


def _clauses(rule: "CatalogueRule"):
    return list(rule.gear or ())


def _members(c) -> set:
    return set(c.allow or ()) | set(c.only or ()) | set(c.ban or ()) | set(c.members or ())


def _slot(rule: "CatalogueRule", slot: str, ok: Callable = lambda c: True) -> bool:
    return any(c.slot.value == slot and ok(c) for c in _clauses(rule))


def _means(rule: "CatalogueRule", token: str) -> bool:
    """The rule speaks about a MEANS of fishing: binds while it is used (`while`), grants or bans
    it (`method` slot), constrains its own gear (the spec slot named for it), or conditions a
    clause on it (`when.method` / `unless`)."""
    if token in (rule.while_ or ()):
        return True
    for c in _clauses(rule):
        if c.slot.value == token or (c.slot.value == "method" and token in _members(c)):
            return True
        for w in ([c.when] if c.when else []) + list(c.unless or ()):
            if w.method is not None and w.method.value == token:
                return True
    return False


def _level(rule: "CatalogueRule", *levels: str) -> bool:
    return (rule.type.value == "vessel_rule" and rule.level is not None
            and rule.level.value in levels)


def _from_powered_boat(rule: "CatalogueRule") -> bool:
    return any(c.when is not None and c.when.angler is not None
               and c.when.angler.value == "in_powered_boat" for c in _clauses(rule))


@dataclass(frozen=True)
class GearPhrase:
    #: The phrase's name, as a miss reports it and `KEPT_GEAR_GAPS` keys it.
    name: str
    #: How the book prints it (case-insensitive, on the rule's own verbatim).
    printed: re.Pattern
    #: The field that must hold it, in words for the message.
    needs: str
    #: Does this rule store it?
    stores: Callable[["CatalogueRule"], bool]


def _p(rx: str) -> re.Pattern:
    return re.compile(rx, re.I)


#: THE VOCABULARY, phrase → field. Ordered: a miss list is reported in this order per rule.
#:
#: The "161 electric motor" hits a first text-only sweep flagged are not misses: "Electric motor
#: only" is a `vessel_rule` at `level: electric_only`, whose 7.5 kW is the level's definition
#: (`PropulsionLevel`). That is why this is a vocabulary and not a word search.
GEAR_PHRASES: Tuple[GearPhrase, ...] = (
    # "single barbless hook" is one point on one hook (`points_per_hook`); a set line's
    # "One line, single hook" is one hook on the line (`hooks_per_line`) — but only on a set line:
    # `hooks_per_line: 1` on an angling rule leaves a treble legal.
    GearPhrase("single_hook", _p(r"\bsingle\s+(?:barbless\s+)?hooks?\b"),
               "`points_per_hook` at max 1 (`hooks_per_line` on a set line)",
               lambda r: _slot(r, "points_per_hook", lambda c: c.max == 1)
               or (_means(r, "set_lining") and _slot(r, "hooks_per_line", lambda c: c.max == 1))),
    GearPhrase("barbless", _p(r"\bbarbless\b"), "`barb: only [barbless]`",
               lambda r: _slot(r, "barb", lambda c: "barbless" in (c.only or ()))),
    GearPhrase("bait_ban", _p(r"\bbait ban\b|\bno (?:natural )?bait\b"), "a `bait` ban",
               lambda r: _slot(r, "bait", lambda c: bool(c.ban))),
    GearPhrase("fly_fishing_only", _p(r"\bfly[- ]fishing only\b"),
               "`method: only [fly_fishing]`",
               lambda r: _slot(r, "method", lambda c: "fly_fishing" in (c.only or ()))),
    GearPhrase("artificial_fly", _p(r"\bartificial fl(?:y|ies)\b"),
               "`artificial_fly` in `lure` or among terminal attachments, or `flies_per_line`",
               lambda r: _slot(r, "lure", lambda c: "artificial_fly" in _members(c))
               or _slot(r, "flies_per_line")
               or _slot(r, "terminal_attachments_per_line",
                        lambda c: "artificial_fly" in _members(c))),
    GearPhrase("treble", _p(r"\btreble"), "`points_per_hook`",
               lambda r: _slot(r, "points_per_hook")),
    GearPhrase("set_line", _p(r"\bset[- ]?lin(?:e|es|ing)\b"),
               "`set_lining` as a means (`while`, `method`, its spec slot or a `when`)",
               lambda r: _means(r, "set_lining")),
    GearPhrase("ice_fishing", _p(r"\bice[- ]?fish"),
               "`ice_fishing` as a means, or the `ice_hut` slot",
               lambda r: _means(r, "ice_fishing") or _slot(r, "ice_hut")),
    GearPhrase("ice_hut", _p(r"\bice (?:fishing )?huts?\b"),
               "the `ice_hut` slot or `remove_ice_hut_before_breakup`",
               lambda r: _slot(r, "ice_hut")
               or "remove_ice_hut_before_breakup" in (r.conduct or ())),
    GearPhrase("electric_motor", _p(r"\belectric motors?\b"),
               "`vessel_rule` at `level: electric_only`",
               lambda r: _level(r, "electric_only")),
    GearPhrase("engine_power", _p(r"\b\d+(?:\.\d+)?\s*(?:kw|hp)\b|\bengine power\b|"
                                  r"\bhorse ?power\b"),
               "`vessel_rule` at `electric_only` / `power_capped`, or `max_power_kw`",
               lambda r: _level(r, "electric_only", "power_capped")
               or (r.type.value == "vessel_rule" and r.max_power_kw is not None)),
    GearPhrase("powered_boats", _p(r"\bpowered boats?\b"),
               "`vessel_rule` at `unpowered` / `none`, or a clause `when angler: in_powered_boat`",
               lambda r: _level(r, "unpowered", "none") or _from_powered_boat(r)),
)


#: GAPS KEPT ON PURPOSE, each with why the corpus cannot (or must not) store a field for it.
#: Keyed (entry_id, rule_id, phrase). The corpus test fails when one stops firing — a fixed rule
#: leaves a silencer behind that would hide the next real miss on it.
KEPT_GEAR_GAPS: Dict[Tuple[str, str, str], str] = {
    ("r2:harrison_river_from_the_fraser_river_upstream_to_harrison_la@2-18", "harrison_river.r3",
     "powered_boats"):
        "printed 'Speed restriction OR no powered boats in small bays … as signed': which one "
        "applies is set by signs, not the book, so neither level can be stored; held as an "
        "undrawn note with a review_reason",
    ("zp:bait", "bait.r4", "bait_ban"):
        "'… as bait unless a bait ban applies' defers to the bait bans held elsewhere (the "
        "regional bait_ban entries and each water's own); it imposes no ban of its own",
    ("r7:one_island_lake@7-20", "one_island_lake.r1", "ice_hut"):
        "'See ice hut warning, page 63' is a pointer; the warning itself is the Region 7 zone "
        "entry's rule, which carries the conduct",
    ("z7b:ice_fishing_huts", "ice_fishing_huts.r1", "ice_fishing"):
        "'Ice fishing huts SHOULD have the owner's contact information displayed' is advice, "
        "not a law, and no conduct token or ice_hut spec says it (owner question in T92)",
    ("z7b:ice_fishing_huts", "ice_fishing_huts.r1", "ice_hut"):
        "'Ice fishing huts SHOULD have the owner's contact information displayed' is advice, "
        "not a law, and no conduct token or ice_hut spec says it (owner question in T92)",
}

#: Where a clause ends: an exemption ("Exempt from …") reaches no further than this.
_CLAUSE_END = re.compile(r"[;\n]|\.\s")
_EXEMPT = re.compile(r"\bexempt", re.I)


def _noted(rule: "CatalogueRule", phrase: GearPhrase) -> bool:
    """THE MODEL'S OWN ESCAPE: what a closed vocabulary cannot say goes in a `note` (on a clause's
    `when`, an `unless`, or a `requires`), and a note COSTS a `review_reason` — so a phrase written
    there is held and its gap is already visible. Only where the note itself prints the phrase."""
    for c in _clauses(rule):
        notes = [w.note for w in ([c.when] if c.when else []) + list(c.unless or ())]
        notes.append(c.requires.note if c.requires else "")
        if any(n and phrase.printed.search(n) for n in notes):
            return True
    return False


def _span(row: str, verbatim: str) -> Optional[Tuple[int, int]]:
    at = row.find(verbatim)
    return None if at < 0 else (at, at + len(verbatim))


#: WHICH PHRASES A ZONE DEFAULT IS. A lift covers a phrase only when it lifts that phrase — Duncan
#: River prints "Exempt from spring closure, exempt from … catch and release, bait ban" with no `;`,
#: and the bait ban there is imposed. Every `EXEMPTABLE_DEFAULTS` slug is classified (a test pins
#: it), so a new default is a decision here, not a silent "lifts nothing".
DEFAULT_LIFTS: Dict[str, frozenset] = {
    "spring_stream_closure": frozenset(),
    "summer_stream_closure": frozenset(),
    "steelhead_stream_closure": frozenset(),
    "trout_char_winter_release": frozenset(),
    "bait_ban_streams": frozenset({"bait_ban"}),
    "single_barbless_hook": frozenset({"single_hook", "barbless"}),
}


def _lifts(rule: "CatalogueRule", phrase: GearPhrase, entry: "CatalogueEntry") -> bool:
    """Does one of the rule's `exempts` lift THIS phrase? A zone default by `DEFAULT_LIFTS`; a
    rule of this entry when that rule stores the phrase; a rule of another entry is taken on
    trust (the bundle resolves it — `pipeline.deliver.bundle.rules._exempts`)."""
    for x in rule.exempts or ():
        if x.default_id:
            if phrase.name in DEFAULT_LIFTS.get(x.default_id, ()):
                return True
        elif x.entry_id and x.entry_id != entry.entry_id:
            return True
        elif any(r.rule_id == x.target and phrase.stores(r) for r in entry.rules):
            return True
    return False


def _lifted(rule: "CatalogueRule", phrase: GearPhrase, row_lines: List[str]) -> bool:
    """Is the phrase, as this rule prints it, what an EXEMPTION lifts — "exempt" earlier in the
    same clause (of the rule's own verbatim, or of the printed line the verbatim sits in)?"""
    v = squash(rule.verbatim)
    texts = [rule.verbatim] + [line for line in row_lines if v and v in squash(line)]
    for text in texts:
        for clause in _CLAUSE_END.split(text):
            m = phrase.printed.search(clause)
            if m and _EXEMPT.search(clause[:m.start()]):
                return True
    return False


def gear_misses(entry: "CatalogueEntry",
                kept: Optional[Dict[Tuple[str, str, str], str]] = None
                ) -> List[Tuple[int, str, str]]:
    """`(rule index, rule_id, phrase name)` for every gear phrase a rule prints that no rule
    quoting the same span stores — less the gaps in `kept` (default `KEPT_GEAR_GAPS`)."""
    kept = KEPT_GEAR_GAPS if kept is None else kept
    row = squash(entry.regs_verbatim)
    row_lines = entry.regs_verbatim.split("\n")
    rules = list(entry.rules)
    said = [squash(r.verbatim) for r in rules]
    spans = [_span(row, s) for s in said]

    def same_span(i: int, j: int) -> bool:
        if said[i] == said[j]:
            return True
        a, b = spans[i], spans[j]
        return a is not None and b is not None and a[0] < b[1] and b[0] < a[1]

    out: List[Tuple[int, str, str]] = []
    for i, rule in enumerate(rules):
        for phrase in GEAR_PHRASES:
            if not phrase.printed.search(rule.verbatim):
                continue
            sibs = [j for j in range(len(rules))
                    if same_span(i, j) and phrase.printed.search(rules[j].verbatim)]
            if any(phrase.stores(rules[j]) or _noted(rules[j], phrase) for j in sibs):
                continue
            if (any(_lifts(rules[j], phrase, entry) for j in sibs)
                    and _lifted(rule, phrase, row_lines)):
                continue
            if (entry.entry_id, rule.rule_id, phrase.name) in kept:
                continue
            out.append((i, rule.rule_id, phrase.name))
    return out


_BY_NAME = {p.name: p for p in GEAR_PHRASES}


def gear_omissions(entry: "CatalogueEntry") -> List[Tuple[list, str]]:
    """`post_model_checks` form: `(path, message)` per miss, addressed to the rule."""
    out = []
    for i, rid, name in gear_misses(entry):
        p = _BY_NAME[name]
        m = p.printed.search(entry.rules[i].verbatim)
        out.append((["rules", i],
                    f"{rid}: prints {m.group(0)!r} (gear phrase `{name}`) and no rule quoting it "
                    f"stores {p.needs} — the conjunct is dropped from every answer"))
    return out
