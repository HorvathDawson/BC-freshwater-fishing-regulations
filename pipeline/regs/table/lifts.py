"""A LIFT IS A SUBTRACTION, not a footnote.

"Only non-game fish may be speared" closes every game fish to the spear. "except burbot, which
may also be speared in Regions 3, 5, 6, 7 and 8" does not argue with that rule — it takes ONE
FISH out of it, and only in five regions. Written as prose beside the ban it is a note the
reader has to apply themselves; written as a subtraction it is just the ban's subject, minus
burbot, wherever the lift bites.

That is the same `excepts` the Subject already carries, so a lift needs no new machinery — only
somewhere to be applied before the fold runs. Two shapes:

    with species   narrows the target: ALL_GAME_FISH minus BB
    without        removes the target here entirely — the Fraser lifting the spring stream
                   closure does not carve a fish out of it, it disapplies it

Both are scoped by `where`, because a lift that names regions is a lift in those regions only —
applying it everywhere would open five regions' fish in the three where the ban is absolute.
"""
from __future__ import annotations
from typing import Dict, FrozenSet, List, Set, Tuple

from pipeline.regs.table.where import parse_where
from pipeline.regs.table.corpus import rid, rule_part
from pipeline.regs.table.subject import Subject


def lifts_here(rules: List[dict], here: FrozenSet[str]
               ) -> Tuple[Dict[str, FrozenSet[str]], Set[str], List[dict]]:
    """(target rule -> species lifted out of it, targets removed outright).

    `here` is the set of region ids this section is in; a lift whose `where` names regions bites
    only in those.

    A LIFT WHOSE PLACE CANNOT BE DRAWN IS NOT APPLIED. This module first applied them, reasoning
    that ignoring an exception the book wrote fails toward the stricter answer. That reasoning is
    backwards for a CLOSURE, and the corpus has the case: Region 6's "No fishing for steelhead in
    streams, May 15 – Jun 15" carries an exemption noting "mainstem Skeena, Nass, Iskut, Stikine
    and Taku". Applied everywhere, the closure disappeared from the Babine — which the note does
    not exempt — and a reader was told a river is open in the middle of a steelhead closure.
    Held back, the closure stands and the exemption rides beside it in the reader's own words,
    which is the one form of this that cannot mislead.

    AND A RULE CANNOT LIFT ITSELF. That same rule names its OWN entry as the default it exempts,
    so it lifted itself on every water in Region 6.
    """
    narrow: Dict[str, Set[str]] = {}
    drop: Set[str] = set()
    unresolved: List[dict] = []
    # Keys are COMPOSITE (`entry::rule`) because a bare rule id is not unique; `exempts` names
    # the bare one, so the match is on the rule half and the key that comes back is whole.
    ids = {rid(r) for r in rules}
    for r in rules:
        for ex in (r.get("exempts") or []):
            bites = parse_where(r.get("extent_text")).bites_in(here)
            if bites is False:
                continue
            if bites is None:
                unresolved.append({"lifter": rid(r), "note": ex.get("note") or "",
                                   "where": r.get("extent_text") or ""})
                continue
            targets = set()
            if ex.get("target"):
                targets |= {i for i in ids if rule_part(i) == ex["target"]}
            if ex.get("default_id"):
                targets |= {i for i in ids
                            if rule_part(i).split(".")[0] == ex["default_id"]}
            targets.discard(rid(r))          # a rule cannot lift itself
            sp = set(r.get("species") or [])
            for t in targets:
                tgt = next((y for y in rules if rid(y) == t), None)
                # A LIFT THAT TAKES THE WHOLE SUBJECT IS A DROP, NOT A NARROWING. Every "Exempt
                # from spring closure" names ALL_GAME_FISH, which is the closure's own subject —
                # narrowed by it the target covered nothing, entered no chain, and vanished
                # instead of being shown as lifted. Corpus-wide, not one rung said "lifted".
                whole = (tgt is not None and sp
                         and Subject(frozenset(sp)).covers(Subject(frozenset(tgt.get("species")
                                                                             or []))))
                if sp and not whole:
                    narrow.setdefault(t, set()).update(sp)
                else:
                    drop.add(t)
    return {k: frozenset(v) for k, v in narrow.items()}, drop, unresolved


def self_lifting(rules: List[dict]) -> List[dict]:
    """Rules whose own exemption names them — a DATA defect, reported rather than absorbed.

    `lifts_here` refuses to let a rule lift itself, which stops the damage. It does not fix the
    entry, and a workaround that leaves no trace is how a corpus defect becomes permanent. One
    rule is in this state:

        z6:steelhead_stream_closure.r1 — "No fishing for steelhead in streams, May 15 – Jun 15"
        exempts: default_id "steelhead_stream_closure"  (its own entry)
        note:    "mainstem Skeena, Nass, Iskut, Stikine and Taku, where not already closed"

    The note says what the exemption is FOR: five named mainstems. The `default_id` should name
    the closure being lifted on those waters — instead it names this rule, so the closure lifted
    itself on every water in Region 6 and the Babine lost a steelhead season the book keeps.
    The fix is in the catalogue, not here: the exemption belongs on the five waters that have
    it, or the note belongs in `extent_text` where a scope can be read from it.
    """
    out = []
    for r in rules:
        me = rule_part(rid(r))
        for ex in (r.get("exempts") or []):
            if ex.get("target") == me or (ex.get("default_id")
                                          and me.split(".")[0] == ex["default_id"]):
                out.append({"rule": rid(r), "note": ex.get("note") or "",
                            "label": r.get("label") or ""})
    return out


def contradicted_closures(rules: List[dict]) -> List[dict]:
    """A superior closure that a lower table opens, with nothing saying the closure is lifted —
    a DATA defect, reported rather than absorbed.

    The order in `resolve` says a take of zero stands unless something lifts it, and the only
    thing that lifts one is `exempts`. So where a regional table writes "White Sturgeon: CATCH
    AND RELEASE ONLY" under a federal closure that covers white sturgeon, the closure stands and
    the reader on the Fraser is told "you may not fish for it" — on the one water in the
    province with a legal sturgeon fishery. Thirty-five sections read that way.

    The module cannot fix it, and must not guess at it. The book's own protected-species entry
    scopes the fish by POPULATION — "White Sturgeon (Nechako, Upper Fraser, Kootenay and
    Columbia populations)" — and the catalogue's `PROTECTED_SPECIES` group holds bare `WSG`,
    which drops the qualifier that is the whole difference between "closed everywhere" and
    "closed except the Fraser fishery". A population is a watershed, and the atlas cannot test
    a watershed; what it can test is a region, and a lift. The types that exist carry the fix:

        the rules that STATE the fishery — z2:species_quotas.r7, z3:species_quotas.r7 ("White
        Sturgeon: CATCH AND RELEASE ONLY"), z5:white_sturgeon.r2 and zp:white_sturgeon_licence
        .r2 ("This is a catch-and-release only fishery") — should each carry
            exempts: [{"target": "protected_species.r1", "note": "the Fraser catch-and-release
                       fishery — the book lists only four populations as protected"}]
        and `lifts_here` will then subtract WSG from the closure exactly where those rules bite:
        region-wide for the regional tables, which is what the tables themselves assert.

        zp:white_sturgeon_licence.r2 also needs r1's `extent_text` ("Fraser watershed, CPR
        Bridge at Mission to Williams Lake River"): with none it is a province-wide release,
        and the book says the opposite in the same sentence.

    Until then the closure stands, the regional release rides beneath it saying so, and this
    report names the pair every run. What it reports: every superior take-of-zero closure, and
    every lower-authority rule that permits fishing for a fish the closure covers, where no
    rule in the corpus names the closure in `exempts`.
    """
    from pipeline.regs.table.subject import Subject
    lifted = {ex.get("target") for r in rules for ex in (r.get("exempts") or []) if ex.get("target")}
    lifted |= {ex.get("default_id") for r in rules for ex in (r.get("exempts") or [])
               if ex.get("default_id")}
    out = []
    for c in rules:
        if str(c.get("authority") or "") != "superior" or c.get("take") != 0:
            continue
        if c.get("may_target"):
            continue                                   # a release is not a closure
        me = rule_part(rid(c))
        if me in lifted or me.split(".")[0] in lifted:
            continue
        shut = Subject(frozenset(c.get("species") or []),
                       excepts=frozenset(c.get("species_except") or []))
        # A closure on the WATER — the parks, the reserves — is a place, not a fish; a lake's
        # trout quota elsewhere does not contradict it. Only a closure that names its fish.
        if shut.is_everything or shut.covers(Subject(frozenset({"ALL_GAME_FISH"}))):
            continue
        seen = set()
        for r in rules:
            if r is c or r.get("method") or not r.get("species"):
                continue
            opens = r.get("unlimited") or (r.get("take") or 0) > 0 or (
                r.get("take") == 0 and r.get("may_target"))
            if not opens:
                continue
            fish = Subject(frozenset(r["species"]),
                           excepts=frozenset(r.get("species_except") or []))
            if not fish.effective() or not shut._fish_covers(fish) or rid(r) in seen:
                continue
            seen.add(rid(r))
            out.append({"closure": rid(c), "opened_by": rid(r),
                        "verbatim": r.get("verbatim") or "", "extent": r.get("extent_text") or ""})
    return out
