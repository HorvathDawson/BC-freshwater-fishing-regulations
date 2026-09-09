"""A quota, in the words a person reads — generated from the numbers, never typed beside them.

THE LABEL AND THE NUMBER MUST NOT BE TWO FACTS. `Rule.details` is written by hand, and this
corpus has already shown twice what that costs: 26 zone quotas read "Daily quota 25" with the
species sitting in a field beside them, and the synopsis rules carry the same defect under
its own note. A sentence composed from the structure cannot drift from it, because there is
nothing to drift from.

ONE FUNCTION, because the app, the review tool and any table that lays quotas out are all
answering the same question and must not each phrase it. This is the same argument that moved
the gauge trust wording into `flow.ts`: where a value is turned into words, it is turned once.
"""

from __future__ import annotations

from typing import Iterable, Sequence

from pipeline.regs.parsing.entry_models import Limit, LimitKind
from pipeline.regs.parsing.species import COMMON_NAME

#: The catalogue's names are not the synopsis's words. "Char, General" is a table entry;
#: "Char" is what the regulation says and what an angler calls it.
_SAY = {
    "Char, General": "Char", "Bass/Sunfish (General)": "Bass", "Signal Crayfish": "Crayfish",
    "Black Crappie": "Crappie", "Salmon (General)": "Salmon", "Trout (General)": "Trout",
    "Whitefish (General)": "Whitefish", "Sturgeon (General)": "Sturgeon",
    "Perch (General)": "Perch", "Minnow (General)": "Minnow", "Sucker (General)": "Sucker",
}


#: Collective words the regulations use, and the set each one IS. Checked longest-first, so
#: "trout and char" wins over "trout" when both would match.
def _collectives() -> list[tuple[str, frozenset[str]]]:
    from pipeline.regs.parsing.species import expand_group

    trout, char = expand_group("TRT"), expand_group("SLV")
    return sorted(
        [("Trout and Char", frozenset(trout | {"SLV"})),
         ("Trout", frozenset(trout)),
         ("Char", frozenset({"SLV"}) | char)],
        key=lambda kv: -len(kv[1]))


def species_words(codes: Sequence[str]) -> str:
    """"Bull trout and lake trout" — the fish, as a reader would list them.

    A COLLECTIVE SET IS SAID AS ITS WORD. The corpus stores the six trout species rather
    than a `TRT` group code, because storing the word only on one side of the corpus made
    the two incomparable — and comparing them is the whole of the override rule. But
    "Rainbow Trout, Cutthroat Trout, Westslope (Yellowstone) Cutthroat Trout, Coastal
    Cutthroat Trout, Brown Trout and Golden Trout" is not what the synopsis says or what
    anybody calls it. Stored as the set, said as the word.
    """
    if not codes:
        return ""
    have = frozenset(codes)
    for word, members in _collectives():
        if members and members <= have:
            rest = sorted(have - members)
            return word if not rest else f"{word} and {species_words(rest)}"
    names = []
    for c in codes:
        n = _SAY.get(COMMON_NAME.get(c, c), COMMON_NAME.get(c, c))
        n = n.replace(" (General)", "").replace(", General", "")
        if n not in names:
            names.append(n)
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return ", ".join(names[:-1]) + " and " + names[-1]


def _count(lim: Limit) -> str:
    if lim.unlimited:
        return "unlimited"
    if lim.kind is LimitKind.POSSESSION and lim.per_daily is not None:
        return f"{lim.per_daily} daily quotas"
    if lim.take == 0:
        return "release all"
    return "no limit stated" if lim.take is None else str(lim.take)


def _qualifiers(lim: Limit) -> list[str]:
    """The conditions on a count, each said once and in a fixed order.

    ORDER IS FIXED so two limits that differ only in one qualifier read differently in that
    one place — a reader comparing a region's quota with a river's should not also have to
    notice that the words were rearranged.
    """
    q: list[str] = []
    if lim.combined:
        q.append("all species combined")
    if lim.over_cm is not None:
        q.append(f"over {lim.over_cm} cm")
    if lim.under_cm is not None:
        q.append(f"under {lim.under_cm} cm")
    if lim.water is not None:
        q.append(f"from {lim.water.value}s")
    if lim.origin is not None:
        q.append(f"{lim.origin.value} only")
    return q


def limit_words(lim: Limit, rule_species: Iterable[str] = (), *, named: bool = True) -> str:
    """One limit as a phrase: "Bull trout: 1" or "none under 60 cm".

    A ZERO WITH A SIZE IS A SIZE RULE, not a quota of nothing. "none under 60 cm" and "release
    all from streams" are both `take: 0` — the first says which fish may not be kept and the
    second says where — so the words follow the qualifier rather than announcing a zero and
    then explaining it.
    """
    who = species_words(list(lim.species) or list(rule_species)) if named else ""
    head = {"daily": "", "possession": "possession ", "annual": "annual "}[lim.kind.value]
    if lim.take == 0 and lim.under_cm is not None:
        line = f"none under {lim.under_cm} cm"
        if lim.water is not None:
            line += f" from {lim.water.value}s"
        return f"{who}: {line}" if who else line
    body = f"{head}{_count(lim)}".strip()
    q = _qualifiers(lim)
    line = f"{body} ({', '.join(q)})" if q else body
    return f"{who}: {line}" if who else line


def _child_words(lim: Limit, rule_species: Iterable[str]) -> str:
    """A sub-limit, read as a continuation of its parent rather than a rule of its own.

    "of those, no more than 1 Rainbow Trout over 50 cm" — the fish goes INSIDE the phrase when
    the child narrows it, and is left out entirely when it does not, because repeating the
    parent's species makes one instruction look like two.
    """
    narrows = bool(lim.species) and list(lim.species) != list(rule_species)
    who = f" {species_words(list(lim.species))}" if narrows else ""
    if lim.take == 0:
        if lim.under_cm is not None:
            tail = f" from {lim.water.value}s" if lim.water else ""
            return f"none{who} under {lim.under_cm} cm{tail}"
        if lim.water is not None:
            return f"none{who} from {lim.water.value}s"
        q = _qualifiers(lim)
        return f"release all{who}" + (f" ({', '.join(q)})" if q else "")
    bits = [f"no more than {_count(lim)}{who}"]
    if lim.over_cm is not None:
        bits.append(f"over {lim.over_cm} cm")
    if lim.water is not None:
        bits.append(f"from {lim.water.value}s")
    if lim.origin is not None:
        bits.append(f"{lim.origin.value} only")
    return " ".join(bits)


def rule_words(limits: Sequence[Limit], rule_species: Iterable[str] = ()) -> str:
    """A whole harvest rule: its top limits, each followed by what narrows it.

    Sub-limits are printed under the limit they narrow rather than in a flat list, because
    "5, and of those no more than 1 over 50 cm" is one instruction and two rows of a list
    read as two.
    """
    if not limits:
        return ""
    tops = [x for x in limits if not x.within]
    parts = []
    for top in tops or list(limits):
        kids = [k for k in limits if k.within and k.within == top.id]
        text = limit_words(top, rule_species)
        if kids:
            text += " — " + "; ".join(_child_words(k, rule_species) for k in kids)
        parts.append(text)
    return " · ".join(parts)
