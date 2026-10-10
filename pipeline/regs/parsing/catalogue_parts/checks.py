"""The cross-field checks the model validators call: printed dates, the dates a row lost,
extents the resolver reads, list markers and undrawn parts.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import TYPE_CHECKING, List, Optional
from pydantic import BaseModel

from .vocab import RuleType
from .text import squash
from .dates import DateRange, When, _days, parse_date_range

if TYPE_CHECKING:  # annotations only — importing these at run time would be circular
    from .rule import CatalogueRule
    from .entry import CatalogueEntry


#: "Nov 1-Apr 30", "Jul 24 - Dec 31", "May 1-31" — the ranges a sentence prints.
_RANGE = re.compile(r"([a-z]+\.?\s*\d{1,2})\s*-\s*([a-z]+\.?\s*\d{1,2}|\d{1,2})\b")


def printed_ranges(text: str) -> List["DateRange"]:
    """Every date range the sentence prints, parsed. Unparseable matches are skipped."""
    out = []
    for m in _RANGE.finditer(squash(text)):
        got = parse_date_range(f"{m.group(1)}-{m.group(2)}")
        if got is not None:
            out.append(got)
    return out


def _dates_are_printed(when: Optional[When], verbatim: str, where: str) -> Optional[str]:
    """Every range in `when.dates` must be printed in the sentence it came from."""
    if when is None or not when.dates:
        return None
    printed = printed_ranges(verbatim)
    missing = [d.words() for d in when.dates if d not in printed]
    if missing:
        return f"{where}: dates {missing} are not printed in {verbatim[:60]!r}"
    return None


#: "all year", "year-round", "year round" — a sentence that prints a window for one place and ALL
#: YEAR for another ("Bull trout from the Liard River watershed Aug 15-Oct 15, and from the Peace
#: River watershed all year") may carry no `when`: an absent `when` IS all year.
_ALL_YEAR_SAID = re.compile(r"\ball year\b|\byear[- ]round\b")


#: A MONTH WITH NO DAY AT THE END OF A QUOTE: a date cut off. Coquihalla River's last line wraps
#: "…, Nov" / "1-Mar 31" onto the next printed line, and extraction kept only "Nov" — the row, a
#: rule's verbatim and both rules' seasons were lost. "May" is also a verb, so it is left out.
_CUT_DATE = re.compile(r"\b(jan|feb|mar|apr|jun|june|jul|july|aug|sep|sept|oct|nov|dec)\.?\s*$")


def _year_days() -> set:
    return _days([DateRange(from_month=1, from_day=1, to_month=12, to_day=31)])


def _own_dates_carried(rule: "CatalogueRule") -> Optional[str]:
    """A RULE WHOSE OWN SENTENCE PRINTS DATES CARRIES THEM. An absent `when` is ALL YEAR, so a rule
    quoting "…, May 1-Oct 31" with no `when` binds 365 days where the book printed 184 — the
    shape of all six `within` clauses the model now refuses, and of any quota, bait ban or closure
    split off a dated sentence without its season.

    The `when` must be one of three things, each a reading of the printed words:
      * a non-empty SUBSET of the printed ranges — a sentence naming two places with two windows
        is two rules, each carrying its own ("downstream of Hell's Gate Sept 1-Nov 15, …");
      * exactly their COMPLEMENT on the year — "Open June 16-Apr 30" is a closure on May 1-June 15,
        "catch and release, EXCEPT Apr 1-Apr 3 and July 1-July 2" a release on every other day
        (`When` has no `excepts` flag, so the complement is what is written);
      * ABSENT, when the sentence also prints "all year" — the other half is another rule.
    A LIFT-ONLY rule names the window of the rule it lifts ("Exempt from July 15-Aug 31 summer
    closure"); that is the lifted rule's season, not its own, and it is exempt."""
    if _CUT_DATE.search(squash(rule.verbatim)):
        return (f"its sentence ends in a month with no day ({squash(rule.verbatim)[-12:]!r}) — a "
                f"date cut off; restore it from the page")
    if rule.lift_only:
        return None
    printed = printed_ranges(rule.verbatim)
    if not printed:
        return None
    mine = list(rule.when.dates) if rule.when else []
    said = ", ".join(d.words() for d in printed)
    if not mine:
        if _ALL_YEAR_SAID.search(squash(rule.verbatim)):
            return None
        return (f"its sentence prints {said} but it has no `when` — an absent `when` is ALL YEAR; "
                f"give it the printed dates")
    if all(d in printed for d in mine):
        return None
    if _days(mine) == _year_days() - _days(printed):
        return None
    return (f"its `when` ({', '.join(d.words() for d in mine)}) is neither the dates its sentence "
            f"prints ({said}), some of them, nor their complement")


#: "catch and release ALL OTHER SPECIES", "… all species", "… all fish" — a water row's release
#: of everything it has not named. NOT "Whitefish: 15 (all species combined)", which is a quota
#: over the whitefish, and not "No Fishing … applies to all species" (a note about closures).
_ALL_SPECIES_SAID = re.compile(r"\ball (?:other )?(?:species|fish)\b(?! combined)")


def _all_species_is_game_fish(rule: "CatalogueRule") -> Optional[str]:
    """"ALL OTHER SPECIES" MEANS GAME FISH, AND NEVER CRAYFISH (user ruling, 2026-09-25).

    Whiteswan Lake's inlet and outlet print "rainbow trout daily quota = 5 (catch and release all
    other species)": the release is about the game fish an angler catches there. Written as
    `ALL_GAME_FISH` it reached crayfish (the closed list names them) and released a crayfish
    trapper's catch; written as `ALL_FIN_FISH` (Pine River's "all fish") it reached every sucker
    and carp. So a retention rule saying "all (other) species / all fish" is `ALL_GAME_FISH` with
    crayfish in `species_except` — game fish, less crayfish, less whatever the row names itself."""
    if rule.type is not RuleType.retention_limit:
        return None
    if _ALL_SPECIES_SAID.search(squash(rule.verbatim)):
        if list(rule.species) != ["ALL_GAME_FISH"] or "CRA" not in rule.species_except:
            return (f"its sentence says 'all (other) species / all fish', which is GAME FISH and "
                    f"never crayfish — species ['ALL_GAME_FISH'] with 'CRA' in species_except (it "
                    f"has species {list(rule.species)}, except {list(rule.species_except)})")
        return None
    # A BARE "CATCH AND RELEASE" IS THE SAME RELEASE (user ruling 2026-09-25): Burnt River's and
    # Clearwater Creek's "Catch and release" name no fish, so they release every GAME fish — and
    # crayfish may still be kept. Held structurally, not by the words: a release (take 0, may fish)
    # over ALL_GAME_FISH, under no means of its own, excepts crayfish. (A release WHILE set lining
    # is the book's "any game fish … other than burbot" about what the set line takes, and a
    # closure — `may_target: false` — is not a release.)
    if list(rule.species) == ["ALL_GAME_FISH"] and rule.take == 0 and rule.may_target \
            and not rule.while_ and not rule.caught and "CRA" not in rule.species_except:
        return ("a catch and release over every game fish releases GAME FISH and never crayfish — "
                "add 'CRA' to species_except (crayfish may be kept; the zone's crayfish quota "
                "stands)")
    return None


#: PRINTED WINDOWS NO RULE OF THEIR ROW CARRIES, BECAUSE THE BOOK'S MEANING IS HELD ELSEWHERE —
#: `(entry_id, range words)` -> where. Every other printed range must be carried by some `when`
#: in its entry (`_printed_windows_carried`); an entry listed here that no longer needs it is
#: refused too, so the list cannot go stale.
WINDOWS_HELD_ELSEWHERE: dict = {
    ("z7b:trout_char_quota", "Oct 16-Aug 14"):
        "the NOTE's retention window for bull trout. Retention is allowed only in the Liard River "
        "watershed, whose row prints its own 'Oct 16-Aug 14' quota; everywhere else in Zone B "
        "r10 releases bull trout all year (user ruling 2026-09-24)",
    ("z7b:trout_char_quota", "Aug 15-Oct 15"):
        "the Liard half of r9's sentence. Outside the Liard row's Oct 16-Aug 14 its quota is not "
        "in force and r10's all-year release speaks there — which is Aug 15-Oct 15",
}


def _whens_in(obj, out: list) -> None:
    """Every `When` a model holds, at any depth — a rule's, a licensing record's, a stamp
    period's. Walks the model's own fields, so a new place a season can live is found."""
    if isinstance(obj, When):
        out.append(obj)
    elif isinstance(obj, BaseModel):
        for k in type(obj).model_fields:
            _whens_in(getattr(obj, k), out)
    elif isinstance(obj, (list, tuple)):
        for x in obj:
            _whens_in(x, out)


def _unique_span(hay: str, needle: str) -> Optional[tuple]:
    """Where `needle` sits in `hay`, when it sits there ONCE — two rules quoting the same words
    at two places ("no trout under 30 cm", twice on the Kootenay) cannot be told apart by text."""
    i = hay.find(needle)
    if i < 0 or not needle or hay.find(needle, i + 1) >= 0:
        return None
    return i, i + len(needle)


_JOIN_NEXT = re.compile(r"[\s,)\]]*(?:\band\b\s*|\(\s*)?")
_BARE_GAP = re.compile(r"[\s,:)\]]*(?:from\s+)?")


def _dates_lost(entry: "CatalogueEntry") -> List[str]:
    """THE SEASONS OF A ROW, AGAINST ITS RULES. Four ways a printed date has been lost, each a shape
    the corpus once held; the first is on the rule (`_own_dates_carried`), these three need the
    row and the rule's siblings.

      1. A PRINTED WINDOW NO `when` CARRIES. Every range in `regs_verbatim` is some rule's or
         licensing record's `when` (or its complement, or the window a lift names), unless
         `WINDOWS_HELD_ELSEWHERE` says where it is held.
      2. A QUOTE INSIDE A DATED SIBLING'S. "Trout daily quota = 2 (none under 30 cm), May 1-Oct 31"
         split into the quota (dated) and "none under 30 cm" (a rule of its own, undated): the
         clause's words overlap the dated rule's, so it is the same sentence and the same season.
      3. THE DATE STRAIGHT AFTER THE QUOTE. "…quota = 2 (none under 30 cm), May 1-Oct 31" with
         the verbatim cut before the date and no `when`: nothing but punctuation separates the
         rule's words from the window, so the window is the rule's.
      4. "A AND B, <dates>". "Trout/char catch and release and bait ban, June 15-Aug 31" governs
         both; the book repeats a date per clause when it means them apart (Findlay Creek:
         "…, June 15-Oct 31; bait ban, June 15-Oct 31"). A `;` or a list comma is not this shape.
    A lift-only rule is exempt from 2-4 (its dates are the lifted rule's), and so is a rule whose
    own sentence prints "all year" (`_own_dates_carried`)."""
    e: List[str] = []
    # THE ROW, SQUASHED LINE BY LINE, so a line break is still visible: `breaks` are the offsets
    # in `hay` where one printed line ends and the next begins.
    hay, breaks = "", []
    for line in entry.regs_verbatim.split("\n"):
        got = squash(line)
        if got:
            if hay:
                breaks.append(len(hay))
                hay += " "
            hay += got
    if _CUT_DATE.search(hay):
        e.append(f"the row ends in a month with no day ({hay[-12:]!r}) — a date cut off; restore "
                 f"it from the page")
    whens: list = []
    _whens_in(list(entry.rules) + list(entry.licensing), whens)
    carried = [d for w in whens for d in w.dates]
    year = _year_days()
    lifts = [printed_ranges(r.verbatim) for r in entry.rules if r.lift_only]
    for d in printed_ranges(entry.regs_verbatim):
        key = (entry.entry_id, d.words())
        if d in carried or any(d in x for x in lifts) or key in WINDOWS_HELD_ELSEWHERE:
            continue
        if any(w.dates and _days(list(w.dates)) == year - _days([d]) for w in whens):
            continue
        e.append(f"the row prints {d.words()} and no rule or licensing record carries it — a "
                 f"season that reaches no `when` is lost, and the rule it governed reads all year")
    for k, why in WINDOWS_HELD_ELSEWHERE.items():
        if k[0] == entry.entry_id and any(d.words() == k[1] for d in carried):
            e.append(f"WINDOWS_HELD_ELSEWHERE lists {k[1]} for this row, but a rule now carries "
                     f"it — take it off the list")

    def dated(r) -> bool:
        # PRINTED DAYS, not any `when`: an hours- or weekday-only `when` is not a season a
        # sibling can have lost ("Youth/Disabled Accompanied Water" is one quote for two rules).
        return r.when is not None and bool(r.when.dates)

    spans = {r.rule_id: _unique_span(hay, squash(r.verbatim)) for r in entry.rules}
    # 5. A DATE DOES NOT CROSS A SEMICOLON (user ruling, 2026-09-25). "Fly fishing only; bait ban
    #    upstream of …, Jul 1-Oct 31" dates the bait ban; "catch and release; bait ban, June 15-Oct
    #    31" dates the bait ban. The clause on the other side of the `;` is its own, so a rule
    #    whose clause (from the `;` before it to the `;` after it, on its printed line) prints no
    #    date may not carry dates that line prints only in ANOTHER clause.
    for r in entry.rules:
        mine = spans[r.rule_id]
        if not dated(r) or r.lift_only or mine is None or printed_ranges(squash(r.verbatim)):
            continue
        start = max([b for b in breaks if b <= mine[0]] + [0])
        end = min([b for b in breaks if b >= mine[1]] + [len(hay)])
        line = hay[start:end]
        if ";" not in line:
            continue
        a, b = mine[0] - start, mine[1] - start
        lo = line.rfind(";", 0, a) + 1
        hi = line.find(";", b)
        hi = len(line) if hi < 0 else hi
        if printed_ranges(line[lo:hi]):
            continue
        others = printed_ranges(line[:lo] + " " + line[hi:])
        if others and all(d in others for d in r.when.dates):
            e.append(f"{r.rule_id}: its `when` ({r.when.words()}) is printed in another clause of "
                     f"its line, across a `;` — a date after a `;` belongs to its own clause, not "
                     f"to '{squash(r.verbatim)[:30]}'")
    for r in entry.rules:
        if dated(r) or r.lift_only or _ALL_YEAR_SAID.search(squash(r.verbatim)):
            continue
        mine = spans[r.rule_id]
        if mine is None:
            continue
        for s in entry.rules:
            other = spans[s.rule_id]
            if s is r or other is None or not dated(s):
                continue
            if mine[0] < other[1] and other[0] < mine[1]:
                e.append(f"{r.rule_id}: its words are part of {s.rule_id}'s sentence, which holds "
                         f"on {s.when.words()} — it has no `when`, so it reads all year")
        after = hay[mine[1]:]
        gap = _BARE_GAP.match(after)
        if gap and gap.end() < len(after) and printed_ranges(after[gap.end():gap.end() + 25]) \
                and _RANGE.match(after[gap.end():]):
            e.append(f"{r.rule_id}: the row prints a date straight after its words "
                     f"({after[gap.end():gap.end() + 20]!r}) and it has no `when`")
            continue
        # A CHAIN OF CLAUSES ENDING IN ONE DATE: walk the siblings that follow on the same
        # printed line, joined by "and", ",", "(" / ")" or nothing, to the first text that is not
        # a sibling. The chain's date is the one standing AFTER it — or, when the last clause was
        # joined by "and", the one closing that clause's own words ("trout/char catch and release
        # and bait ban, June 15-Aug 31"). If a sibling in the chain carries that date, the date
        # governs the whole chain and this rule has lost it. A date that closes a clause joined by
        # a COMMA is that clause's own: "ALL STEELHEAD, Bull trout from streams, Aug 1-Oct 31" is
        # a list of releases, and the date is the bull trout's. A `;` ends the chain, as does a
        # line break (the next printed line is the next regulation).
        at, chain, via_and = mine[1], [], False
        while True:
            joined = _JOIN_NEXT.match(hay, at)
            if any(at <= b < joined.end() for b in breaks):
                break
            nxt = next((s for s in entry.rules if s is not r and s not in chain
                        and spans[s.rule_id] is not None
                        and spans[s.rule_id][0] == joined.end()), None)
            if nxt is None:
                break
            chain.append(nxt)
            via_and = " and" in f" {joined.group(0)}"
            at = spans[nxt.rule_id][1]
        if not chain:
            continue
        g = _BARE_GAP.match(hay, at)
        m = _RANGE.match(hay, g.end())
        final = printed_ranges(m.group(0)) if m else []
        if not final and via_and:
            final = printed_ranges(hay[spans[chain[-1].rule_id][0]:at][-25:])[-1:]
        if final and any(dated(s) and final[0] in s.when.dates for s in chain):
            e.append(f"{r.rule_id}: '{squash(r.verbatim)[:30]}… {squash(chain[-1].verbatim)[:30]}"
                     f"…, {final[0].words()}' — the date governs every clause before it, and "
                     f"{r.rule_id} has no `when`")
    return e


def _extents_the_resolver_reads(extents: Optional[List[dict]]) -> List[str]:
    """A rule's or a licensing record's extent may not carry a flag the reach builder never reads.

    `includes_tributaries` INSIDE an extent is one: `classify.wants_tributaries` reads the flag on
    the rule or record (inheriting the entry's), never on an extent, so "the Fraser River Watershed
    (including tributaries)" written that way bound the mainstem alone and said nothing. Nineteen
    rules were in that state — every watershed quota and closure in Zones 5, 6, 7A and 7B. The
    flag goes on the rule or record, where the builder walks it."""
    return [f"extent {i} carries includes_tributaries, which the reach builder does not read on "
            f"an extent — set it on the rule or record" for i, x in enumerate(extents or [])
            if isinstance(x, dict) and "includes_tributaries" in x]


#: A LIST MARKER at the head of a phrase — "3.", "4)", "(b)", "(iv)", "• ", "– ". The book prints
#: them on its numbered lists, but they are layout, never the regulation: no VERBATIM may start
#: with one (user ruling 2026-09-25 — the two standing no-fishing buffers read "3. Within 23 m …"
#: on every water; `CatalogueEntry._chain_of_custody`), nor a generated label, nor a place named in
#: `extent_text` — "No fishing — (b) Chimdemash Creek" is a list item, not a place.
LIST_MARKER = re.compile(r"^\s*(\d{1,2}[.)]\s|\([a-z0-9ivx]{1,3}\)\s*|[•–-]\s)")


#: A RULE'S VALUE IN ITS PLACE PHRASE — a speed, an engine power, a size, a quota or a date inside
#: `extent_text` / `undrawn_part`. Strawberry Slough's part read "on parts (8 km/h)": the speed
#: leaked out of the rule into its place, and the not-yet-mapped sentence then said the rule holds
#: "on parts (8 km/h)". A place is where; the value is the rule's own field (`CatalogueRule`).
PLACE_VALUE = re.compile(
    r"\d+(?:\.\d+)?\s*(?:km/h|kw|hp|cm)\b|\bquotas?\b|\bper day\b"
    r"|\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?"
    r"|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+\d{1,2}\b", re.I)

#: A PART THE BOOK DOES NOT IDENTIFY — "Speed restriction on parts (8 km/h)", "no towing on parts",
#: "various locations (as buoyed and signed)". These are the book's words for the part
#: (`undrawn_part`), but they name no place: the page never says which parts. Quoted as a place
#: they read "applies only in one part of this water — on parts —"; `part_words` says instead what
#: is true (the regulations do not identify them). Exactly these shapes and nothing looser, so a
#: part that does name a place is never read as unidentified.
UNIDENTIFIED_PART = re.compile(
    r"^(?:on (?:a )?(?P<n>parts?)|various locations)(?P<signed>\s*\(as buoyed and signed\))?$",
    re.I)


def part_identifies_place(part: str) -> bool:
    """Does this undrawn part name a place (see `UNIDENTIFIED_PART`)?"""
    return not UNIDENTIFIED_PART.match(strip_list_marker(part))


def part_words(part: str) -> str:
    """The undrawn part, as a reader should see it: the book's words when they name a place, else
    what is true of it — "parts the regulations do not identify" (never "on parts")."""
    text = strip_list_marker(part)
    m = UNIDENTIFIED_PART.match(text)
    if not m:
        return text
    if m.group("signed"):
        return "parts marked by buoys and signs, which the regulations do not identify"
    if (m.group("n") or "").lower() == "part":
        return "a part the regulations do not identify"
    return "parts the regulations do not identify"


def strip_list_marker(text: str) -> str:
    """`text` without a leading list marker (see `LIST_MARKER`)."""
    return LIST_MARKER.sub("", text or "", count=1).strip()


def bare_whole(extents: Optional[List[dict]]) -> bool:
    """Extents that say "the whole water" and nothing else — every one `{"op": "whole"}`, with no
    item, area, feature or tributary qualifier. A `whole` that names an item (`item_id`), an area
    (`within_area`) or a kind (`feature_types`) is a place of its own, and its `extent_text` only
    describes it."""
    return bool(extents) and all(isinstance(x, dict) and x.get("op") == "whole"
                                 and set(x) == {"op"} for x in extents)
