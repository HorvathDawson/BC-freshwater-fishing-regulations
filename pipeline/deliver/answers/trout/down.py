"""(T892) A TROUT ROW THAT COMES FROM ANOTHER WATER'S TRIBUTARY RULE SAYS SO: `TRow.down`, one line under the row's name
("Dutch Creek’s rule for its tributaries"; two waters "A’s and B’s rules for their tributaries").

Owner 2026-11-19 ~00:10, verbatim: "The table should say if it came from tributaries too.." The Other fish rows and the
trout fold already say it (app `flow.ts` `downLine`, `sheets/trout.ts` `troutFrom`); the trout table's own rows are
Python's words (T810: the app adds none), so the line ships on the row.

THE SAME INPUT AS THE ROW'S FISH SHEET, NOTHING DECIDED AGAIN (AGENTS 56). The app's sheet (`tf:<row>`) draws its "How
this was decided" rail from the row's own rule list (`TRow.src`) and, for each rule, its role in the rows frame's stored
answer for that fish (`rows_decided.roles`); the line is that rail's own verdict (`flow.ts` `downNames`), read here from
the same three things: `src`, the stored answers (`win`, `narrow`, `roles`) and the rules as the key's rule set holds them
(a rule the set lists under `trib` ranks 1, "<that water> · its tributaries"). So, per row:
  rail       every rule of `src` has a stored role in the fish's hatchery or wild answer (else the sheet keeps its plain
             Sources list and says nothing, `troutItems`); and every number of the sheet's head is said by a step of the
             rail (`troutFlow`'s re-review b);
  shown      the rail's deciding steps: `src`'s rules in the rail's order (level, then role), less possession / duty /
             yearly ("Other rules"), the rules that changed nothing, a rule replaced by one of the other rules, a lost
             step saying a deciding one's words and a second step with the same words (`flowOf`, `unique`);
  names      for every answer the row speaks for (its origin's, else hatchery and wild), the rule that wins it and, where
             its number was lowered (`narrow`), each rule stored as narrowing it is a shown tributary rule, and no shown
             rule stored as lowering the number ("narrows", or "limit" on a subcap) is any other kind; the waters are
             those rules' entries, in that order. A size cap beside it ("limit" on a sizecap: Nygaard Creek's "1 over
             50 cm" from Region 5) leaves the line (owner 2026-11-19 ~01:25: "this is good").
The app checks it the other way round over a whole release (`app/tools/release-tributary-lines.test.ts`: a row with the
line ⇒ its sheet's rail is set, for each origin the row covers, by exactly those waters' tributary steps; a row without
it ⇒ proved partial), and draws the words unchanged.

`TRow.down` is OPTIONAL on the wire (absent: none) so a reader of the trout 5 shape reads a table carrying it, and this
reader reads a table without it (the release before T892).
"""
from __future__ import annotations

import re
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.trout import words as W

#: (mutation pin, `test_trout_down.py`) False = a row whose rail is not drawn (a src rule with no stored role, or a head
#: number no step says) still gets the line, which its sheet then cannot show
A_ONLY_WHERE_THE_RAIL_IS_DRAWN = True
#: (mutation pin) False = another rule stored as lowering the number no longer removes the line (review 3)
A_LOWERED_ELSEWHERE_REMOVES_IT = True


class DownRule:
    """One rule as the row's rail sees it on this key: `key` "entry::rule"; `rank` the app's (`rules.ts` `rankOf`: a rule
    the key's set lists under `trib` ranks 1 unless federal, else its provenance rank); `part` its undrawn part (or None);
    `name` its entry's name (`provenance.entry_name`); `kind` its display kind (`RuleFacts.kind`) and `says` its one line
    on the rail (`RuleFacts.title`), both read only when asked (`fact`: (kind, title), or a thunk giving them): most rows
    end at the first setter that is no tributary rule, and never need a title."""
    __slots__ = ("key", "rank", "part", "name", "_fact")

    def __init__(self, key: str, rank: int, part: Optional[str], name: Optional[str], fact):
        self.key, self.rank, self.part, self.name, self._fact = key, rank, part, name, fact

    def _facts(self) -> Tuple[Optional[str], str]:
        if callable(self._fact):
            self._fact = self._fact()
        return self._fact

    @property
    def kind(self) -> Optional[str]:
        return self._facts()[0]

    @property
    def says(self) -> str:
        return self._facts()[1]


def rank_on(base_rank: int, trib: bool) -> int:
    """The app's `rankOf` (makePlace.R): a rule the tributary walk brought ranks 1 unless its base rank is federal."""
    return 1 if trib and base_rank >= 0 else base_rank


#: the rail's roles (app `flow.ts`): read as "Other rules", and the ones that changed nothing
OTHER = ("season", "possession_cap", "duty", "possession")
QUIET = ("agrees", "moot", "falls")
#: the reading order inside a level (app `decided.ts` QORDER, `story.ts` LOST / `orderOf`)
QORDER = ("governs", "contains", "narrows", "also", "floor", "limit", "season", "possession_cap", "duty", "possession")
LOST = ("lifted", "replaced", "moot", "falls", "agrees")


def _order(role: str) -> int:
    if role in QORDER:
        return QORDER.index(role)
    return len(QORDER) + (LOST.index(role) if role in LOST else len(LOST))


def _layer(r: DownRule) -> int:
    """The app's `layerOf` with no water to compare: a water's own row and a listed water's share a level here (the
    level only orders the steps; the water's name never enters the line)."""
    if r.part:
        return 9
    return {-1: 8, 4: 0, 3: 1, 2: 2, 1: 3}.get(r.rank, 4)


def _squash(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def reader(rules: Callable[[int], object], trib: frozenset, fact: Callable[[str], Tuple[str, str]]
           ) -> Callable[[int], DownRule]:
    """A key's `DownRule` per rule index: `rules` the trout table's (`table.Rules`: index -> the rows engine's `R`),
    `trib` the rule keys the key's rule set lists under `trib`, `fact` a rule key -> (display kind, title)."""
    cache: Dict[int, DownRule] = {}

    def rule(ix: int) -> DownRule:
        got = cache.get(ix)
        if got is None:
            R = rules(ix)
            got = cache[ix] = DownRule(R.key, rank_on(R.x.get("_rank", 0), R.key in trib), R.f.get("undrawn_part") or None,
                                       R.x.get("entry_name"), lambda k=R.key: fact(k))
        return got
    return rule


def line(names: Sequence[str]) -> Optional[str]:
    """The row's words (`words.row_down`, the Other fish rows' line); None for no water."""
    return W.row_down(names) if names else None


def _roles(d: Optional[dict]) -> Dict[int, Tuple[str, Optional[int]]]:
    return {ix: (role, by) for ix, role, by in (d or {}).get("roles") or ()}


def names_of(row: Mapping, fa: Optional[Mapping], rule: Callable[[int], DownRule]) -> List[str]:
    """The waters whose rows, carried down to their tributaries, set this table row (the module doc); [] for none.
    `row` the table row (`key`, `origin`, `src`, `sheet.head`), `fa` the rows frame's answer for its fish ({hatchery,
    wild}: `win`, `narrow`, `roles`), `rule` a rule index as `DownRule` on this key."""
    src = list(row["src"])
    if fa is None or not src:
        return []
    o = row.get("origin")
    cells = [d for d in ([fa.get("hatchery")] if o == "hatchery" else [fa.get("wild")] if o == "wild"
                         else [fa.get("hatchery"), fa.get("wild")]) if d is not None]
    setters = [[d["win"]] + ([ix for ix, role, _ in d.get("roles") or () if role == "narrows"]
                             if d.get("narrow") is not None else []) for d in cells]
    if not cells or any(rule(ix).part or rule(ix).rank != 1 for ixs in setters for ix in ixs):
        return []                                          # some answer set by a rule that is no tributary one
    h, w = _roles(fa.get("hatchery")), _roles(fa.get("wild"))
    # the rail's items (`troutItems`): a rule both answers hold with one role is one item, else one per origin
    items: List[Tuple[int, str, Optional[int]]] = []
    for ix in src:
        ph, pw = h.get(ix), w.get(ix)
        if ph is None and pw is None:
            if A_ONLY_WHERE_THE_RAIL_IS_DRAWN:
                return []                                  # no rail: the sheet keeps its Sources list
            continue
        if ph is not None and pw is not None and ph == pw:
            items.append((ix, *ph))
        else:
            items += [(ix, *p) for p in (ph, pw) if p is not None]
    other_keys = {rule(ix).key for ix, role, _ in items if role in OTHER}
    main = [(ix, role, by) for ix, role, by in items
            if role not in OTHER and role not in QUIET
            and not (role in ("replaced", "lifted") and by is not None and rule(by).key in other_keys)]
    main.sort(key=lambda it: (_layer(rule(it[0])), _order(it[1])))          # stable, as the app's sort
    # `unique`: a lost step saying a deciding step's words, and a second step with the same words, are dropped
    won_words = {_squash(rule(ix).says) for ix, role, _ in main if role not in ("replaced", "lifted")}
    seen, kept = set(), []
    for ix, role, by in main:
        won, says = role not in ("replaced", "lifted"), _squash(rule(ix).says)
        if not won and says in won_words:
            continue
        if (won, says) in seen:
            continue
        seen.add((won, says))
        kept.append((ix, won))
    if A_ONLY_WHERE_THE_RAIL_IS_DRAWN:
        said = " ".join(_squash(rule(ix).says) for ix, _ in kept)
        if not all(re.search(rf"(^|\D){n}(\D|$)", said) for n in re.findall(r"\d+", row["sheet"]["head"])):
            return []                                      # a head number no step says: the sheet keeps its list
    shown = {rule(ix).key for ix, won in kept if won}
    names: Dict[str, None] = {}
    for d, sets in zip(cells, setters):
        for ix in sets:
            r = rule(ix)
            if r.part or r.rank != 1 or r.key not in shown:
                return []
            if not r.name:
                raise AnswersError(f"trout table: row {row['key']}: tributary rule {r.key} has no entry name to say")
            names[r.name] = None
        if A_LOWERED_ELSEWHERE_REMOVES_IT:
            for ix, role, _ in d.get("roles") or ():
                if role != "narrows" and role != "limit":
                    continue
                r = rule(ix)
                if role == "limit" and r.kind != "subcap":
                    continue
                if r.key in shown and (r.part or r.rank != 1):
                    return []
    return list(names)


def with_down(table: dict, frame: Mapping, rule: Callable[[int], DownRule]) -> dict:
    """The table with `down` on each row a tributary rule sets (absent on the others: the field is optional)."""
    fish = frame.get("fish") or {}
    boxes = []
    for b in table["boxes"]:
        rows = []
        for r in b["rows"]:
            said = line(names_of(r, fish.get(r["fish"]), rule))
            rows.append({**r, "down": said} if said else r)
        boxes.append({**b, "rows": tuple(rows)})
    return {**table, "boxes": tuple(boxes)}
