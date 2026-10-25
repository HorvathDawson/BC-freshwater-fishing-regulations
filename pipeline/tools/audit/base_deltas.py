"""Z1 — a section's answer is its REGION BASE plus the NAMED ROWS that reach it (RULINGS Z1; memory
`base-plus-deltas-invariant`: "section == region base + named overrides, every difference attributable").

The owner's reason: "that way any this water/trib updates actually are updating it correctly (because the
base reg tables are correct) so we dont chase our tail around in a circle". A defect report then becomes
"which row did this, and is that row's extent right?" instead of "why is this number wrong?".

THE CHECK, per section, day and fish — both sides are the REFERENCE READER's answers
(`read.effective_rules_bound`, traced), never a copy of its steps:

  * BASE = the rules bound here whose source (`read.source_of`) is a region-wide or province-wide table
    (`Scope.region`); OVERRIDES = every other bound rule, each belonging to one named entry (a water row,
    an area row, a row reaching the section by the tributary walk, a book-known steelhead twin).
  * the base answer is the reader over the base alone; the page's answer is the reader over everything
    bound. Every rule whose state differs between the two is a DIFFERENCE, and each must be NAMED BY ONE
    ROW (`attribute`):
      - an override rule is its own row;
      - a base rule the page loses (lifted, displaced, moot) lost to a rule `by` an override — that row;
        or to another base rule that itself lost to an override (followed down the chain);
      - a base rule the page keeps but the base answer lost (it was beaten by a base rule that an
        override has since beaten — SP-4: a displaced rule displaces nothing) — the override that beat
        its beater;
      - a base rule partly lifted on the page — the override that lifts it (`lifted_in_part_by`).
    A difference no override explains is UNATTRIBUTABLE, and that is the failure: the base moved without
    a named row moving it.
  * BASE HOME: every base rule bound here belongs to a region the section lies in (`region_place`) or to
    the province, and each region it lies in contributes a table (`base_problems`).
  * NO CARVE-OUT, over EVERY section (`base_sets`, `peer_problems`): sections sharing a region place, a
    management-unit set, a water kind and tidal-ness carry ONE base. The attribution above cannot see a
    base line that binds neither answer; this sees it rule by rule wherever the section's peers carry it.

`sample` draws the stratified sample the test reads (per region: water-row bound by its own place,
reached by the walk, area-row bound, base only — streams and lakes). T57's vitest
(`app/conformance/src/regs/unnamed.sample.release.test.ts`) pins the unnamed strata against the RELEASE's
page; this pins every stratum against the reader.

    python -m pipeline.tools.audit.base_deltas [--bundle PATH] [--per N]
"""
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from typing import Dict, FrozenSet, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.bundle import read as R

#: The days asked: one in each season, so dated rules and closures take part.
DAYS: Tuple[Tuple[int, int], ...] = ((1, 15), (5, 20), (8, 15), (10, 22))

#: The strata, each a SQL predicate over `section_ruleset s` (and the joined `region_place p`).
STRATA: Dict[str, str] = {
    # a water row (or a zone rule written for one water) bound by its own place
    "water_row": "EXISTS (SELECT 1 FROM ruleset r WHERE r.set_id = s.set_id AND r.via = 'reach' "
                 "AND r.entry_id LIKE 'r%')",
    # a row reaching the section by the tributary walk
    "by_walk": "EXISTS (SELECT 1 FROM ruleset r WHERE r.set_id = s.set_id AND r.via = 'trib')",
    # a lake (named or not) of the region
    "lake": "s.sid IN (SELECT x.sid FROM item_section x JOIN item i ON i.ord = x.ord WHERE i.kind = 'lake')",
    # nothing but the zone and provincial tables (and their area rules)
    "base_only": "NOT EXISTS (SELECT 1 FROM ruleset r WHERE r.set_id = s.set_id "
                 "AND (r.via = 'trib' OR r.entry_id LIKE 'r%'))",
}


def is_base(rule: dict) -> bool:
    """A rule of a region's or the province's standing table — `Scope.region`, whoever wrote it. The
    reader's own rank (`read._rules_of` works out `_rank` once) answers for every rule but a superior
    authority's, whose rank (-1) says nothing of its scope; nothing is written into the reader's rules."""
    rank = rule.get("_rank")
    if rank in (3, 4):
        return True
    if rank is not None and rank != -1:
        return False
    return R.source_of(rule).scope is R.Scope.region


@dataclass(frozen=True)
class Difference:
    """One rule whose state on the page is not its state under the base alone."""
    rule: str                   # rid, `entry::rule`
    base: Optional[str]         # its state in the base answer (None: not in it)
    page: Optional[str]         # its state on the page (None: not in it)
    row: Optional[str]          # the ONE entry that explains it; None = unattributable
    why: str                    # how it was attributed (or why not)


def _states(answer: Iterable[dict]) -> Dict[str, dict]:
    return {R.rid(x): x for x in answer}


def _sig(x: Optional[dict]) -> Optional[Tuple]:
    """What a rule's appearance says: its state, and — for a loser — why and to whom; partly lifted."""
    if x is None:
        return None
    return (x["state"], x.get("reason"), x.get("by"), bool(x.get("partly_lifted")))


def attribute(base_ans: Sequence[dict], page_ans: Sequence[dict], rules: dict) -> List[Difference]:
    """Every difference between the base answer and the page's, each with the one row that explains it
    (or `row=None`). `rules` is the bundle's rule table by rid (to tell base from override)."""
    b, p = _states(base_ans), _states(page_ans)
    over = lambda k: not is_base(rules[tuple(k.split("::", 1))])            # noqa: E731
    entry = lambda k: k.split("::", 1)[0]               # noqa: E731

    def root(k: str, side: Dict[str, dict], seen=()) -> Optional[str]:
        """The override at the bottom of the chain of `by`s that removed `k` on `side`."""
        x = side.get(k)
        by = x.get("by") if x else None
        if by is None or by in seen:
            return None
        if over(by):
            return by
        return root(by, side, seen + (k,))

    out: List[Difference] = []
    for k in sorted(set(b) | set(p)):
        sb, sp = _sig(b.get(k)), _sig(p.get(k))
        if sb == sp:
            continue
        if over(k):
            out.append(Difference(k, None, sp and sp[0], entry(k), "its own row"))
            continue
        x, y = p.get(k), b.get(k)
        found: List[str] = []
        if x is not None and x["state"] in R.LOSER_STATES:
            # the page lost it — to an override, or to a base rule an override beat (down the chain)
            found = [o for o in [root(k, p)] if o]
        if not found and y is not None and y["state"] in R.LOSER_STATES \
                and (x is None or x.get("by") != y.get("by")):
            # the base lost it to a rule an override has since removed from the page (SP-4)
            found = [o for o in [root(y["by"], p)] if o]
        if not found and x is not None and x["state"] in R.LOSER_STATES and x.get("by") \
                and (b.get(x["by"]) or {}).get("state") in R.LOSER_STATES:
            # the page lost it to a base rule that the BASE had lost — revived on the page because an
            # override removed what beat it there (T183: a straddler's two tables, a water row lifting
            # one region's spring closure, so the other region's identical quota is shown instead)
            found = [o for o in [root(b[x["by"]]["by"], p)] if o]
        if x is not None and x.get("partly_lifted"):
            found += [o for o in x.get("lifted_in_part_by") or [] if over(o)]
        ents = sorted({entry(o) for o in found})
        row = ents[0] if len(ents) == 1 else None
        why = f"by {sorted(set(found))}" if row else f"unexplained ({sorted(set(found))})"
        out.append(Difference(k, sb and sb[0], sp and sp[0], row, why))
    return out


def base_problems(db: sqlite3.Connection, sid: int, bound: Sequence[Tuple[str, str, str]],
                  rules: dict) -> List[str]:
    """The base's HOME: every base rule bound here is its section's regions' or the province's, and
    every region the section lies in contributes at least one rule of its table."""
    regions = set((db.execute("SELECT p.regions FROM section_ruleset s JOIN region_place p "
                              "ON p.place = s.place WHERE s.sid = ?", (sid,)).fetchone() or ("",))[0]
                  .split(",")) - {""}
    got = {R.base_region(e) for e, r, _v in bound if is_base(rules[(e, r)])} - {None}
    out = [f"{sid}: base rules of Region {x} bound outside it ({sorted(regions)})" for x in sorted(got - regions)]
    out += [f"{sid}: no base rule of Region {x}" for x in sorted(regions - got)]
    return out


#: (regions, MU codes, water kind, tidal) -> {base rule set: (sections, lowest sid)}
BaseGroups = Dict[Tuple[str, str, str, int], Dict[FrozenSet[Tuple[str, str]], Tuple[int, int]]]


def base_sets(db: sqlite3.Connection, rules: dict) -> BaseGroups:
    """EVERY section's base (its bound rules that are `is_base`), grouped by what decides a standing
    table's reach — the region place, the management-unit set, the water kind, tidal water — counted
    per distinct base. A whole-bundle read: one row per (group, ruleset), not per section."""
    acc: Dict[int, set] = {}
    for set_id, e, r in db.execute("SELECT set_id, entry_id, rule_id FROM ruleset"):
        s = acc.setdefault(set_id, set())
        if is_base(rules[(e, r)]):
            s.add((e, r))
    base = {k: frozenset(v) for k, v in acc.items()}
    out: BaseGroups = {}
    for regions, codes, kind, tidal, set_id, n, sid in db.execute(
            "SELECT p.regions, coalesce(m.codes, '-'), k.kind, s.sid IN (SELECT sid FROM tidal), s.set_id, "
            "count(*), min(s.sid) FROM section_ruleset s JOIN region_place p ON p.place = s.place "
            "JOIN rule_key k ON k.key_ix = s.key_ix LEFT JOIN mu_set m ON m.mus = s.mus GROUP BY 1, 2, 3, 4, 5"):
        g = out.setdefault((regions, codes, kind or "-", int(tidal)), {})
        b = base.get(set_id, frozenset())
        m, low = g.get(b, (0, sid))
        g[b] = (m + n, min(low, sid))
    return out


def peer_problems(groups: BaseGroups) -> List[str]:
    """Z1 rule by rule: within a group every section carries ONE base. A section lacking a line its
    peers carry was carved out of its table; one carrying a line they do not had it added by hand. Each
    minority base is named against the group's most common one (ties: the larger base)."""
    out = []
    for (regions, codes, kind, tidal), g in sorted(groups.items()):
        if len(g) < 2:
            continue
        top = max(g, key=lambda b: (g[b][0], len(b), sorted(b)))
        peers = g[top][0]
        where = f"Region {regions} MUs {codes} {kind}{' tidal' if tidal else ''}"
        for b, (n, sid) in sorted(g.items(), key=lambda kv: kv[1][1]):
            if b == top:
                continue
            many = f"{n} section{'s' if n != 1 else ''} (sid {sid})"
            if top - b:
                out.append(f"{where}: {many} lacks {sorted(f'{e}::{r}' for e, r in top - b)} "
                           f"that its {peers} peers carry")
            if b - top:
                out.append(f"{where}: {many} carries {sorted(f'{e}::{r}' for e, r in b - top)} "
                           f"that its {peers} peers do not")
    return out


def answers(db: sqlite3.Connection, sid: int, day, fish: str, path: str,
            bound: Optional[Sequence[Tuple[str, str, str]]] = None, rules: Optional[dict] = None):
    """(base answer, page answer) for one section, day and fish; `bound` overrides the section's
    stored binding (a mutation test drops one)."""
    rules = rules if rules is not None else R._rules_of(path)
    if bound is None:
        bound = bind(db, sid)
    sh_water = fish == "RB" and R.steelhead_water(db, sid)
    sh_rules = True if fish != "ST" else R.steelhead_rules(db, sid)
    base = [x for x in bound if is_base(rules[(x[0], x[1])])]
    ask = lambda b: R.effective_rules_bound(b, sh_water, day, fish, path,           # noqa: E731
                                            steelhead_rules_here=sh_rules, trace=True)
    return ask(base), ask(bound)


def bind(db: sqlite3.Connection, sid: int) -> List[Tuple[str, str, str]]:
    return [tuple(x) for x in db.execute(
        "SELECT r.entry_id, r.rule_id, r.via FROM section_ruleset s JOIN ruleset r ON r.set_id = s.set_id "
        "WHERE s.sid = ? ORDER BY r.entry_id, r.rule_id", (sid,))]


def regions_of(db: sqlite3.Connection) -> List[str]:
    return sorted({x for (rs,) in db.execute("SELECT regions FROM region_place") for x in rs.split(",")},
                  key=lambda r: (int(r[0]), r))


#: The sample's order: a fixed scramble of the sid (Knuth's multiplicative hash), so the sections drawn
#: spread over the region instead of clustering at its lowest handles — and are the same on every run.
SPREAD = "(s.sid * 2654435761) % 4294967296"


def sample(db: sqlite3.Connection, per: int = 2, regions: Optional[Sequence[str]] = None,
           strata: Optional[Sequence[str]] = None) -> Dict[Tuple[str, str], List[int]]:
    """(region, stratum) -> `per` sections lying in that ONE region, in that stratum (`STRATA`), and
    (place, "region_line") -> `per` sections of every place on a region line ("3,8"). `regions` /
    `strata` narrow it (a test asking for one region's lakes); the region lines come only with neither."""
    out = {}
    for region in regions or regions_of(db):
        for name, pred in STRATA.items():
            if strata and name not in strata:
                continue
            out[(region, name)] = [sid for (sid,) in db.execute(
                f"SELECT s.sid FROM section_ruleset s JOIN region_place p ON p.place = s.place "
                f"WHERE p.regions = ? AND {pred} ORDER BY {SPREAD}, s.sid LIMIT {int(per)}", (region,))]
    for (place,) in ([] if regions or strata else
                     db.execute("SELECT regions FROM region_place WHERE regions LIKE '%,%' ORDER BY regions")):
        out[(place, "region_line")] = [sid for (sid,) in db.execute(
            f"SELECT s.sid FROM section_ruleset s JOIN region_place p ON p.place = s.place "
            f"WHERE p.regions = ? ORDER BY {SPREAD}, s.sid LIMIT {int(per)}", (place,))]
    return out


def leaf_fish() -> List[str]:
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES
    return sorted(BOOK_SPECIES)


def sweep(path: str, per: int = 2, days=DAYS, fish: Optional[Sequence[str]] = None) -> dict:
    """The whole check over the sample: counts, and every unattributable difference / base problem."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    rules = R._rules_of(path)
    fish = list(fish or leaf_fish())
    strata = sample(db, per)
    n = {"sections": 0, "questions": 0, "differences": 0, "attributed": 0}
    bad: List[str] = []
    rows: Dict[str, int] = {}
    for (region, name), sids in sorted(strata.items()):
        for sid in sids:
            n["sections"] += 1
            bound = bind(db, sid)
            bad += base_problems(db, sid, bound, rules)
            for day in days:
                for f in fish:
                    n["questions"] += 1
                    for d in attribute(*answers(db, sid, day, f, path, bound, rules), rules):
                        n["differences"] += 1
                        if d.row is None:
                            bad.append(f"{region}/{name} {sid} {day} {f}: {d.rule} {d.base}->{d.page} ({d.why})")
                        else:
                            n["attributed"] += 1
                            rows[d.row] = rows.get(d.row, 0) + 1
    peers = peer_problems(base_sets(db, rules))
    db.close()
    return {"counts": n, "strata": {f"{r}/{s}": len(v) for (r, s), v in sorted(strata.items())},
            "rows": dict(sorted(rows.items())), "unattributable": bad, "carved_out": peers}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--bundle", default=R.BUNDLE)
    ap.add_argument("--per", type=int, default=2)
    a = ap.parse_args(argv)
    got = sweep(a.bundle, a.per)
    print(got["counts"], f"{len(got['rows'])} rows")
    for line in (got["unattributable"] + got["carved_out"])[:50]:
        print(" ", line)
    return 1 if got["unattributable"] or got["carved_out"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
