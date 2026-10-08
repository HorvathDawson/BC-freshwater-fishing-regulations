"""THE BUNDLE'S DERIVED TABLES — facts decided ONCE, at bundle time, from the bundle's own rows
(DATAFLOW P2; AGENTS 56: every derived fact is computed once and READ everywhere else).

  rule_ix          every rule's index in the sorted `entry_id::rule_id` list — the export's `rules`
                   array, every stage's `RuleIx`; `meta.rule_ids_sha256` names that order
  rule.closure_grade   `rules.closure_grade` of every rule, stored: nothing downstream reads
                   `may_target` (`test_dataflow_gates` gate 2)
  rule_key         the reader's three inputs per section — (rule set, a rainbow over 50 cm is a
                   steelhead here, the steelhead rules apply here) — interned in that order, and
                   `section_ruleset.key_ix` on every section (decision U2). Written by
                   `rules.write` (`rule_keys`), where the steelhead facts are born
  part             THE PARTS OF EVERY NAMED WATER (Q16, finding H4): every section of one water
  part_section     carrying the same rule set, licensing set, province exceptions, steelhead
                   water and steelhead presence — in the export's order, so `part_ix` IS the
                   export's `waters[item].parts` index. The export and the answers read this
                   table; nothing else groups sections into parts (gate 3)

Every table is held to SQL CHECK / UNIQUE / FOREIGN KEY constraints (schema.sql; the build runs
`PRAGMA foreign_keys = ON`), and `prove` refuses a build whose rows break a fact the constraints
cannot state (a part's sections disagreeing on whether the steelhead rules apply, tidal water or
the water kind; a part with no rule set that is not wholly outside B.C.; a key that does not
reproduce the steelhead views).
"""
from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver import types as T


class DerivedError(SystemExit):
    """A derived table that cannot be written truthfully: the build stops, naming it."""


def rule_ids_digest(rule_ids: Sequence[str]) -> str:
    """The SHA-256 (first 16 hex digits) of the rule ids, one per line: integer rule refs mean
    these rules and no others (`meta.rule_ids_sha256`, the export's and the answers' stamp)."""
    return hashlib.sha256("\n".join(rule_ids).encode("utf-8")).hexdigest()[:16]


def rule_id(entry_id: str, rule_id_: str) -> str:
    return f"{entry_id}::{rule_id_}"


# --------------------------------------------------------------------------------------------
# rule_key (written by rules.write, where the steelhead facts are born)
# --------------------------------------------------------------------------------------------

def rule_keys(section_set: Dict[int, int], anadromous: Iterable[int], applies: Iterable[int],
              kind_of_section: Dict[int, str]) -> Tuple[List[tuple], Dict[int, int]]:
    """The rule keys and every section's key: `(rows, key_of)` — rows `(key_ix, set_id,
    steelhead_water, steelhead_rules, kind, sections, rep_sid)` in (set, sw, sr) order, `key_of`
    {sid: key_ix}. `anadromous` / `applies` are the reach run's sections where a rainbow over 50 cm
    is a steelhead / where the steelhead rules apply (`rules.write_steelhead_presence`, proved
    equal to the `steelhead_water` / `section_steelhead_rules` views); `kind_of_section` the
    item kind of every NAMED section. Refuses a steelhead water where the steelhead rules do not
    apply, and a rule set carried by named waters of two kinds."""
    anadromous, applies = set(anadromous), set(applies)
    bad = sorted(anadromous - applies)
    if bad:
        raise DerivedError(f"rule_key: {len(bad)} section(s) where a rainbow over 50 cm is a "
                           f"steelhead but no steelhead rule applies (e.g. {bad[:3]})")
    kinds: Dict[int, set] = defaultdict(set)
    for sid, s in section_set.items():
        k = kind_of_section.get(sid)
        if k is not None:
            kinds[s].add(T.WaterKind(k).value)
    mixed = sorted(s for s, v in kinds.items() if len(v) > 1)
    if mixed:
        raise DerivedError(f"rule_key: {len(mixed)} rule set(s) carry named waters of two kinds "
                           f"(e.g. set {mixed[0]}: {sorted(kinds[mixed[0]])}) — a key needs one")
    count: Dict[tuple, int] = defaultdict(int)
    rep: Dict[tuple, int] = {}
    for sid in sorted(section_set):
        k = (section_set[sid], sid in anadromous, sid in applies)
        count[k] += 1
        rep.setdefault(k, sid)
    ix = {k: i for i, k in enumerate(sorted(count))}
    rows = [(ix[k], k[0], int(k[1]), int(k[2]), next(iter(kinds[k[0]])) if kinds.get(k[0]) else None,
             count[k], rep[k]) for k in sorted(count)]
    key_of = {sid: ix[(s, sid in anadromous, sid in applies)] for sid, s in section_set.items()}
    return rows, key_of


# --------------------------------------------------------------------------------------------
# rule_ix, rule.closure_grade, part, part_section
# --------------------------------------------------------------------------------------------

def write(db: sqlite3.Connection, cov) -> None:
    """Write `rule_ix`, `rule.closure_grade`, `part` and `part_section`, and prove them."""
    from pipeline.deliver.bundle.read import rules_from
    from pipeline.deliver.bundle.rules import closure_grade

    # ---- rule_ix: the export's order (sorted `entry_id::rule_id` strings) ----------------
    rules = rules_from(db)
    ids = sorted(rule_id(x["entry_id"], x["rule_id"]) for x in rules)
    if len(set(ids)) != len(ids):
        raise DerivedError("rule_ix: two rules share an `entry_id::rule_id`")
    db.executemany("INSERT INTO rule_ix (ix, entry_id, rule_id) VALUES (?,?,?)",
                   [(i, *k.split("::", 1)) for i, k in enumerate(ids)])
    cov.filled("rule_ix", len(ids))
    db.execute("INSERT INTO meta (k, v) VALUES ('rule_ids_sha256', ?)", (rule_ids_digest(ids),))

    # ---- rule.closure_grade: the one closure predicate, stored ---------------------------
    grades = [(closure_grade(x), x["entry_id"], x["rule_id"]) for x in rules]
    db.executemany("UPDATE rule SET closure_grade = ? WHERE entry_id = ? AND rule_id = ?", grades)
    cov.filled("rule.closure_grade", sum(1 for g, _, _ in grades if g is not None))

    # ---- part / part_section --------------------------------------------------------------
    rows = part_rows(db)
    db.executemany(
        "INSERT INTO part (ord, part_ix, set_id, key_ix, licensing_set, province_except, "
        "steelhead_water, steelhead, steelhead_rules, home_regions, tidal, sections, rep_sid) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [r[:13] for r in rows])
    db.executemany("INSERT INTO part_section (ord, sid, part_ix) VALUES (?,?,?)",
                   [(r[0], s, r[1]) for r in rows for s in r[13]])
    cov.filled("part", len(rows))
    cov.filled("part_section", sum(len(r[13]) for r in rows))
    prove(db)


def part_rows(db: sqlite3.Connection) -> List[tuple]:
    """Every part of every named water, in the export's order: `(ord, part_ix, set_id, key_ix,
    licensing_set, province_except, steelhead_water, steelhead, steelhead_rules, home_regions,
    tidal, sections, rep_sid, [sid])`. Grouped by (item, rule set, licensing set, province
    exceptions, steelhead water, steelhead presence) and ordered as the export always listed them
    (rule set first, null last; licensing set; no province exception first; not steelhead water
    first; presence known, possible, none). The facts every section of a part must share
    (steelhead rules apply, tidal water, the rule key) are read per section and refused when they
    differ."""
    item_id = dict(db.execute("SELECT ord, item_id FROM item"))
    rset = dict(db.execute("SELECT sid, set_id FROM section_ruleset"))
    key_of = dict(db.execute("SELECT sid, key_ix FROM section_ruleset"))
    lset = dict(db.execute("SELECT sid, set_id FROM section_licensing"))
    pe: Dict[int, List[str]] = defaultdict(list)
    for kind, sid in db.execute("SELECT area_kind, sid FROM province_except ORDER BY area_kind"):
        pe[sid].append(T.ProvinceExceptKind(kind).value)
    sw = {s for (s,) in db.execute("SELECT sid FROM steelhead_water")}
    st = dict(db.execute("SELECT sid, code FROM section_steelhead"))
    sr = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
    home = dict(db.execute("SELECT sid, region FROM section_home"))
    tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}
    outside = {s for (s,) in db.execute("SELECT sid FROM outside_bc")}

    groups: Dict[tuple, List[int]] = defaultdict(list)
    for o, sid in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        groups[(o, rset.get(sid), lset.get(sid), ",".join(pe.get(sid, ())), sid in sw,
                st.get(sid))].append(sid)

    def order(g):
        o, rs, ls, p, w, s = g
        return (item_id[o], rs is None, rs or 0, ls is None, ls or 0, p != "", p, w,
                s is None, s or 0)

    out: List[tuple] = []
    n_of: Dict[int, int] = defaultdict(int)
    for g in sorted(groups, key=order):
        o, rs, ls, p, w, s = g
        sids = groups[g]
        name = f"{item_id[o]} part {n_of[o]}"
        rules_apply = {x in sr for x in sids}
        tides = {x in tidal for x in sids}
        keys = {key_of.get(x) for x in sids}
        if len(rules_apply) != 1:
            raise DerivedError(f"part: {name}: its sections disagree on whether the steelhead "
                               f"rules apply")
        if len(tides) != 1:
            raise DerivedError(f"part: {name}: its sections disagree on whether it is tidal water")
        if len(keys) != 1:
            raise DerivedError(f"part: {name}: its sections carry {len(keys)} rule keys")
        if rs is None and not all(x in outside for x in sids):
            raise DerivedError(f"part: {name}: no rule set, yet not wholly outside B.C.")
        homes = sorted({home[x] for x in sids if x in home})
        out.append((o, n_of[o], rs, next(iter(keys)), ls, p, int(w), s,
                    int(next(iter(rules_apply))), ",".join(T.Region(h).value for h in homes),
                    int(next(iter(tides))), len(sids), min(sids), sids))
        n_of[o] += 1
    return out


def prove(db: sqlite3.Connection) -> None:
    """The derived tables against the rows they came from — refused, naming the first failure."""
    bad = db.execute("PRAGMA foreign_key_check").fetchall()
    if bad:
        raise DerivedError(f"derived: foreign keys broken (e.g. {bad[:3]})")
    # every section's key reproduces the steelhead views and its rule set
    wrong = db.execute(
        "SELECT r.sid FROM section_ruleset r JOIN rule_key k ON k.key_ix = r.key_ix "
        "WHERE k.set_id != r.set_id "
        "OR k.steelhead_water != (r.sid IN (SELECT sid FROM steelhead_water)) "
        "OR k.steelhead_rules != (r.sid IN (SELECT sid FROM section_steelhead_rules)) "
        "LIMIT 3").fetchall()
    if wrong:
        raise DerivedError(f"rule_key: section(s) {wrong} carry a key their rule set or steelhead "
                           f"views do not reproduce")
    n = db.execute("SELECT (SELECT COUNT(*) FROM item_section), "
                   "(SELECT COUNT(*) FROM part_section), "
                   "(SELECT COALESCE(SUM(sections), 0) FROM part)").fetchone()
    if not n[0] == n[1] == n[2]:
        raise DerivedError(f"part: {n[1]} part sections and {n[2]} counted for {n[0]} named "
                           f"sections")
    ids = [f"{e}::{r}" for e, r in db.execute("SELECT entry_id, rule_id FROM rule_ix ORDER BY ix")]
    if ids != sorted(ids):
        raise DerivedError("rule_ix: not in sorted `entry_id::rule_id` order")
    if db.execute("SELECT COUNT(*) FROM rule").fetchone()[0] != len(ids):
        raise DerivedError("rule_ix: does not hold every rule once")


# --------------------------------------------------------------------------------------------
# Reading the parts back (the export and the answers)
# --------------------------------------------------------------------------------------------

def parts(db: sqlite3.Connection) -> Dict[str, List[T.Part]]:
    """{item_id: [Part in part_ix order]} — the bundle's parts, typed (`types.Part`)."""
    names = {c: n for c, n in read_steelhead_codes()}
    out: Dict[str, List[T.Part]] = defaultdict(list)
    for (item, kind, ix, s, k, ls, pe, sw, st, sr, home, td, n, rep) in db.execute(
            "SELECT i.item_id, i.kind, p.part_ix, p.set_id, p.key_ix, p.licensing_set, "
            "p.province_except, p.steelhead_water, p.steelhead, p.steelhead_rules, "
            "p.home_regions, p.tidal, p.sections, p.rep_sid FROM part p JOIN item i "
            "ON i.ord = p.ord ORDER BY i.item_id, p.part_ix"):
        out[item].append(T.Part(
            item_id=item, part_ix=ix, set_id=s, key=k, licensing_set=ls,
            province_except=tuple(pe.split(",")) if pe else (), steelhead_water=bool(sw),
            steelhead=None if st is None else names[st], steelhead_rules=bool(sr),
            home_regions=tuple(home.split(",")) if home else (), kind=kind, tidal=bool(td),
            sections=n, rep_sid=rep))
    return dict(out)


def read_steelhead_codes() -> List[Tuple[int, str]]:
    from pipeline.deliver.bundle.read import STEELHEAD_CODES
    return sorted(STEELHEAD_CODES.items())


def schema_check_values() -> Dict[str, str]:
    """The CHECK lists schema.sql spells, generated from the enums (`test_derived_tables` holds
    the schema text to these, so the SQL cannot drift from the types)."""
    def q(vals):
        return ", ".join(f"'{v}'" for v in vals)
    return {"province_except": q(T.province_except_values()),
            "kind": q(m.value for m in T.WaterKind),
            "closure_grade": q(m.value for m in T.ClosureGrade)}
