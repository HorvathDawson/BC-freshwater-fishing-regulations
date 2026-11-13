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
  part             THE PARTS OF EVERY NAMED WATER (Q16, finding H4): every CONNECTED RUN of one
  part_section     water's sections carrying the same rule set, licensing set, province exceptions,
                   steelhead water and steelhead presence (T517, owner 2026-11-08: "one part per
                   connected adjacent stretch"), numbered in river order from the mouth, so
                   `part_ix` IS the export's `waters[item].parts` index. The export and the answers
                   read this table; nothing else groups sections into parts (gate 3)
  region_place     WHERE EVERY SECTION IN B.C. IS (S1b): its region set and its MU set (the
  mu_set           atlas's, handed in by the bundle build), on `section_ruleset.place` / `.mus`
  unnamed_part     THE UNNAMED WATER (S1, user ruling 2026-10-10: every section has an answer):
  section_ruleset.up   every section no named water owns, grouped by exactly an answers key's
                   content (the named part's facts + the water kind). The answers give each part
                   the key of its content (a named part's where one holds it); nothing else
                   groups unnamed sections (gate 3)

Every table is held to SQL CHECK / UNIQUE / FOREIGN KEY constraints (schema.sql; the build runs
`PRAGMA foreign_keys = ON`), and `prove` refuses a build whose rows break a fact the constraints
cannot state (a part's sections disagreeing on whether the steelhead rules apply, tidal water or
the water kind; a part with no rule set that is not wholly outside B.C. or tidal water; a key that does not
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

#: One section's place as the bundle build hands it over: (kind, regions, mus) — the water kind
#: (`reach.water_kind.kind_of`; read for unnamed sections only), the sorted regions it takes zone
#: tables from, the sorted MUs it touches.
Place = Tuple[str, Tuple[str, ...], Tuple[str, ...]]


def write(db: sqlite3.Connection, cov, places: Optional[Dict[int, Place]] = None) -> None:
    """Write `rule_ix`, `rule.closure_grade`, `part` and `part_section` — and, given `places` (the
    atlas's place of every section, by handle: the bundle build always gives it), `region_place`,
    `mu_set`, `section_ruleset.place` / `.mus` (every section in B.C.), `unnamed_part` and
    `section_ruleset.up` — and prove them. A hand-built bundle with no atlas (the tests' fixtures)
    passes no places and gets none of these."""
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
        "steelhead_water, steelhead, steelhead_rules, home_regions, tidal, sections, rep_sid, "
        "no_access, grp, run, chain) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [r[:14] + r[15:18] for r in rows])
    # (T387) each section's length along the water (access.py `section_length`): what the app weighs a part's access by
    length = dict(db.execute("SELECT sid, m FROM section_length"))
    db.executemany("INSERT INTO part_section (ord, sid, part_ix, m) VALUES (?,?,?,?)",
                   [(r[0], s, r[1], length.get(s)) for r in rows for s in r[14]])
    cov.filled("part", len(rows))
    cov.filled("part_section", sum(len(r[14]) for r in rows))

    # ---- region_place / mu_set / unnamed_part / section_ruleset (S1, S1b) -----------------
    if places is not None:
        place_rows, mu_rows, where, part_rows_, up_of = unnamed_rows(db, places)
        db.executemany("INSERT INTO region_place (place, regions, sections) VALUES (?,?,?)",
                       place_rows)
        db.executemany("INSERT INTO mu_set (mus, codes, sections) VALUES (?,?,?)", mu_rows)
        db.executemany(
            "INSERT INTO unnamed_part (up, set_id, key_ix, licensing_set, province_except, "
            "steelhead_water, steelhead, steelhead_rules, home_regions, tidal, kind, sections, "
            "rep_sid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", part_rows_)
        db.executemany("UPDATE section_ruleset SET up = ?, place = ?, mus = ? WHERE sid = ?",
                       ((up_of.get(s), pl, mu, s) for s, (pl, mu) in sorted(where.items())))
        # THE KIND OF EVERY RULE KEY (S1): a key only unnamed sections carry (138 on 2026-10-10)
        # takes their kind; one kind per key, named and unnamed alike, or the build stops
        kinds: Dict[int, set] = defaultdict(set)
        for k, kind in db.execute("SELECT key_ix, kind FROM unnamed_part"):
            kinds[k].add(kind)
        for k, kind in db.execute("SELECT key_ix, kind FROM rule_key WHERE kind IS NOT NULL"):
            kinds[k].add(kind)
        mixed = sorted(k for k, v in kinds.items() if len(v) > 1)
        if mixed:
            raise DerivedError(f"rule_key: {len(mixed)} key(s) carry sections of two water kinds "
                               f"(e.g. key {mixed[0]}: {sorted(kinds[mixed[0]])}) — a key needs one")
        filled = [(next(iter(v)), k) for k, v in sorted(kinds.items())]
        db.executemany("UPDATE rule_key SET kind = ? WHERE key_ix = ? AND kind IS NULL", filled)
        cov.filled("region_place", len(place_rows))
        cov.filled("mu_set", len(mu_rows))
        cov.filled("unnamed_part", len(part_rows_))
        cov.filled("section_ruleset.up", len(up_of))
    prove(db, unnamed=places is not None)


def join_touching(pieces: List[List[int]], touch: Dict[int, set]) -> List[List[int]]:
    """T517 review: two stretches whose sections TOUCH are one run. `stretches` files a side channel under the
    stretch it re-enters, so a braid that leaves above another group's piece of main stem and re-enters below it
    (Wap Creek's channel inside Mount Griffin Ecological Reserve, past the 275 m of stem outside it; Nimpkish,
    Skeena) left two parts of one key meeting with no mark — one painted run listed twice."""
    root = list(range(len(pieces)))

    def find(i: int) -> int:
        while root[i] != i:
            root[i] = root[root[i]]
            i = root[i]
        return i
    at = {x: i for i, p in enumerate(pieces) for x in p}
    for i, p in enumerate(pieces):
        for x in p:
            for y in touch.get(x, ()):
                j = at.get(y)
                if j is not None and find(i) != find(j):
                    a, b = sorted((find(i), find(j)))
                    root[b] = a
    out: Dict[int, List[int]] = defaultdict(list)
    for i, p in enumerate(pieces):
        out[find(i)] += p
    return [sorted(v) for _, v in sorted(out.items())]


def _runs_of(db: sqlite3.Connection):
    """T517: what splits a same-rules group into its CONNECTED RUNS — the bundle's spans and touches, read once.
    Returns `split(o, groups)`: for one water `o` and its groups `{g: [sid]}`, `{g: [[sid] per run]}` plus each run's
    river key (`part_km`, km from the mouth of its main-stem top; None for no main stem). A run is
    `topology.stretches` (T204): a group's main-stem runs merged unless ANOTHER group of the water has a main-stem
    section in the gap between them (a lake on the course, or an unnamed piece, is not another part), side channels
    joining the run they re-enter, and any two of those whose sections touch (`section_touch`: a braid round another
    group's piece of stem) joined; a lake / polygon water, or a group with a section the spans do not hold, is one
    run. So two runs of one group never touch, and every run is maximal (`release.paint_sync` proves it on the
    shipped parts against `section_touch`)."""
    from pipeline.deliver.bundle import spans as SP
    from pipeline.deliver.bundle.topology import part_km, stretches
    kind = dict(db.execute("SELECT ord, kind FROM item"))
    span = {s: (a, b, lo, hi, off, shape) for s, a, b, lo, hi, off, shape in db.execute(
        "SELECT s.sid, s.lo_m, s.hi_m, a.token, b.token, s.off_stem, s.shape FROM section_span s "
        "JOIN span_end a ON a.eid = s.lo JOIN span_end b ON b.eid = s.hi")}
    touch: Dict[int, set] = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        touch[a].add(b)
        touch[b].add(a)

    def one(ss: List[int]):
        poly = all(span.get(x) is not None and span[x][5] == 1 and span[x][0] is None for x in ss)
        if poly or any(x not in span for x in ss):
            return None
        return SP.compose_runs(span, touch, ss)

    def split(o: int, groups: Dict[tuple, List[int]]) -> Dict[tuple, List[Tuple[List[int], Optional[float], list]]]:
        out: Dict[tuple, List[Tuple[List[int], Optional[float], list]]] = {}
        if kind.get(o) != "stream":
            return {g: [(ss, None, [])] for g, ss in groups.items()}
        stem = {g: [(span[x][0], span[x][1]) for x in ss if x in span and not span[x][4]
                    and span[x][0] is not None and span[x][1] is not None] for g, ss in groups.items()}
        for g, ss in groups.items():
            runs = one(ss)
            if runs is None:
                out[g] = [(ss, None, [])]
                continue
            other = [ab for h, v in stem.items() if h != g for ab in v]
            pieces = join_touching([p["sids"] for p in stretches(runs, span, other)] or [ss], touch)
            out[g] = [(p, part_km(r), r) for p in pieces for r in [one(p) or []]]
        return out

    return split, touch


def part_rows(db: sqlite3.Connection) -> List[tuple]:
    """Every part of every named water, in the export's order: `(ord, part_ix, set_id, key_ix,
    licensing_set, province_except, steelhead_water, steelhead, steelhead_rules, home_regions,
    tidal, sections, rep_sid, no_access, [sid], grp, run, chain)`. Grouped by (item, rule set, licensing set, province
    exceptions, steelhead water, steelhead presence, mostly inside no-access land, DFO salmon chain), each group cut into
    its CONNECTED RUNS (T517, `_runs_of`: one part per run), and numbered in RIVER ORDER (`topology.order_parts`,
    the one ranking): by km from the mouth (`part_km`); a side-channel part with no km right after the main-stem
    part it runs beside (T710, owner 2026-11-13 "List it where it sits"); then the other parts with no km (a lake, a
    side part with no neighbour) — ties and the km-less in the old group order (rule set first, null last; licensing
    set; no province exception first; not steelhead water first; presence known, possible, none; outside
    no-access land first), then lowest section.
    NO ACCESS IS A HARD BREAK (T226, owner 2026-10-28): a section mostly inside no-access land
    (`access.mostly_inside`, the app's fourth colour; `section_access` must be written first) never
    shares a part with one outside, though their rules agree. The facts every section of a part must share
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
    from pipeline.deliver.bundle.access import mostly_inside
    closed_land = mostly_inside(db)

    # T517: each section's DFO salmon chain is IN THE KEY (0: a section in no chain — the unlisted chain, as the salmon
    # file says it), so every part is uniform for the salmon paint too, by construction. The schema always holds the
    # table (`salmon.write_bundle` fills it first); a bundle without it is refused by SQLite, never read as "no chain"
    chain_of: Dict[int, int] = dict(db.execute("SELECT sid, chain FROM salmon_section"))
    groups: Dict[tuple, List[int]] = defaultdict(list)
    for o, sid in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        groups[(o, rset.get(sid), lset.get(sid), ",".join(pe.get(sid, ())), sid in sw,
                st.get(sid), sid in closed_land, chain_of.get(sid, 0))].append(sid)

    def order(g):
        o, rs, ls, p, w, s, na = g[:7]
        return (item_id[o], rs is None, rs or 0, ls is None, ls or 0, p != "", p, w,
                s is None, s or 0, na) + tuple(g[7:])

    from pipeline.deliver.bundle.topology import order_parts
    split, touch = _runs_of(db)
    by_ord: Dict[int, Dict[tuple, List[int]]] = defaultdict(dict)
    for g, sids in groups.items():
        by_ord[g[0]][g] = sids
    # `grp`: the chain-free rule-set group (what the picker, the full label and the closed-merge read, as before T517);
    # `run`: which chain-free connected run of its grp a part lies in (`_runs_of` without the chain). Parts sharing
    # (grp, run) are one run cut only where the salmon chain changes.
    pieces: List[Tuple[tuple, List[int], int, int]] = []
    for o in sorted(by_ord, key=lambda o: item_id[o]):
        runs = split(o, by_ord[o])
        free: Dict[tuple, List[int]] = defaultdict(list)
        for g, ss in by_ord[o].items():
            free[g[:7]] += ss
        grp_of = {g: i for i, g in enumerate(sorted(free, key=order))}
        free_runs = split(o, free)
        run_of: Dict[int, int] = {}
        for g, rs_ in free_runs.items():
            ranked = sorted(rs_, key=lambda x: (x[1] is None, x[1] or 0, min(x[0])))
            for k, (ss, _, _) in enumerate(ranked):
                for x in ss:
                    run_of[x] = k
        # RIVER ORDER is `topology.order_parts` (the rank `part_geo` asserts): `v` here is the tie order — the group
        # order, then lowest section; `touches` the pieces any section borders (T710: a side channel's neighbour)
        mine = sorted(((g, ss, km, rr) for g in by_ord[o] for ss, km, rr in runs[g]),
                      key=lambda x: (order(x[0]), min(x[1])))
        at = {x: i for i, m in enumerate(mine) for x in m[1]}
        ranked_ = order_parts([{"v": i, "km": km, "runs": rr, "poly": not rr,
                                "touches": sorted({at[y] for x in ss for y in touch.get(x, ()) if at.get(y, i) != i})}
                               for i, (g, ss, km, rr) in enumerate(mine)])
        for g, ss, _, _ in (mine[p["v"]] for p in ranked_):
            ks = {run_of[x] for x in ss}
            if len(ks) != 1:
                raise DerivedError(f"part: {item_id[o]}: a salmon-chain run straddles two runs of its rule set")
            pieces.append((g, sorted(ss), grp_of[g[:7]], ks.pop()))

    out: List[tuple] = []
    n_of: Dict[int, int] = defaultdict(int)
    for g, sids, grp, run in pieces:
        o, rs, ls, p, w, s, na, ch = g
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
        # T166: a piece below a tidal boundary carries no freshwater rule (the walk took it out of every binding);
        # a part with no rule set must be wholly outside B.C. or wholly tidal water
        if rs is None and not all(x in outside or x in tidal for x in sids):
            raise DerivedError(f"part: {name}: no rule set, yet not wholly outside B.C. or tidal water")
        homes = sorted({home[x] for x in sids if x in home})
        out.append((o, n_of[o], rs, next(iter(keys)), ls, p, int(w), s,
                    int(next(iter(rules_apply))), ",".join(T.Region(h).value for h in homes),
                    int(next(iter(tides))), len(sids), min(sids), int(na), sids, grp, run, ch))
        n_of[o] += 1
    return out


def unnamed_rows(db: sqlite3.Connection, places: Dict[int, Place]
                 ) -> Tuple[List[tuple], List[tuple], Dict[int, Tuple[int, int]], List[tuple],
                            Dict[int, int]]:
    """`(region_place rows, mu_set rows, {sid: (place, mus)}, unnamed_part rows, {sid: up})`.
    WHERE: every section in B.C. (it carries a rule set), named or not, gets its region set and MU
    set — `places` must name every such section; a place with no region, an unknown region or MU is
    refused. THE UNNAMED PARTS: every such section no named water owns, grouped by (rule set,
    licensing set, province exceptions, steelhead water, steelhead presence, home region, tidal
    water, water kind) — an answers key's content, never where it lies (S1b) — and numbered in that
    order; the facts a group must share (the steelhead rules apply, the rule key) are read per
    section and refused when they differ."""
    named = {s for (s,) in db.execute("SELECT sid FROM item_section")}
    rset, key_of = {}, {}
    in_bc = []
    for sid, set_id, k in db.execute("SELECT sid, set_id, key_ix FROM section_ruleset"):
        in_bc.append(sid)
        if sid not in named:
            rset[sid], key_of[sid] = set_id, k
    lset = dict(db.execute("SELECT sid, set_id FROM section_licensing"))
    pe: Dict[int, List[str]] = defaultdict(list)
    for kind, sid in db.execute("SELECT area_kind, sid FROM province_except ORDER BY area_kind"):
        pe[sid].append(T.ProvinceExceptKind(kind).value)
    sw = {s for (s,) in db.execute("SELECT sid FROM steelhead_water")}
    st = dict(db.execute("SELECT sid, code FROM section_steelhead"))
    sr = {s for (s,) in db.execute("SELECT sid FROM section_steelhead_rules")}
    home = dict(db.execute("SELECT sid, region FROM section_home"))
    tidal = {s for (s,) in db.execute("SELECT sid FROM tidal")}

    missing = sorted(s for s in in_bc if s not in places)
    if missing:
        raise DerivedError(f"region_place: {len(missing)} section(s) in B.C. have no place "
                           f"(e.g. {missing[:3]}) — every section needs a region (and an unnamed "
                           f"one a kind)")
    n_regions: Dict[tuple, int] = defaultdict(int)
    n_mus: Dict[tuple, int] = defaultdict(int)
    for sid in in_bc:
        n_regions[places[sid][1]] += 1
        n_mus[places[sid][2]] += 1
    place_ix = {r: i for i, r in enumerate(sorted(n_regions))}
    mu_ix = {m: i for i, m in enumerate(sorted(n_mus))}
    try:                                        # the types' checks: regions (never none), MUs
        for r, i in place_ix.items():
            T.RegionPlace(place=i, regions=r, sections=n_regions[r])
        for m, i in mu_ix.items():
            T.MuSet(mus=i, codes=m, sections=n_mus[m])
    except ValueError as e:
        raise DerivedError(f"region_place / mu_set: {e}")
    place_rows = [(place_ix[r], ",".join(r), n_regions[r]) for r in sorted(n_regions)]
    mu_rows = [(mu_ix[m], ",".join(m), n_mus[m]) for m in sorted(n_mus)]
    where = {sid: (place_ix[places[sid][1]], mu_ix[places[sid][2]]) for sid in in_bc}

    groups: Dict[tuple, List[int]] = defaultdict(list)
    for sid in sorted(rset):
        kind = places[sid][0]
        try:
            T.WaterKind(kind)
        except ValueError:
            raise DerivedError(f"unnamed_part: section {sid} has water kind {kind!r}")
        groups[(rset[sid], lset.get(sid), ",".join(pe.get(sid, ())), sid in sw, st.get(sid),
                home.get(sid), sid in tidal, kind)].append(sid)

    def order(g):
        rs, ls, p, w, s, h, td, kd = g
        return (rs, ls is None, ls or 0, p != "", p, w, s is None, s or 0, h is None, h or "",
                td, kd)

    part_rows_: List[tuple] = []
    up_of: Dict[int, int] = {}
    for up, g in enumerate(sorted(groups, key=order)):
        rs, ls, p, w, s, h, td, kd = g
        sids = groups[g]
        rules_apply = {x in sr for x in sids}
        keys = {key_of[x] for x in sids}
        if len(rules_apply) != 1:
            raise DerivedError(f"unnamed_part {up}: its sections disagree on whether the steelhead "
                               f"rules apply")
        if len(keys) != 1:
            raise DerivedError(f"unnamed_part {up}: its sections carry {len(keys)} rule keys")
        part_rows_.append((up, rs, next(iter(keys)), ls, p, int(w), s,
                           int(next(iter(rules_apply))), T.Region(h).value if h else "", int(td),
                           kd, len(sids), min(sids)))
        for x in sids:
            up_of[x] = up
    return place_rows, mu_rows, where, part_rows_, up_of


def prove(db: sqlite3.Connection, unnamed: Optional[bool] = None) -> None:
    """The derived tables against the rows they came from — refused, naming the first failure.
    `unnamed` True: the unnamed parts were written (the bundle build), so every section in B.C. must
    be in exactly one part, named or unnamed; False: none may be (a fixture with no places); None
    (a built bundle read back): True exactly when it holds unnamed parts."""
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
    # THE UNNAMED PARTS (S1): every section in B.C. is named XOR in an unnamed part, and an
    # unnamed part's facts are its sections'
    named_up = db.execute("SELECT COUNT(*) FROM section_ruleset r JOIN item_section s "
                          "ON s.sid = r.sid WHERE r.up IS NOT NULL").fetchone()[0]
    if named_up:
        raise DerivedError(f"unnamed_part: {named_up} named section(s) carry an unnamed part")
    if unnamed is None:
        unnamed = bool(db.execute("SELECT COUNT(*) FROM unnamed_part").fetchone()[0])
    if not unnamed:
        if db.execute("SELECT COUNT(*) FROM unnamed_part").fetchone()[0]:
            raise DerivedError("unnamed_part: rows written without the places they come from")
        return
    loose = db.execute("SELECT COUNT(*) FROM section_ruleset r WHERE r.up IS NULL AND NOT EXISTS "
                       "(SELECT 1 FROM item_section s WHERE s.sid = r.sid)").fetchone()[0]
    if loose:
        raise DerivedError(f"unnamed_part: {loose} unnamed section(s) in B.C. are in no part — "
                           f"every section has an answer")
    kindless = db.execute("SELECT COUNT(*) FROM rule_key WHERE kind IS NULL").fetchone()[0]
    if kindless:
        raise DerivedError(f"rule_key: {kindless} key(s) have no water kind — every section has one")
    wrong = db.execute(
        "SELECT r.sid FROM section_ruleset r JOIN unnamed_part u ON u.up = r.up "
        "LEFT JOIN section_licensing l ON l.sid = r.sid "
        "WHERE u.set_id != r.set_id OR u.key_ix != r.key_ix OR NOT (u.licensing_set IS l.set_id) "
        "OR u.tidal != (r.sid IN (SELECT sid FROM tidal)) LIMIT 3").fetchall()
    if wrong:
        raise DerivedError(f"unnamed_part: section(s) {wrong} carry facts their part does not")
    n = db.execute("SELECT (SELECT COUNT(*) FROM section_ruleset WHERE up IS NOT NULL), "
                   "(SELECT COALESCE(SUM(sections), 0) FROM unnamed_part)").fetchone()
    if n[0] != n[1]:
        raise DerivedError(f"unnamed_part: {n[0]} sections, {n[1]} counted by the parts")
    # WHERE (S1b): every section in B.C. has a region set and an MU set, counted by them
    unplaced = db.execute("SELECT COUNT(*) FROM section_ruleset WHERE place IS NULL OR mus IS NULL"
                          ).fetchone()[0]
    if unplaced:
        raise DerivedError(f"region_place: {unplaced} section(s) in B.C. have no region set or MU "
                           f"set")
    for tbl, col in (("region_place", "place"), ("mu_set", "mus")):
        got = dict(db.execute(f"SELECT {col}, COUNT(*) FROM section_ruleset GROUP BY {col}"))
        bad = [x for x, n in db.execute(f"SELECT {col}, sections FROM {tbl}")
               if got.get(x) != n][:3]
        if bad:
            raise DerivedError(f"{tbl}: row(s) {bad} count sections section_ruleset does not")


# --------------------------------------------------------------------------------------------
# Reading the parts back (the export and the answers)
# --------------------------------------------------------------------------------------------

def parts(db: sqlite3.Connection) -> Dict[str, List[T.Part]]:
    """{item_id: [Part in part_ix order]} — the bundle's parts, typed (`types.Part`)."""
    names = {c: n for c, n in read_steelhead_codes()}
    out: Dict[str, List[T.Part]] = defaultdict(list)
    for (item, kind, ix, s, k, ls, pe, sw, st, sr, home, td, n, rep, grp, run) in db.execute(
            "SELECT i.item_id, i.kind, p.part_ix, p.set_id, p.key_ix, p.licensing_set, "
            "p.province_except, p.steelhead_water, p.steelhead, p.steelhead_rules, "
            "p.home_regions, p.tidal, p.sections, p.rep_sid, p.grp, p.run FROM part p JOIN item i "
            "ON i.ord = p.ord ORDER BY i.item_id, p.part_ix"):
        out[item].append(T.Part(
            item_id=item, part_ix=ix, set_id=s, key=k, licensing_set=ls,
            province_except=tuple(pe.split(",")) if pe else (), steelhead_water=bool(sw),
            steelhead=None if st is None else names[st], steelhead_rules=bool(sr),
            home_regions=tuple(home.split(",")) if home else (), kind=kind, tidal=bool(td),
            sections=n, rep_sid=rep, grp=grp, run=run))
    return dict(out)


def region_places(db: sqlite3.Connection) -> List[T.RegionPlace]:
    """Every region set sections lie in, in `place` order (`types.RegionPlace`, S1b)."""
    return [T.RegionPlace(place=p, regions=tuple(r.split(",")), sections=n)
            for p, r, n in db.execute("SELECT place, regions, sections FROM region_place "
                                      "ORDER BY place")]


def mu_sets(db: sqlite3.Connection) -> List[T.MuSet]:
    """Every MU set sections touch, in `mus` order (`types.MuSet`, S1b)."""
    return [T.MuSet(mus=m, codes=tuple(c.split(",")) if c else (), sections=n)
            for m, c, n in db.execute("SELECT mus, codes, sections FROM mu_set ORDER BY mus")]


def unnamed_parts(db: sqlite3.Connection) -> List[T.UnnamedPart]:
    """Every part of the unnamed water, in `up` order (`types.UnnamedPart`). Empty for a bundle
    written without places (a test fixture)."""
    names = {c: n for c, n in read_steelhead_codes()}
    return [T.UnnamedPart(
        up=up, set_id=s, key=k, licensing_set=ls,
        province_except=tuple(pe.split(",")) if pe else (), steelhead_water=bool(sw),
        steelhead=None if st is None else names[st], steelhead_rules=bool(sr),
        home_regions=(home,) if home else (), kind=kind, tidal=bool(td), sections=n,
        rep_sid=rep)
        for (up, s, k, ls, pe, sw, st, sr, home, td, kind, n, rep) in db.execute(
            "SELECT up, set_id, key_ix, licensing_set, province_except, steelhead_water, "
            "steelhead, steelhead_rules, home_regions, tidal, kind, sections, rep_sid "
            "FROM unnamed_part ORDER BY up")]


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
