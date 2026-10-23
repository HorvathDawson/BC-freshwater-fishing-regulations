"""T57 — does every "and tributaries" rule reach the unnamed streams it should?

The reach run's tributary walk (`pipeline.atlas.graph.tributaries`) binds a walking rule to the streams
joining its reach; the bundle stores the result per section (`ruleset.via = 'trib'`). An unnamed stream
has no row of its own, so what it is fished under is the region's base plus exactly those walks (T53).
This tool checks the walk's answer ONE FLOW STEP AT A TIME, which is a different question from the
walk's own traversal and so can catch what it drops:

    an unnamed STREAM section `u` flowing into a section `d` that carries a walking rule R — by the walk
    (`trib`), or in R's own reach (`reach`) when R walks — must carry R too.

Lakes and wetlands are never collected by a walk (p.80, `tributaries.STREAMS_ONLY`) but the walk climbs
through them, so a lake or a section with no rule row (outside B.C.) between `u` and `d` is passed
through. Two of the walk's own stops are applied while looking, because they are not misses at all:

  * THE RIVER CONTINUING: `u` joins `d` by a mainstem edge (`continuation` / `lake_out`, or a lake
    draining straight into a lake) and R holds `d` by its REACH — `u` is the reach's own water (or the
    lake at its top) going on past the cut, not a tributary ("X River downstream of Y Lake" does not take
    Y Lake's inflows). Unless the lake is MID-REACH: a piece of R's reach on `d`'s blue line flows into
    it from further up (`tributaries._lake_mid_reach`), so the river passes through and its inflows are
    tributaries.
  * a section already carrying R is no miss.

What is left is the FRONTIER: the lowest unnamed sections where R stops though the section below has
it. Each is EXPLAINED by a stop the walk makes on purpose (`REASONS`, first match wins) or counted as
UNEXPLAINED — a real miss. The report counts frontier pairs and the unnamed stream sections above each
(the whole subtree that loses R with it), by rule and by region, with examples.

NOT SEEN by this check (said, not hidden): a `tributaries_only` rule binds no reach, so a stream joining
its reach DIRECTLY is not checked (one joining a bound tributary is); and a miss on a NAMED tributary
shows only through the unnamed streams above it.

Two more things are REPORTED beside it: `unnamed_lake_at_reach_top` (a nameless lake at the top of a
reach stops the walk like a named one — not a miss by the walk's rule, listed so a person can judge),
and `mu_groups` — every rule `within` an MU group against the group's membership and the sections' MU
stamps (`mu_lines`: the "near an MU line" check; straddling pieces are MARKED in, never cut).

    python -m pipeline.tools.unnamed_trib_sweep [--build ...] [--bundle ...] [--reach ...]
                                                 [--topo-cache PATH] [--out report.json] [--write-result]

It unpickles the atlas graph (~6 s, a few GB): run it through `scripts/heavy.sh` (AGENTS 18). The
committed result is `pipeline/tests/fixtures/t57_trib_sweep.json`; `test_unnamed_trib_sweep.py` pins the
rules on stubs and, on the live data, that the committed result is this bundle's answer.
"""
from __future__ import annotations

import argparse
import collections
import json
import pickle
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Set, Tuple

from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS as MAINSTEM

#: Why a frontier pair is not a miss, in the order they are asked. Each is a stop the walk makes ON
#: PURPOSE, named after the switch or ruling that makes it:
REASONS = (
    # `guarded`: the walk never enters a 2300 barrier piece (a canal) or anything above it.
    "barrier",
    # `tributaries._breaks_strahler`: a bigger river cannot be a tributary (codes silent or equal).
    "strahler",
    # `_through_blks`: the inflow on the reach lake's outlet line is the river running through the lake.
    "through_lake",
    # RU-14 (`build._stem_to_first_lake`): a joining water with a row of its own that the signs pull
    # into the cut is walked from its mouth to its FIRST LAKE; the streams feeding that lake are not.
    "stem_ends_at_lake",
    # `within_area` (Williston Lake "in Zone B", West Road's tributaries in one region): the area is
    # applied AFTER the walk and takes away what lies outside it.
    "within_area",
    # `outside.region_limit`: a per-region row (its water printed by another region too) is held, walk
    # and all, to its own region(s).
    "region_clip",
    # `feature_types`: the rule is for another kind of water, applied after the walk.
    "feature_types",
)


@dataclass(frozen=True)
class Topo:
    """The atlas graph in section HANDLES — only what the check reads. `down[h]` / `up[h]` are
    `(neighbour, edge kind)` pairs; a lake carries no blue line (`blk` "")."""
    kind: Mapping[int, str]
    order: Mapping[int, Optional[int]]
    wsc: Mapping[int, str]
    barrier: Mapping[int, bool]
    blk: Mapping[int, str]
    down_m: Mapping[int, float]
    up_m: Mapping[int, float]
    down: Mapping[int, Tuple[Tuple[int, str], ...]]
    up: Mapping[int, Tuple[Tuple[int, str], ...]]
    handles_digest: str = ""


def topo_of(nodes: Mapping[int, tuple], edges: Iterable[Tuple[int, int, str]], digest: str = "") -> Topo:
    """`nodes[h] = (kind, order, wsc, barrier, blk, down_m, up_m)`; `edges` = (from, to, kind)."""
    down: Dict[int, list] = collections.defaultdict(list)
    up: Dict[int, list] = collections.defaultdict(list)
    for f, t, k in edges:
        if f and t:
            down[f].append((t, k))
            up[t].append((f, k))
    col = lambda i: {h: n[i] for h, n in nodes.items()}  # noqa: E731
    return Topo(kind=col(0), order=col(1), wsc=col(2), barrier=col(3), blk=col(4), down_m=col(5),
                up_m=col(6), down={h: tuple(sorted(v)) for h, v in down.items()},
                up={h: tuple(sorted(v)) for h, v in up.items()}, handles_digest=digest)


def topology(build_dir: Path) -> Topo:
    """Read `graph.pkl` into a `Topo` (heavy: run through scripts/heavy.sh)."""
    from pipeline.common import section_handles
    from pipeline.common.io.serialize import read_artifact
    _ids, handle = section_handles.read(build_dir)
    g = read_artifact(str(Path(build_dir) / "graph.pkl"))
    nodes = {}
    for nid, n in g.nodes.items():
        h = handle[nid]
        nodes[h] = (str(getattr(n.kind, "value", n.kind)), n.stream_order, n.wsc or "",
                    bool(n.is_barrier), n.blk or "", float(n.down_m), float(n.up_m))
    edges = [(handle.get(e.from_node, 0), handle.get(e.to_node, 0), e.kind) for e in g.edges]
    return topo_of(nodes, edges, section_handles.digest_for(Path(build_dir)))


Rule = Tuple[str, str]          # (entry_id, rule_id) — AGENTS 8: rule_id is unique only within an entry


@dataclass
class Bound:
    """The bundle's binding, as the check needs it. `rules[sid] = {rule: via}` holds every rule
    that is not a zone or provincial base rule (`z*`); `unnamed` the unnamed STREAM sections."""
    rules: Mapping[int, Mapping[Rule, str]]
    unnamed: Set[int]
    regions: Mapping[int, Tuple[str, ...]]
    walking: Mapping[Rule, dict]
    facts: Mapping[Rule, Set[str]] = field(default_factory=dict)
    #: the sections a rule's `within_area` lets it hold (the area's own sections, from the registry)
    limits: Mapping[Rule, frozenset] = field(default_factory=dict)
    #: the water kinds a rule's `feature_types` limit it to
    kinds: Mapping[Rule, frozenset] = field(default_factory=dict)
    #: the unnamed lake and wetland sections
    unnamed_lakes: Set[int] = field(default_factory=set)


def _breaks_strahler(t: Topo, a: int, b: int) -> bool:
    """`tributaries._breaks_strahler` on handles (outside B.C. is not in a `Topo`: order None there)."""
    oa, ob = t.order.get(a), t.order.get(b)
    if oa is None or ob is None or oa <= ob:
        return False
    wa, wb = t.wsc.get(a, ""), t.wsc.get(b, "")
    return not (wa and wb and wa != wb and wa.startswith(wb))


def _through_blks(t: Topo, lake: int) -> Set[str]:
    """The blue lines a lake's own flow leaves by, through any lakes below it (`tributaries._through_blks`)."""
    out: Set[str] = set()
    seen, stack = {lake}, [lake]
    while stack:
        cur = stack.pop()
        for to, _k in t.down.get(cur, ()):
            if t.kind.get(to) == "lake":
                if to not in seen:
                    seen.add(to)
                    stack.append(to)
            elif t.blk.get(to):
                out.add(t.blk[to])
    return out


def _mid_reach(t: Topo, b: Bound, lake: int, below: int, rule: Rule) -> bool:
    """Does `rule`'s REACH carry on above `lake` on `below`'s line (`tributaries._lake_mid_reach`)?"""
    bb = t.blk.get(below)
    if not bb:
        return False
    seen, stack = {lake}, [lake]
    while stack:
        cur = stack.pop()
        for src, _k in t.up.get(cur, ()):
            in_reach = b.rules.get(src, {}).get(rule) == "reach"
            if in_reach and (src == below or (t.blk.get(src) == bb
                                              and t.down_m.get(src, 0.0) >= t.up_m.get(below, 0.0) - 1.0)):
                return True
            if t.kind.get(src) == "lake" and src not in seen and not in_reach:
                seen.add(src)
                stack.append(src)
    return False


def _carried(t: Topo, b: Bound, d: int, edge: str, came_from: Optional[int]) -> Set[Rule]:
    """The walking rules `d` hands to what flows into it by `edge` (from `came_from`, a lake, or None
    for the unnamed stream itself)."""
    out: Set[Rule] = set()
    from_lake = came_from is not None and t.kind.get(came_from) == "lake"
    for r, via in b.rules.get(d, {}).items():
        if via == "trib":
            out.add(r)
        elif r in b.walking:
            mainstem = edge in MAINSTEM or (from_lake and t.kind.get(d) == "lake")
            if mainstem and not (from_lake and _mid_reach(t, b, came_from, d, r)):
                continue                    # the reach's own water (or the lake at its top) going on
            out.add(r)
    return out


def _passes(t: Topo, b: Bound, n: int) -> bool:
    """A lake, or a section with no rule row (outside B.C.), is climbed through, never collected."""
    return t.kind.get(n) == "lake" or n not in b.rules


@dataclass(frozen=True)
class Miss:
    sid: int                    # the unnamed stream section that lacks the rule
    rule: Rule
    below: int                  # the section it flows into that hands the rule on (through lakes)
    via: str                    # how `below` holds the rule: reach | trib
    lake: Optional[int]         # the lake between them, when there is one


def frontier(t: Topo, b: Bound) -> List[Miss]:
    """Every (unnamed stream section, walking rule) where the section below hands the rule on and
    the section does not carry it — sorted, deterministic."""
    memo: Dict[int, Dict[Rule, Tuple[int, str, Optional[int]]]] = {}

    def through(n: int) -> Dict[Rule, Tuple[int, str, Optional[int]]]:
        # what a lake / no-row section `n` hands up: rules of what it drains into, recursively
        if n in memo:
            return memo[n]
        memo[n] = {}
        got: Dict[Rule, Tuple[int, str, Optional[int]]] = {}
        for d, k in t.down.get(n, ()):
            for r in sorted(_carried(t, b, d, k, n)):
                got.setdefault(r, (d, b.rules[d][r], n))
            if _passes(t, b, d):
                for r, v in through(d).items():
                    got.setdefault(r, v)
        memo[n] = got
        return got

    out: List[Miss] = []
    for u in sorted(b.unnamed):
        if t.kind.get(u) != "stream":
            continue
        mine = b.rules.get(u, {})
        want: Dict[Rule, Tuple[int, str, Optional[int]]] = {}
        for d, k in t.down.get(u, ()):
            for r in sorted(_carried(t, b, d, k, None)):
                want.setdefault(r, (d, b.rules[d][r], None))
            if _passes(t, b, d):
                for r, v in through(d).items():
                    want.setdefault(r, v)
        for r in sorted(want):
            if r not in mine:
                d, via, lake = want[r]
                out.append(Miss(u, r, d, via, lake))
    return out


def _region_of_entry(entry_id: str) -> str:
    """`r7:…` → "7"; the bundle's regions are "1".."8", "7a", "7b"."""
    m = re.match(r"r(\d)", entry_id)
    return m.group(1) if m else ""


def explain(t: Topo, b: Bound, m: Miss) -> Optional[str]:
    """The walk's own stop that accounts for `m` (one of `REASONS`), or None: a real miss."""
    facts = b.facts.get(m.rule, set())
    first = m.lake if m.lake is not None else m.below
    if t.barrier.get(m.sid):
        return "barrier"
    if _breaks_strahler(t, m.sid, first):
        return "strahler"
    reach_lake = m.lake if m.lake is not None else (m.below if t.kind.get(m.below) == "lake" else None)
    if reach_lake is not None and m.via == "reach" and t.blk.get(m.sid) in _through_blks(t, reach_lake):
        return "through_lake"
    if m.lake is not None and m.via == "trib" and "stem_ends_at_lake" in facts \
            and any(d == m.below and k in MAINSTEM for d, k in t.down.get(m.lake, ())):
        return "stem_ends_at_lake"
    if m.rule in b.limits and m.sid not in b.limits[m.rule]:
        return "within_area"
    if "region_clip" in facts:
        reg = _region_of_entry(m.rule[0])
        if reg and not any(r.startswith(reg) for r in b.regions.get(m.sid, ())):
            return "region_clip"
    if m.rule in b.kinds and "stream" not in b.kinds[m.rule]:
        return "feature_types"
    return None


def _subtree(t: Topo, b: Bound, sid: int, rule: Rule) -> int:
    """Unnamed stream sections at and above `sid` that lack `rule` (what the miss costs)."""
    seen, stack, n = {sid}, [sid], 0
    while stack:
        cur = stack.pop()
        if cur in b.unnamed and t.kind.get(cur) == "stream" and rule not in b.rules.get(cur, {}):
            n += 1
        for src, _k in t.up.get(cur, ()):
            if src not in seen and (src in b.unnamed or t.kind.get(src) == "lake"):
                seen.add(src)
                stack.append(src)
    return n


def sweep(t: Topo, b: Bound, examples: int = 3) -> dict:
    """The whole report: frontier pairs by reason, the unexplained by rule and region, examples."""
    misses = frontier(t, b)
    by_reason: Dict[str, int] = collections.Counter()
    explained: Dict[str, Dict[str, int]] = collections.defaultdict(collections.Counter)
    open_: List[Tuple[Miss, int]] = []
    for m in misses:
        why = explain(t, b, m)
        by_reason[why or "unexplained"] += 1
        if why is not None:
            explained[why][f"{m.rule[0]}::{m.rule[1]}"] += 1
        if why is None:
            open_.append((m, _subtree(t, b, m.sid, m.rule)))
    by_rule: Dict[str, dict] = {}
    by_region: Dict[str, int] = collections.Counter()
    for m, cost in open_:
        key = f"{m.rule[0]}::{m.rule[1]}"
        row = by_rule.setdefault(key, {"frontier": 0, "sections": 0, "examples": []})
        row["frontier"] += 1
        row["sections"] += cost
        if len(row["examples"]) < examples:
            row["examples"].append({"sid": m.sid, "below": m.below, "via": m.via, "lake": m.lake})
        by_region[",".join(b.regions.get(m.sid, ())) or "?"] += 1
    return {
        "frontier": len(misses),
        "by_reason": dict(sorted(by_reason.items())),
        "explained": {k: dict(sorted(v.items())) for k, v in sorted(explained.items())},
        "unexplained": sum(1 for _ in open_),
        "unexplained_sections": sum(c for _, c in open_),
        "by_rule": dict(sorted(by_rule.items())),
        "by_region": dict(sorted(by_region.items())),
        "unnamed_lake_at_reach_top": top_lakes(t, b),
    }


def top_lakes(t: Topo, b: Bound) -> Dict[str, int]:
    """Not a miss, REPORTED: an UNNAMED lake at the top of a walking rule's reach (it drains by a
    mainstem edge into a reach section and is not mid-reach) stops the walk, so the unnamed streams
    feeding it do not carry the rule — "X River downstream of Y Lake" does not take Y Lake's inflows,
    and the walk cannot tell a nameless pond from Y Lake. Per rule: the inflows left without it."""
    out: Dict[str, int] = collections.Counter()
    for lake in sorted(b.unnamed_lakes):
        for d, k in t.down.get(lake, ()):
            if k not in MAINSTEM or t.kind.get(d) != "stream":
                continue
            for r, via in sorted(b.rules.get(d, {}).items()):
                if via != "reach" or r not in b.walking or r in b.rules.get(lake, {}) \
                        or _mid_reach(t, b, lake, d, r):
                    continue
                n = sum(1 for u, _k in t.up.get(lake, ()) if u in b.unnamed and r not in b.rules.get(u, {}))
                if n:
                    out[f"{r[0]}::{r[1]}"] += n
    return dict(sorted(out.items()))


#: An MU GROUP is cut first-entry/last-exit with BOTH-SIDES membership (`areas.json` `cut: first_last`,
#: `membership: both_sides`): a piece the cut leaves straddling the group's line is IN the group. So a
#: rule `within` the group binds exactly the group's members of its kinds, and a member is exactly a
#: section stamped with one of the group's MUs (`section_ruleset.mus`) — straddlers included.
def mu_lines(sections: Mapping[int, Tuple[str, Tuple[str, ...]]], members: frozenset, group: frozenset,
             kinds: Optional[frozenset], bound: Set[int]) -> dict:
    """One MU-group rule over `sections` ({sid: (kind, mus)}, the unnamed ones): `members` the area's
    sections, `group` its MUs, `kinds` its `feature_types` (None: every kind), `bound` the sections the
    bundle binds it to. Counts, each a list that should be empty but `straddling` (informational)."""
    out = {"straddling": 0, "straddling_bound": 0, "bound_not_member": 0, "member_not_bound": 0,
           "stamped_not_member": 0, "member_not_stamped": 0}
    for sid, (kind, mus) in sections.items():
        stamped = bool(set(mus) & group)
        member = sid in members
        want = member and (kinds is None or kind in kinds)
        has = sid in bound
        if stamped and not set(mus) <= group:
            out["straddling"] += 1
            out["straddling_bound"] += has
        out["bound_not_member"] += has and not want
        out["member_not_bound"] += want and not has
        out["stamped_not_member"] += stamped and not member
        out["member_not_stamped"] += member and not stamped
    return out


# ---------------------------------------------------------------------------------------------- data

#: `reach.extent.area_sections` takes a REGION's members by home: a stream piece straddling a region
#: line is its home region's only (`registry.regions.in_region`, Mara Lake ruling 2026-09-25).
REGION_PREFIX = "area:region:"


def area_sections(build: Path, area_ids: Iterable[str],
                  home: Optional[Mapping[int, str]] = None) -> Dict[str, frozenset]:
    """Each area item's sections, as handles — the membership a `within_area` resolves to
    (`registry[area].section_ids`; a region's less the straddlers whose `home` is another)."""
    from pipeline.common import section_handles
    _ids, handle = section_handles.read(Path(build))
    want = set(area_ids)
    items = {it["id"]: it for it in json.loads((Path(build) / "registry.json").read_text(encoding="utf-8"))["items"]
             if it["id"] in want}
    home = home or {}
    out: Dict[str, frozenset] = {}
    for a in sorted(want):
        got = (handle[s] for s in items[a]["section_ids"])
        if a.startswith(REGION_PREFIX):
            reg = a[len(REGION_PREFIX):]
            got = (h for h in got if home.get(h, reg) == reg)
        out[a] = frozenset(got)
    return out


def rule_limits(db: sqlite3.Connection) -> Tuple[Dict[Rule, Set[str]], Dict[Rule, frozenset]]:
    """Per rule: the areas its extents' `within_area` / `within` name, and the kinds its extents'
    `feature_types` allow (only rules where EVERY extent states one)."""
    areas: Dict[Rule, Set[str]] = {}
    kinds: Dict[Rule, frozenset] = {}
    for e, r, cond in db.execute("SELECT entry_id, rule_id, conditions FROM rule"):
        exts = (json.loads(cond or "{}") or {}).get("extents") or []
        got = {x.get("within_area") or (x.get("area_id") if x.get("op") == "within" else None)
               for x in exts if isinstance(x, dict)}
        if exts and None not in got:
            areas[(e, r)] = set(got)
        ft = [x.get("feature_types") for x in exts if isinstance(x, dict)]
        if exts and all(ft):
            kinds[(e, r)] = frozenset(k for f in ft for k in f)
    return areas, kinds


def bound_from(bundle: Path, reach_dir: Path, build: Optional[Path] = None) -> Tuple[Bound, dict]:
    """The bundle's binding and the reach run's per-rule facts; and the digests they carry. With
    `build`, each walking rule's `within_area` membership is read from the atlas's registry."""
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    meta = dict(db.execute("SELECT k, v FROM meta"))
    by_set: Dict[int, Dict[Rule, str]] = collections.defaultdict(dict)
    for set_id, e, r, via in db.execute("SELECT set_id, entry_id, rule_id, via FROM ruleset"):
        if not e.startswith("z"):
            by_set[set_id][(e, r)] = via
    places = {p: tuple(regs.split(",")) for p, regs in db.execute("SELECT place, regions FROM region_place")}
    kind = dict(db.execute("SELECT up, kind FROM unnamed_part"))
    rules: Dict[int, Mapping[Rule, str]] = {}
    regions: Dict[int, Tuple[str, ...]] = {}
    unnamed: Set[int] = set()
    lakes: Set[int] = set()
    for sid, set_id, up, place in db.execute("SELECT sid, set_id, up, place FROM section_ruleset"):
        rules[sid] = by_set.get(set_id, {})
        regions[sid] = places.get(place, ())
        if up is not None:
            (unnamed if kind[up] == "stream" else lakes).add(sid)
    walking: Dict[Rule, dict] = {}
    facts: Dict[Rule, Set[str]] = collections.defaultdict(set)
    for line in (reach_dir / "rule_diagnostic.jsonl").read_text(encoding="utf-8").splitlines():
        d = json.loads(line)
        key, p = (d["entry_id"], d["rule_id"]), json.loads(d["payload"])
        if d["kind"] == "tributaries":
            walking[key] = p
        elif d["kind"] == "confluence_cut" and p.get("bounded_at_lake"):
            facts[key].add("stem_ends_at_lake")
        elif d["kind"] == "region_clip":
            facts[key].add("region_clip")
    areas, kinds = rule_limits(db)
    limits: Dict[Rule, frozenset] = {}
    if build is not None:
        wanted = {r: a for r, a in areas.items() if r in walking}
        home = dict(db.execute("SELECT sid, region FROM section_home"))
        members = area_sections(build, {a for v in wanted.values() for a in v}, home)
        limits = {r: frozenset().union(*(members[a] for a in a_s)) for r, a_s in wanted.items()}
    stamps = {k: meta.get(k) for k in ("reach_digest", "section_handles", "rule_ids_sha256")}
    return Bound(rules, unnamed, regions, walking, dict(facts), limits,
                 {r: k for r, k in kinds.items() if r in walking}, lakes), stamps


MU_GROUP = "area:mu_group:"


def mu_groups(areas_json: Path) -> Dict[str, frozenset]:
    """Each `mu_group` area's MUs, by its area id (the slug of its `combine` label), from `areas.json`."""
    out: Dict[str, frozenset] = {}
    for d in json.loads(Path(areas_json).read_text(encoding="utf-8"))["areas"]:
        if d.get("kind") != "mu_group":
            continue
        slug = re.sub(r"[^a-z0-9]+", "_", str(d["combine"]).lower()).strip("_")
        out[MU_GROUP + slug] = frozenset(re.findall(r"'(\d+-\d+)'", d["where"]))
    return out


def mu_sweep(bundle: Path, build: Path, areas_json: Path) -> Dict[str, dict]:
    """`mu_lines` for every rule `within` an MU group, over every unnamed section."""
    db = sqlite3.connect(f"file:{bundle}?mode=ro", uri=True)
    groups = mu_groups(areas_json)
    areas, kinds = rule_limits(db)
    rules = {r: a for r, a in areas.items() if any(x.startswith(MU_GROUP) for x in a)}
    unknown = sorted({x for a in rules.values() for x in a if x.startswith(MU_GROUP)} - set(groups))
    if unknown:
        raise SystemExit(f"MU-group areas with no areas.json def: {unknown}")
    members = area_sections(build, {x for a in rules.values() for x in a})
    codes = {m: tuple(c.split(",")) for m, c in db.execute("SELECT mus, codes FROM mu_set")}
    ukind = dict(db.execute("SELECT up, kind FROM unnamed_part"))
    sections = {sid: (ukind[up], codes.get(mus, ())) for sid, up, mus in
                db.execute("SELECT sid, up, mus FROM section_ruleset WHERE up IS NOT NULL")}
    out: Dict[str, dict] = {}
    for r, a in sorted(rules.items()):
        if len(a) != 1:
            raise SystemExit(f"{r}: an MU-group rule over several areas ({sorted(a)}) is not checked here")
        (area,) = a
        bound = {sid for (sid,) in db.execute(
            "SELECT s.sid FROM section_ruleset s JOIN ruleset r ON r.set_id = s.set_id "
            "WHERE s.up IS NOT NULL AND r.entry_id = ? AND r.rule_id = ?", r)}
        out[f"{r[0]}::{r[1]}"] = mu_lines(sections, members[area], groups[area], kinds.get(r), bound)
    return out


#: The committed result: what the live bundle answered, no section handles (they do not survive a rebuild).
RESULT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "t57_trib_sweep.json"


def summary(report: dict) -> dict:
    """The report without its examples' section handles — what is committed and pinned."""
    out = dict(report)
    out["by_rule"] = {k: {"frontier": v["frontier"], "sections": v["sections"]}
                      for k, v in report.get("by_rule", {}).items()}
    return out


def run(build: Path, bundle: Path, reach: Path, areas_json: Path, t: Optional[Topo] = None) -> dict:
    """The whole sweep on one build: the tributary frontier and the MU-group lines."""
    t = t or topology(Path(build))
    b, stamps = bound_from(Path(bundle), Path(reach), Path(build))
    if stamps["section_handles"] != t.handles_digest:
        raise SystemExit(f"the bundle's section handles ({stamps['section_handles']}) are not the atlas's "
                         f"({t.handles_digest}): not one build")
    return {"bundle": stamps, "walking_rules": len(b.walking), "unnamed_streams": len(b.unnamed),
            **sweep(t, b), "mu_groups": mu_sweep(Path(bundle), Path(build), Path(areas_json))}


def main(argv=None) -> int:
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--build", type=Path, default=None, help="atlas build (default: the live one)")
    ap.add_argument("--bundle", type=Path, default=None)
    ap.add_argument("--reach", type=Path, default=None)
    ap.add_argument("--topo-cache", type=Path, default=None,
                    help="pickle the graph's topology here once and reuse it (scratch only)")
    ap.add_argument("--out", type=Path, default=None, help="the full report (with example handles)")
    ap.add_argument("--write-result", action="store_true",
                    help=f"write the committed summary ({RESULT.name}) the live test pins")
    a = ap.parse_args(argv)
    build = a.build or GENERATED.require_build()
    root = Path(build).parents[1]
    bundle = a.bundle or root / "bundle" / "bundle.sqlite"
    reach = a.reach or root / "reaches" / "full"
    if a.topo_cache and a.topo_cache.exists():
        t = Topo(**pickle.loads(a.topo_cache.read_bytes()))
    else:
        t = topology(Path(build))
        if a.topo_cache:
            a.topo_cache.write_bytes(pickle.dumps(dict(vars(t)), protocol=5))
    from pipeline.common.curated import CURATED
    report = run(Path(build), Path(bundle), Path(reach), Path(CURATED.waters.areas), t)
    if a.out:
        a.out.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    if a.write_result:
        RESULT.write_text(json.dumps(summary(report), indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("frontier", "by_reason", "unexplained",
                                             "unexplained_sections")}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
