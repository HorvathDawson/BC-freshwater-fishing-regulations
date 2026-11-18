"""What the map says, as plain data (T853): the water, its system, its parts and each part's cascade on a date.

Every word comes from the release: rule titles and book pages from the answers' display section (`display.rules`,
aligned with the export's `rule_ids`), today's BC answer from the page's own lookup (`encode.tap`: the display
frame's status, strip and closers, the trout table's top line, the gear summary), end names from `display.ends`, DFO
words and verdicts from the salmon file (a chain's places narrowest first, `a` = which place answers each species on
a day). Nothing here re-derives a rule; it groups and labels what the release decided.

    resolve(rel, "Nass River")          -> Water (or raises Ambiguous with the choices)
    system(rel, water, ...)             -> System: the main water(s), the rule-carrying tributaries, context waters
    units(rel, sys, mode, day)          -> [Unit]: numbered map parts for one mode, each with its panel rows
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from pipeline.common import entry_id as eid

MODES = ("salmon", "freshwater")
SPECIES_NAMES = {"CH": "chinook", "CO": "coho", "SK": "sockeye", "PK": "pink", "CM": "chum"}
#: a rule kind that never says what the angler may do on the water (notices, possession arithmetic)
QUIET_KINDS = frozenset({"duty", "possession", "possession_cap", "standing"})
#: how many rule-carrying tributaries a map numbers before it names the rest in a note (`--max-tribs`)
MAX_TRIBS = 14
#: how many numbered stretches of the main water fit one map; more needs `--focus`
MAX_MAIN_UNITS = 16


class Ambiguous(SystemExit):
    """A name that matches more than one water: carries the choices (`choices`) for the CLI to print."""

    def __init__(self, query: str, choices: Sequence["Water"]):
        self.choices = list(choices)
        lines = [f"  {w.item_id:<20} {w.name} ({w.kind}, region {', '.join(w.regions) or '?'}, "
                 f"{w.sections} sections)" for w in self.choices]
        super().__init__(f"water_map: {query!r} names {len(self.choices)} waters — give an item id:\n" + "\n".join(lines))


class NotFound(SystemExit):
    pass


class TooBig(SystemExit):
    pass


# ------------------------------------------------------------------------------------------------------------ waters
@dataclass(frozen=True)
class Water:
    ord: int
    item_id: str
    name: str
    kind: str
    regions: Tuple[str, ...]
    sections: int
    part_of: Optional[str] = None


def _key(s: str) -> str:
    """A name compared loosely: case, apostrophes and punctuation folded."""
    s = s.lower().replace("’", "'").replace("‘", "'")
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def waters(rel) -> Dict[int, Water]:
    core = rel.db("core")
    tables = {t for (t,) in core.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    # `item_area` is not in a regs fixture's core: regions are then unknown (only words use them)
    regions = ({o: tuple(r.split(",")) for o, r in core.execute("SELECT ord, regions FROM item_area")}
               if "item_area" in tables else {})
    secs = dict(core.execute("SELECT ord, count(*) FROM item_section GROUP BY ord"))
    return {o: Water(o, iid, name, kind or "", regions.get(o, ()), secs.get(o, 0), part_of)
            for o, iid, name, kind, part_of in core.execute("SELECT ord, item_id, name, kind, part_of FROM item")}


def resolve(rel, query: str) -> Water:
    """An item id, or a name: exact (folded) name first, then an alias, then a name containing every word. More than
    one match raises `Ambiguous` listing them; none raises `NotFound`."""
    ws = waters(rel)
    by_id = {w.item_id: w for w in ws.values()}
    q = query.strip()
    if q in by_id:
        return by_id[q]
    k = _key(q)
    hits = [w for w in ws.values() if _key(w.name) == k]
    if not hits:
        core = rel.db("core")
        ids = {iid for iid, a in core.execute("SELECT item_id, alias FROM alias") if _key(a) == k}
        hits = [by_id[i] for i in sorted(ids) if i in by_id]
    if not hits:
        words = k.split()
        hits = [w for w in ws.values() if all(x in _key(w.name).split() for x in words)]
    hits = sorted(hits, key=lambda w: (-w.sections, w.item_id))
    if not hits:
        raise NotFound(f"water_map: no water named {query!r} in release {rel.id}")
    if len(hits) > 1:
        raise Ambiguous(query, hits)
    return hits[0]


# ------------------------------------------------------------------------------------------------------------ parts
@dataclass
class Part:
    water: Water
    ix: int
    sids: List[int]
    km: Optional[Tuple[float, float]]           # (low km, high km) along its own water, mouth = 0
    runs: List[dict]                              # the export's runs: from/to end tokens and km
    key: Optional[int]                            # the answers key (`wire.parts[item][ix]`), None: no rule set
    ruleset: Optional[str]
    licensing_set: Optional[str]
    tidal: bool
    role: str = "main"                            # main | trib | context
    depth: int = 0                                # tributary depth (main 0)
    junction_km: Optional[float] = None           # where its top-level tributary meets the main water (km)
    order: Tuple = ()                             # sort key for numbering

    @property
    def ref(self) -> Tuple[str, int]:
        return (self.water.item_id, self.ix)


def _parts_of(rel, w: Water) -> List[Part]:
    wire, data = rel.answers()
    W = rel.db("water")
    xw = (data["waters"].get(w.item_id) or {})
    xparts = xw.get("parts") or []
    keys = wire["parts"].get(w.item_id) or []
    sids: Dict[int, List[int]] = defaultdict(list)
    for sid, pix in W.execute("SELECT sid, part_ix FROM part_section WHERE ord = ? ORDER BY sid", (w.ord,)):
        sids[pix].append(sid)
    out = []
    for ix, xp in enumerate(xparts):
        runs = list(xp.get("runs") or [])
        kms = [v for r in runs for v in (r.get("km_from"), r.get("km_to")) if v is not None]
        tidal = "tidal" in (xp.get("province_except") or []) and xp.get("ruleset") is None
        out.append(Part(w, ix, sids.get(ix, []), (min(kms), max(kms)) if kms else None, runs,
                        keys[ix] if ix < len(keys) else None, xp.get("ruleset"), xp.get("licensing_set"), tidal))
    return out


# ------------------------------------------------------------------------------------------------------------ system
@dataclass
class System:
    target: Water
    main: List[Water]                       # the target and the waters it goes on as (Vedder -> Chilliwack)
    parts: List[Part]                       # every part drawn and numbered (main + rule-carrying tributaries)
    context: List[Part]                     # parts drawn for context only (a lake's inflow/outflow river)
    left_out: List[Water]                   # rule-carrying tributaries not drawn (the cap), named in a note
    notes: List[str] = field(default_factory=list)
    focus: Optional[Tuple[float, float]] = None


def _continuation(rel, w: Water) -> List[int]:
    """The ords of the watercourse `w` is one piece of (`water_continue`, both ways), mouth first."""
    W = rel.db("water")
    nxt = {o: n for o, _, n, _ in W.execute("SELECT ord, part, next_ord, next_part FROM water_continue")}
    prv = {n: o for o, n in nxt.items()}
    chain = [w.ord]
    while chain[-1] in nxt and nxt[chain[-1]] not in chain:
        chain.append(nxt[chain[-1]])
    while chain[0] in prv and prv[chain[0]] not in chain:
        chain.insert(0, prv[chain[0]])
    # `ord` goes on as `next_ord` DOWNSTREAM (`part` is ord's mouth part, `next_part` next_ord's top part), so the
    # chain runs source to sea; mouth first is its reverse
    return list(reversed(chain))


def _has_own_rows(data: dict, w: Water) -> bool:
    return bool((data["waters"].get(w.item_id) or {}).get("entries"))


def system(rel, target: Water, *, focus: Optional[Tuple[float, float]] = None, max_tribs: int = MAX_TRIBS,
           tribs: bool = True) -> System:
    """The target, the watercourse it is a piece of, and its tributaries (any depth, by `water_exit`) that carry rules
    of their own: a BC row (the export names an entry for the water) or a DFO place its receiver's chains do not have.
    A lake brings the parts of the waters it flows out as or into that lie near it, as context. `focus` (km on the
    main water) keeps the main parts and the tributaries meeting it inside that window."""
    ws = waters(rel)
    wire, data = rel.answers()
    W = rel.db("water")
    lake = target.kind == "lake"
    main_ords = [target.ord] if lake else _continuation(rel, target)
    main = [ws[o] for o in main_ords]
    parts: List[Part] = []
    offset = 0.0
    for w in main:
        ps = _parts_of(rel, w)
        for p in ps:
            if p.km is not None:
                p.km = (p.km[0] + offset, p.km[1] + offset)
        tops = [p.km[1] for p in ps if p.km]
        # a continuation's km keep counting from the first water's mouth (Vedder km 0 -> Chilliwack)
        parts.extend(ps)
        offset = max(tops) if tops else offset
    main_set = set(main_ords)
    notes: List[str] = []
    if focus is not None:
        lo, hi = focus
        kept = [p for p in parts if p.km is None or (p.km[1] > lo and p.km[0] < hi)]
        if not kept:
            raise TooBig(f"water_map: --focus {lo:g}-{hi:g} km holds no part of {target.name}")
        parts = kept
        notes.append(f"Focus: {target.name} km {lo:g}–{hi:g} and the tributaries that meet it there.")
    for p in parts:
        p.order = (0, main_ords.index(p.water.ord), p.km[0] if p.km else 1e9, p.ix)
    kept_main = {(p.water.ord, p.ix) for p in parts}

    # tributaries, by water_exit (ord -> receiver, the receiver's part at the junction)
    exits = defaultdict(list)
    for o, d, dp in W.execute("SELECT ord, down_ord, down_part FROM water_exit ORDER BY ord"):
        exits[d].append((o, dp))
    salmon = rel.salmon()
    chain_of = (lambda sid: salmon[1].get(sid, salmon[0]["default"])) if salmon else None
    chains = salmon[0]["chains"] if salmon else None

    def places(sids: Iterable[int]) -> Set[str]:
        """The DFO places (not the rule-book closures the salmon file mirrors) holding on these sections."""
        if not salmon:
            return set()
        pl = salmon[0].get("p") or {}
        return {loc for s in sids for loc in chains[chain_of(s)]["l"] if not (pl.get(loc) or {}).get("static")}

    left_out: List[Water] = []
    candidates: List[Tuple[Tuple, Water, int, Optional[float]]] = []
    if tribs:
        main_parts_by = {(p.water.ord, p.ix): p for p in parts}
        seen = set(main_set)
        queue = [(o, 0, None) for o in main_ords]
        receiver_places: Dict[int, Set[str]] = {}
        while queue:
            rcv, depth, jkm = queue.pop(0)
            for o, dp in sorted(exits.get(rcv, [])):
                if o in seen or o not in ws:
                    continue
                seen.add(o)
                w = ws[o]
                j = jkm
                if rcv in main_set:
                    mp = main_parts_by.get((rcv, dp))
                    if focus is not None and (rcv, dp) not in kept_main:
                        continue
                    j = mp.km[0] if mp is not None and mp.km else None
                if rcv not in receiver_places:
                    receiver_places[rcv] = places(s for (s,) in rel.db("core").execute(
                        "SELECT sid FROM item_section WHERE ord = ?", (rcv,)))
                own_secs = [s for (s,) in rel.db("core").execute("SELECT sid FROM item_section WHERE ord = ?", (o,))]
                dfo_own = bool(places(own_secs) - receiver_places[rcv]) if salmon else False
                carries = _has_own_rows(data, w) or dfo_own
                if carries:
                    n_rules = len((data["waters"].get(w.item_id) or {}).get("entries") or [])
                    candidates.append(((depth, -int(_has_own_rows(data, w)), -n_rules, -w.sections, w.item_id),
                                       w, depth + 1, j))
                queue.append((o, depth + 1, j))
        candidates.sort(key=lambda c: c[0])
        chosen = candidates[:max_tribs]
        left_out = [c[1] for c in candidates[max_tribs:]]
        for rank, (_, w, depth, j) in enumerate(sorted(chosen, key=lambda c: (c[3] if c[3] is not None else 1e9,
                                                                                 c[2], c[1].name))):
            for p in _parts_of(rel, w):
                p.role, p.depth, p.junction_km = "trib", depth, j
                p.order = (1, rank, p.km[0] if p.km else 1e9, p.ix)
                parts.append(p)
        if left_out:
            notes.append(f"{len(left_out)} more rule-carrying tributar{'y' if len(left_out) == 1 else 'ies'} not "
                         f"drawn (the map numbers {max_tribs}): " + ", ".join(sorted(w.name for w in left_out)[:12])
                         + (" …" if len(left_out) > 12 else "") + ". Use --focus KM-KM or --max-tribs.")

    context: List[Part] = []
    if lake:
        # the waters it goes on as and is fed by, near it: drawn and numbered only where they touch its box
        near: List[int] = []
        nxt = {o: n for o, _, n, _ in W.execute("SELECT ord, part, next_ord, next_part FROM water_continue")}
        for o, n in nxt.items():
            if o == target.ord:
                near.append(n)
            if n == target.ord:
                near.append(o)
        ex = W.execute("SELECT down_ord FROM water_exit WHERE ord = ?", (target.ord,)).fetchone()
        if ex:
            near.append(ex[0])
        from pipeline.tools.water_map.geo import part_boxes, union_box
        near = sorted(set(near) - {p.water.ord for p in parts})
        boxes = part_boxes(rel, [target.ord, *near])
        lb = union_box(b for (o, _), b in boxes.items() if o == target.ord)
        for o in near:
            for p in _parts_of(rel, ws[o]):
                b = boxes.get((o, p.ix))
                if lb is None or b is None:
                    continue
                pad = max(0.6 * max(lb[2] - lb[0], lb[3] - lb[1]), 0.05)
                if b[2] < lb[0] - pad or b[0] > lb[2] + pad or b[3] < lb[1] - pad or b[1] > lb[3] + pad:
                    continue
                p.role = "context"
                p.order = (2, near.index(o), p.km[0] if p.km else 1e9, p.ix)
                context.append(p)
        if context:
            parts.extend(context)
            notes.append("A lake is drawn with the parts of the river it flows out as or into that lie near it.")
    return System(target, main, parts, context, left_out, notes, focus)


# ------------------------------------------------------------------------------------------------------------ words
def end_words(ends: dict, tok: Optional[str]) -> Optional[str]:
    """An end token's words (`display.ends`), the confidence clause folded to "≈" and a gauge's station kept."""
    if not tok:
        return None
    ws = ends.get(tok)
    if not ws:
        return None
    s = ws[0]
    approx = re.search(r",? approximate location( \([^)]*\))?", s)
    if approx:
        s = "≈ " + s[:approx.start()] + s[approx.end():]
    if tok.startswith("gauge__") and len(ws) > 1:
        s += f" (gauge {tok.split('__', 1)[1]})"
    return s


def km_words(km: Optional[Tuple[float, float]]) -> str:
    if km is None:
        return ""
    lo, hi = km
    return f"km {lo:.1f}–{hi:.1f}"


@dataclass
class Today:
    """A unit's answer on the day, for one mode."""
    state: str                       # open | release | closed | closed_all | tidal | check | none
    marks: Tuple[str, ...] = ()      # freshwater secondary marks: bait_ban, fly_only, artificial_fly_only
    words: str = ""                  # the TODAY line


@dataclass
class Unit:
    number: int
    parts: List[Part]
    title: str
    span: str                        # km words or "whole water"
    today: Today
    rows: List[Tuple[str, str]]      # (tag, text): BASE / BC / DFO / TODAY
    role: str = "main"
    where: str = ""                  # the display's words for the stretch

    @property
    def sids(self) -> List[int]:
        return [s for p in self.parts for s in p.sids]


class Reader:
    """The release's answers read for one date: BC (tap) and DFO (salmon chains)."""

    def __init__(self, rel, day: date):
        self.rel = rel
        self.day = day
        self.wire, self.data = rel.answers()
        self.disp = self.wire["sections"]["display"]
        self.rix = {r: i for i, r in enumerate(self.data["rule_ids"])}
        self.ends = self.disp["ends"]
        self.strip = {s["bit"]: s for s in self.disp["strip"]}
        sal = rel.salmon()
        self.salmon = sal[0] if sal else None
        self.chain_of_sid = sal[1] if sal else {}
        if self.salmon:
            from pipeline.deliver import salmon as SA
            self.SA = SA
            self.dix = SA.DAYS.index(day.month * 100 + day.day)
            self.loc_words = self._loc_words()
        self._taps: Dict[int, Optional[dict]] = {}

    # ---- BC
    def tap(self, p: Part) -> Optional[dict]:
        if p.key is None:
            return None
        if p.key not in self._taps:
            from pipeline.deliver.answers import encode as E
            from pipeline.deliver.calendar import WEEKDAYS
            self._taps[p.key] = E.tap_key(self.wire, self.data, p.key, self.day.month, self.day.day,
                                          weekday=WEEKDAYS[self.day.weekday()], inside=False)
        return self._taps[p.key]

    def rule(self, rid: str) -> dict:
        return self.disp["rules"][self.rix[rid]]

    def page(self, rid: str) -> Optional[int]:
        b = self.rule(rid).get("book") or []
        return b[0][0] if b else None

    def rules_of(self, p: Part) -> List[str]:
        rs = self.data["rulesets"].get(str(p.ruleset)) if p.ruleset is not None else None
        return list((rs or {}).get("reach") or [])

    def bc_rows(self, p: Part) -> Tuple[List[Tuple[str, str]], List[str]]:
        """(rows, base rule ids): the water's own rows and the rows it inherits from another water, as (tag, text)
        with book pages; the region-table rules that reach it (its base) returned apart."""
        own_entries = set((self.data["waters"].get(p.water.item_id) or {}).get("entries") or [])
        own: Dict[str, List[str]] = defaultdict(list)
        inh: Dict[str, List[str]] = defaultdict(list)
        base: List[str] = []
        for rid in self.rules_of(p):
            entry = rid.split("::", 1)[0]
            r = self.rule(rid)
            if eid.is_province(entry):
                continue
            if eid.is_zone(entry):
                if r.get("kind") not in QUIET_KINDS:
                    base.append(rid)
                continue
            if r.get("kind") in QUIET_KINDS and r.get("kind") != "duty":
                continue
            (own if entry in own_entries else inh)[entry].append(rid)
        rows: List[Tuple[str, str]] = []
        for entry in sorted(own):
            rows.append(("BC", f"Own row{self._pages(own[entry])}: " + self._titles(own[entry])))
        for entry in sorted(inh):
            name = (self.data["entries"].get(entry) or {}).get("name") or entry
            rows.append(("BC", f"From the {name} row{self._pages(inh[entry])}: " + self._titles(inh[entry])))
        return rows, base

    def _pages(self, rids: Sequence[str]) -> str:
        ps = sorted({x for x in (self.page(r) for r in rids) if x})
        return f" p.{', '.join(map(str, ps))}" if ps else ""

    def _titles(self, rids: Sequence[str]) -> str:
        seen, out = set(), []
        for r in rids:
            t = self.rule(r)["title"].rstrip(".")
            if t not in seen:
                seen.add(t)
                out.append(t)
        return "; ".join(out) + "."

    def bc_today(self, p: Part) -> Today:
        if p.tidal:
            guide = ((self.data["waters"].get(p.water.item_id) or {}).get("tidal") or {}).get("guide")
            return Today("tidal", (), "Tidal water: B.C. freshwater rules do not apply (federal tidal rules)."
                         if not guide else guide.split(".")[0] + ".")
        t = self.tap(p)
        if t is None:
            return Today("none", (), "No rule set in the release for this part.")
        d = t["display"]
        if d.get("status") == "tidal":
            return Today("tidal", (), "Tidal water: B.C. freshwater rules do not apply.")
        bits = d.get("strip") or 0
        marks = tuple(self.strip[b]["state"] for b in (4, 8, 16) if bits & b)
        if d.get("status") == "closed" or bits & 1:
            closers = d.get("closers") or [c[0] for c in d.get("closing") or []]
            why = "; ".join(dict.fromkeys(self.disp["rules"][i]["title"] for i in closers[:2]))
            return Today("closed_all", (), "CLOSED to all fishing" + (f" ({why})" if why else "") + ".")
        state = "release" if bits & 2 else "open"
        words = ["Open"]
        if bits & 2:
            words = ["Open, release every game fish"]
        words += [self.strip[b]["label"] for b in (4, 8, 16) if bits & b]
        tr = ((t.get("trout") or {}).get("table") or {}).get("top") or {}
        lead = (tr.get("lead") or "").replace("{day}", "today").strip()
        if lead:
            words.append(lead[0].lower() + lead[1:] if not lead.startswith(("Keep", "Release")) else lead)
        return Today(state, marks, " · ".join(words) + ".")

    def gear_words(self, p: Part) -> Optional[str]:
        t = self.tap(p)
        s = (((t or {}).get("gear") or {}).get("summary") or {}).get("says")
        return s if s and s.strip(" .") else None

    def trout_lead(self, p: Part) -> Optional[str]:
        t = self.tap(p)
        tr = ((((t or {}).get("trout") or {}).get("table") or {}).get("top") or {})
        lead = (tr.get("lead") or "").replace("{day}", "today").strip()
        return lead or None

    # ---- DFO
    def _loc_words(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for c in self.salmon["chains"]:
            if c.get("l") and c.get("w") and c["l"][0] not in out:
                out[c["l"][0]] = c["w"]
        for u in self.salmon.get("unmapped") or []:
            ch = self.salmon["chains"][u["chain"]]
            if ch.get("l") and u.get("w"):
                out.setdefault(ch["l"][0], u["w"])
        return out

    def place_words(self, loc: str) -> str:
        p = (self.salmon.get("p") or {}).get(loc) or {}
        if p.get("static") and p.get("book"):
            rid = p["book"].get("rule")
            title = self.rule(rid)["title"] if rid in self.rix else "closure"
            pages = p["book"].get("pages") or []
            return f"Rule book{(' p.' + ', '.join(map(str, pages))) if pages else ''}: {title}"
        w = self.loc_words.get(loc)
        if w:
            a, sep, b = w.partition(" — ")
            return a if sep and a.strip() == b.strip() else w
        if p.get("water") and loc.count(":") >= 2:
            tail = loc.split(":", 2)[2].split("~")[0].replace("-", " ")
            return f"{p['water']} — {tail}…"
        if p.get("sec"):
            return f"Region {p.get('r')} Section {p['sec']} default"
        return loc

    def chain_index(self, sids: Sequence[int]) -> Optional[int]:
        if not self.salmon:
            return None
        counts: Dict[int, int] = defaultdict(int)
        for s in sids:
            counts[self.chain_of_sid.get(s, self.salmon["default"])] += 1
        return max(sorted(counts), key=lambda c: counts[c]) if counts else self.salmon["default"]

    @staticmethod
    def at(runs: Sequence[int], d: int) -> Optional[int]:
        v = None
        for i in range(0, len(runs), 2):
            if runs[i] <= d:
                v = runs[i + 1]
            else:
                break
        return v

    def dfo(self, c: int, tags: Optional[Dict[str, str]] = None) -> Tuple[str, List[Tuple[str, str, str, str]], bool]:
        """(map status, [(species words, verdict words + who answers)], all-fishing closed) for chain `c` today."""
        ch = self.salmon["chains"][c]
        m = self.SA.MAP_CODES[self.at(ch["m"], self.dix)]
        groups: Dict[Tuple[str, str], List[str]] = {}
        closed_all = False
        for k, sp in enumerate(self.SA.SPECIES):
            v = self.salmon["verdicts"][self.at(ch["sp"][k], self.dix)]
            closed_all |= bool(v.get("f"))
            who = ""
            if ch.get("a"):
                a = self.at(ch["a"][k], self.dix)
                if a is not None and a >= 0 and a < len(ch["l"]):
                    who = ch["l"][a]
            groups.setdefault((v.get("s") or "closed", v.get("t") or "", who), []).append(SPECIES_NAMES[sp])
        tags = tags or {}
        out = [(", ".join(sps), st, t, (tags.get(who) or self.short_place(who)) if who else "")
               for (st, t, who), sps in groups.items()]
        return m, out, closed_all

    def short_place(self, loc: str) -> str:
        p = (self.salmon.get("p") or {}).get(loc) or {}
        if p.get("static"):
            return "rule book"
        if p.get("water"):
            return f"{p['water']} row"
        if p.get("sec"):
            return f"Section {p['sec']}"
        return "DFO"

    def gear_today(self, c: int) -> List[str]:
        ch = self.salmon["chains"][c]
        g = self.at(ch["g"], self.dix)
        return list(self.salmon["gears"][g]) if g is not None and g >= 0 else []


# ------------------------------------------------------------------------------------------------------------ units
def _part_signature(r: Reader, p: Part, mode: str) -> Tuple:
    if p.tidal:
        return ("tidal",)
    if mode == "freshwater":
        return ("key", p.key)
    c = r.chain_index(p.sids)
    closed = r.bc_today(p).state == "closed_all"
    return ("chain", c, closed)


def _join_spans(parts: Sequence[Part]) -> str:
    kms = [p.km for p in parts if p.km]
    if not kms:
        return "whole water" if all(p.water.kind == "lake" for p in parts) else ""
    kms.sort()
    merged: List[List[float]] = []
    for lo, hi in kms:
        if merged and lo <= merged[-1][1] + 0.05:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return " and ".join(f"km {lo:.1f}–{hi:.1f}" for lo, hi in merged)


APPROX = re.compile(r",? approximate location \((?:\w+) confidence\)")


def fold_approx(s: str) -> str:
    """The display's confidence clause, shortened for a map ("…bridge (approx.)")."""
    return APPROX.sub(" (approx.)", s or "")


def _where(r: Reader, ps: Sequence[Part], whole: bool, mode: str) -> Tuple[str, str]:
    """(title, where): the water's name (and the place it is also known as), and the display's own words for the
    stretch — a part's `runs` ("From the mouth up to the Nass Road Bridge"), the picker group's `label` where a
    freshwater unit is the whole group, nothing for a whole water."""
    w = ps[0].water
    dw = (r.disp["waters"].get(w.item_id) or {}).get("parts") or []
    dp = [dw[p.ix] if p.ix < len(dw) else None for p in ps]
    place = next((d.get("place") for d in dp if d and d.get("place")), None)
    title = w.name
    lead = f"{place} · " if place else ""
    if whole:
        return title, place or ""
    labels = {d.get("label") for d in dp if d}
    if len(ps) > 1 and mode == "freshwater" and len(labels) == 1 and None not in labels:
        lab = fold_approx(labels.pop())
        return title, lab if place and lab.startswith(place) else lead + lab
    runs = [fold_approx(d.get("runs") or "") for d in dp if d and d.get("runs")]
    return title, lead + "; ".join(runs)


_DATES = re.compile(r"\(([^()]*\d[^()]*)\)\s*$")
ALL_YEAR = "Apr 1–Mar 31"


def _species_words(verdicts: Sequence[Tuple[str, str, str, str]]) -> str:
    """DFO's verdicts for the day, grouped: an opening says DFO's own words; a closure names its dates (when it is not
    all year) and which place closes it."""
    out, shut = [], []
    for sps, state, t, who in verdicts:
        if state in ("open", "release", "check"):
            out.append(f"{sps.capitalize()}: {t}" + (f" [{who}]" if who else ""))
        else:
            m = _DATES.search(t or "")
            when = m.group(1) if m and m.group(1) != ALL_YEAR else ""
            shut.append(sps + (f", {when}" if when else "") + (f" [{who}]" if who else ""))
    if shut:
        out.append("closed: " + "; ".join(shut))
    return "; ".join(out)


def units(r: Reader, sys: System, mode: str) -> Tuple[List[Unit], List[Tuple[str, str]]]:
    """The numbered units of one mode — a water's parts joined where they read the same for that mode — and the
    BASE rows (region tables, DFO defaults) said once above the panels."""
    if mode not in MODES:
        raise ValueError(mode)
    if mode == "salmon" and not r.salmon:
        raise SystemExit(f"water_map: release {r.rel.id} has no salmon file — use --mode freshwater")
    groups: Dict[Tuple, List[Part]] = {}
    for p in sorted(sys.parts, key=lambda p: p.order):
        if p.key is None and not p.tidal:
            continue            # no rule set: outside B.C. (drawn as context, never numbered)
        groups.setdefault((p.order[0], p.water.ord, _part_signature(r, p, mode)), []).append(p)
    ordered = sorted(groups.values(), key=lambda ps: ps[0].order)
    main_units = sum(1 for ps in ordered if ps[0].role == "main")
    if main_units > MAX_MAIN_UNITS and sys.focus is None:
        lines = [f"  part {p.ix}: {km_words(p.km) or 'no km'}" for p in sys.parts if p.role == "main"]
        raise TooBig(f"water_map: {sys.target.name} has {main_units} {mode} stretches — more than one map numbers "
                     f"({MAX_MAIN_UNITS}). Pick a reach with --focus KM-KM:\n" + "\n".join(lines))
    out: List[Unit] = []
    base_rules: Dict[str, None] = {}
    dfo_defaults: Dict[str, None] = {}
    pl = (r.salmon or {}).get("p") or {}
    defaults: Set[str] = set()
    if mode == "salmon":
        # a DFO place is a DEFAULT here (said once, in the base box) when it is a section or region default, or holds
        # on most of the drawn units (a region-wide "waters flowing into Areas 28 and 29")
        held = [set(r.salmon["chains"][r.chain_index([s for p in ps for s in p.sids])]["l"])
                for ps in ordered if not ps[0].tidal]
        for loc in sorted(set().union(*held) if held else set()):
            if (pl.get(loc) or {}).get("static"):
                continue
            if not (pl.get(loc) or {}).get("water") or (len(held) >= 4 and sum(loc in h for h in held) > 0.6 * len(held)):
                defaults.add(loc)
    for n, ps in enumerate(ordered, start=1):
        p0 = ps[0]
        whole = len(ps) == len([q for q in sys.parts if q.water.ord == p0.water.ord]) and p0.role != "context"
        rows: List[Tuple[str, str]] = []
        bc_rows, base = r.bc_rows(p0)
        for b in base:
            base_rules[b] = None
        today_bc = r.bc_today(p0)
        if p0.tidal:
            if mode == "salmon":
                rows.append(("BC", today_bc.words))
        else:
            rows += bc_rows or [("BC", "No row of its own: the region table (above) holds here.")]
        if mode == "freshwater":
            if not p0.tidal and today_bc.state not in ("closed_all", "none"):
                g = r.gear_words(p0)
                if g:
                    rows.append(("GEAR", g))
            today = today_bc
            rows.append(("TODAY", today.words))
        else:
            c = r.chain_index([s for p in ps for s in p.sids])
            ch = r.salmon["chains"][c]
            if p0.tidal:
                today = Today("tidal", (), "Tidal: DFO tidal-water rules apply (not in the freshwater salmon file).")
                rows.append(("TODAY", today.words))
            else:
                own = [loc for loc in ch["l"] if loc not in defaults and not (pl.get(loc) or {}).get("static")]
                for loc in ch["l"]:
                    if loc in defaults:
                        dfo_defaults[loc] = None
                tag = {loc: f"row {i}" for i, loc in enumerate(own, start=1)}
                tag.update({loc: "default" for loc in ch["l"] if loc in defaults})
                if own:
                    lim = 120 if len(own) <= 2 else 90
                    said, shown = None, []
                    for loc in own:
                        words = r.place_words(loc)
                        head, sep, tail = words.partition(" — ")
                        # a water named once: its other places say only their area
                        shown.append(tail if sep and head == said else words)
                        said = head if sep else None
                    rows.append(("DFO", "  ".join(f"({i}) {_clip(x, lim)}" for i, x in enumerate(shown, start=1))
                                 + "  then the defaults above."))
                elif ch["l"]:
                    rows.append(("DFO", "No row of its own: the defaults above."))
                else:
                    rows.append(("DFO", "DFO lists no place here: closed to salmon unless DFO opens it."))
                m, verdicts, closed_all = r.dfo(c, tag)
                state = {"open": "open", "release": "release", "closed": "closed", "check": "check",
                         "none": "closed"}[m]
                if closed_all or today_bc.state == "closed_all":
                    state = "closed_all"
                head = {"open": "SALMON OPEN", "release": "Salmon catch and release", "closed": "Salmon closed",
                        "check": "Salmon: DFO season unreadable", "closed_all": "CLOSED to all fishing"}[state]
                gear = r.gear_today(c)
                words = f"{head} — {_species_words(verdicts)}"
                if today_bc.state == "closed_all":
                    words = today_bc.words + " Salmon: " + _species_words(verdicts)
                today = Today(state, (), words + (f" Gear: {'; '.join(gear)}." if gear else ""))
                rows.append(("TODAY", today.words))
        title, where = _where(r, ps, whole, mode)
        span = _join_spans(ps) or ("whole lake" if p0.water.kind == "lake" else "")
        out.append(Unit(n, ps, title, span, today, rows, p0.role, where))
    base_rows: List[Tuple[str, str]] = []
    # one gear line most parts share is said once, above the panels
    gears = [t for u in out for tag, t in u.rows if tag == "GEAR"]
    common = max(sorted(set(gears)), key=gears.count) if gears else None
    if common and gears.count(common) >= max(2, 0.6 * len(out)):
        for u in out:
            u.rows = [(tag, t) for tag, t in u.rows if not (tag == "GEAR" and t == common)]
        base_rows.append(("GEAR", f"Every part unless its panel says otherwise: {common}"))
    if base_rules:
        by_entry: Dict[str, List[str]] = defaultdict(list)
        for rid in base_rules:
            by_entry[rid.split("::", 1)[0].split(":", 1)[0]].append(rid)
        for z, rids in sorted(by_entry.items()):
            region = z[1:].upper()
            base_rows.append((f"BC R{region}", f"Region {region} table{r._pages(rids)}: " + r._titles(rids)))
    if mode == "salmon" and dfo_defaults:
        for loc in dfo_defaults:
            base_rows.append(("DFO", _clip(r.place_words(loc), 220)))
    return out, base_rows


def _clip(s: str, n: int) -> str:
    s = " ".join(s.split())
    return s if len(s) <= n else s[:n - 1].rstrip() + "…"


# ------------------------------------------------------------------------------------------------------------ breaks
@dataclass
class Break:
    lon: float
    lat: float
    words: str
    km: Optional[float]
    water: Water
    down_part: int
    up_parts: Tuple[int, ...]


def breaks(r: Reader, sys: System, us: Sequence[Unit]) -> List[Break]:
    """The part marks (`part_mark`) where the unit changes for this mode: its end words and km from the mouth."""
    unit_of = {(p.water.ord, p.ix): u.number for u in us for p in u.parts}
    W = r.rel.db("water")
    out = []
    drawn = {}
    for p in sys.parts:
        drawn.setdefault(p.water.ord, p.water)
    offsets = _km_offsets(sys)
    for o, w in sorted(drawn.items()):
        km_of = {}
        for p in sys.parts:
            if p.water.ord != o:
                continue
            for run in p.runs:
                if run.get("from") and run.get("km_from") is not None:
                    km_of.setdefault(run["from"], run["km_from"])
                if run.get("to") and run.get("km_to") is not None:
                    km_of.setdefault(run["to"], run["km_to"])
        for (_, mix, lon, lat, tok, dp, ups, conf, alt) in W.execute(
                "SELECT ord, mark_ix, lon, lat, token, down_part, up_parts, confidence, alt FROM part_mark "
                "WHERE ord = ? ORDER BY mark_ix", (o,)):
            up = tuple(json.loads(ups))
            a = unit_of.get((o, dp))
            bs = {unit_of.get((o, x)) for x in up}
            if a is None and not (bs - {None}):
                continue
            if bs == {a}:
                continue
            words = end_words(r.ends, tok) or (json.loads(alt)[0] if alt else "break")
            km = km_of.get(tok)
            if km is not None:
                km += offsets.get(o, 0.0)
            out.append(Break(lon / 1e6, lat / 1e6, words, km, w, dp, up))
    return out


def _km_offsets(sys: System) -> Dict[int, float]:
    """A continuation's km offset (its own km 0 is where the water below it ends)."""
    off, acc = {}, 0.0
    for w in sys.main:
        off[w.ord] = acc
        tops = [p.km[1] - acc for p in sys.parts if p.water.ord == w.ord and p.km]
        acc += max(tops) if tops else 0.0
    return off


def continuation_breaks(r: Reader, sys: System, us: Sequence[Unit]) -> List[Tuple[Break, Part, Part]]:
    """Where the watercourse changes name (Vedder -> Chilliwack) and the answer changes there: the lower water's top
    part's upper end (its words), at the km the lower water ends. No mark stands there (`part_mark` is per water), so
    the caller places it where the two parts' lines meet; lon/lat are left 0 here."""
    unit_of = {(p.water.ord, p.ix): u.number for u in us for p in u.parts}
    out = []
    for lo_w, hi_w in zip(sys.main, sys.main[1:]):
        lows = [p for p in sys.parts if p.water.ord == lo_w.ord and p.km]
        highs = [p for p in sys.parts if p.water.ord == hi_w.ord and p.km]
        if not lows or not highs:
            continue
        top = max(lows, key=lambda p: p.km[1])
        bottom = min(highs, key=lambda p: p.km[0])
        if unit_of.get((top.water.ord, top.ix)) == unit_of.get((bottom.water.ord, bottom.ix)):
            continue
        runs = [run for run in top.runs if run.get("km_from") is not None]
        tok = max(runs, key=lambda run: run["km_from"]).get("from") if runs else None
        words = end_words(r.ends, tok) or f"{lo_w.name} becomes {hi_w.name}"
        out.append((Break(0.0, 0.0, words, top.km[1], lo_w, top.ix, (bottom.ix,)), top, bottom))
    return out
