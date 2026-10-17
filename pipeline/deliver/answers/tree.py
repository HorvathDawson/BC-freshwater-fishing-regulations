"""THE CARD AS A TREE (ROUND18, owner review of v19, 2026-10-17) — `group.tree` and `group.header`.

The owner reads a quota card as FOLDERS: tier lines on the left, each holding what it limits — the region's
total, then a water's own rule or the stream share, then a count over some kinds or a size cap, and the kinds
inside. This module re-reads the card `card.card_of` has already worded and the gate proves (its buckets,
count groups, size groups and notes) as that tree; it decides nothing new.

  tier   {node: "tier", id, tier, n, says, by, children, …}
         total  the group's total ("4 a day across Region 2, lakes included"; a water's own total by whose it is)
         own    a water's own rule's bucket ("This river’s rule: up to 4 a day"), with `whose`, `replaces`
                ("Replaces Region 2’s 2-from-streams limit and 30 cm / 12 in minimum size for these fish"),
                `why_here` when the rule reached this water by the TRIBUTARY WALK ("its row covers its
                tributaries, so it applies here", ROUND18 8) and `line` (all three in one line); under the
                share where it holds share kinds to a smaller number, else under the total
         share  the stream (lake) share ("2 a day from this stream")
         count  a count over some of a tier's kinds ("1 between them")
         size   a size cap holding the kinds wholly over its size ("1 over 50 cm / 20 in"); RULINGS Q16's pair
                (1 over 50 cm, 2 with a steelhead) is ONE tier: "1 over 50 cm / 20 in — a second may be a
                hatchery steelhead" (`extra` {n, species}); a cap with no kind wholly over its size is no
                tier, only the kinds' `also_counts`
  kind   {node: "kind", species, name, n, says, detail, sizes {cm, in}, also_counts, duties, sentence}: each
         kind EXACTLY ONCE, under the tightest tier holding it; `says` "Up to 2 may be brook, brown or
         cutthroat trout", `detail` its origin and sizes ("hatchery, 30 cm / 12 in or longer"), `also_counts`
         every cap the tier does not hold that still binds it, in words — "only 1 over 50 cm / 20 in, if you
         haven’t already kept a Dolly Varden/bull trout or lake trout over 50 cm" — and `sentence` the whole
         line: "Of the 2 a day: up to 2 may be … (…) — only 1 over 50 cm / 20 in, …".
  header {says, tag}: "2 a day from this stream · 4 across Region 2, lakes included", tag "hatchery only".

Sizes are said in cm AND inches, the inch the nearest whole one (`common.inches`). Every number is proved by
the card gate (`card_check.check_tree`).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common

INF = math.inf


@dataclass
class Ctx:
    """What `card.card_of` knows when the card is worded."""
    grp: dict
    buckets: List[dict]
    info: Dict[str, dict]
    side: Dict[int, dict]
    share: Optional[dict]
    unlimited: bool
    head: dict
    origin_one: Optional[str]
    lifting: List[dict]
    what: str


def _js(v) -> str:
    from pipeline.deliver.answers.rows import _js as js
    return js(v)


def _num(v):
    if v is None or v == INF:
        return None
    return int(v) if float(v).is_integer() else v


def kind_names(species: Sequence[str], steelhead: bool = False, conj: str = " or ") -> str:
    """A kind row's fish in a sentence: a closed group by its name ("whitefish"); fish that are all "<x> trout"
    share the noun ("brook, brown or cutthroat trout"); else each named ("Dolly Varden/bull trout or lake
    trout"); the steelhead "steelhead"."""
    if steelhead:
        return "steelhead"
    g = common.group_for(species)
    if g:
        return common.lc(g)
    names = [common.lc(common.sp_name(c)) for c in species]
    if len(names) > 1 and all(n.endswith(" trout") and "/" not in n and len(n.split()) == 2 for n in names):
        return common.join([n[:-len(" trout")] for n in names], conj) + " trout"
    return common.join(names, conj)


def _short(t: dict) -> str:
    """What a kind's sentence says it is "of": "the 2 a day", "the 1 between them", "the 1 over 50 cm / 20 in"."""
    if t["tier"] == "count":
        return f"{_js(t['n'])} between them"
    if t["tier"] == "size":
        return f"{'none' if not t['n'] else _js(t['n'])} over {common.cm_in(t['over_cm'])}"
    return "no daily limit" if t["n"] is None else f"{_js(t['n'])} a day"


def _kind_says(k: dict, parent_n) -> str:
    names = kind_names(k["species"], k["name"] == "Steelhead")
    if k["n"] == 0:
        return f"No {names} may be kept"
    if k["n"] is None:
        return f"Any number may be {names}"
    if parent_n is not None and k["n"] < parent_n:
        return f"Only {_js(k['n'])} may be {names}"
    return f"Up to {_js(k['n'])} may be {names}"


def _size_also(kn, n, cm, others: List[str], then: List[dict], some: Optional[str] = None) -> Optional[str]:
    """A size cap a kind's tier does not hold, said on the kind (ROUND18 7); `some` names the fish of the kind row
    it holds where it holds only some of them ("only 1 cutthroat trout over 50 cm / 20 in")."""
    over = f"{some} over {common.cm_in(cm)}" if some else f"over {common.cm_in(cm)}"
    nw = "none" if not n else f"only {_js(n)}"
    o = kind_names(others) if others else ""
    if (INF if kn is None else kn) > n:
        s = f"{nw} {over}"
        for t in then:
            s += f", and {'none' if not t['n'] else 'only ' + _js(t['n'])} over {common.cm_in(t['over_cm'])}"
        if others and n:
            s += (f", if you haven’t already kept a {o} over {_js(cm)} cm" if n == 1
                  else f", counting any {o} over {_js(cm)} cm")
        return s
    if others and n:
        return (f"none {over} if you’ve already kept a {o} over {_js(cm)} cm" if n == 1
                else f"no more than {_js(n)} {over}, counting any {o} over {_js(cm)} cm")
    return None


def _count_also(kn, n, mine: List[str], others: List[str], some: bool = False) -> Optional[str]:
    """A count over some kinds a kind's tier does not hold, said on the kind (`some`: it holds only `mine` of the
    kind row's fish)."""
    if some:
        return f"only {_js(n)} may be {kind_names(mine)}" + (f", counting any {kind_names(others)} you keep"
                                                            if others else "")
    if others:
        return f"no more than {_js(n)} counting any {kind_names(others)} you keep"
    if (INF if kn is None else kn) > n:
        return f"only {_js(n)} may be {kind_names(mine)}"
    return None


def _why_here(L) -> Optional[str]:
    """ROUND18 8 (owner): why a water's rule applies on this water where the TRIBUTARY WALK brought it."""
    if L is None or L.via != "trib":
        return None
    return ("its row prints it for its tributaries, so it applies here" if L.f.get("tributaries_only")
            else "its row covers its tributaries, so it applies here")


def tree_of(P, model: dict, c: Ctx) -> Tuple[List[dict], dict]:
    from pipeline.deliver.answers.card import _replaces, book_order, kind_rows
    from pipeline.deliver.answers.rows import badge_of
    grp = c.grp
    T = P.by_key[grp["pool"]]
    total = grp["total"]
    zone = badge_of(T) in ("region", "bc")
    scope = T.x.get("entry_name") or ("B.C." if badge_of(T) == "bc" else "the region")
    wk = c.share["water"] if c.share is not None else ("stream" if P.kind == "stream" else "lake")
    other = "lakes" if wk == "stream" else "streams"
    head_n = c.head.get("n")
    said_total = not c.unlimited and (c.share is not None or (head_n is not None and head_n < total))
    seq = [0]

    def tier(kind: str, n, says: str, by, **kw) -> dict:
        seq[0] += 1
        return {"node": "tier", "id": f"t{seq[0]}", "tier": kind, "n": _num(n), "says": says, "by": by,
                **kw, "children": []}

    # -- the total ---------------------------------------------------------------------------------------
    if c.unlimited:
        root = tier("total", None, c.head["says"], grp["pool"])
    elif zone:
        root = tier("total", total, f"{_js(total)} a day across {scope}"
                    + (f", {other} included" if said_total else ""), grp["pool"])
    else:
        # a water's (an area's) own total: whose it is, and why it applies here where the walk brought it
        from pipeline.deliver.answers.card import _whose
        whose = common.cap(_whose(P, T)[1])
        why = _why_here(T)
        root = tier("total", total, f"{whose}: up to {_js(total)} a day", grp["pool"], whose=whose, why_here=why,
                    line=whose + (f" ({why})" if why else ""))

    # -- the kinds ---------------------------------------------------------------------------------------
    node_of: Dict[int, dict] = {}
    for b in c.buckets:
        for k in kind_rows(b):
            sd = c.side.get(id(k)) or {}
            segs = sd.get("segs") or []
            kn = {"node": "kind", "species": list(k["species"]), "name": k["name"], "n": k["n"],
                  "says": "", "detail": sd.get("detail") or k["detail"],
                  "sizes": {"cm": [[_num(a), _num(b_)] for a, b_ in segs],
                            "in": [[common.inches(a), None if b_ == INF else common.inches(b_)] for a, b_ in segs]},
                  "also_counts": [], "duties": list(k["duties"]), "sentence": ""}
            if k.get("presence"):
                kn["presence"] = k["presence"]
            node_of[id(k)] = kn
            kn["_lo"] = min((a for a, _b in segs), default=None)
            kn["_origins"] = list(sd.get("origins") or [])

    def wholly_over(kn: dict, cm) -> bool:
        return kn["n"] != 0 and kn["_lo"] is not None and kn["_lo"] >= cm

    # -- a tier per bucket ---------------------------------------------------------------------------------
    share_t = None
    own_ts: List[Tuple[dict, dict]] = []
    placed_in: Dict[int, dict] = {}              # kind node id -> its tier
    tier_of: Dict[int, dict] = {}                # bucket -> its tier (the total for the rest)
    for b in c.buckets:
        if b["why"] == "rule":
            why_here = _why_here(P.by_key.get(b["by"]))
            rep = None
            if b.get("replaces") is not None:
                lifters = {Lx["by"] for Lx in (c.share or {}).get("lifted", [])
                           if P.by_key[Lx["by"]].entry_id == P.by_key[b["by"]].entry_id}
                got = _replaces(P, model, [S for k in kind_rows(b) for S in k["species"]], lifters, inch=True)
                rep = got["says"] if got else None
            whose = b["title"]
            t = tier("own", b["cap"], f"{whose}: " + ("any number" if b["cap"] is None else f"up to {_js(b['cap'])} a day"),
                     b["by"], whose=whose, why_here=why_here, replaces=rep,
                     line=whose + (f" ({why_here})" if why_here else "") + (f" · {rep}" if rep else ""))
            own_ts.append((t, b))
        elif b["why"] == "share":
            t = share_t = tier("share", b["cap"], f"{_js(b['cap'])} a day from this {c.share['water']}", b["by"])
        else:
            t = root
        content: List[dict] = []
        grouped = {id(k) for s in b.get("groups") or [] for k in s["kinds"]}
        for k in b["kinds"]:
            if id(k) not in grouped:
                content.append(node_of[id(k)])
                placed_in[id(node_of[id(k)])] = t
        for s in b.get("groups") or []:
            ct = tier("count", s["n"], f"{_js(s['n'])} between them", s["by"])
            for k in s["kinds"]:
                ct["children"].append(node_of[id(k)])
                placed_in[id(node_of[id(k)])] = ct
            content.append(ct)
        t["children"].extend(content)
        tier_of[id(b)] = t

    # -- nesting: a water's own rule under the share where it holds share kinds to fewer, else the total --------
    under_share, under_total = [], []
    for t, b in own_ts:
        ks = {S for k in kind_rows(b) for S in k["species"]}
        if share_t is not None and b not in c.lifting and c.share is not None and ks <= set(c.share["of"]) \
                and t["n"] is not None and share_t["n"] is not None and t["n"] < share_t["n"]:
            under_share.append(t)
        else:
            under_total.append(t)
    if share_t is not None:
        share_t["children"][:0] = under_share
    root["children"][:0] = under_total + ([share_t] if share_t is not None else [])

    # -- size caps: a tier holding the kinds wholly over its size, in the tier holding every kind it names (the
    # total where it spans tiers), when it limits them below that tier's number; its other kinds say it
    for b in c.buckets:
        for sz in b.get("sizes") or []:
            cm = sz["over_cm"]
            holders = [x for x in c.buckets if any(set(k["species"]) & set(sz["species"]) for k in kind_rows(x))]
            t = tier_of[id(holders[0])] if len(holders) == 1 else root
            content = t["children"]
            kids = [x for x in content if x["node"] == "kind" and set(x["species"]) <= set(sz["species"])
                    and wholly_over(x, cm)]
            extra = None
            if sz["not_counting"] == ["ST"]:
                st = next((x for x in content if x["node"] == "kind" and x["species"] == ["ST"]
                           and wholly_over(x, cm)), None)
                if st is not None:
                    takes = [s["take"] for s in grp["subcaps"] if s["over_cm"] == cm
                             and set(sz["species"]) | {"ST"} <= set(s["of"])]
                    room = min(takes + [INF if t["n"] is None else t["n"], INF if st["n"] is None else st["n"]])
                    if room != INF and room - sz["n"] >= 1:
                        extra = {"n": _num(room - sz["n"]), "species": ["ST"]}
                        kids.append(st)
            if not kids or (extra is None and t["n"] is not None and sz["n"] >= t["n"]):
                continue
            says = f"{'None' if not sz['n'] else _js(sz['n'])} over {common.cm_in(cm)}"
            for th in sz.get("then") or []:
                says += f", and {'none' if not th['n'] else _js(th['n'])} over {common.cm_in(th['over_cm'])}"
            if extra is not None:
                # RULINGS Q16 (Region 1/2): "no more than 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)"
                os_ = st["_origins"]
                who = f"{os_[0]} steelhead" if len(os_) == 1 else "steelhead"
                if extra["n"] == 1:
                    lead = "a second" if sz["n"] == 1 else ("one" if not sz["n"] else "one more")
                    says += f" — {lead} may be a {who}"
                else:
                    says += f" — {_js(extra['n'])} more may be {who}"
            st_ = tier("size", sz["n"], says, sz["by"], over_cm=_num(cm), species=list(sz["species"]),
                       then=[{"n": th["n"], "over_cm": th["over_cm"]} for th in sz.get("then") or []], extra=extra)
            for x in kids:
                content.remove(x)
                st_["children"].append(x)
                placed_in[id(x)] = st_
            content.append(st_)
            sz["_tier"] = st_["id"]

    # -- caps a kind's tier does not hold, said on the kind -------------------------------------------------
    every = [(id(k), node_of[id(k)]) for b in c.buckets for k in kind_rows(b)]
    # smaller sizes first; a kind already held to fewer over a smaller size says a larger cap only through the
    # other kinds it counts (Region 2's 2 over 50 cm with a steelhead beside its 1 over 50 cm)
    for sz in sorted((sz for b in c.buckets for sz in b.get("sizes") or []), key=lambda z: (z["over_cm"], z["n"])):
        cm = sz["over_cm"]
        for _i, kn in every:
            mine = [S for S in kn["species"] if S in sz["species"]]
            if not mine or placed_in.get(id(kn), {}).get("id") == sz.get("_tier"):
                continue
            others = [S for S in book_order(sz["species"]) if S not in kn["species"]]
            then = [th for th in sz.get("then") or [] if set(kn["species"]) <= set(th["species"])]
            held = min([INF if kn["n"] is None else kn["n"]]
                       + [a["n"] for a in kn["also_counts"] if a["over_cm"] and a["over_cm"] <= cm])
            some = kind_names(mine) if set(mine) != set(kn["species"]) else None
            says = _size_also(held, sz["n"], cm, others, then, some)
            if says is None:
                continue
            kn["also_counts"].append({"tier": sz.get("_tier"), "by": sz["by"], "n": sz["n"], "over_cm": _num(cm),
                                      "then": [{"n": th["n"], "over_cm": th["over_cm"]} for th in then],
                                      "species": book_order(sz["species"]), "says": says})
    for nt in (nt for b in c.buckets for nt in b.get("notes") or []):
        K = list(nt["kinds"])
        for _i, kn in every:
            mine = [S for S in kn["species"] if S in K]
            if not mine:
                continue
            others = [S for S in book_order(K) if S not in kn["species"]]
            says = _count_also(kn["n"], nt["n"], mine, others, set(mine) != set(kn["species"]))
            if says is None:
                continue
            kn["also_counts"].append({"tier": None, "by": None, "n": nt["n"], "over_cm": 0, "then": [],
                                      "species": book_order(K), "says": says})
    for b in c.buckets:
        for sz in b.get("sizes") or []:
            sz.pop("_tier", None)

    # -- the sentences --------------------------------------------------------------------------------------
    def word(t: dict) -> None:
        for x in t["children"]:
            if x["node"] == "tier":
                word(x)
                continue
            x.pop("_lo", None)
            x.pop("_origins", None)
            x["says"] = _kind_says(x, t["n"])
            s = f"Of the {_short(t)}: {common.lc(x['says'])}"
            if x["detail"]:
                s += f" ({x['detail']})"
            if x["also_counts"]:
                s += " — " + "; ".join(a["says"] for a in x["also_counts"])
            x["sentence"] = s
    word(root)

    head = c.head["says"]
    if said_total and zone:
        head += f" · {_js(total)} across {scope}, {other} included"
    header = {"says": head, "tag": f"{c.origin_one} only" if c.origin_one else None}
    return [root], header


def walk(nodes: Sequence[dict]):
    """Every node of a tree, depth first, each with its parent tier (None at the top)."""
    def go(ns, parent):
        for n in ns:
            yield n, parent
            if n["node"] == "tier":
                yield from go(n["children"], n)
    yield from go(nodes, None)


def map_rules(nodes: Sequence[dict], f) -> List[dict]:
    """The tree with every rule (`by` of a tier and of an `also_counts`) mapped by `f` (to an export index on the
    wire, back to its id in the gate)."""
    out = []
    for n in nodes:
        if n["node"] == "tier":
            out.append({**n, "by": None if n["by"] is None else f(n["by"]), "children": map_rules(n["children"], f)})
        else:
            out.append({**n, "also_counts": [{**a, "by": None if a["by"] is None else f(a["by"])}
                                             for a in n["also_counts"]]})
    return out
