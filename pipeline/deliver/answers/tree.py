"""THE CARD AS A TREE (ROUND18, owner review of v19; ROUND19, review of v20) — `group.tree` and `group.header`.

The owner reads a quota card as FOLDERS: tier lines on the left, each holding what it limits — the region's
total, then a water's own rule or the stream share, then a count over some kinds or a size cap, and the kinds
inside. This module re-reads the card `card.card_of` has already worded and the gate proves (its buckets,
count groups, size groups and notes) as that tree; it decides nothing new.

  tier   {node: "tier", id, tier, n, says, by, children, …}
         total  the group's total ("4 a day per angler across Region 2, lakes included", UR1 A; a water's own total by
                whose it is)
         own    a water's own rule's bucket ("This river’s rule: up to 4 a day"), with `whose`, `replaces`
                ("Replaces Region 2’s 2-from-streams limit and 30 cm (12 in) minimum size for hatchery rainbow
                trout" — ROUND19 6: the fish by name), `why_here` when the rule reached this water by the
                TRIBUTARY WALK ("its row covers its tributaries, so it applies here", ROUND18 8) and `line` (all
                three in one line); under the share where it holds share kinds to a smaller number, else under the
                total
         share  the stream (lake) share ("2 a day from this stream (part of the 4)" — ROUND19 6: the nesting in
                the words, wherever a tier holds fewer than the count tier around it; UR1 A: "— your hatchery rainbow
                trout don’t use these 2", the fish a water's own rule lifts out of it)
         count  a count over some of a tier's kinds, naming them ("1 cutthroat or rainbow trout in all", UR1 D)
         size   ROUND19 3 (owner): a size cap HOLDING EVERY KIND IT CAPS — each kind row that may be kept over its
                size — nested in the tightest tier holding them all (wrapping whole tiers where they span tiers);
                a kind that can never pass the size stays outside ("Only 1 of the fish in this box may be over 50 cm
                (20 in)", UR1 B). RULINGS Q16's pair (1 over 50 cm, 2 with a steelhead) is ONE tier, the steelhead
                INSIDE: "… — 2 if one is a hatchery steelhead" (`extra` {n, species, by}),
                and the steelhead's `size_note` says it counts toward the 1 ("it counts toward the 1 over 50 cm
                (20 in), but once you keep one, a second fish over 50 cm (20 in) may be kept"). A cap on one kind
                row, on part of a row, or over kinds no tier can hold without a kind that cannot pass it (Region 2's
                Bella Coola count over a cutthroat that may and a rainbow that may not) is no tier: the kinds'
                `also_counts` say it
  kind   {node: "kind", species, name, n, says, detail, sizes {cm, in}, also_counts, size_note, duties, sentence}:
         each kind EXACTLY ONCE; `says` "Up to 2 may be hatchery brook, brown or cutthroat trout" (ROUND19 1: the
         origin with the fish where it keeps one, never again in the detail), `detail` its sizes ("30 cm (12 in) or
         longer"), `also_counts` every cap no tier around it holds that still binds it, in words, and `sentence` the
         whole line, "of" the nearest COUNT tier around it (a size tier is no count): "Of the 2 a day: up to 2 may
         be hatchery brook, brown or cutthroat trout, 30 cm (12 in) or longer".
  header {says, tag}: "2 a day from this stream · part of 4 a day per angler across Region 2, lakes included"; `tag` None
         (ROUND19 1: the kinds' names carry the origin).

Sizes are said in cm with the inches in brackets, "50 cm (20 in)" (`common.cm_in`, ROUND19 2). Every number and
the placement of every kind are proved by the card gate (`card_check.check_tree`).
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


def kind_names(species: Sequence[str], steelhead: bool = False, conj: str = " or ", origin: Optional[str] = None
               ) -> str:
    """A kind row's fish in a sentence: a closed group by its name ("whitefish"); fish that are all "<x> trout"
    share the noun ("brook, brown or cutthroat trout"); else each named ("Dolly Varden/bull trout or lake
    trout"); the steelhead "steelhead"; ROUND19 1: `origin` before them where the kind keeps only one ("hatchery
    brook, brown or cutthroat trout")."""
    pre = f"{origin} " if origin else ""
    if steelhead:
        return pre + "steelhead"
    g = common.group_for(species)
    if g:
        return pre + common.lc(g)
    names = [common.lc(common.sp_name(c)) for c in species]
    if len(names) > 1 and all(n.endswith(" trout") and "/" not in n and len(n.split()) == 2 for n in names):
        return pre + common.join([n[:-len(" trout")] for n in names], conj) + " trout"
    return pre + common.join(names, conj)


def _count_names(t: dict) -> str:
    """ROUND19 6 (owner: "1 between them" must say who): the fish a count tier holds; UR1 D (user panel: "between"
    read as each): "cutthroat or rainbow trout", said "1 cutthroat or rainbow trout in all"."""
    sp = list(dict.fromkeys(S for x, _p in walk(t["children"]) if x["node"] == "kind" for S in x["species"]))
    return kind_names(sp, conj=" or ")


def _count_says(t: dict) -> str:
    return f"{_js(t['n'])} {_count_names(t)} in all"


def _short(t: dict) -> str:
    """What a kind's sentence says it is "of" (its nearest COUNT tier): "the 2 a day", "the 1 between cutthroat and
    rainbow trout"."""
    if t["tier"] == "count":
        return _count_says(t)
    return "no daily limit" if t["n"] is None else f"{_js(t['n'])} a day"


def _kind_says(k: dict, parent_n) -> str:
    names = kind_names(k["species"], k["name"] == "Steelhead", origin=k.get("_origin"))
    if k["n"] == 0:
        return f"No {names} may be kept"
    if k["n"] is None:
        return f"Any number may be {names}"
    if parent_n is not None and k["n"] < parent_n:
        return f"Only {_js(k['n'])} may be {names}"
    return f"Up to {_js(k['n'])} may be {names}"


def _size_also(kn, n, cm, others: List[str], then: List[dict], some: Optional[str] = None) -> Optional[str]:
    """A size cap no tier around a kind holds, said on the kind (ROUND18 7); `some` names the fish of the kind row
    it holds where it holds only some of them ("only 1 cutthroat trout over 50 cm (20 in)")."""
    over = f"{some} over {common.cm_in(cm)}" if some else f"over {common.cm_in(cm)}"
    nw = "none" if not n else f"only {_js(n)}"
    o = kind_names(others) if others else ""
    if (INF if kn is None else kn) > n:
        s = f"{nw} {over}"
        for t in then:
            s += f", and {'none' if not t['n'] else 'only ' + _js(t['n'])} over {common.cm_in(t['over_cm'])}"
        if others and n:
            s += (f", if you haven’t already kept a {o} over {common.cm_in(cm)}" if n == 1
                  else f", counting any {o} over {common.cm_in(cm)}")
        return s
    if others and n:
        return (f"none {over} if you’ve already kept a {o} over {common.cm_in(cm)}" if n == 1
                else f"no more than {_js(n)} {over}, counting any {o} over {common.cm_in(cm)}")
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
        # UR1 A (user panel: "per angler or per group?"): the total is each angler's
        root = tier("total", total, f"{_js(total)} a day per angler across {scope}"
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
            os_ = list(sd.get("origins") or [])
            kn["_hi"] = max((b_ for _a, b_ in segs), default=0)
            kn["_origins"] = os_
            kn["_origin"] = os_[0] if len(os_) == 1 and k["n"] != 0 else None

    # UR1 A: the fish a water's own rule lifts out of the share, by name ("hatchery rainbow trout")
    lifted_names = list(dict.fromkeys(kind_names(node_of[id(k)]["species"], k["name"] == "Steelhead",
                                                 origin=node_of[id(k)]["_origin"])
                                      for b in c.lifting for k in kind_rows(b) if k["n"] != 0))

    def can_pass(kn: dict, cm) -> bool:
        """The kind may be kept over the size here (ROUND19 3: a size tier holds exactly these)."""
        return kn["n"] != 0 and kn["_hi"] > cm

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

    # -- size caps (ROUND19 3, owner): a tier HOLDING every kind row its cap binds (each that may be kept over its
    # size), in the tightest tier holding them all — wrapping whole tiers where they span tiers, never a kind that
    # cannot pass the size. RULINGS Q16's steelhead (`not_counting` ST, let past the number) sits inside, the tier
    # saying how many more it lets past. A cap on one kind row, on part of a row, or that no tier can hold so, is
    # said on its kinds (below).
    def parents() -> Dict[int, dict]:
        return {id(n): p for n, p in walk([root]) if p is not None}

    def chain(par: Dict[int, dict], n: dict) -> List[dict]:
        out = []
        while id(n) in par:
            n = par[id(n)]
            out.append(n)
        return out[::-1]

    def count_of(par: Dict[int, dict], t: dict) -> dict:
        """The nearest tier at or above `t` that counts (a size tier limits only what is over its size)."""
        while t["tier"] == "size":
            t = par[id(t)]
        return t

    every_kind = [node_of[id(k)] for b in c.buckets for k in kind_rows(b)]

    def bound(sz) -> List[dict]:
        sp = set(sz["species"])
        return [kn for kn in every_kind if set(kn["species"]) & sp and can_pass(kn, sz["over_cm"])]

    def holding(kids: List[dict]) -> Optional[Tuple[dict, List[dict]]]:
        """(the tier to nest in, its children the size tier takes), or None where no tier holds exactly them."""
        par = parents()
        chains = [chain(par, k) for k in kids]
        box = None
        for level in zip(*chains):
            if any(t is not level[0] for t in level):
                break
            box = level[0]
        ids = {id(k) for k in kids}
        taken = []
        for x in box["children"]:
            if id(x) in ids:
                taken.append(x)
            elif x["node"] == "tier":
                under = [y for y, _p in walk(x["children"]) if y["node"] == "kind"]
                if any(id(y) in ids for y in under):
                    if not all(id(y) in ids for y in under):
                        return None
                    taken.append(x)
        return box, taken

    sizes_all = [sz for b in c.buckets for sz in b.get("sizes") or []]
    for sz in sorted(sizes_all, key=lambda z: (-len(bound(z)), z["over_cm"], z["n"])):
        cm = sz["over_cm"]
        kids = bound(sz)
        if not kids or any(not set(kn["species"]) <= set(sz["species"]) for kn in kids):
            continue                               # part of a kind row: said on the row
        st, extra, ex_by = None, None, None
        if sz["not_counting"] == ["ST"]:
            st = next((kn for kn in every_kind if kn["species"] == ["ST"] and can_pass(kn, cm)), None)
        got = holding(kids + ([st] if st is not None else []))
        if st is not None and got is not None:
            box = count_of(parents(), got[0])
            subs = [s for s in grp["subcaps"] if s["over_cm"] == cm and set(sz["species"]) | {"ST"} <= set(s["of"])]
            room = min([s["take"] for s in subs] + [INF if box["n"] is None else box["n"],
                                                     INF if st["n"] is None else st["n"]])
            if room != INF and room - sz["n"] >= 1:
                extra = {"n": _num(room - sz["n"]), "species": ["ST"]}
                ex_by = min(subs, key=lambda s: s["take"])["r"] if subs else None
        if extra is None:
            st = None
            got = holding(kids)
        if got is None or (len(kids) < 2 and st is None):
            continue
        box, taken = got
        cn = count_of(parents(), box)["n"]
        if extra is None and cn is not None and sz["n"] >= cn:
            continue
        # UR1 B (user panel: "which these?"): the fish in the tier's box (the page draws a size tier as a box)
        n_w = "None" if not sz["n"] else f"Only {_js(sz['n'])}"
        says = f"{n_w} of the fish in this box may be over {common.cm_in(cm)}"
        for th in sz.get("then") or []:
            says += f", and {'none' if not th['n'] else 'only ' + _js(th['n'])} over {common.cm_in(th['over_cm'])}"
        who = f"{st['_origin']} steelhead" if st is not None and st["_origin"] else "steelhead"
        if extra is not None:
            # RULINGS Q16 (Region 1/2): "no more than 1 over 50 cm (2 hatchery steelhead over 50 cm allowed)"
            e, n0 = extra["n"], sz["n"]
            if not n0:
                says += f" — except {_js(e)} {who}"
            else:
                says += f" — {_js(n0 + e)} if " + (f"one is a {who}" if e == 1 else f"{_js(e)} are {who}")
            extra["by"] = ex_by
        st_ = tier("size", sz["n"], says, sz["by"], over_cm=_num(cm), species=list(sz["species"]),
                   then=[{"n": th["n"], "over_cm": th["over_cm"]} for th in sz.get("then") or []], extra=extra)
        at = min(box["children"].index(x) for x in taken)
        for x in taken:
            box["children"].remove(x)
        st_["children"] = taken
        box["children"].insert(at, st_)
        if extra is not None:
            e, n0, w = extra["n"], sz["n"], common.cm_in(cm)
            st["size_note"] = {"tier": st_["id"], "says": (
                f"it counts toward the 1 over {w}, but once you keep one, a second fish over {w} may be kept"
                if (n0, e) == (1, 1) else
                f"only {who} may be over {w}, up to {_js(e)}" if not n0 else
                f"it counts toward the {_js(n0)} over {w}, but with {who} up to {_js(n0 + e)} fish over {w} may be kept")}
        sz["_tier"] = st_["id"]
    par = parents()
    above: Dict[int, List[dict]] = {id(kn): chain(par, kn) for kn in every_kind}

    # -- caps a kind's tier does not hold, said on the kind -------------------------------------------------
    every = [(id(k), node_of[id(k)]) for b in c.buckets for k in kind_rows(b)]
    # smaller sizes first; a kind already held to fewer over a smaller size says a larger cap only through the
    # other kinds it counts (Region 2's 2 over 50 cm with a steelhead beside its 1 over 50 cm)
    # (at one size and number, the cap over more kinds first: a narrower one it implies then says nothing)
    for sz in sorted((sz for b in c.buckets for sz in b.get("sizes") or []),
                     key=lambda z: (z["over_cm"], z["n"], -len(z["species"]))):
        cm = sz["over_cm"]
        for _i, kn in every:
            mine = [S for S in kn["species"] if S in sz["species"]]
            if not mine or any(t["id"] == sz.get("_tier") for t in above[id(kn)]):
                continue
            others = [S for S in book_order(sz["species"]) if S not in kn["species"]]
            then = [th for th in sz.get("then") or [] if set(kn["species"]) <= set(th["species"])]
            # a size tier around it at no larger size holds it to its number (and its extra, a steelhead)
            held = min([INF if kn["n"] is None else kn["n"]]
                       + [a["n"] for a in kn["also_counts"] if a["over_cm"] and a["over_cm"] <= cm
                          and set(kn["species"]) <= set(a["species"])]
                       + [t["n"] + (((t.get("extra") or {}).get("n") or 0) if kn["species"] == ["ST"] else 0)
                          for t in above[id(kn)] if t["tier"] == "size" and t["over_cm"] <= cm])
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
    # ROUND19 6 (owner: the region's total and the stream share read as competing): a tier holding fewer than the
    # count tier around it says it is part of it ("2 a day from this stream (part of the 4)")
    for t, p in walk([root]):
        if t["node"] == "tier" and t["tier"] in ("own", "share") and p is not None:
            up = count_of(par, p)
            if up["n"] is not None and t["n"] is not None and t["n"] < up["n"]:
                t["says"] += f" (part of the {_js(up['n'])})"
    # UR1 A (user panel: do the 4 hatchery rainbows count toward the 2?): the share says the fish a water's own rule
    # lifts out of it don't use it
    if share_t is not None and lifted_names and share_t["n"] is not None:
        share_t["says"] += f" — your {common.join(lifted_names, ' and ')} don’t use these {_js(share_t['n'])}"
    for t, p in walk([root]):
        if t["node"] == "tier" and t["tier"] == "count":
            t["says"] = _count_says(t)
    for x, p in walk([root]):
        if x["node"] != "kind":
            continue
        up = count_of(par, p)
        x["says"] = _kind_says(x, up["n"])
        for k_ in ("_hi", "_origins", "_origin"):
            x.pop(k_, None)
        s = f"Of the {_short(up)}: {common.lc(x['says'])}"
        if x["detail"]:
            s += f", {x['detail']}"
        notes = [a["says"] for a in x["also_counts"]] + ([x["size_note"]["says"]] if x.get("size_note") else [])
        if notes:
            s += " — " + "; ".join(notes)
        x["sentence"] = s

    head = c.head["says"]
    if said_total and zone:
        head += f" · part of {_js(total)} a day per angler across {scope}, {other} included"
    # ROUND19 1: the kinds' names carry the origin; the tag does not repeat it
    header = {"says": head, "tag": None}
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
            ex = n.get("extra")
            if ex is not None:
                ex = {**ex, "by": None if ex.get("by") is None else f(ex["by"])}
            out.append({**n, "by": None if n["by"] is None else f(n["by"]), "children": map_rules(n["children"], f),
                        **({"extra": ex} if "extra" in n else {})})
        else:
            out.append({**n, "also_counts": [{**a, "by": None if a["by"] is None else f(a["by"])}
                                             for a in n["also_counts"]]})
    return out
