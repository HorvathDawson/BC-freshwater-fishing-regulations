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
                trout" — ROUND19 6: the fish by name), `relation` (ROUND UR6 6) where it stands beside the share
                under a region total ("Not part of the 2 from streams; part of the 5 a day across Region 4"), `why_here`
                when the rule reached this water by the
                TRIBUTARY WALK ("its row covers its tributaries, so it applies here", ROUND18 8) and `line` (all
                three in one line); under the share where it holds share kinds to a smaller number, else under the
                total
         share  the stream (lake) share, said INSIDE the count tier around it ("Only 2 of the 4 may be from this
                stream" — ROUND20 2, owner: the UR3 "— your hatchery rainbow trout count toward the 4, not these 2"
                read as 4 + 2; the fish a water's own rule lifts out of it ", not counting hatchery rainbow trout")
                — ROUND20g 1: its `origin` where the one-origin rule holds on the share's water only ("From streams, keep
                hatchery fish only — wild trout and char go back.", Region 2 p.21; the group's `origin` is then None)
         count  a count over some of a tier's kinds, naming them ("1 cutthroat or rainbow trout in all", UR1 D)
         size   ROUND19 3 (owner): a size cap HOLDING EVERY KIND IT CAPS — each kind row that may be kept over its
                size — nested in the tightest tier holding them all (wrapping whole tiers where they span tiers);
                a kind that can never pass the size stays outside ("Of these, only 1 may be over 50 cm (20 in)", UR1 B,
                ROUND UR6 1). RULINGS Q16's pair (1 over 50 cm, 2 with a steelhead) is ONE tier, the steelhead
                INSIDE: "… — 2 if one is a hatchery steelhead" (`extra` {n, species, by}; ROUND20g 2: said once, the
                steelhead's own `size_note` is gone). A cap on one kind
                row, on part of a row, or over kinds no tier can hold without a kind that cannot pass it (an own
                rule's tier holding one of them beside a Dolly Varden of 50 cm or less) is no tier: the kinds'
                `also_counts` say it. T203: a COUNT tier it crosses is split, not a reason (`holding`)
  kind   {node: "kind", species, name, n, says, detail, sizes {cm, in}, also_counts, duties, sentence}:
         each kind EXACTLY ONCE; `says` "Up to 2 may be hatchery brook, brown or cutthroat trout" (ROUND19 1: the
         origin with the fish where it keeps one, never again in the detail), `detail` its sizes ("30 cm (12 in) or
         longer"), `also_counts` every cap no tier around it holds that still binds it, in words, and `sentence` the
         whole line, "of" the nearest COUNT tier around it (a size tier is no count): "Of the 2 a day: up to 2 may
         be hatchery brook, brown or cutthroat trout, 30 cm (12 in) or longer". T128 (owner 2026-10-22): a kind line
         NAMES THE TOTAL it is part of (`total_named`, `total_words`) — `says` "Of your 2 trout and char a day in
         streams, up to 2 may be hatchery brook, brown or cutthroat trout", its `sentence` the same line with the
         detail and notes — unless it is that total's only kind at its number (the tier's line says it all).
  header {says, tier, tag}: ROUND20 1 (owner: the group's top and the first tier are ONE block) — the header IS the
         root tier: `says` its line ("4 a day per angler across Region 2, lakes included"; a water's own total "This
         river’s rule: up to 1 a day per angler"), `tier` its id; the page draws the root's children at the top of
         the tree. `tag` None (ROUND19 1: the kinds' names carry the origin).

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
    #: ROUND20g 1: the origin rule said on the share's tier ("From streams, keep hatchery fish only — …"), where it holds
    #: on the share's kind of water only (`card.origin_place`)
    share_origin: Optional[str] = None


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
    sp = t.get("_of") or list(dict.fromkeys(S for x, _p in walk(t["children"]) if x["node"] == "kind"
                                            for S in x["species"]))
    return kind_names(sp, conj=" or ")


def _count_says(t: dict) -> str:
    return f"{_js(t['n'])} {_count_names(t)} in all"


def _short(t: dict) -> str:
    """What a kind's sentence says it is "of" (its nearest COUNT tier): "the 2 a day", "the 1 between cutthroat and
    rainbow trout"."""
    if t["tier"] == "count":
        return _count_says(t)
    return "no daily limit" if t["n"] is None else f"{_js(t['n'])} a day"


def _kind_says(k: dict, parent_n, plain: bool = False, lead: Optional[str] = None) -> str:
    """A kind's line. UR2 3 (user panel: "Only 1 may be Dolly Varden" — inside the 2 or extra?): a kind held to fewer
    than its count tier says of how many, "Only 1 of the 2 may be …"; UR2 4 (wild steelhead release unsaid on the
    Vedder): a steelhead kind keeping hatchery fish only, "Up to 2 may be hatchery steelhead only — wild steelhead go
    back". `plain`: the bare line the kind's `sentence` reads after "Of the 2 a day: ", which says the count itself.
    T128 (owner 2026-10-22: "up to 2 may be hatchery …" read as 2 more on top of the quota): `lead` the total the line is
    part of (`total_named`), said first — "Of your 2 trout and char a day in streams, up to 2 may be hatchery brook, brown
    or cutthroat trout"; the kind is then "only" N where it is held to fewer than that total (`parent_n`). It replaces ROUND
    UR7 2's "(part of the 2 from streams)": the lead names the share for every kind under it."""
    names = kind_names(k["species"], k["name"] == "Steelhead", origin=k.get("_origin"))
    wild_back = not plain and k["name"] == "Steelhead" and k.get("_origin") == "hatchery"
    if k["n"] == 0:
        return f"No {names} may be kept"
    if k["n"] is None:
        return f"Any number may be {names}"
    if lead is not None:
        if parent_n is not None and k["n"] < parent_n:
            return f"{lead}, only {_js(k['n'])} may be {names}" + (" — wild steelhead go back" if wild_back else "")
        return f"{lead}, up to {_js(k['n'])} may be {names}" + (" only — wild steelhead go back" if wild_back else "")
    if parent_n is not None and k["n"] < parent_n:
        of = "" if plain else f" of the {_js(parent_n)}"
        return f"Only {_js(k['n'])}{of} may be {names}" + (" — wild steelhead go back" if wild_back else "")
    return f"Up to {_js(k['n'])} may be {names}" + (" only — wild steelhead go back" if wild_back else "")


def total_named(chain: Sequence[dict], kind: dict) -> Optional[dict]:
    """T128 (owner 2026-10-22: "it needs to add 'of your total X daily quota in a stream for trout and char' in there to
    make it more obvious to an untrained user"): THE total a kind's line names — the nearest tier above it that is a total,
    the stream (lake) share or a water's own rule (a count or a size tier is a part of one, never a total of its own) —
    or None where the line names none: a kind keeping none or any number, a tier of no number, or the kind is that tier's
    ONLY kind at its number (the tier's own line says the whole of it: the card's header, or the share / own tier the kind
    is folded into). `chain` the tiers from the root down to the kind's parent. The one decision; the card gate proves
    the number it names like every other (`card_check.check_tree`)."""
    t = next((t for t in reversed(chain) if t["tier"] in ("total", "share", "own")), None)
    if t is None or t["n"] is None or kind["n"] in (0, None):
        return None
    under = [x for x, _p in walk(t["children"]) if x["node"] == "kind"]
    if len(under) == 1 and under[0] is kind and kind["n"] == t["n"]:
        return None
    return t


def total_fish(t: dict, kinds: Sequence[str], what: str) -> Optional[str]:
    """T128 review: the fish a named total is OF — the group's (`what`, "trout and char") for a total or a share, and for a
    water's own rule holding every one of the group's kinds; None for an own rule holding only SOME of them (Kamloops
    Lake's "Rainbow trout daily quota = 2" and "bull trout daily quota = 1", one tier of 3 beside Region 3's 5; Adams
    Lake's "1 bull trout or lake trout"): it is no "3 trout and char a day" of yours, the 5 is."""
    if t["tier"] != "own":
        return what
    held = {S for x, _p in walk(t["children"]) if x["node"] == "kind" for S in x["species"]}
    return what if held >= set(kinds) else None


def total_words(t: dict, what: Optional[str], where: str) -> str:
    """T128: the named total in the reader's words — its number, the group's fish, the period and, only where the total
    is limited to one, its kind of water: "Of your 2 trout and char a day in streams", "Of your 5 trout and char a day",
    a water's own rule "Of your 2 trout and char a day under this river’s rule"; an own rule of only some of the group's
    fish (`total_fish` None) by its number and whose it is, "Of the 3 a day under this lake’s rule"."""
    if what is None:
        return f"Of the {_js(t['n'])} a day{where}"
    return f"Of your {_js(t['n'])} {what} a day{where}"


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


def share_place(water: str, region_wide: bool) -> str:
    """ROUND20f 3 (owner; a reader took "ALL REGION 2 STREAMS" + "from this stream" both ways): where the share is the
    REGION'S — a zone or province rule, the Region 2 streams' combined 2 — it says "from streams" ("from lakes"); a share
    a water's own row sets keeps "from this stream". The one place the phrase comes from (the share tier, its fold, the
    headline's unit)."""
    return f"{water}s" if region_wide else f"this {water}"


def share_region_wide(P, share: Optional[dict]) -> bool:
    """The share's rule is a region's or the province's (never a water's or an area's own row)."""
    from pipeline.deliver.answers.rows import badge_of
    return share is not None and badge_of(P.by_key[share["r"]]) in ("region", "bc")


def fold_words(says: str, tier: str, n, names: str) -> str:
    """ROUND20b C1 (owner: "This river’s rule: up to 4 a day" over "Up to 4 may be hatchery rainbow trout" says 4
    twice): a tier whose ONLY child is one kind row at its number names the fish itself, and the kind is folded into it.
      own    "This river’s rule: up to 4 a day (part of the 5)" -> "This river’s rule: up to 4 hatchery rainbow trout a
             day (part of the 5)"
      share  "Only 2 of the 5 may be from this stream, not counting rainbow trout" -> "Only 2 of the 5 may be brook,
             brown, cutthroat or lake trout from this stream" (the fish named, nothing else is in the 2, so the lifted
             fish need no mention); the fallback "2 a day per angler from this stream" -> "2 brook, … a day per angler
             from this stream"."""
    if tier == "own":
        head, _sep, tail = says.partition(" a day")
        return f"{head} {names} a day{tail}"
    base = says.split(", not counting ")[0]
    if base.startswith("Only ") and " may be from " in base:
        return base.replace(" may be from ", f" may be {names} from ", 1)
    return base.replace(f"{_js(n)} a day per angler from ", f"{_js(n)} {names} a day per angler from ", 1)


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
        # ROUND20 1: the root is the card's header, so it says "per angler" like a zone total
        root = tier("total", total, f"{whose}: up to {_js(total)} a day per angler", grp["pool"], whose=whose,
                    why_here=why, line=whose + (f" ({why})" if why else ""))

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
            # ROUND20b C2 (owner: fewer words; the panel read "Replaces Region 2’s 2-from-streams limit" against the
            # share's "not counting hatchery rainbow trout" as two different limits): the line is whose rule and why it
            # applies here; what it replaced stays in `replaces` (history, not a rule in force)
            t = tier("own", b["cap"], f"{whose}: " + ("any number" if b["cap"] is None else f"up to {_js(b['cap'])} a day"),
                     b["by"], whose=whose, why_here=why_here, replaces=rep, relation=None,
                     line=whose + (f" ({why_here})" if why_here else ""))
            own_ts.append((t, b))
        elif b["why"] == "share":
            # ROUND20f 3: a region's share says "from streams"; a water's own share "from this stream"
            t = share_t = tier("share", b["cap"], f"{_js(b['cap'])} a day per angler from {share_place(c.share['water'], share_region_wide(P, c.share))}", b["by"])
            if c.share_origin is not None:
                t["origin"] = c.share_origin           # ROUND20g 1: the origin rule where its scope is
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
            # T203: every fish the count holds, said by its tier even where a size tier takes some of its kinds
            ct["_of"] = [S for x in ct["children"] for S in x["species"]]
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
    # ROUND20b C3 (owner: after the header's 4, the 2 reads as the next line): the share before a water's own tiers
    root["children"][:0] = ([share_t] if share_t is not None else []) + under_total

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

    def holding(kids: List[dict]) -> Optional[Tuple[dict, List[dict], List[Tuple[dict, List[dict]]]]]:
        """(the tier to nest in, its children the size tier takes, the count tiers it splits), or None where no tier
        holds exactly them. T203 (owner 2026-10-28, the Dala River: Region 6's "1 over 50 cm" said on four kinds because
        the stream's "1 brown, cutthroat, rainbow trout or steelhead in all" also held a rainbow of 50 cm or less): a
        COUNT tier holding some of the kinds and others beside them is split — it goes inside the size tier with the
        kinds the cap binds, and the others leave it for the tier around (each says the count on its line, the count
        tier still names every fish it counts); a cap shared by several kinds is one size tier holding exactly them.
        An own or share tier is never split (its kinds are its rule's)."""
        par = parents()
        chains = [chain(par, k) for k in kids]
        box = None
        for level in zip(*chains):
            if any(t is not level[0] for t in level):
                break
            box = level[0]
        ids = {id(k) for k in kids}
        taken, splits = [], []
        for x in box["children"]:
            if id(x) in ids:
                taken.append(x)
            elif x["node"] == "tier":
                under = [y for y, _p in walk(x["children"]) if y["node"] == "kind"]
                if any(id(y) in ids for y in under):
                    if not all(id(y) in ids for y in under):
                        if x["tier"] != "count" or any(y["node"] != "kind" for y in x["children"]):
                            return None
                        splits.append((x, [y for y in x["children"] if id(y) not in ids]))
                    taken.append(x)
        return box, taken, splits

    sizes_all = [sz for b in c.buckets for sz in b.get("sizes") or []]
    left: List[Tuple[dict, dict]] = []           # T203: (a kind out of its count tier, that tier)
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
        box, taken, splits = got
        cn = count_of(parents(), box)["n"]
        if extra is None and cn is not None and sz["n"] >= cn:
            continue
        # UR1 B (user panel: "which these?"): the fish the tier holds. ROUND UR6 1 (owner: "this box" with no box since the flat
        # redesign): "Of these, only 1 may be over 50 cm (20 in)" — the page draws the section's extent (a rule from its head to
        # its last row); a fresh-reader single-frame test picked these words over naming every fish (rows 28)
        n_w = "none" if not sz["n"] else f"only {_js(sz['n'])}"
        says = f"Of these, {n_w} may be over {common.cm_in(cm)}"
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
        out_at = at + 1
        for ct, outs in splits:                    # T203: the kinds the cap does not bind leave the count tier
            for y in outs:
                ct["children"].remove(y)
                box["children"].insert(out_at, y)
                out_at += 1
                left.append((y, ct))
        # ROUND20g 2 (owner): the steelhead's own note ("it counts toward the 1 over 50 cm (20 in), but once you keep one,
        # a second …") said the band head's "— 2 if one is a hatchery steelhead" again; the band head says it once
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
    for kn, ct in left:                          # T203: a kind split out of its count tier says the count on its line
        others = [S for S in book_order(ct["_of"]) if S not in kn["species"]]
        says = _count_also(kn["n"], ct["n"], list(kn["species"]), others)
        if says is not None:
            kn["also_counts"].append({"tier": None, "by": ct["by"], "n": ct["n"], "over_cm": 0, "then": [],
                                      "species": book_order(ct["_of"]), "says": says})
    for b in c.buckets:
        for sz in b.get("sizes") or []:
            sz.pop("_tier", None)

    # -- the sentences --------------------------------------------------------------------------------------
    # ROUND19 6 (owner: the region's total and the stream share read as competing): a tier holding fewer than the
    # count tier around it says it is part of it ("2 a day from this stream (part of the 4)")
    # UR1 A (user panel: do the 4 hatchery rainbows count toward the 2?): the share says the fish a water's own rule
    # lifts out of it don't use it; UR3 4 (user panel: then where do they go?): where it is part of a count tier, they
    # count toward that one instead ("— your hatchery rainbow trout count toward the 4, not these 2")
    # ROUND20 2 (owner: "— your hatchery rainbow trout count toward the 4, not these 2" read as 4 + 2): the share
    # says it sits INSIDE the total, in the kinds' own shape — "Only 2 of the 4 may be from this stream", the lifted
    # fish ", not counting hatchery rainbow trout" (they are in the 4, and only the OTHER fish are held to the 2)
    # ROUND UR7 A (owner: the Vedder said the 2 twice — the head's "2 a day from streams" and the share's "Only 2 of the 4 may
    # be from streams, not counting hatchery rainbow trout"): where every lifting rule's own tier says its `relation` ("Not
    # part of the 2 from streams; …", below), the lifted fish are said ONCE, there; the share says nothing more than the head
    rel_on = share_t is not None and any(x["node"] == "kind" for x, _p in walk(share_t["children"])) and zone \
        and not c.unlimited and share_t["n"] is not None and root["n"] is not None
    said_by_own = rel_on and bool(c.lifting) and all(
        any(b is lb and any(t is u for u in under_total) for t, b in own_ts) for lb in c.lifting)
    lifted = common.join(lifted_names, " and ") if share_t is not None and lifted_names and share_t["n"] is not None \
        and not said_by_own else None
    for t, p in walk([root]):
        if t["node"] == "tier" and t["tier"] in ("own", "share") and p is not None:
            up = count_of(par, p)
            if up["n"] is not None and t["n"] is not None and t["n"] < up["n"]:
                if t is share_t:
                    t["says"] = f"Only {_js(t['n'])} of the {_js(up['n'])} may be from {share_place(c.share['water'], share_region_wide(P, c.share))}"
                    if lifted:
                        t["says"] += f", not counting {lifted}"
                        lifted = None
                else:
                    t["says"] += f" (part of the {_js(up['n'])})"
    if lifted:
        share_t["says"] += f", not counting {lifted}"
    # UR3 3 (user panel: is the 1 inside the 2 or extra?): a count tier inside a water's own tier or the share says it
    # is part of it ("1 cutthroat or rainbow trout in all (part of the 2)")
    for t, p in walk([root]):
        if t["node"] == "tier" and t["tier"] == "count":
            t["says"] = _count_says(t)
            up = count_of(par, p) if p is not None else None
            if up is not None and up["tier"] in ("own", "share") and up["n"] is not None and t["n"] is not None \
                    and t["n"] < up["n"]:
                t["says"] += f" (part of the {_js(up['n'])})"
    # ROUND UR6 6 (owner; UR5 Tom on Glenn/Hamill: "the big '2 a day from streams' fights the Duncan's 5"): a water's own
    # rule standing beside the stream share under the region's total says how it stands to both — its fish are not in the
    # share, they are in the total: "Not part of the 2 from streams; part of the 5 a day across Region 4" (`relation`)
    for t in under_total:
        if rel_on:
            t["relation"] = (f"Not part of the {_js(share_t['n'])} from "
                             f"{share_place(c.share['water'], share_region_wide(P, c.share))}; "
                             f"part of the {_js(root['n'])} a day across {scope}")
    from pipeline.deliver.bundle import read as RD
    steel_cm = RD._steelhead_min_cm()
    kind_origin: Dict[int, Optional[str]] = {}

    def where_of(t: dict) -> str:
        """T128: where the named total holds, said only where it is limited: the share's kind of water ("in streams",
        "in this stream"), a water's own rule by whose it is, a zone total printed for one kind of water."""
        from pipeline.deliver.answers.card import _whose
        if t["tier"] == "share":
            return f" in {share_place(c.share['water'], share_region_wide(P, c.share))}"
        if t["tier"] == "own":
            return f" under {_whose(P, P.by_key[t['by']])[1]}"
        return f" in {share_place(T.f['water'], True)}" if zone and T.f.get("water") else ""

    for x, p in walk([root]):
        if x["node"] != "kind":
            continue
        up = count_of(par, p)
        named = total_named(chain(par, x), x)
        lead = total_words(named, total_fish(named, grp["kinds"], c.what), where_of(named)) if named is not None else None
        x["says"] = _kind_says(x, named["n"] if named is not None else up["n"], lead=lead)
        bare = _kind_says(x, named["n"] if named is not None else up["n"], plain=True, lead=lead)
        kind_origin[id(x)] = x.get("_origin")
        x["folded"] = False
        x["size_rule"] = size_rule(x, P.steelhead_water, steel_cm)     # ROUND20i 1
        for k_ in ("_hi", "_origins", "_origin"):
            x.pop(k_, None)
        # T128: a line naming its total is the sentence's head itself; else "Of the 2 a day: …" as before
        s = bare if lead is not None else f"Of the {_short(up)}: {common.lc(bare)}"
        if x["detail"]:
            s += f", {x['detail']}"
        notes = [a["says"] for a in x["also_counts"]]
        if notes:
            s += " — " + "; ".join(notes)
        x["sentence"] = s
    # ROUND20b C1 (owner: "This river’s rule: up to 4 a day" over "Up to 4 may be hatchery rainbow trout" says 4 twice):
    # an own tier whose ONLY child is one kind row at the tier's number names the fish itself — "This river’s rule: up to
    # 4 hatchery rainbow trout a day" — and the kind is FOLDED into it (`folded`: the page draws its size, notes and
    # pictures under the line, no sentence; `says` and `sentence` stay for the gate)
    # (the share too — Glenn Creek's "Only 2 of the 5 may be from this stream" over "Up to 2 may be brook, brown,
    # cutthroat or lake trout" — so a number is never said twice in a row on a tier and its only kind)
    for t, _p in walk([root]):
        if t["node"] == "tier" and t["tier"] in ("own", "share") and t["n"] is not None and len(t["children"]) == 1 \
                and t["children"][0]["node"] == "kind" and t["children"][0]["n"] == t["n"]:
            k = t["children"][0]
            names = kind_names(k["species"], k["name"] == "Steelhead", origin=kind_origin.get(id(k)))
            t["says"] = fold_words(t["says"], t["tier"], t["n"], names)
            k["folded"] = True

    # ROUND20 1 (owner: the group's top and the first tier are ONE block): the header IS the root tier — its words,
    # named by `tier`; the page draws the root's line as the header and the root's children at the top of the tree.
    # ROUND19 1: the kinds' names carry the origin; the tag does not repeat it
    for t, _p in walk([root]):
        t.pop("_of", None)
    header = {"says": root["says"], "tier": root["id"], "tag": None}
    return [root], header


def size_rule(kn: dict, steelhead_water: bool, steel_cm) -> Optional[str]:
    """The kind's size as an assertive RULE on its count line (owner 2026-10-27: "the one rainbow has to be under 50 cm,
    release above"), from its `sizes` (exactly the lengths it may be kept at here, gate-proved):
    top only "must be 50 cm (20 in) or less; release any longer"; bottom only "must be 30 cm (12 in) or longer; release
    any shorter"; a range "must be 30–50 cm (12–20 in); release any other size"; gaps "must be A, or B; release any other
    size". A rainbow's top at the steelhead line on a steelhead water is no release (over it the fish IS a steelhead, its
    own row): "must be 30 cm (12 in) or longer; release any shorter". None for a kind kept at any size, at none, or a
    steelhead (its row's size is what it is)."""
    from pipeline.deliver.answers.card import _size_words
    segs = [(a, INF if b is None else b) for a, b in kn["sizes"]["cm"]]
    if kn["n"] == 0 or not segs or kn["species"] == ["ST"]:
        return None
    steel_cut = "RB" in kn["species"] and steelhead_water and segs[-1][1] == steel_cm
    below, gaps = segs[0][0] > 0, len(segs) > 1
    above = segs[-1][1] != INF and not steel_cut
    if not (below or above or gaps):
        return None
    if steel_cut and not gaps:
        return f"must be {_size_words(segs[0][0], INF, inch=True)}; release any shorter"
    words = ", or ".join(_size_words(a, b, inch=True) for a, b in segs)
    if gaps or (below and above):
        rest = "release any other size"
    else:
        rest = "release any shorter" if below else "release any longer"
    return f"must be {words}; {rest}"


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
