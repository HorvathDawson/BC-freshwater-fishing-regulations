"""THE GEAR RESOLVER — which gear rule wins, per (rule key, segment). Consumer Stage 7.1-7.6.

TWO STEPS, AND ONLY THE SECOND IS NEW.

1. WHICH GEAR RULES ARE IN FORCE is the reference reader's answer (`read.effective_rules_bound`,
   traced) for the key's gear bindings (`states`): its lifts (the Quatse's dated bait ban
   replacing Region 1's all-year one), `beside` for a rule held some hours or on one side of the
   channel, `not_yet_mapped` for one in an undrawn part, and its (type, dimension) competition. A
   gear or conduct rule's dimension carries its clause conditions, its means (`while`) and its
   acts (`catalogue.CatalogueRule.dimension`, 2026-10-06), so the competition only ever sets the
   same subject at two rungs against each other: Kootenay Lake's boat-only "unlimited rods" no
   longer removes the province's "1 line" from shore, and a region's set-line or ice-hut duty no
   longer removes "no gear in the water during a closure". A gear rule names no fish (0 of 1,347
   gear, conduct and vessel rules carry `species`); each is read for the first fish it speaks for
   ("RB", or a `when_targeting` target).

2. HOW THE IN-FORCE CLAUSES RESOLVE is the page's `settleGear` (v35), ported:
     * a clause whose `when.water` is the other kind of water drops out;
     * its CIRCUMSTANCE is the rule's `while`, the methods of the rule it is a proviso of
       (`condition_of`), and the clause's `when.method` / `when.angler` / `when.gear_in_use`;
       with a circumstance, a target or a note it is CIRCUMSTANTIAL, else MAIN;
     * COUNTS (`max` / `min` / `unlimited`) by slot: closest rank, then the more specific rule,
       then the smaller max — the first wins, the rest are overruled;
     * SPECS (`must_be` / `requires`) by slot, all of them;
     * ELEMENTS, per slot and member: the main clause saying something about it (a `ban` naming
       it or a parent, minus `except`; an `allow` or `only` naming it or a parent; an `only`
       naming a sibling bans it), closest rank first, a ban before an allow at equal rank.
   Then the derived answers the tiles read: the hook, bait per element (worms follow only a
   whole-bait ban), ways to fish against the province's lawful methods, conduct by moment.

Decisions where the page is silent or the book says otherwise are listed in `DECISIONS`.
"""
from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.answers.common import AnswersError, Bundle, RuleKey, dumps, \
    expand, rule_id
from pipeline.deliver.calendar import month_day
from pipeline.deliver import types as T
from pipeline.deliver.bundle import read

GEAR_FAMILIES = ("gear_and_method", "conduct", "vessel")

#: The element tree (consumer 7.1 step 6, the page's PARENT): a member's parent is the wider
#: member a clause may name instead ("bait ban" is `ban: [any_bait]`).
PARENT = {"angling": "any_method", "fly_fishing": "any_method", "ice_fishing": "any_method",
          "set_lining": "any_method", "spear_fishing": "any_method",
          "crayfish_trapping": "any_method", "netting": "any_method", "snagging": "any_method",
          "chumming": "any_method", "roe": "any_bait", "invertebrate": "any_bait",
          "fin_fish": "any_bait", "dead_fin_fish": "fin_fish", "live_fin_fish": "fin_fish",
          "artificial_fly": "any_lure", "artificial_lure": "any_lure", "barbed": "any_barb",
          "barbless": "any_barb"}

METHODS = ("angling", "fly_fishing", "ice_fishing", "set_lining", "spear_fishing",
           "crayfish_trapping", "netting", "snagging", "chumming")

#: The members each set slot is answered for (the page's tiles ask exactly these).
ELEMENT_SLOTS = (("bait", ("roe", "invertebrate", "fin_fish")),
                 ("lure", ("artificial_fly", "artificial_lure")),
                 ("barb", ("barbed",)),
                 ("method", METHODS))

#: The tokens of a circumstance that are a way of fishing or a device (the page's METHOD_TOKENS):
#: a count held only while doing one of these is that method's, not the line's.
METHOD_TOKENS = ("fly_fishing", "ice_fishing", "set_lining", "spear_fishing", "crayfish_trapping",
                 "netting", "snagging", "chumming", "downrigger", "light", "ice_hut")

#: Conduct acts by MOMENT, with the page's short phrase (consumer 7.6). The order inside each
#: moment is the page's card order. An act in none of them goes under "also" with the model's own
#: sentence (`catalogue.CONDUCT_ACTS`, shipped as `guide.gear.conduct.acts[].means`).
MOMENTS: Tuple[Tuple[str, Tuple[Tuple[str, str], ...]], ...] = (
    ("release", (("release_immediately", "Right away, where you caught it"),
                 ("return_unfit_fish_gently", "Gently, back in the water"),
                 ("do_not_release_harmfully", "Without harming it"))),
    ("keep", (("do_not_high_grade", "It stays kept: no swapping for a bigger one"),
              ("do_not_waste_catch", "Don’t waste it"),
              ("do_not_keep_catch_alive", "Don’t hold it alive (no stringer or livewell)"),
              ("leave_head_tail_and_fins_until_residence", "Leave head, tail and fins on until home"),
              ("do_not_can_bottle_or_fillet_away_from_residence",
               "No canning or filleting until home"),
              ("do_not_freeze_in_unrecognizable_block",
               "Don’t freeze fish into one block: each must stay countable"),
              ("keep_catch_identifiable", "Each fish must stay identifiable and measurable"))),
    ("carry", (("transport_no_more_than_legal_limit", "Never more than your legal limit"),
               ("keep_licence_handy_while_travelling", "Licence at hand while you travel with fish"),
               ("produce_licence_on_request", "Show your licence when an officer asks"),
               ("carry_paper_licence", "Carry your paper licence"),
               ("carry_signed_letter_when_transporting_for_another",
                "Carrying someone else’s fish? Have their signed letter"),
               ("show_letter_when_exporting", "Taking fish out of B.C.? Show the letter if asked"),
               ("keep_signed_letter_for_gifted_fish",
                "Given fish? Keep the signed letter until eaten"))),
    ("never", (("do_not_buy_sell_or_barter_catch", "Sell, buy or trade your catch"),
               ("do_not_possess_or_move_live_fish", "Keep or move live fish or crayfish"),
               ("do_not_release_aquarium_fish", "Release aquarium fish"),
               ("no_gear_in_water_during_closure", "Put gear in the water during a closure"),
               ("do_not_interfere_with_furbearer_trap", "Interfere with a fur trap"),
               ("do_not_enter_land_without_permission",
                "Cross private, posted or reserve land without permission"))),
)

#: The page's order of the "Always" acts before they are dealt into moments.
ALWAYS_ORDER = ("return_unfit_fish_gently", "release_immediately", "do_not_release_harmfully",
                "do_not_high_grade", "do_not_waste_catch",
                "leave_head_tail_and_fins_until_residence", "transport_no_more_than_legal_limit",
                "do_not_keep_catch_alive", "do_not_enter_land_without_permission")

DECISIONS = [
    "G1 A rule-level `when_targeting` (white sturgeon's 'dead fin fish may be used', 3 rules) "
    "makes its clauses circumstantial ('when fishing for white sturgeon'); the page reads only a "
    "CLAUSE's `when.targeting` and would show it as a main clause.",
    "G2 Which gear rules are in force is the reader's traced answer for the key's gear bindings "
    "(lifts, beside, not_yet_mapped, competition). The (type, dimension) competition used to drop "
    "whole rules where only some clauses lose (Kootenay Lake's shore line count, the "
    "no-gear-during-closure duty); fixed at the source (gear and conduct dimensions carry their "
    "conditions, means and acts), so the rule-by-rule workaround is gone. A rule the reader lifts "
    "or displaces is listed under `overruled` with its state, reason and `by`; precedence among "
    "the rules in force is per slot and member (step 2), as the page does.",
    "G3 A clause's `when.water` on a key of unknown kind (a rule set on no named water) is kept "
    "as a circumstance `water=<kind>`, never resolved as main.",
    "G4 Within one rule, clauses on one slot are 'first match wins, no `when` is the last word' "
    "(guide.gear.reading). A circumstantial clause never answers the main slot, so this agrees "
    "with the page while the angler is unknown.",
    "G5 Worms have no element: they follow a whole-bait ban (`ban: [any_bait]`) only (page 7.3).",
    "G6 A method no clause answers: 'not allowed here' when the province allows it (an `allow` "
    "on `method` in a `zp:` rule with no `while`), else 'not a lawful way to sport fish'.",
]


# --------------------------------------------------------------------------------------------
# The element tree
# --------------------------------------------------------------------------------------------

def ancestors(e: str) -> List[str]:
    out = [e]
    while PARENT.get(out[-1]):
        out.append(PARENT[out[-1]])
    return out


def siblings_of(e: str) -> List[str]:
    return [k for k, p in PARENT.items() if p == PARENT.get(e)]


def says_about(c: dict, el: str) -> Optional[str]:
    """What a set clause says about one member: 'ban', 'allow' or None (page `saysAbout`)."""
    anc = ancestors(el)
    if c.get("ban") and any(x in anc for x in c["ban"]):
        return None if any(x in anc for x in c.get("except") or []) else "ban"
    if c.get("allow") and any(x in anc for x in c["allow"]):
        return "allow"
    if c.get("only"):
        if any(x in anc for x in c["only"]):
            return "allow"
        if any(el in siblings_of(x) for x in c["only"]):
            return "ban"
    return None


def province_methods(rules: Iterable[dict]) -> List[str]:
    """The ways the PROVINCE allows you to sport fish: an `allow` on the `method` slot in a `zp:`
    rule with no `while` (`guide.gear.methods.allowed_by_the_province`, the same predicate)."""
    return sorted({m for x in rules if str(x.get("entry_id")).startswith("zp:")
                   and not x.get("while")
                   for c in x.get("gear") or [] if c.get("slot") == "method"
                   for m in c.get("allow") or []})


# --------------------------------------------------------------------------------------------
# Step 2: resolve the clauses of the rules in force
# --------------------------------------------------------------------------------------------

def _spec(x: dict) -> int:
    from pipeline.deliver.answers.common import when_dates
    return (1 if x.get("origin") else 0) + (1 if x.get("water") else 0) + \
        (1 if when_dates(x.get("when")) else 0)


def _count_max(c: dict) -> float:
    return c["max"] if c.get("max") is not None else 1e9


class Rule:
    """A rule in force, as the resolver needs it: its key, its rank on this section (a rule
    reached by the tributary walk ranks 1 unless federal), its fields, and the methods of the rule
    it is a proviso of."""
    __slots__ = ("key", "rank", "x", "cond_methods")

    def __init__(self, key, rank: int, x: dict, cond_methods: Sequence[str] = ()):
        self.key, self.rank, self.x, self.cond_methods = key, rank, x, list(cond_methods)


def cond_methods(x: dict, parent: Optional[dict]) -> List[str]:
    """The methods a proviso's parent allows (page `condMethods`), angling aside."""
    if not x.get("condition_of") or not parent:
        return []
    return [m for c in parent.get("gear") or [] if c.get("slot") == "method"
            for m in (c.get("allow") or c.get("only") or []) if m != "angling"]


def _entries(active: Sequence[Rule], kind: Optional[str]) -> List[dict]:
    out = []
    for r in active:
        x = r.x
        if not (x.get("gear") or x.get("conduct")):
            continue
        for i, c in enumerate(x.get("gear") or []):
            w = c.get("when") or {}
            circ = list(x.get("while") or []) + list(r.cond_methods) + \
                [v for v in (w.get("method"), w.get("angler"), w.get("gear_in_use")) if v]
            if w.get("water"):
                if kind is None:
                    circ.append(f"water={w['water']}")
                elif w["water"] != kind:
                    continue
            circ = list(dict.fromkeys(circ))          # `while` and the proviso can both say it
            tgt = list(w.get("targeting") or []) or list(x.get("when_targeting") or [])
            out.append({"r": r, "i": i, "c": c, "circ": circ,
                        "targeting": expand(tgt) or tgt or None, "targeting_codes": tgt,
                        "note": w.get("note") or None})
    return out


def _main(e: dict) -> bool:
    return not e["circ"] and not e["targeting"] and not e["note"]


def resolve(active: Sequence[Rule], kind: Optional[str], lawful: Sequence[str], *,
            timed: Sequence[Rule] = (), in_part: Sequence[Rule] = (),
            side: Sequence[Rule] = (), overruled: Sequence = (),
            while_rules: Sequence[Rule] = (), caught: Sequence[Rule] = ()) -> dict:
    """The gear answer for one place and day, from the gear/conduct/vessel rules IN FORCE there
    (`active`, in the page's order: reach rules then walked ones, each by id). Rule references in
    the answer are the rules' `key`s; a clause is [key, clause index]."""
    entries = _entries(active, kind)
    main = [e for e in entries if _main(e)]
    circ = [e for e in entries if not _main(e)]

    def ref(e):
        return [e["r"].key, e["i"]]

    def order(e):
        return (e["r"].rank, -_spec(e["r"].x))
    counts: Dict[str, List[dict]] = {}
    specs: Dict[str, List[dict]] = {}
    for e in main:
        c = e["c"]
        if c.get("must_be") or c.get("requires"):
            specs.setdefault(c["slot"], []).append(e)
        elif "max" in c or "min" in c or c.get("unlimited"):
            counts.setdefault(c["slot"], []).append(e)
    for s in counts:
        counts[s].sort(key=lambda e: (order(e), _count_max(e["c"])))

    elems: Dict[str, dict] = {}
    for slot, members in ELEMENT_SLOTS:
        for el in members:
            hits = [(e, says_about(e["c"], el)) for e in main if e["c"].get("slot") == slot]
            hits = [(e, s) for e, s in hits if s]
            hits.sort(key=lambda h: (order(h[0]), 0 if h[1] == "ban" else 1))
            if hits:
                elems[f"{slot}:{el}"] = {"e": hits[0][0], "s": hits[0][1], "over": hits[1:]}

    def top(slot):
        return counts.get(slot, [None])[0]

    def verdict(k):
        x = elems.get(k)
        return x["s"] if x else None

    def method_ok(m):
        return verdict(f"method:{m}") in (None, "allow")

    def circ_for(slot):
        got = [e for e in circ if e["c"].get("slot") == slot
               and not any(t in METHOD_TOKENS for t in e["circ"])]
        if any("in_boat" in e["circ"] and e["c"].get("unlimited") for e in got):
            got = [e for e in got if "alone_in_boat" not in e["circ"]]
        return got

    def circ_ref(e):
        out = {"clause": ref(e)}
        if e["circ"]:
            out["while"] = e["circ"]
        if e["targeting_codes"]:
            out["targeting"] = e["targeting_codes"]
        if e["note"]:
            out["note"] = e["note"]
        return out

    ans: dict = {}
    ans["counts"] = {}
    for s, L in sorted(counts.items()):
        row = {"by": ref(L[0]), "over": [ref(e) for e in L[1:]]}
        also = [circ_ref(e) for e in circ_for(s)]
        if also:
            row["also"] = also
        ans["counts"][s] = row
    ans["specs"] = {s: [ref(e) for e in L] for s, L in sorted(specs.items())}
    ans["elements"] = {k: {"verdict": v["s"], "by": ref(v["e"]),
                           "over": [ref(e) + [s] for e, s in v["over"]]}
                       for k, v in sorted(elems.items())}
    ans["main"] = [ref(e) for e in main]
    ans["circumstantial"] = [circ_ref(e) for e in circ]

    # ---- the tiles' answers (7.2-7.6), structured; the page writes the words ------------------
    pts, barbed = top("points_per_hook"), elems.get("barb:barbed")
    barb_ban = bool(barbed and barbed["s"] == "ban")
    if pts or barb_ban:
        single = bool(pts and pts["c"].get("max") == 1)
        hook = ("single" if single else "any") + ("_barbless" if barb_ban else "")
    else:
        hook = "trebles_and_barbs"
    fly = None
    if verdict("lure:artificial_lure") == "ban" and verdict("lure:artificial_fly") == "allow":
        fly = "artificial_fly_only"
    fly_only = bool(verdict("method:fly_fishing") == "allow"
                    and elems["method:fly_fishing"]["e"]["c"].get("only"))
    ans["hook"] = hook
    ans["fly"] = "fly_fishing_only" if fly_only else fly

    any_ban = next((elems[f"bait:{k}"] for k in ("roe", "invertebrate", "fin_fish")
                    if verdict(f"bait:{k}") == "ban"
                    and "any_bait" in (elems[f"bait:{k}"]["e"]["c"].get("ban") or [])), None)
    pos = top("bait_possession_kg")
    bait = []
    for el in ("worms", "roe", "invertebrate", "fin_fish"):
        x = any_ban if el == "worms" else elems.get(f"bait:{el}")
        ok = x is None or x["s"] == "allow"
        row: dict = {"element": el, "ok": ok, "by": ref(x["e"]) if x else None}
        if x is not None:
            c, rx = x["e"]["c"], x["e"]["r"].x
            if x["s"] == "ban" and "any_bait" in (c.get("ban") or []):
                row["why"] = "bait_ban"
            elif rx.get("water"):
                row["why"] = f"{'not_on' if x['s'] == 'ban' else 'on'}_{rx['water']}s"
            elif x["s"] == "ban" and "roe" in (c.get("except") or []):
                row["why"] = "banned_roe_ok"
            elif x["s"] != "ban":
                row["why"] = "allowed"
        if el == "roe" and x is not None and x["s"] == "allow" and pos and \
                "roe" in (pos["c"].get("of") or []):
            row["carry_kg"] = pos["c"].get("max")
        probe = "dead_fin_fish" if el == "fin_fish" else el
        also = [circ_ref(e) for e in circ if e["c"].get("slot") == "bait"
                and (e["c"].get("allow") or e["c"].get("only"))
                and says_about(e["c"], probe) == "allow"
                and all(method_ok(m) for m in e["circ"])]
        if also:
            row["also_allowed"] = also
        bait.append(row)
    ans["bait"] = bait
    ans["bait_ban"] = all(verdict(f"bait:{k}") == "ban" for k in ("roe", "invertebrate", "fin_fish"))

    conduct_rules = [r for r in active if r.x.get("conduct")]
    ways = []
    for k in ["angling"] + ([] if kind == "stream" else ["ice_fishing"]) + \
            ["set_lining", "spear_fishing", "crayfish_trapping", "netting", "snagging", "chumming"]:
        x = elems.get(f"method:{k}")
        w: dict = {"method": k}
        if k == "angling" and fly_only:
            w.update(allowed=True, by=ref(elems["method:fly_fishing"]["e"]),
                     why="fly_fishing_only")
        elif x is None:
            w.update(allowed=False, by=None,
                     why="not_allowed_here" if k in lawful else "not_lawful")
        elif fly_only and k != "angling" and x["s"] == "ban" and x["e"]["c"].get("only"):
            w.update(allowed=False, by=ref(x["e"]), why="fly_fishing_only_here")
        else:
            w.update(allowed=x["s"] == "allow", by=ref(x["e"]))
            if x["s"] == "allow":
                tg = [e for e in circ if e["c"].get("slot") == "method"
                      and (e["c"].get("when") or {}).get("targeting") and says_about(e["c"], k)]
                nf = list(dict.fromkeys(t for e in tg if says_about(e["c"], k) == "ban"
                                        for t in e["c"]["when"]["targeting"]))
                of = list(dict.fromkeys(t for e in tg if says_about(e["c"], k) == "allow"
                                        for t in e["c"]["when"]["targeting"]))
                if nf:
                    w["not_for"] = nf
                if of:
                    w["for"] = of
                info = [circ_ref(e) for e in circ if k in e["circ"]
                        and e["c"].get("slot") != "bait"]
                if info:
                    w["while"] = info
                acts = list(dict.fromkeys(a for r in conduct_rules
                                          if k in (r.x.get("while") or [])
                                          for a in r.x["conduct"]))
                if acts:
                    w["conduct"] = acts
                wr = [r.key for r in while_rules if k in (r.x.get("while") or [])]
                if wr:
                    w["while_rules"] = wr
                if k == "angling":
                    dev = {}
                    for d in ("downrigger", "light"):
                        if d in specs:
                            dev[d] = {"must_be": [m for e in specs[d]
                                                  for m in e["c"].get("must_be") or []],
                                      "by": [ref(e) for e in specs[d]]}
                    lt = top("light_to_hook_mm")
                    if "light" in dev and lt:
                        dev["light"]["within_mm"] = lt["c"].get("max")
                    if dev:
                        w["devices"] = dev
        ways.append(w)
    ans["ways"] = ways

    # conduct, by moment (the "Always" tile): acts of conduct-carrying rules with no `while`
    rank_of = {a: i for i, a in enumerate(ALWAYS_ORDER)}
    always = sorted(((a, r.key) for r in conduct_rules if not r.x.get("while")
                     for a in r.x["conduct"]), key=lambda t: rank_of.get(t[0], 99))
    by_act: Dict[str, List] = {}
    for a, k in always:
        if k not in by_act.setdefault(a, []):
            by_act[a].append(k)
    known = {a for _, acts in MOMENTS for a, _ in acts}
    moments = {}
    for m, acts in MOMENTS:
        got = [[a, by_act[a]] for a, _ in acts if a in by_act]
        if got:
            moments[m] = got
    also = [[a, by_act[a]] for a in dict.fromkeys(a for a, _ in always) if a not in known]
    if also:
        moments["also"] = also
    ans["conduct"] = moments

    vessel = [r.key for r in active if r.x.get("family") == "vessel"]
    vessel_timed = [r.key for r in timed if r.x.get("family") == "vessel"]
    ans["vessel"] = {"active": vessel, "timed": vessel_timed}
    ans["timed"] = [r.key for r in timed if r.x.get("gear")]
    ans["in_part"] = [r.key for r in in_part]
    ans["side"] = [r.key for r in side]
    ans["while_rules"] = [r.key for r in while_rules]
    # a duty for a fish CAUGHT some way, in force here (user ruling Q38/G6): "Any fish snagged —
    # even by accident — must be released" — whatever the angler does, never a way to fish
    ans["caught"] = [r.key for r in caught]
    ans["overruled"] = list(overruled)
    # every rule that decides something, and every rule in force that decides nothing here (its
    # clauses all beaten, or it repeats a closer rule): the page's "All gear sources"
    won = {c[0] for v in ans["counts"].values() for c in [v["by"]]} | \
        {c[0] for v in ans["specs"].values() for c in v} | \
        {v["by"][0] for v in ans["elements"].values()} | \
        {c["clause"][0] for c in ans["circumstantial"]} | {r.key for r in conduct_rules}
    ans["decides"] = [r.key for r in active if r.key in won]
    ans["repeats"] = [r.key for r in active if r.key not in won
                      and (r.x.get("gear") or r.x.get("conduct"))]
    return ans


# --------------------------------------------------------------------------------------------
# Step 1: the reader says which rules are in force
# --------------------------------------------------------------------------------------------

def gear_subset(B: Bundle, bound: Sequence[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    """The bindings the gear answer reads: gear, conduct and vessel rules, rules with a `while` or
    a `caught` (a duty for a fish caught some way), and every rule lifting one of them."""
    from pipeline.regs.parsing.catalogue import PROTECTED_FISH
    every = B.rules
    stray = [rule_id((e, r)) for e, r, _ in bound if (e, r) not in every]
    if stray:
        raise AnswersError(f"gear: the rule set names rules the bundle lacks: {stray[:3]}")
    # A duty about PROTECTED fish only ("If you accidentally catch one, you must release it right
    # away", zp:protected_species.r3 — named since the RULES round) is about fish no gear answer is
    # asked for; the rows show it beside the protected-species closure.
    protected = lambda x: bool(x.get("species")) and all(c in PROTECTED_FISH  # noqa: E731
                                                         for c in x["species"])
    want = {(e, r) for e, r, _ in bound if (e, r) in every and not protected(every[(e, r)])
            and (every[(e, r)].get("family") in GEAR_FAMILIES or every[(e, r)].get("while")
                 or every[(e, r)].get("caught"))}
    lifters = {(e, r) for e, r, _ in bound if (e, r) in every
               and any((l["entry_id"], l["rule_id"]) in want
                       for l in every[(e, r)].get("exempts") or [])}
    keep = want | lifters
    return [b for b in bound if (b[0], b[1]) in keep]


class NoFishToAsk(ValueError):
    """A gear-relevant rule that speaks for no fish the reader can be asked about."""


def _ask_fish(x: dict) -> str:
    from pipeline.regs.parsing.catalogue import BOOK_SPECIES, SALMON_FISH
    for f in ("RB",) + tuple(f for f in BOOK_SPECIES if f != "RB") + tuple(SALMON_FISH):
        if read.speaks_for(x, f):
            return f
    raise NoFishToAsk(f"gear: {x['entry']}::{x['rule']} speaks for no fish the reader answers")


def states(B: Bundle, bound: Sequence[Tuple[str, str, str]], md, store, key_ix: int,
           reading: int, at=None) -> Dict:
    """`{(entry, rule): (state, partly_lifted) | (loss state, reason, by)}` for every
    gear-relevant rule in force on day `md`: THE READER'S ANSWER for the key's FULL rule set —
    the stored verdict of the key's reading (`verdicts.sqlite`, DATAFLOW P6; it used to re-ask the
    reader over a subset of the bindings) — each rule read for the first fish it speaks for (a
    gear rule names no fish; one held only while fishing for white sturgeon speaks for that fish).
    Its lifts (dated, per fish), `beside` (some hours, one side of the channel),
    `not_yet_mapped` and its (type, dimension) competition — which no longer drops a clause the
    winner never spoke about: gear and conduct dimensions carry their clause conditions, their
    means and their acts (`catalogue.CatalogueRule.dimension`, 2026-10-06). A rule that lost
    carries the reader's loss state, `reason` and `by`."""
    every = B.rules
    groups: Dict[str, List[Tuple[str, str]]] = {}
    for e, r, v in bound:
        k = (e, r)
        x = every[k]
        if not (x.get("family") in GEAR_FAMILIES or x.get("while") or x.get("caught")):
            continue
        if read.in_force(x.get("when"), md, at) == "no" or x.get("dimension") == "lift":
            continue
        groups.setdefault(_ask_fish(x), []).append(k)
    out: Dict = {}
    for fish, ks in sorted(groups.items()):
        got = {}
        for r, st, rs, by, lift in store.rows(store.verdict_id(key_ix, reading, fish, "none")):
            got[tuple(store.rule_ids[r].split("::", 1))] = (
                T.by_code(T.RuleState, st).value, bool(lift),
                None if rs is None else T.by_code(T.LossReason, rs).value,
                None if by is None else tuple(store.rule_ids[by].split("::", 1)))
        for k in ks:
            y = got.get(k)
            if y is None:
                raise AnswersError(f"gear: the stored verdict does not hold {rule_id(k)} for "
                                   f"{fish}, though it is in force and speaks for it")
            if y[0] in read.SPEAKER_STATES:
                out[k] = (y[0], y[1])
            else:
                out[k] = (y[0], y[2], y[3])
    return out


def gear_answer(B: Bundle, key: RuleKey, md, lawful: Sequence[str],
                ref: Callable = None, *, store, reading: int, at=None) -> dict:
    """The gear answer for one rule key on one reading of its year (asked on day `md` of it, at
    moment `at` — `calendar.Moment`)."""
    every = B.rules
    bound = gear_subset(B, B.sets[key.set_id])
    kind = B.set_kind.get(key.set_id)
    st = states(B, bound, md, store, B.key_ix[key], reading, at)
    via = {(e, r): v for e, r, v in bound}
    order = sorted(via, key=lambda k: (via[k] != "reach", f"{k[0]}::{k[1]}"))
    ref = ref or (lambda k: k)

    def mk(k):
        x = every[k]
        parent = every.get((k[0], x["condition_of"])) if x.get("condition_of") else None
        return Rule(ref(k), read.place(x, via[k]), x, cond_methods(x, parent))
    active, timed, in_part, side, wr, overruled, caught = [], [], [], [], [], [], []
    for k in order:
        x = every[k]
        if kind and x.get("water") and x["water"] != kind:
            # M3: the binding enforces a rule's `water` (feature_types); one bound to the other
            # kind of water is a binding defect, never filtered here
            raise AnswersError(f"gear: {rule_id(k)} is for {x['water']}s, bound to a {kind}")
        s = st.get(k)
        if s is None:
            continue                                     # not in force today
        state = s[0]
        if state not in read.SPEAKER_STATES:
            overruled.append({"rule": ref(k), "state": state, "reason": s[1], "by": ref(s[2])})
            continue
        if x.get("family") not in GEAR_FAMILIES:
            if state == "speaks":
                # a `caught` duty (Q38), else a `while` rule of the retention family
                (caught if x.get("caught") else wr).append(mk(k))
            continue
        if state == "speaks":
            active.append(mk(k))
        elif state == "beside":
            (side if x.get("side") else timed).append(mk(k))
        elif state == "not_yet_mapped":
            in_part.append(mk(k))
    return resolve(active, kind, lawful, timed=timed, in_part=in_part, side=side,
                   overruled=overruled, while_rules=wr, caught=caught)


def gear_readings(B: Bundle, key: RuleKey, lawful: Sequence[str], ref: Callable = rule_id, *,
                  store) -> List[dict]:
    """The key's gear answer per READING of the verdicts (each asked on its first day, at its
    moment)."""
    k = B.key_ix[key]
    ms = store.moments(k)
    return [gear_answer(B, key, month_day(rd.first_day), lawful, ref, store=store, reading=rd.ix,
                        at=ms[rd.moment])
            for rd in store.readings(k)]


def gear_year(B: Bundle, key: RuleKey, lawful: Sequence[str], ref: Callable = rule_id, *,
              store, moment: Optional[int] = None) -> Dict[int, dict]:
    """The key's gear answer over the year at one moment (a key with one: None), `{start_day:
    answer}`: one per READING of the key (the verdicts', `store.runs`), equal neighbours merged;
    equal readings are answered once."""
    k = B.key_ix[key]
    got = gear_readings(B, key, lawful, ref, store=store)
    out: Dict[int, dict] = {}
    last = None
    for d, i in store.runs(k, moment):
        s = dumps(got[i])
        if s != last:
            out[d] = got[i]
            last = s
    return out


def static_tables() -> dict:
    """What every gear answer refers to: the element tree, the conduct moments with the page's
    short phrase and the model's own sentence (`guide.gear.conduct.acts[].means`)."""
    from pipeline.regs.parsing.catalogue import CONDUCT_ACTS
    return {"parent": PARENT, "methods": list(METHODS),
            "moments": [[m, [[a, phrase, CONDUCT_ACTS.get(a)] for a, phrase in acts]]
                        for m, acts in MOMENTS],
            "conduct_means": dict(sorted(CONDUCT_ACTS.items()))}


# --------------------------------------------------------------------------------------------
# The `gear` section of the answers file
# --------------------------------------------------------------------------------------------

def section_scope(key: tuple, B) -> RuleKey:
    """Gear reads the part's rule key (its set's gear bindings, the water kind of the set, the
    steelhead flags)."""
    from pipeline.deliver.answers.common import TIDAL_SCOPE, is_tidal, rule_key
    return TIDAL_SCOPE if is_tidal(key) else rule_key(key)


def section_prepare(scope: RuleKey, ctx):
    """(reading per day, [gear answer per run]) for one rule key, rules by export index. Tidal
    water: all year the documented tidal state (`common.TIDAL_STATE`) — no provincial gear rule,
    never "angling not allowed here" (FIX D12)."""
    import json
    from pipeline.deliver.answers.common import TIDAL_SCOPE, TIDAL_STATE
    from pipeline.deliver.calendar import DAYS
    if scope == TIDAL_SCOPE:
        return [0] * DAYS, [dict(TIDAL_STATE)]
    B = ctx.B
    lawful = ctx.cache.get("lawful")
    if lawful is None:
        lawful = ctx.cache["lawful"] = province_methods(B.rules.values())
    # per SLOT (day x moment, `answers.slots`): each reading's answer, equal answers once
    st, k = ctx.store, B.key_ix[scope]
    got = gear_readings(B, scope, lawful, ref=lambda k: ctx.rule_index[rule_id(k)], store=st)
    values: List[dict] = []
    seen: Dict[str, int] = {}
    ix = []
    for g in got:
        s = dumps(g)
        if s not in seen:
            seen[s] = len(values)
            values.append(json.loads(json.dumps(g)))
        ix.append(seen[s])
    from pipeline.deliver.answers.answers import slots
    return [ix[r] for r in slots(st, k)], values


def section_static(ctx, data: dict, guide: dict, keys, parts) -> dict:
    """What every gear answer refers to: the province's lawful methods and the element tree,
    conduct moments and phrases (`static_tables`)."""
    return {"province_methods": province_methods(ctx.B.rules.values()), **static_tables()}
