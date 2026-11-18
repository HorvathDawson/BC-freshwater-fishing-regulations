"""THE TROUT AND CHAR TABLE BUILDER (trout table plan, T810 ticket F; contract 2.2 to 2.6). Not wired into a release:
ticket I calls `compose_key` once per answer key, after the rows post-passes and the display compose, behind
`--trout-table`.

    compose_key(rows, displays, starts, moments, rules) -> [TroutFrame dict | None per segment]

For each (answer key, segment) it builds the `model.trout.TroutFrame` of the contract, validated by that model (THE
source of the shape): keep boxes in Python's order, their rows and bands, the release and closed groups, the top line
with its one `{day}` slot, the fold, `together`, the year bar, the `DateSet` (E's next change, G's date words), and
`covers` (the rows-frame rows the table replaces, every row that mixes trout or char with other fish SPLIT first:
`split_mixed`, contract A0 amendment). Every text comes from G (`trout/words.py`); every number from the engine's
answers as the rows frame stores them. The table carries no notes (owner 17:40: angler-only notes stay in the page's
caveat block).

WHAT IT READS, AND WHY NOT A SEPARATE DRAFT IN THE DERIVE (contract 2.2). The contract drafts the body in the rows
derive because the shared T805 helpers need the reader's `P` and `model`. Every input those helpers read is in the
POST-PASS rows frame and the rule table: `card.possession_duty` / `possession_rules` read the decided answers' roles and
lines and `P.by_key` (`reading` rebuilds exactly those, as F0's frame-level test does); `tree.across_rows` reads the
rows' conditions; `A_BOTH_LAKES_SAID` and the card's size, duty and count words are already in the stored group tree
(`card_of` ran in the derive). So the body is built in `compose_key` from post-pass values only: the weekday words
(`rows[].week` after `say_weekdays`), the stamps and the steelhead line can never disagree with the page, and one code
path serves the build, the committed fixture releases and a built release (`pipeline/tools/audit/trout_table.py`). The
group tree must still be on the frame: I composes before L drops `group` from covered rows at encoding.

THE RULES (spec 3.1 and the T810 accepted list; each pinned by a mutation flag below and a test that fails with it off):
  rows        one per fish; wild and hatchery with the same rule: one row, no mention; only hatchery may be kept: one
              row with the filled "Hatchery only" label, no wild row under Release, and the label moves to the box when
              every row of the box carries it; wild shown apart (twin rows "RB/hatchery", "RB/wild") only where wild
              fish have a quota or size of their own
  bands       "up to the N" one cell per row; a tighter limit as a white box: own "only 3", shared "only 1 of these 4"
              over consecutive rows (rows are ordered so each shared limit is one piece); a limit inside another stays a
              bold line under its fish; a tier whose number is not its rule's (a summed tier: Kamloops) is never drawn
  strip       the box's size cap when it holds every fish of the box that can pass it ("only 1 over 50 cm (20 in)");
              a wild quota of the box's own ("only 2 wild, each 40 cm (16 in) or less"); a possession line every row
              shares; a fish that can only be kept over the mark (or a steelhead under a plain 1-over-50) "uses the 1"
  possession  on a row only where `card.possession_duty` gives a line (it differs from the page's lead, F0); below the
              day's number a "possession" band and a quiet line (Cunningham)
  together    a one-origin limit across two keep boxes (`tree.across_rows`: Lois's wild 2) or the lake's own count
              "from both lakes" (`tree.A_BOTH_LAKES_SAID`: Echoes); the top line's second half in the same words
  year bar    only when every trout and char fish shares one state today (all release, or all closed) on an open water
  NULL        exactly per Q-D3: the display frame says `closed` or `tidal`, or the frame asks about no trout or char fish
  one day     a one-day segment that only loosens a row keeps the day before's stricter row, the sheet says the day
              (spec 3.3 rule 6: Adams River's Jul 31, owner "80 cm on the row")

LEFT TO OTHER TICKETS (the fields exist and are valid; their content is the other ticket's): ticket H fills the fish
sheet (here only its head, the row's own lines and the first 4 rules; H adds the licence / stamp line from the post-pass
stamps, the "how to tell it" link and the coming-change sentence's place) and `elsewhere` (empty here, the link hidden:
its lines say "today", H words them); ticket I wires `compose_key`, the codec and `FRAMES["trout"]`.
"""
from __future__ import annotations

import types
from collections import OrderedDict
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.trout import sheets as S
from pipeline.deliver.answers.trout import timeline as TL
from pipeline.deliver.answers.trout import words as W

TC = common.trout_char()
INF = float("inf")

# Mutation pins (False = the broken rule; each has a test in `test_trout_table.py` that fails with it off):
#: a tier whose number differs from its rule's take, or that its fish cannot reach together, is the sum of other
#: limits and is never drawn (spec 3.1; Kamloops' 3, Adams p3, Williston B)
A_SUMMED_TIER_NEVER_DRAWN = True
#: wild and hatchery with the same rule: one row and no origin word (owner 15:48). False = twin rows wherever both
#: origins keep
A_SAME_RULE_NO_ORIGIN = True
#: only hatchery may be kept: one row with the label. False = the fish also gets a wild row under Release
A_HATCHERY_ONLY_ONE_ROW = True
#: the label moves to the box when every row of the box carries it (owner 16:00). False = it stays on each row
A_BOX_LABEL_WHEN_EVERY_ROW = True
#: a possession line only where `possession_duty` gives one. False = every kept row also says the page's lead
A_POSSESSION_ONLY_WHERE_IT_DIFFERS = True
#: a row that mixes trout or char with other fish is split (A0 amendment). False = the mixed row is covered whole
A_MIXED_ROWS_SPLIT = True
#: the year bar only when every fish shares one state (owner 12:42). False = on every table
A_YEAR_BAR_ONE_STATE = True
#: spec 3.3 rule 6 (owner 15:36 "Yes, 80 cm on the row"): a ONE-DAY segment that only loosens a row between two equal
#: neighbours (Adams River's Jul 31, 60 cm for one day) keeps the neighbours' stricter row; its sheet says the day.
#: False = the one day's looser words on the row
A_ONE_DAY_STRICTER_ON_ROW = True
# Found by the proof (ticket J, `trout/check.py`) over the whole side build; each pin's False is the old wording:
#: a kind's number holds its fish TOGETHER ("4 brook, brown, cutthroat or rainbow trout combined"): drawn as a shared
#: band "only 4 of these 4", never as each fish's own "only 4" (the summed-tier rule was dropping it)
A_KIND_NUMBER_IS_SHARED = True
#: a kind's size cap holds its fish together ("up to 1 over 50 cm" on a kind of four fish): the box's strip when it holds
#: every fish of the box that can pass it, else each row's "only 1 over 50 cm (20 in), counting any … you keep"
A_KIND_SIZE_CAP_IS_SHARED = True
#: a rainbow over the steelhead size is a steelhead where the definition holds (its decided answer carries the `steel`
#: line): a rainbow row never passes a size cap above it, and "uses the 1" is said only of a fish kept only over the cap
A_RAINBOW_STOPS_AT_THE_STEELHEAD_SIZE = True
#: a wild quota is read off every keep row of the frame holding the box's fish (a fish's own row: Kitimat's rainbow),
#: and stands in the strip only when it holds every row of the box that may keep wild fish
A_WILD_QUOTA_FROM_EVERY_ROW = True
#: boxes counted apart: the top line says each box's most here (its card's headline: a share inside the total), never
#: the region's total the box head says ("Up to 8 brook trout and 2 other trout and char", not "… 5 other …")
A_APART_SAYS_THE_BOX_MOST = True
#: a stream share holds every fish its rule holds that no water's rule lifts out of it (the group's `subcaps[].of`), a
#: fish under its water's own number beside the share included: Kaslo River and Keen Creek's "Bull trout daily quota = 2"
#: lifts nothing (Lardeau's and the Duncan's same words do: their rows print the lift), so Dolly Varden count toward the
#: 2 from streams — one band "only 2 of these 6" and the top line "Up to 2 {day}". False = the fish gets its own band
#: beside the share and the top line adds ", plus 2 Dolly Varden/bull trout" (the engine keeps 2 in all)
A_SHARE_HOLDS_ITS_UNLIFTED_FISH = True
#: a one-origin quota that is a count the box's other fish share (its rule is one of the group's `subcaps`, same number:
#: Region 6's "1 trout from streams Jul 1-Oct 31", which Kitimat's hatchery rainbow quota lifts for hatchery fish only)
#: joins that count's band: "only 1 of these 3" over brown, cutthroat and the wild rainbow row, the twin rows kept side
#: by side. False = the wild row's own "only 1 wild" beside "only 1 of these 2" (the engine holds the three to 1)
A_WILD_QUOTA_JOINS_ITS_COUNT = True


#: T834: with a lift past the share (the top line's "or 20 if they’re brook trout"), its first number is what the OTHER
#: fish come to here: the headline, or less where the root's tiers holding them add up to fewer (Perry Creek on Jun 16:
#: its other trout under the St. Mary's "trout/char 1 on all tributaries", 1, beside the brook trout's 20). False = the
#: headline's 2 (the proof refuses it)
A_TOP_SAYS_WHAT_THE_OTHER_FISH_COME_TO = True


def _others_most(g: dict) -> float:
    """The most the fish no lift of `g` names may come to together: each top-level node of the tree holding any of them,
    at its number (a tier's, or a kind's), summed. INF where one says no number."""
    lifted = {S for L in g.get("lifts") or () for S in L["kinds"]}

    def most(nodes) -> float:
        total = 0.0
        for x in nodes:
            fish = {S for n, _c in _walk([x]) for S in n["species"]} if x["node"] == "tier" else set(x["species"])
            if not fish - lifted:
                continue
            if x["node"] == "tier" and x["tier"] == "size":
                total += most(x["children"])            # a size cap is no count: what it holds counts
                continue
            if x.get("n") is None:
                return INF
            total += x["n"]
        return total
    return most(g["tree"][0]["children"])


#: T834: a share RAISED below the total is the box's number over every fish of the box (the Peace River's "Keep 3 in
#: all"). False = the zone's 5 (the proof finds a basket the table accepts and the engine refuses)
A_RAISED_SHARE_IS_A_BAND = True


#: T834: a wild quota holds every fish of the box its rule names, whichever row printed it (Lois / Khartoum Lake's "wild
#: trout/char 2" over the rainbow the raised total brought into the box). False = the condition's own row's members only
A_WILD_QUOTA_HOLDS_EVERY_FISH_OF_ITS_RULE = True


#: T834 (owner 13:15): a box whose region cap a water's own quota raised leads with what the other fish come to and
#: names the exception — "Keep 5, brook trout up to 20". False = "Keep 20 in all"
A_HEAD_LEADS_WITH_THE_REGION = True


# T810 G2 (the owner's sentence answers, OWNER-TODO 2026-11-17 15:34, 15:41, 15:46; pictures scratchpad t810-g2). Each
# flag's False is today's (pre-G2) wording; a test in `test_trout_g2.py` fails with it off.
#: Q1 "What you keep here": a box whose limits never let its fish reach its head's number says what they come to here
#: ("Keep 1 in all", each fish "any of the 1": Echoes Lakes), never the region's 5 beside "only 1 of these 6"
A_HEAD_SAYS_WHAT_IS_KEPT_HERE = True
#: Q2: a shared limit equal to a raised box's first number ("Keep 5, brook trout up to 20") is each row's "any of the 5"
A_SHARED_BOX_NUMBER_IS_ANY_OF = True
#: Q3a: a size cap on one fish's own row reads "only 1 over 50 cm (20 in)", never the card's "up to 1 cutthroat trout
#: over 50 cm (20 in)"
A_ONE_FISH_SIZE_CAP_SAYS_ONLY = True
#: Q3b: a size cap every fish of the box holds but a few is said once in the strip, "…, not counting lake trout"
A_SIZE_CAP_ONCE_WITH_ITS_EXCEPTION = True
#: Q3b follow-up (15:41, Tagish lake trout): a keep range with a slot says what goes back, the book's bounds exact
A_SLOT_SAYS_WHAT_TO_RELEASE = True
#: Q3b follow-up: a water's possession count equal to the fish's own day number is not said again on its row
A_POSSESSION_EQUAL_TO_THE_DAY_UNSAID = True
#: Q4: a possession every row of a box shares stands on its own line under the strip (`Box.possession`), "In your …"
A_BOX_POSSESSION_OWN_LINE = True
#: Q6: every fish of the box at any size: the strip says "any size" once, the rows nothing
A_ANY_SIZE_ONCE = True
#: Q7: the stop duty names the water by the kind its own name says (`stop_noun`), else "this water"
A_STOP_NOUN_FROM_THE_NAME = True
#: Q8: a closed table whose fish reopen on one day, but for fish closed all year: the day once under the top line, and
#: "closed all year" on those rows
A_REOPENING_ONCE_UNDER_THE_TOP = True
#: Q9 (owner, option 1): a wild quota over twin rows groups them: hatchery rows first, then the wild rows under one band
#: "only 2 wild in all" (the strip no longer says it)
A_WILD_ROWS_GROUPED = True
#: Q10: possession below the day's number is said once beside the white box: the quiet line takes the size clause
A_POSSESSION_SAID_ONCE = True
#: bug (owner 15:41): the lakes' own count ("only 1 from both lakes together", Echoes) is said once, under the top line
A_LAKES_LIMIT_SAID_ONCE = True
#: bug (owner 15:41): no year bar on a closed table (Fraser River in Region 3, October)
A_NO_YEAR_BAR_ON_A_CLOSED_TABLE = True


def stop_noun(names: Iterable[Optional[str]]) -> str:
    """Q7 (owner 15:41, verbatim "waters own kind default to thisa water when not known"): the noun the stop duty names a
    key's waters by — the kind their own names end in ("creek", "river", "slough": `card._WATER_NOUNS`) when every water
    of the key says the same one, else "water" (a key's table is shared by every water of the key)."""
    import re
    from pipeline.deliver.answers.card import _WATER_NOUNS
    kinds = set()
    for nm in names:
        last = re.sub(r"[^a-z]", "", ((nm or "").split() or [""])[-1].lower())
        kinds.add(_WATER_NOUNS.get(last))
    return kinds.pop() if len(kinds) == 1 and None not in kinds else "water"


class TableError(AnswersError):
    """A frame the table cannot say truthfully: the build stops (AGENTS W46), it never guesses."""


# ============================================================================================
# The rules a frame names, and the reading the shared helpers need
# ============================================================================================

def rule_of(d: dict):
    """One exported or stored rule (`export_subset()["rules"]`, the export doc's `rules`, the store's `rule` table)
    as the rows engine's `R`."""
    from pipeline.deliver.answers.rows import R
    prov = d.get("provenance") or d.get("prov") or {}
    x = {**(d.get("fields") or {}), "entry": d["entry_id"], "rule": d["rule_id"], "entry_id": d["entry_id"],
         "rule_id": d["rule_id"], "type": d["type"], "family": d.get("family"), "dimension": d.get("dimension"),
         "label": d.get("label"), "entry_name": prov.get("entry_name"), "_rank": prov.get("rank", 0)}
    return R(f"{d['entry_id']}::{d['rule_id']}", x, "reach")


class Rules:
    """A frame's rule refs (`RuleIx`, the export index) -> the rows engine's `R`."""

    def __init__(self, by_ref: Mapping[int, object]):
        self.by_ref = dict(by_ref)
        self.by_key = {r.key: r for r in self.by_ref.values()}

    def __call__(self, ref):
        try:
            return self.by_ref[ref]
        except KeyError:
            raise TableError(f"trout table: the frame names rule {ref!r}, which the rule table does not hold") from None

    @classmethod
    def from_export(cls, data: dict, doc: dict) -> "Rules":
        """The export pair as decoded (`common.load_export`, `export.codec.expand`): ref = index into `rule_ids`."""
        return cls({i: rule_of(doc["rules"][rid]) for i, rid in enumerate(data["rule_ids"])})

    @classmethod
    def from_store(cls, st) -> "Rules":
        """A release's regs store (`store.release.province_store`): ref = its `rule.rix`."""
        return cls({rix: rule_of(st.val("rule", b)) for rix, b in st.con.execute("SELECT rix, j FROM rule")})


def reading(frame: dict, rules: Rules):
    """(P, model) as `card.possession_duty` reads them: `P.by_key` and per fish the decided answers with their roles by
    rule key and their lines holding their rules (the reader's answers as stored; nothing is decided again)."""
    def res(d):
        if d is None:
            return None
        return {**d, "roles": {rules(k).key: {"role": role, "by": by} for k, role, by in d["roles"]},
                "lines": [{**ln, "r": rules(ln["r"])} for ln in d["lines"]]}
    P = types.SimpleNamespace(by_key=rules.by_key)
    model = {"R": {S: {"H": res(v.get("hatchery")), "W": res(v.get("wild"))} for S, v in (frame.get("fish") or {}).items()}}
    return P, model


# ============================================================================================
# Mixed rows (A0 amendment): the trout part and a remainder row
# ============================================================================================

_FISH_LISTS = ("members", "all_members", "prot")


def _only(row: dict, keep: Callable[[str], bool]) -> dict:
    o = dict(row)
    for k in _FISH_LISTS:
        if o.get(k) is not None:
            o[k] = [S for S in o[k] if keep(S)]
    o["groups"] = [g for g in (o.get("groups") or []) if any(keep(S) for S in g["members"])]
    o["groups"] = [{**g, "members": [S for S in g["members"] if keep(S)]} for g in o["groups"]]
    if o.get("week") is not None:
        sp = [S for S in o["week"]["species"] if keep(S)]
        o["week"] = {**o["week"], "species": sp} if sp else None
    if o.get("presence") is not None and not keep("ST"):
        # the steelhead presence line sits on the steelhead's own row (`model.Row`): the part holding ST keeps it
        # (T810 cutover, found on the live bundle: a release row of steelhead and other fish)
        o["presence"] = None
    return o


def split_mixed(frame: dict) -> Tuple[dict, List[int]]:
    """THE A0 AMENDMENT (contract 2.0, Q-D4): every rows-frame row whose fish are partly trout or char and partly not is
    SPLIT into its trout part (covered by the table) and, right after it, a remainder row holding the other fish, so
    `covers` stays whole rows and the app never splits or rewords a row. Returns (the frame with the split rows, the
    index of every row the table covers). A keep row that mixes them stops the build (none in r-46e4355a7277: the 622
    mixed rows are closed or release; a keep row's group would have to be split, which no rule here says)."""
    rows = frame.get("rows") or []
    out: List[dict] = []
    covers: List[int] = []
    moved: Dict[int, int] = {}
    for i, r in enumerate(rows):
        fish = set(r.get("all_members") or ()) | set(r.get("members") or ())
        t, o = fish & TC, fish - TC
        moved[i] = len(out)
        if t and o and A_MIXED_ROWS_SPLIT:
            if r.get("kind") in common.KEEPS or r.get("group") is not None:
                raise TableError(f"trout table: keep row {i} mixes trout or char {sorted(t)} with {sorted(o)}; the "
                                 "table cannot cover part of a keep row")
            covers.append(len(out))
            out.append(_only(r, lambda S: S in TC))
            out.append(_only(r, lambda S: S not in TC))
            continue
        if t:
            covers.append(len(out))
        out.append(r)
    for k, r in enumerate(out):
        # a copy, never the row itself: rows are shared between keys and segments, and the cutover splits the same
        # frame again (`answers.cut_over`), so a parent moved in place would move twice (T810 L)
        if r.get("parent") is not None and moved[r["parent"]] != r["parent"]:
            out[k] = {**r, "parent": moved[r["parent"]]}
    return {**frame, "rows": out}, covers


# ============================================================================================
# One fish: its state, its origin case, its law
# ============================================================================================

def fish_states(frame: dict) -> Dict[str, str]:
    """Every trout and char fish the frame answers -> its state today (keep / release / closed), its leading origin's
    (`common.lead_name`, the one picker)."""
    out = {}
    for S, fa in (frame.get("fish") or {}).items():
        if S not in TC:
            continue
        lead = common.lead_name(fa.get("hatchery"), fa.get("wild"))
        if lead is not None:
            out[S] = TL._STATE[fa[lead]["status"]]
    return out


def origin_case(fa: dict) -> str:
    """"same" (one row, no origin word), "hatchery_only" (one row, the label), "twin" (wild has a quota or size of its
    own: two rows), "wild_only" (refused: no word for it)."""
    h, w = TL.law_of(fa.get("hatchery")), TL.law_of(fa.get("wild"))
    hk, wk = TL._keeps(h), TL._keeps(w)
    if hk and wk:
        return "same" if (h == w and A_SAME_RULE_NO_ORIGIN) else "twin"
    if hk:
        return "same" if w is None else "hatchery_only"
    if wk:
        return "same" if h is None else "wild_only"
    return "same"


def _keep_range(law) -> Tuple[float, float]:
    ks = [(x, y) for x, y, n in TL.pieces([law], law) if n > 0]
    if not ks:
        raise TableError("trout table: a keep row keeps nothing")
    return ks[0][0], ks[-1][1]


def _sides(rules: Rules, refs: Iterable) -> Dict[float, str]:
    from pipeline.deliver.answers.caps import rule_sides
    sides: Dict[float, str] = {}
    got = []
    for ref in dict.fromkeys(refs):
        got += rule_sides(rules(ref))
    for x, side, _w in sorted(got, key=lambda t: t[2]):
        sides.setdefault(x, side)
    return sides


def law_size(dec: dict, rules: Rules) -> str:
    """One origin's keep range in the boundary standard's words, its edges as the deciding rules draw them ("under
    40 cm (16 in)", "40 cm (16 in) or less"): the size line of a twin row."""
    from pipeline.deliver.answers.caps import edges
    law = TL.law_of(dec)
    a, b = _keep_range(law)
    sides = _sides(rules, [ln["r"] for ln in dec.get("lines") or () if ln["t"] in ("rel", "cap", "outersize")])
    return common.band_words(a, None if b == INF else b, *edges(sides, a, b))


# ============================================================================================
# The body of one segment
# ============================================================================================

class _Row:
    """A table row while the body is built."""
    __slots__ = ("key", "fish", "origin", "label", "sub", "lines", "quiet", "src", "kind_n", "kind_ix", "chain",
                 "detail", "duties", "also", "band", "state", "poss", "sizes", "size_text", "size")

    def __init__(self, key, fish, origin=None):
        self.key, self.fish, self.origin = key, fish, origin
        self.label = self.sub = None
        self.lines: List[str] = []
        self.quiet: List[str] = []
        self.src: List[int] = []
        self.kind_n = None
        self.kind_ix = None
        self.chain: Tuple[dict, ...] = ()
        self.detail = None
        self.duties: Tuple[str, ...] = ()
        self.also: Tuple[dict, ...] = ()
        self.band = None               # (kind, n, group id)
        self.state = "keep"
        self.poss = None               # (line, count) from possession_duty / possession_rules
        self.sizes = None              # the kind's keep ranges as the card states them (`tree` kind `sizes.cm`)
        self.size_text = None          # the size line the row says first (the sheet's "Size: …")
        self.size = None               # the row's size, said or not ("any size" under a box's size strip)

    def add(self, text: Optional[str], where: str = "lines"):
        lst = getattr(self, where)
        if text and text not in lst:
            lst.append(text)


def _walk(nodes, chain=()):
    for n in nodes:
        if n["node"] == "tier":
            yield from _walk(n["children"], chain + (n,))
        else:
            yield n, chain


def _summed(t: dict, rules: Rules) -> bool:
    """A tier whose number is not its rule's take: the sum of two limits (Kamloops, Adams p3, Williston B)."""
    take = rules(t["by"]).f.get("take")
    return A_SUMMED_TIER_NEVER_DRAWN and isinstance(take, (int, float)) and t["n"] is not None and take != t["n"]


def _row_src(fa: dict, key: str, extra: Iterable = ()) -> List[int]:
    """A row's rules: the rule that decides its state or count first, then its size, origin, possession and duty
    lines, the other origin's deciding rule, then `extra` (contract Q-D6), no repeats."""
    S, _, o = key.partition("/")
    lead = o or common.lead_name(fa.get("hatchery"), fa.get("wild"))
    out: List[int] = []
    for org in [lead] + [x for x in common.ORIGINS if x != lead and not o]:
        d = fa.get(org)
        if d is None:
            continue
        out.append(d["win"])
        out += [ln["r"] for ln in d.get("lines") or ()]
        out += [k for k, role, _by in d.get("roles") or () if role in ("possession", "possession_cap")]
    out += list(extra)
    return list(dict.fromkeys(out))


class Body:
    """One segment's table body, built from the post-pass rows frame (`split_mixed` already applied)."""

    def __init__(self, frame: dict, rules: Rules, licence: Optional[dict] = None, noun: Optional[str] = None):
        self.frame, self.rules = frame, rules
        #: Q7: the stop duty's noun for this key's waters (None: the stored duty's)
        self.noun = noun if A_STOP_NOUN_FROM_THE_NAME else None
        #: the licence answer at the same key and segment, post-pass (`drop_closed_steelhead_stamp` ran): the sheet's
        #: stamps are read from it (ticket H, contract 2.2)
        self.docs = S.documents(licence)
        self.fish = frame.get("fish") or {}
        self.states = fish_states(frame)
        self.P, self.model = reading(frame, rules)
        self.rows_out = frame.get("rows") or []
        self.together: Optional[dict] = None
        self.boxes: List[dict] = []
        self.placed: Dict[str, int] = {}
        self.extra_release: List[_Row] = []

    # -- possession ------------------------------------------------------------------------
    def possession(self, S: str) -> Optional[Tuple[str, Optional[float], List]]:
        """(possession_duty's line, the fish's own possession count or None, its rules) where the line exists."""
        from pipeline.deliver.answers import card as CD
        line = CD.possession_duty(self.P, self.model, S)
        if line is None:
            if A_POSSESSION_ONLY_WHERE_IT_DIFFERS:
                return None
            lead = CD.page_possession(self.P, self.model)
            if lead is None:
                return None
            from pipeline.deliver.answers.display.rule import POSSESSION_HOME
            return (f"in your possession: {lead.f['per_daily']} days’ limits ({POSSESSION_HOME})", None, [])
        per, caps = CD.possession_rules(self.P, self.model, S)
        tops = [r for r in caps if not r.f.get("within")]
        count = tops[0].f.get("take") if tops else None
        return line, count, [r.key for r in per + caps]

    def possession_count(self, S: str) -> Optional[float]:
        """The fish's own possession as a number (E's `Answer.possession`): a water's count, else k daily quotas."""
        got = self.possession(S) if self.states.get(S) == "keep" else None
        if got is None:
            return None
        line, count, keys = got
        if count is not None:
            return count
        from pipeline.deliver.answers import card as CD
        per, _caps = CD.possession_rules(self.P, self.model, S)
        fa = self.fish[S]
        lead = fa[common.lead_name(fa.get("hatchery"), fa.get("wild"))]
        ks = {float(r.f["per_daily"]) for r in per}
        if len(ks) == 1 and lead.get("daily"):
            return ks.pop() * lead["daily"]
        return None

    # -- the keep boxes ----------------------------------------------------------------------
    def build(self) -> dict:
        home_rows = [i for i, r in enumerate(self.rows_out) if r.get("group") and r.get("kind") in common.KEEPS]
        for i in home_rows:
            self._group_box(i)
        self._loose_boxes()
        for b in self.boxes:
            self._kept_here(b)
        self._out_boxes()
        missing = sorted(S for S in self.states if S not in self.placed)
        if missing:
            raise TableError(f"trout table: {missing} answered on the frame but said nowhere in the table")
        return self._table()

    def _rows_for(self, S: str, node: dict, chain, kix: int, rows: List[_Row]) -> None:
        fa = self.fish[S]
        case = origin_case(fa)
        if case == "wild_only":
            raise TableError(f"trout table: only wild {S} may be kept here; the table has no word for that")
        keys = [f"{S}/hatchery", f"{S}/wild"] if case == "twin" else [S]
        for k in keys:
            r = _Row(k, S, k.partition("/")[2] or None)
            if case == "hatchery_only":
                r.label = W.HATCHERY_ONLY
            r.kind_n, r.kind_ix, r.chain = node["n"], kix, chain
            r.detail, r.also = node["detail"], tuple(node.get("also_counts") or ())
            r.duties = tuple(self._noun_duty(d) for d in node.get("duties") or ())
            if node.get("sizes") and r.origin is None:
                r.sizes = [(a, INF if b is None else b) for a, b in node["sizes"]["cm"]]
            rows.append(r)
        if case == "hatchery_only" and not A_HATCHERY_ONLY_ONE_ROW:
            r = _Row(f"{S}/wild", S, "wild")
            r.state = "release"
            self.extra_release.append(r)

    def _group_box(self, home: int) -> None:
        row = self.rows_out[home]
        g = row["group"]
        root = g["tree"][0]
        kinds = list(_walk(root["children"]))
        species = [S for n, _c in kinds for S in n["species"]]
        if not set(species) & TC:
            return
        if not set(species) <= TC:
            raise TableError(f"trout table: the group on row {home} mixes trout or char with {sorted(set(species) - TC)}")
        box_n = root["n"]
        if box_n is None:
            raise TableError(f"trout table: the trout group on row {home} has no daily number")
        if A_RAISED_SHARE_IS_A_BAND:
            # T834: a stream share a water's own quota RAISED below the total, holding every kind of the group (the
            # group's count clause as that quota's: the Peace River's lake trout 3 over Zone 7's 2 from streams), IS
            # the box's number here — "Keep 3 in all", never the zone's 5
            lifters = {L["by"] for L in g.get("lifts") or ()}
            for sc in g.get("subcaps") or ():
                if sc["water"] is None and not sc["lifted"] and sc["take"] is not None and sc["take"] < box_n \
                        and sc["r"] in lifters and set(sc["of"]) >= set(species) & TC:
                    box_n = sc["take"]
        bi = len(self.boxes)
        rows: List[_Row] = []
        for kix, (node, chain) in enumerate(kinds):
            for S in node["species"]:
                if self.states.get(S) != "keep" or S in self.placed:
                    continue
                self._rows_for(S, node, tuple(t for t in chain if not _summed(t, self.rules)), kix, rows)
        if not rows:
            return
        for r in rows:
            self.placed[r.fish] = bi
        strip: List[str] = []
        parts: List[tuple] = []                # the strip's parts as the sheet reads them: (kind, text, cm)
        lines_of: Dict[str, List[str]] = {}
        # -- bands: groups of rows sharing a count tighter than the box's ---------------------------------
        groups: "OrderedDict[tuple, dict]" = OrderedDict()
        for r in rows:
            for t in r.chain:
                if t["tier"] in ("count", "own", "share") and t["n"] is not None and t["n"] < box_n:
                    groups.setdefault(("tier", t["id"]), {"n": t["n"], "rows": [], "by": t["by"], "depth": 0,
                                                          "say": None})["rows"].append(r)
        if A_SHARE_HOLDS_ITS_UNLIFTED_FISH:
            for r in rows:
                for t in r.chain:
                    if t["tier"] != "share" or ("tier", t["id"]) not in groups:
                        continue
                    gd = groups[("tier", t["id"])]
                    for x in rows:
                        if x.fish in _share_of(g, t) and x not in gd["rows"]:
                            gd["rows"].append(x)
        tier_by = {gd["by"]: gk for gk, gd in groups.items()}
        across = self._across(home, species)
        across_rules = {a["r"] for a in across}
        # T834: a one-origin limit across rows ALL in this box (Lois Lake's wild 2 over its rainbow row and its other
        # trout, one box since its raised total) is the box's wild strip — never a count over every origin
        from pipeline.deliver.answers.tree import across_rows
        box_across = {a["r"] for a in across_rows(self.rows_out, home, species) if set(a["others"]) <= set(species)}
        consumed_also: set = set()
        for r in rows:
            for a in r.also:
                if a["by"] in across_rules or a["by"] in box_across or a.get("over_cm"):
                    continue
                gk = tier_by.get(a["by"])
                if gk is not None and groups[gk]["n"] == a["n"]:
                    if r not in groups[gk]["rows"]:
                        groups[gk]["rows"].append(r)
                    consumed_also.add((id(r), a["says"]))
                    continue
                inbox = [x for x in rows if x.fish in a["species"] or x.kind_ix == r.kind_ix]
                if a["n"] < box_n and {x.fish for x in inbox} >= set(a["species"]):
                    gd = groups.setdefault(("also", a["by"], a["n"]), {"n": a["n"], "rows": [], "by": a["by"],
                                                                         "depth": 1, "say": None})
                    for x in inbox:
                        if x not in gd["rows"]:
                            gd["rows"].append(x)
                    consumed_also.add((id(r), a["says"]))
        for kix in sorted({r.kind_ix for r in rows}):
            ks = [r for r in rows if r.kind_ix == kix]
            n = ks[0].kind_n
            if len({r.fish for r in ks}) > 1 and n is not None and n < box_n:
                groups.setdefault(("kind", kix), {"n": n, "rows": ks, "by": None, "depth": 2, "say": None})
        joined_wild: Dict[tuple, tuple] = {}   # a one-origin quota -> the count's group holding it (drawn: not said again)
        if A_WILD_QUOTA_JOINS_ITS_COUNT:
            subs = {s["r"]: s for s in g.get("subcaps") or ()}
            for x in self._wild_holders(row, rows):
                for c in x.get("conds") or []:
                    s = subs.get(c.get("r")) if c.get("c") == "origin2" and c.get("daily") else None
                    if s is None or s.get("take") != c["daily"] or s.get("over_cm") is not None \
                            or s.get("under_cm") is not None or c["daily"] >= box_n:
                        continue
                    who = set(c.get("who") or x.get("members") or ())
                    mine = [r for r in rows if r.origin == c["o"] and r.fish in who]
                    held = [r for r in rows if r.fish in set(s.get("of") or ()) and r.fish not in who]
                    if not mine or not held:
                        continue
                    ids = {id(r) for r in held}
                    gk = next((k for k, gd in groups.items() if gd["n"] == c["daily"]
                               and {id(r) for r in gd["rows"]} == ids), None)
                    if gk is None:
                        gk = ("sub", c["r"])
                        groups.setdefault(gk, {"n": c["daily"], "rows": list(held), "by": c["r"], "depth": 0,
                                               "say": None})
                    groups[gk]["rows"] += [r for r in mine if r not in groups[gk]["rows"]]
                    joined_wild[(c["r"], c["o"], c["daily"])] = gk
        for gk in [gk for gk, gd in groups.items() if A_SUMMED_TIER_NEVER_DRAWN and gd["n"] >= self._most(gd["rows"])
                   and not (A_KIND_NUMBER_IS_SHARED and gk[0] == "kind")]:
            del groups[gk]                     # a count its fish cannot reach (a summed tier by the numbers): no limit
        drawn: List[tuple] = []
        taken: set = set()
        for gk, gd in sorted(groups.items(), key=lambda kv: (-len(kv[1]["rows"]), kv[1]["depth"])):
            ids = {id(r) for r in gd["rows"]}
            if ids & taken:
                continue
            drawn.append(gk)
            taken |= ids
        # rows in Python's order: each drawn group one piece, where its first row stands
        order: List[_Row] = []
        for r in rows:
            if r in order:
                continue
            gk = next((k for k in drawn if r in groups[k]["rows"]), None)
            piece = [x for x in rows if x in groups[gk]["rows"] and x not in order] if gk else [r]
            order += piece
            if gk is not None and gk in joined_wild.values():
                # a wild row a count's band joined (A_WILD_QUOTA_JOINS_ITS_COUNT): its twin follows the band
                order += [x for x in rows if x not in order and x.origin is not None
                          and not any(x in groups[k]["rows"] for k in drawn)
                          and any(y.fish == x.fish and y.origin is not None for y in piece)]
        rows = order
        for gk in drawn:
            gd = groups[gk]
            for r in gd["rows"]:
                r.band = ("shared" if len(gd["rows"]) > 1 else "own", gd["n"], gk)
        for r in rows:
            if r.band is None:
                k = r.kind_n
                r.band = ("own", k, None) if k is not None and k < box_n else ("upto", box_n, None)
        # a limit inside the row's band stays a bold line under its fish, said once: the tightest first
        said = {r.key: r.band[1] for r in rows}
        # (each fish once: twin wild / hatchery rows are one fish — Lois Lake's "wild trout/char 2", T834)
        inner = [(gd["n"], list(dict.fromkeys(x.fish for x in gd["rows"])), gd["rows"]) for gk, gd in groups.items()
                 if gk not in drawn]
        inner += [(r.kind_n, list(dict.fromkeys(x.fish for x in rows if x.kind_ix == r.kind_ix)), [r]) for r in rows
                  if r.kind_n is not None]
        for n_, fish, members in sorted(inner, key=lambda x: x[0]):
            for r in members:
                if n_ >= said[r.key]:
                    continue
                others = [S for S in fish if S != r.fish]
                lines_of.setdefault(r.key, []).append(W.count_line(n_, others) if others else W.only_line(n_))
                said[r.key] = n_
        # -- the size caps: the strip when one holds every fish of the box that can pass it ---------------
        has_size_strip = False
        size_tiers = OrderedDict((t["id"], t) for r in rows for t in r.chain if t["tier"] == "size")
        for tid, t in size_tiers.items():
            cm = t["over_cm"]
            extra = t.get("extra") or {}
            holds = set(t.get("species") or ()) | set(extra.get("species") or ())
            passing = {r.fish for r in rows if self._passes(r, cm)}
            if passing <= holds:
                strip.append(W.strip_size(t["n"], cm, extra.get("n")))
                parts.append(("size", strip[-1], cm))
                has_size_strip = True
                for r in rows:
                    if r.fish in (t.get("species") or ()) and (self._min_keep(r) >= cm or
                                                               (r.fish == "ST" and not extra
                                                                and not A_RAINBOW_STOPS_AT_THE_STEELHEAD_SIZE)):
                        lines_of.setdefault(r.key, []).insert(0, W.uses_the(t["n"], cm))
            else:
                under = [r for r in rows if any(x["id"] == tid for x in r.chain)]
                once = None if extra else self._except_strip(t["n"], cm, [x.fish for x in under], rows)
                if once is not None:
                    strip.append(once)
                    parts.append(("size_except", once, cm))
                    has_size_strip = True
                    for r in under:
                        if self._min_keep(r) >= cm:
                            lines_of.setdefault(r.key, []).insert(0, W.uses_the(t["n"], cm))
                    continue
                for r in under:
                    others = list(dict.fromkeys(x.fish for x in under if x.fish != r.fish))
                    lines_of.setdefault(r.key, []).append(W.size_count_line(t["n"], cm, others))
        # -- the wild quota of the box's own; the one across boxes is `together` --------------------------
        holders = self._wild_holders(row, rows)
        seen_c: set = set()
        for x, c in [(x, c) for x in holders for c in x.get("conds") or []]:
            if c.get("c") != "origin2" or not c.get("daily") or c["r"] in across_rules \
                    or joined_wild.get((c["r"], c["o"], c["daily"])) in drawn:
                continue
            if (c["r"], c["o"], c["daily"]) in seen_c:
                continue
            seen_c.add((c["r"], c["o"], c["daily"]))
            who = set(c.get("who") or x.get("members") or ())
            rule = self.rules.by_ref.get(c["r"]) if A_WILD_QUOTA_HOLDS_EVERY_FISH_OF_ITS_RULE else None
            if rule is not None:
                # T834: a box a raised total joined (Lois Lake: its rainbow 6 beside its wild trout/char 2) holds fish of
                # the wild quota's rule from another row: the rule holds them all
                who |= {r.fish for r in rows if r.fish in rule.species and r.fish not in rule.except_}
            mine = [r for r in rows if r.origin == c["o"] and r.fish in who]
            if not mine:
                continue
            every = [r for r in rows if r.origin == c["o"]] if not A_WILD_QUOTA_FROM_EVERY_ROW else \
                [r for r in rows if r.origin == c["o"] or (r.origin is None and not r.label and
                                                           TL._keeps(TL.law_of(self.fish[r.fish].get(c["o"]))))]
            twins = {r.fish for r in mine}
            if A_WILD_ROWS_GROUPED and len(mine) == len(every) and c["o"] == "wild" and len(mine) > 1 \
                    and all(r.origin == "wild" for r in mine) \
                    and all(r.band[0] == "upto" for r in rows if r.fish in twins and r.origin is not None):
                # Q9 (owner, option 1): hatchery rows first, then the wild rows under one white box "only 2 wild in all";
                # each wild row keeps its own size ("40 cm (16 in) or less")
                first = min(i for i, r in enumerate(rows) if r.fish in twins and r.origin is not None)
                hat = [r for r in rows if r.fish in twins and r.origin == "hatchery"]
                rest = [r for r in rows[first:] if not (r.fish in twins and r.origin is not None)]
                rows = rows[:first] + hat + list(mine) + rest
                for r in mine:
                    r.band = ("shared", c["daily"], ("wild", c["r"]))
            elif len(mine) == len(every) and c["o"] == "wild":
                strip.append(W.strip_wild(c["daily"], self._origin_size([r.fish for r in mine], c["o"])))
                parts.append(("wild", strip[-1], None))
            else:
                for r in mine:
                    lines_of.setdefault(r.key, []).append(W.strip_wild(c["daily"], None))
        for a in across:
            if self.together is not None and self.together["src"] != [a["r"]]:
                raise TableError("trout table: two limits across boxes; `together` holds one")
            size = self._origin_size(list(a["mine"]) + list(a["others"]), a["o"])
            self.together = {"text": W.together_origin(a["n"], a["o"], size), "src": [a["r"]], "origin": a["o"]}
        from pipeline.deliver.answers.tree import A_BOTH_LAKES_SAID
        for t in OrderedDict((t["id"], t) for r in rows for t in r.chain).values():
            if A_BOTH_LAKES_SAID and t["tier"] == "own" and "from both lakes together" in t["says"]:
                if self.together is not None and self.together["src"] != [t["by"]]:
                    raise TableError("trout table: two limits across waters; `together` holds one")
                self.together = {"text": W.together_lakes(t["n"]), "src": [t["by"]], "origin": None}
        # -- a kind's size cap over several fish holds them together (the proof found it said as each fish's own) ---
        said_also: set = set()
        if A_KIND_SIZE_CAP_IS_SHARED:
            caps: "OrderedDict[tuple, list]" = OrderedDict()
            for r in rows:
                for a in r.also:
                    sp = set(a.get("species") or ())
                    if not a.get("over_cm") or a["by"] in across_rules or a["by"] in box_across \
                            or (id(r), a["says"]) in consumed_also \
                            or r.fish not in sp:
                        continue
                    held = [x for x in rows if x.fish in sp]
                    if len({x.fish for x in held}) < 2:
                        continue                       # one fish: its own line says it
                    then = tuple((t["n"], t["over_cm"]) for t in a.get("then") or ())
                    caps.setdefault((a["by"], a["n"], a["over_cm"], tuple(sorted(sp)), then), held)
                    said_also.add((id(r), a["says"]))
            for (_by, n, cm, sp, then), held in caps.items():
                levels = [(n, cm), *then]
                if {x.fish for x in rows if self._passes(x, cm)} <= set(sp):
                    for n_, cm_ in levels:
                        t_ = W.strip_size(n_, cm_)
                        if t_ not in strip:
                            strip.append(t_)
                            parts.append(("size", t_, cm_))
                            for x in held:
                                if self._min_keep(x) >= cm_:
                                    lines_of.setdefault(x.key, []).insert(0, W.uses_the(n_, cm_))
                    has_size_strip = True
                else:
                    once = self._except_strip(n, cm, [x.fish for x in held], rows) if not then else None
                    if once is not None:
                        if once not in strip:
                            strip.append(once)
                            parts.append(("size_except", once, cm))
                            for x in held:
                                if self._min_keep(x) >= cm:
                                    lines_of.setdefault(x.key, []).insert(0, W.uses_the(n, cm))
                        has_size_strip = True
                        continue
                    for x in held:
                        others = list(dict.fromkeys(y.fish for y in held if y.fish != x.fish))
                        for n_, cm_ in levels:
                            lines_of.setdefault(x.key, []).append(W.size_count_line(n_, cm_, others))
        # -- each row's words --------------------------------------------------------------------------------
        for r in rows:
            r.size = law_size(self.fish[r.fish][r.origin], self.rules) if r.origin is not None else r.detail
        any_once = A_ANY_SIZE_ONCE and not has_size_strip and all(r.size == "any size" for r in rows)
        if any_once:
            strip.insert(0, W.STRIP_ANY_SIZE)
        for r in rows:
            fa = self.fish[r.fish]
            size = r.size
            sub = W.steelhead_sub(size) if r.fish == "ST" else None
            if sub is not None:
                r.sub = sub
            elif not (size == "any size" and (has_size_strip or any_once)):
                r.size_text = W.size_line(_slot_words(size))
                r.add(r.size_text)
            for x in lines_of.get(r.key, []):
                r.add(x)
            for a in r.also:
                if a["by"] in across_rules or a["by"] in box_across or (id(r), a["says"]) in consumed_also \
                        or (id(r), a["says"]) in said_also:
                    continue
                if a.get("species") and r.fish not in a["species"] and set(a["species"]) & {x.fish for x in rows if x.kind_ix == r.kind_ix}:
                    continue
                if not a.get("over_cm") and any(gk[0] in ("tier", "also") and gk[1] == a["by"] for gk in drawn):
                    continue
                r.add(_one_fish_cap(a["says"], r.fish))
            got = self.possession(r.fish)
            if got is not None:
                line, count, keys = got
                r.src += [self._ref(k) for k in keys]
                own_said = count is not None and ((r.band[0] == "own" and r.band[1] == count) or
                                                  W.only_line(count) in lines_of.get(r.key, []))
                if A_POSSESSION_EQUAL_TO_THE_DAY_UNSAID and count is not None and own_said \
                        and W.possession_clause(W.possession_row(line)) is None:
                    # (only a number the ROW says for its own fish: a box's count stays, Bowron Lake's "2 in all")
                    pass                               # Tagish's lake trout: "only 2" a day, 2 in possession
                else:
                    r.poss = (line, count)
            for d in r.duties:
                r.add(W.stop_duty(d))
            self._week_and_steel(r)
        # -- possession: below the day's number a band; shared by every row: the strip ---------------------
        plines = {r.poss[0] if r.poss else None for r in rows}
        box_poss = None
        if len(rows) > 1 and len(plines) == 1 and None not in plines:
            if A_BOX_POSSESSION_OWN_LINE:
                box_poss = W.box_possession(plines.pop())
            else:
                strip.append(W.strip_possession(plines.pop()))
        else:
            for r in rows:
                if r.poss is None:
                    continue
                line, count = r.poss
                alone = r.band[0] in ("own", "upto")
                if count is not None and alone and count < r.band[1]:
                    clause = W.possession_clause(W.possession_row(line)) if A_POSSESSION_SAID_ONCE else None
                    r.quiet.insert(0, W.possession_quiet(r.band[1], count, W.fish_words([r.fish]), clause))
                    r.band = ("possession", count, None)
                    if ", only " in line and not A_POSSESSION_SAID_ONCE:
                        r.add(W.possession_row(line))
                else:
                    r.add(W.possession_row(line))
        # -- the label: on the box when every row carries it ------------------------------------------------
        label = None
        if A_BOX_LABEL_WHEN_EVERY_ROW and all(r.label for r in rows):
            label = W.HATCHERY_ONLY
            for r in rows:
                r.label = None
        whose = root.get("whose")
        tag = W.tag_of(whose) if whose and whose.startswith("This ") else None
        head = W.head_keep(box_n)
        first_n = None
        if A_HEAD_LEADS_WITH_THE_REGION and self._raised(g):
            # T834 (owner 13:15, "lead with 5"): "Keep 5, brook trout up to 20" — what the other fish come to first, the
            # lifted fish's own number after; the box's number stays the day's most (the exception's)
            ex = list(dict.fromkeys(S for L in g.get("lifts") or () for S in L["kinds"]
                                    if any(r.fish == S for r in rows)))
            n_other = min(_headline_n({"group": g, "n": box_n}), _others_most(g))
            if ex and n_other < box_n:
                head = W.head_keep_except(n_other, W.fish_words(ex), box_n)
                first_n = n_other
        bands = []
        i = 0
        while i < len(rows):
            kind, n, gk = rows[i].band
            j = i
            if kind == "shared":
                while j + 1 < len(rows) and rows[j + 1].band[2] == gk:
                    j += 1
            if kind == "shared" and A_SHARED_BOX_NUMBER_IS_ANY_OF and first_n is not None and n == first_n \
                    and not (isinstance(gk, tuple) and gk[0] == "wild"):
                # Q2 (owner 15:34): the raised box's first number shared by its other fish is each row's "any of the 5"
                bands += [{"from": x, "to": x, "text": W.band_upto(n), "kind": "upto"} for x in range(i, j + 1)]
                i = j + 1
                continue
            if kind == "shared" and isinstance(gk, tuple) and gk[0] == "wild":
                text = W.band_wild(n)
            else:
                text = {"upto": lambda: W.band_upto(n), "own": lambda: W.band_own(n),
                        "possession": lambda: W.band_possession(n),
                        "shared": lambda: W.band_shared(n, j - i + 1)}[kind]()
            bands.append({"from": i, "to": j, "text": text, "kind": kind})
            i = j + 1
        for r in rows:
            r.src = list(dict.fromkeys(_row_src(self.fish[r.fish], r.key, [t["by"] for t in r.chain]) + r.src))
        self.boxes.append({"kind": "keep", "head": head, "label": label, "strip": W.strip(strip),
                           "possession": box_poss, "tag": tag, "rows": rows, "bands": bands, "n": box_n, "home": home,
                           "group": g, "parts": parts, "passes": self._passes, "first": first_n})

    def _kept_here(self, b: dict) -> None:
        """Q1 (owner 15:34 "What you keep here"): a keep box whose bands never let its fish reach the head's number —
        each band holds at most its number, a row with no limit of its own the box's — says what they come to here:
        "Keep 1 in all", a band of that number becoming each row's "any of the 1" (Echoes Lakes; a Region 4 stream's
        "Keep 2 in all"). The region's number still speaks under "Fished elsewhere today?"."""
        if not A_HEAD_SAYS_WHAT_IS_KEPT_HERE or b["first"] is not None or b["head"] != W.head_keep(b["n"]):
            return
        if any(x["kind"] == "possession" for x in b["bands"]):
            return
        reach = sum(b["n"] if x["kind"] == "upto" else W.parse_band(x["text"])[1] for x in b["bands"])
        if reach >= b["n"]:
            return
        b["n"] = reach
        b["head"] = W.head_keep(reach)
        bands = []
        for x in b["bands"]:
            k = b["n"] if x["kind"] == "upto" else W.parse_band(x["text"])[1]
            if k == reach and W.band_wild_n(x["text"]) is None:
                bands += [{"from": i, "to": i, "text": W.band_upto(reach), "kind": "upto"}
                          for i in range(x["from"], x["to"] + 1)]
            else:
                bands.append(x)
        b["bands"] = bands
        for r in b["rows"]:
            if r.band[0] != "upto" and r.band[1] == reach:
                r.band = ("upto", reach, None)

    def _noun_duty(self, d: str) -> str:
        """Q7: a stored stop duty ("stop fishing this river after 1 a day") with this key's noun."""
        import re
        if self.noun is None:
            return d
        return re.sub(r"^stop fishing this \w+ after", f"stop fishing this {self.noun} after", d)

    def _except_strip(self, n, cm, held: Sequence[str], rows: Sequence[_Row]) -> Optional[str]:
        """Q3b: the strip part saying a size cap once, "only 1 over 50 cm (20 in), not counting lake trout", where the
        cap holds every fish of the box that can pass it but a few, and those few are fewer than the fish it holds."""
        if not A_SIZE_CAP_ONCE_WITH_ITS_EXCEPTION:
            return None
        held_set = set(held)
        out = list(dict.fromkeys(r.fish for r in rows if self._passes(r, cm) and r.fish not in held_set))
        if not out or len(out) >= len(held_set):
            return None
        return W.strip_size_except(n, cm, W.fish_words(sorted(out, key=_book_order)))

    def _wild_holders(self, home: dict, rows: Sequence[_Row]) -> List[dict]:
        """The keep rows of the frame whose conditions may hold a wild quota of the box's fish (A_WILD_QUOTA_FROM_EVERY_ROW:
        every keep row holding one of them, a fish's own row included: Kitimat's rainbow), else the box's home row."""
        if not A_WILD_QUOTA_FROM_EVERY_ROW:
            return [home]
        box_fish = {r.fish for r in rows}
        return [x for x in self.rows_out if x.get("kind") in common.KEEPS and set(x.get("members") or ()) & box_fish]

    def _licence_duties(self, r: _Row) -> List[str]:
        """The row's yearly limit and record duties (`card.licence_duty`), from the decided answer the row says (its
        origin's on a twin row, else the leading one's), banded over the lengths it keeps."""
        fa = self.fish[r.fish]
        dec = self.model["R"][r.fish]["H" if (r.origin or common.lead_name(fa.get("hatchery"), fa.get("wild")))
                                      == "hatchery" else "W"]
        a, b = _keep_range(self._law(r))
        return S.licence_duties(dec, a, b)

    def _most(self, rows: Sequence[_Row]) -> float:
        """The most the rows can hold together: each kind's own number once (the 2nd attack's arithmetic)."""
        per_kind: Dict[int, float] = {}
        for r in rows:
            n = INF if r.kind_n is None else r.kind_n
            per_kind[r.kind_ix] = min(per_kind.get(r.kind_ix, INF), n)
        return sum(per_kind.values())

    def _ref(self, key: str):
        if not hasattr(self, "_refs"):
            self._refs = {r.key: ref for ref, r in self.rules.by_ref.items()}
        return self._refs[key]

    def _origin_size(self, fish: Sequence[str], o: str) -> Optional[str]:
        """The one size every `o` fish of `fish` keeps today (each origin's own law, `law_size`), or None when they
        differ (then each twin row says its own)."""
        got = {law_size(self.fish[S][o], self.rules) for S in fish
               if self.fish.get(S) and TL._keeps(TL.law_of(self.fish[S].get(o)))}
        return got.pop() if len(got) == 1 else None

    def _across(self, home: int, kinds: Sequence[str]):
        from pipeline.deliver.answers.tree import A_WILD_LIMIT_ACROSS_ROWS_SAID, across_rows
        if not A_WILD_LIMIT_ACROSS_ROWS_SAID:
            return ()
        # (T834: a limit across rows of ONE box is the box's own — its wild strip says it — never a line across boxes)
        return [a for a in across_rows(self.rows_out, home, kinds) if set(a["others"]) & TC
                and not set(a["others"]) <= set(kinds)]

    def _law(self, r: _Row):
        fa = self.fish[r.fish]
        o = r.origin or common.lead_name(fa.get("hatchery"), fa.get("wild"))
        return TL.law_of(fa[o])

    def _ranges(self, r: _Row) -> List[Tuple[float, float]]:
        """The lengths a row keeps: the kind's ranges as the card states them (a steelhead's "rainbow trout over 50 cm"
        and a rainbow's 30-50 cm are the card's, not the bare decided lines'), a twin row's from its origin's law — a
        rainbow's stopping at the steelhead size where a rainbow over it is a steelhead (its answer's `steel` line)."""
        if r.sizes:
            return r.sizes
        got = [(x, y) for x, y, n in TL.pieces([self._law(r)], self._law(r)) if n > 0]
        if A_RAINBOW_STOPS_AT_THE_STEELHEAD_SIZE and r.fish == "RB" and any(
                ln["t"] == "steel" for d in (self.fish["RB"].get("hatchery"), self.fish["RB"].get("wild")) if d
                for ln in d.get("lines") or ()):
            from pipeline.deliver.bundle import read as RD
            st = RD._steelhead_min_cm()
            got = [(x, min(y, st)) for x, y in got if x < st]
        return got

    def _passes(self, r: _Row, cm: float) -> bool:
        return any(y > cm for _x, y in self._ranges(r))

    def _min_keep(self, r: _Row) -> float:
        got = self._ranges(r)
        if not got:
            raise TableError(f"trout table: row {r.key} keeps nothing")
        return min(x for x, _y in got)

    def _week_and_steel(self, r: _Row) -> None:
        for x in self.rows_out:
            wk = x.get("week")
            if wk and r.fish in wk["species"] and r.fish in (x.get("all_members") or x.get("members") or ()):
                r.add(W.week_line(wk["says"]))
                r.src += [rr for p in wk.get("parts") or () for rr in p["rules"]]
        line = self.frame.get("steelhead_line")
        if line in ("possible_with_rules", "known_with_rules") and r.fish == "ST":
            r.add(W.steel_quiet(line), "quiet")
        if line == "known_no_rules" and r.fish == "RB":
            r.add(W.steel_quiet(line), "quiet")

    # -- loose keep rows, and the release and closed groups -----------------------------------------
    def _loose_boxes(self) -> None:
        """A kept trout or char fish no group holds (t670 K4/K5: Region 8's stream brook trout): its own row's box."""
        for i, row in enumerate(self.rows_out):
            if row.get("kind") not in common.KEEPS:
                continue
            fish = [S for S in row.get("members") or () if self.states.get(S) == "keep" and S not in self.placed]
            if not fish:
                continue
            n = row.get("daily")
            if n is None:
                raise TableError(f"trout table: kept {fish} on row {i}, which has no daily number and no group")
            bi = len(self.boxes)
            rows: List[_Row] = []
            for S in fish:
                fa = self.fish[S]
                lead = fa[common.lead_name(fa.get("hatchery"), fa.get("wild"))]
                node = {"n": lead.get("daily"), "detail": law_size(lead, self.rules), "duties": (), "also_counts": ()}
                self._rows_for(S, node, (), len(rows), rows)
                self.placed[S] = bi
            for r in rows:
                r.band = ("own", r.kind_n, None) if r.kind_n is not None and r.kind_n < n else ("upto", n, None)
                fa = self.fish[r.fish]
                if r.origin is not None:
                    r.detail = law_size(fa[r.origin], self.rules)
                r.size = r.detail
                if r.detail != "any size":
                    r.size_text = W.size_line(r.detail)
                    r.add(r.size_text)
                got = self.possession(r.fish)
                if got is not None:
                    r.poss = (got[0], got[1])
                    r.add(W.possession_row(got[0]))
                self._week_and_steel(r)
                r.src = _row_src(fa, r.key, [row["pool"]] if row.get("pool") is not None else [])
            label = None
            if A_BOX_LABEL_WHEN_EVERY_ROW and all(r.label for r in rows):
                label = W.HATCHERY_ONLY
                for r in rows:
                    r.label = None
            bands = [{"from": k, "to": k, "kind": r.band[0],
                      "text": W.band_upto(n) if r.band[0] == "upto" else W.band_own(r.band[1])}
                     for k, r in enumerate(rows)]
            self.boxes.append({"kind": "keep", "head": W.head_keep(n), "label": label, "strip": None,
                               "possession": None, "tag": None, "rows": rows, "bands": bands, "n": n, "home": i,
                               "group": None, "parts": [], "passes": self._passes, "first": None})

    def _out_boxes(self) -> None:
        for kind, head in (("release", W.HEAD_RELEASE), ("closed", W.HEAD_CLOSED)):
            rows = []
            for S in sorted(self.states, key=_book_order):
                if self.states[S] != kind or S in self.placed:
                    continue
                r = _Row(S, S)
                r.state = kind
                self._week_and_steel(r)
                r.src = _row_src(self.fish[S], S)
                rows.append(r)
            if kind == "release":
                for r in self.extra_release:
                    r.src = _row_src(self.fish[r.fish], r.key)
                    rows.append(r)
            if not rows:
                continue
            bi = len(self.boxes)
            for r in rows:
                self.placed.setdefault(r.fish, bi)
            self.boxes.append({"kind": kind, "head": head, "label": None, "strip": None, "possession": None, "tag": None,
                               "rows": rows, "bands": [], "n": None, "home": None, "group": None, "parts": [],
                               "passes": None, "first": None})

    def _raised(self, g: dict) -> bool:
        """T834: the group's total or its stream share was RAISED by a water's own quota (`rows.pools`): its total is
        above its rule's number (Jewel Lake's 20 over Region 8's 5; Beaver Creek's 8 over Region 4's 5), or a count
        clause of the group is a lifting rule's (the Peace River's lake trout 3 over Zone 7's 2 from streams)."""
        rule = self.rules.by_ref.get(g["pool"])
        take = rule.f.get("take") if rule is not None else None
        if isinstance(take, (int, float)) and isinstance(g.get("total"), (int, float)) and g["total"] > take:
            return True
        lifters = {L["by"] for L in g.get("lifts") or ()}
        return any(sc["water"] is None and not sc["lifted"] and sc["r"] in lifters for sc in g.get("subcaps") or ())

    # -- the whole table ------------------------------------------------------------------------------
    def _top(self) -> Tuple[str, dict]:
        keep = [b for b in self.boxes if b["kind"] == "keep"]
        if not keep:
            kinds = {b["kind"] for b in self.boxes}
            if kinds == {"release"}:
                return W.TOP_RELEASE, {"value": W.FOLD_RELEASE, "tone": "release"}
            if kinds == {"closed"}:
                return W.TOP_CLOSED, {"value": W.FOLD_CLOSED, "tone": "closed"}
            return W.TOP_NONE, {"value": W.FOLD_NONE, "tone": "none"}
        if len(keep) == 1:
            b = keep[0]
            g = b["group"]
            if g is None:
                return W.top_up_to(b["n"]), {"value": W.fold_up_to(b["n"]), "tone": "keep"}
            h = g["headline"]
            n = h["n"] if h.get("n") is not None else b["n"]
            alsos = [a for a in h.get("also") or ()]
            also_ns = {a["n"] for a in alsos}
            rest: List[Tuple[float, str]] = []
            if n < b["n"] and any(t["node"] == "tier" and t["tier"] == "share" for t in g["tree"][0]["children"]):
                # the headline is a share (Region 4's "2 from streams"): a water's own rule BESIDE it (not inside it)
                # adds its fish to the top line — "or 5 if they’re rainbow trout" (the stored `also`), then each fish
                # under its own number "and no more than 2 Dolly Varden/bull trout", or with no `also` ", plus 2 …"
                share = next(t for t in g["tree"][0]["children"] if t["node"] == "tier" and t["tier"] == "share")
                for t in g["tree"][0]["children"]:
                    if t["node"] != "tier" or t["tier"] != "own" or t["n"] is None:
                        continue
                    for node, _c in _walk(t["children"]):
                        k = t["n"] if node["n"] is None else min(node["n"], t["n"])
                        fish = [S for S in node["species"] if self.states.get(S) == "keep"
                                and S not in _share_of(g, share)]
                        if fish and k not in also_ns:
                            rest.append((k, W.fish_words(fish)))
            rest = list(dict.fromkeys(rest))
            if alsos and A_TOP_SAYS_WHAT_THE_OTHER_FISH_COME_TO:
                n = min(n, _others_most(g))
            lead = W.top_up_to(n, [a["says"] for a in alsos], rest if alsos else (), () if alsos else rest)
            return lead, {"value": W.fold_up_to(n, [a["n"] for a in alsos]), "tone": "keep"}
        named, others = [], []
        for b in keep:
            sc = (self.rows_out[b["home"]].get("scope") or {})
            (named if sc.get("apart") else others).append(b)
        if len(others) > 1:
            named += others[:-1]
            others = others[-1:]
        fish = lambda b: list(dict.fromkeys(r.fish for r in b["rows"]))
        n_of = lambda b: _headline_n(b) if A_APART_SAYS_THE_BOX_MOST else b["n"]
        lead = W.top_apart([(n_of(b), W.fish_words(fish(b))) for b in named], n_of(others[0]) if others else None)
        fold = W.fold_apart([(n_of(b), W.short_fish(fish(b)[0])) for b in named], n_of(others[0]) if others else None)
        return lead, {"value": fold, "tone": "keep"}

    def _table(self) -> dict:
        lead, fold = self._top()
        # `elsewhere` (ticket H): the card's kept-elsewhere lines (`group.worked`, `rows/groups.py elsewhere_of`) of every
        # keep box's group, in box order, under the app's "Fished elsewhere today? ›" (empty: no link). A line about
        # keeping HERE ("If you keep 4 hatchery rainbow trout here", `at` "here": a lifted kind past the stream share) is
        # not a kept-elsewhere line and is not under that link (G2 lists it for the owner)
        elsewhere: List[str] = [W.elsewhere_line(w["q"], w["a"]) for b in self.boxes if b["group"] is not None
                                for w in b["group"].get("worked") or () if w["at"] != "here"]
        boxes = []
        src: List[int] = []
        for b in self.boxes:
            rows = []
            for ri, r in enumerate(b["rows"]):
                sheet = S.fish_sheet(b, ri, r, stamps=S.stamps_for(self.frame, r.fish, self.docs),
                                     duties=self._licence_duties(r) if b["kind"] == "keep" else (),
                                     together=self.together)
                rows.append({"key": r.key, "fish": r.fish, "name": W.fish_name(r.fish), "origin": r.origin,
                             "label": r.label, "sub": r.sub, "lines": tuple(r.lines), "quiet": tuple(r.quiet),
                             "src": tuple(r.src), "sheet": sheet})
                src += r.src
            boxes.append({"kind": b["kind"], "head": b["head"], "label": b["label"], "strip": b["strip"],
                          "possession": b["possession"], "tag": b["tag"], "rows": tuple(rows),
                          "bands": tuple(b["bands"])})
        together = None
        second = None
        if self.together is not None:
            together = {"text": self.together["text"], "src": tuple(self.together["src"])}
            second = together["text"]
            src += self.together["src"]
            if A_LAKES_LIMIT_SAID_ONCE and self.together["origin"] is None:
                together = None                        # the lakes' count: once, under the top line (Echoes Lakes)
        marked = any(b["label"] for b in boxes) or any(r["label"] or r["origin"] for b in boxes for r in b["rows"])
        return {"top": {"lead": lead, "second": second}, "fold": fold,
                "hatch_link": marked, "boxes": tuple(boxes), "together": together, "year": None,
                "elsewhere": tuple(dict.fromkeys(elsewhere)), "src": tuple(dict.fromkeys(src))}


def _slot_words(size: str) -> str:
    """A slot keep range said as what goes back (A_SLOT_SAYS_WHAT_TO_RELEASE): "60 cm (24 in) or less, or 90 cm (35 in)
    or longer" -> "release over 60 cm (24 in) and under 90 cm (35 in)" (60 and 90 exactly may be kept, as the book
    says); every other size as it is."""
    import re
    if not A_SLOT_SAYS_WHAT_TO_RELEASE:
        return size
    m = re.fullmatch(rf"({W._CM}) or less, or ({W._CM}) or longer", size)
    if not m:
        return size
    return W.size_release(W._cm(m[2], m[3]), W._cm(m[5], m[6]))


def _one_fish_cap(says: str, S: str) -> str:
    """A kind's cap on one fish's own row (A_ONE_FISH_SIZE_CAP_SAYS_ONLY): the card's "up to 1 cutthroat trout over 50 cm
    (20 in)" (or "up to 1 over 90 cm (35 in)") said as every other row limit is, "only 1 over 50 cm (20 in)"; any other
    cap as the card says it."""
    if not A_ONE_FISH_SIZE_CAP_SAYS_ONLY:
        return W.also_line(says)
    try:
        got = W.parse_also(says)
    except W.WordsError:
        return W.also_line(says)
    if got[0] == "size":
        _s, n, cm, mine, others, then = got
        if n and not others and not then and mine in (None, (S,)) and "if you" not in says:
            return W.size_count_line(n, cm, ())
    return W.also_line(says)


def _share_of(g: dict, t: dict) -> frozenset:
    """The fish a share tier's rule holds, every lift taken out (the group's `subcaps[].of` for the tier's rule); none
    where the flag is off or the group names no such sub-cap."""
    if not A_SHARE_HOLDS_ITS_UNLIFTED_FISH:
        return frozenset()
    s = next((s for s in g.get("subcaps") or () if s["r"] == t["by"]), None)
    return frozenset(s.get("of") or ()) if s is not None else frozenset()


def _headline_n(b: dict):
    """A keep box's most here as its card's headline states it (the share inside the region's total: Region 4's "2 a
    day from streams" in a box of the 5), else the box's own number."""
    h = (b.get("group") or {}).get("headline") or {}
    return h["n"] if h.get("n") is not None and h["n"] < b["n"] else b["n"]


def _book_order(S: str) -> int:
    from pipeline.deliver.answers.tree import book_order_
    return book_order_([S, *sorted(TC - {S})]).index(S) if S in TC else 99


# ============================================================================================
# The whole key: NULL cells, the dates, the year bar, the model
# ============================================================================================

def is_null(frame: dict, display: dict) -> bool:
    """Q-D3: no table where the display frame (the banner's) says closed or tidal, or the frame asks about no trout or
    char fish."""
    return display.get("status") in ("closed", "tidal") or not fish_states(frame)


def _year_runs(frames: Sequence[dict], starts, moments, fish: Sequence[str]) -> list:
    """The trout and char year strip (`year.stamp_years`, the page's strip rule) over the table's fish."""
    from pipeline.deliver.answers.year import stamp_years
    got = stamp_years([{"fish": f.get("fish") or {}, "rows": [{"members": list(fish)}]} for f in frames],
                      list(starts), moments)
    return [tuple(r) for r in got[0]["rows"][0]["year"]]


def _hours_guard(frames, displays, starts, moments) -> None:
    """Contract 2.2: the table is drawn from outside any hours window (`inside=false`); a trout answer that changes
    inside one, with the water open on both sides, has no words in the table. A0 counted none in r-46e4355a7277, so this
    is a guard: the build stops naming the key's segments."""
    if moments is None:
        return
    from pipeline.deliver import calendar as K
    at = [K.Moment.from_json(m) for m in moments]
    for j, mj in enumerate(at):
        if not mj.inside:
            continue
        for i, mi in enumerate(at):
            if starts[i] != starts[j] or not (mi.weekdays & mj.weekdays) or mi.inside:
                continue
            if any(d.get("status") in ("closed", "tidal") for d in (displays[i], displays[j])):
                continue
            a = {S: (v.get("hatchery"), v.get("wild")) for S, v in (frames[i].get("fish") or {}).items() if S in TC}
            b = {S: (v.get("hatchery"), v.get("wild")) for S, v in (frames[j].get("fish") or {}).items() if S in TC}
            la = {S: tuple(TL.law_of(x) for x in v) for S, v in a.items()}
            lb = {S: tuple(TL.law_of(x) for x in v) for S, v in b.items()}
            if la != lb:
                raise TableError(f"trout table: the trout answer changes inside an hours window (segments {i} and {j}, "
                                 f"day {starts[i]}); the table has no words for it")


def compose_key(rows: Sequence[dict], displays: Sequence[dict], starts: Sequence[int],
                moments: Optional[Sequence[dict]], rules: Rules, *, licences: Sequence[dict],
                validate: bool = True, noun: Optional[str] = None) -> List[Optional[dict]]:
    """THE WHOLE-KEY COMPOSE (contract 2.2): one answer key's post-pass rows, display and licence frames (one per
    segment), its segment starts and moments -> per segment the `TroutFrame` (as a dict, validated by
    `model.trout.TroutFrame`) or None (Q-D3). Adds no day cut: its value is a function of the rows, display and licence
    values (the licence answer's only for the fish sheets' stamps, ticket H)."""
    from pipeline.deliver.answers.model.trout import TroutFrame
    n = len(starts)
    if not (len(rows) == len(displays) == len(licences) == n):
        raise TableError(f"trout table: {len(rows)} rows, {len(displays)} display and {len(licences)} licence frames "
                         f"for {n} segments")
    _hours_guard(rows, displays, starts, moments)
    split = [split_mixed(f) for f in rows]
    bodies: List[Optional[Body]] = []
    tables: List[Optional[dict]] = []
    for j in range(n):
        if is_null(rows[j], displays[j]):
            bodies.append(None)
            tables.append(None)
            continue
        b = Body(split[j][0], rules, licences[j], noun=noun)
        tables.append(b.build())
        bodies.append(b)
    year = TL.KeyYear(starts, rows, moments, water_closed=[d.get("status") == "closed" for d in displays],
                      scope_of=lambda ref: rules(ref).f.get("species") or (),
                      possession=[{S: c for S in TC if b is not None and (c := b.possession_count(S)) is not None}
                                  if b is not None else {} for b in bodies])
    after = _one_day(tables, year, starts, moments)
    out: List[Optional[dict]] = []
    for j in range(n):
        t = tables[j]
        if t is None:
            out.append(None)
            continue
        states = fish_states(rows[j])
        closed_table = A_NO_YEAR_BAR_ON_A_CLOSED_TABLE and set(states.values()) == {"closed"}
        if ((not A_YEAR_BAR_ONE_STATE) or TL.year_bar(states.values(), displays[j].get("status") != "closed")) \
                and not closed_table:
            t = {**t, "year": {"title": W.YEAR_TITLE, "runs": tuple(_year_runs(rows, starts, moments, sorted(states)))}}
        dates, all_year = _dates(year, j, t, after.get(j, {}))
        if all_year:
            # Q8: the fish that never reopen this year say so on their row (the others' day stands under the top line)
            t = {**t, "boxes": tuple({**b, "rows": tuple({**r, "lines": tuple(r["lines"]) + (W.CLOSED_ALL_YEAR,),
                                                          "sheet": {**r["sheet"], "here": tuple(r["sheet"]["here"]) + (
                                                              {"t": W.sentence(W.CLOSED_ALL_YEAR), "bold": False},)}}
                                                         if r["key"] in all_year else r for r in b["rows"])}
                                     for b in t["boxes"])}
        frame = {"table": t, "dates": dates, "covers": tuple(split[j][1])}
        if validate:
            TroutFrame.model_validate(frame)
        out.append(frame)
    return out


def _one_day(tables: List[Optional[dict]], year: TL.KeyYear, starts, moments) -> Dict[int, Dict[str, int]]:
    """Spec 3.3 rule 6: in a one-day segment, each keep row whose answer only loosens from the day before keeps the day
    before's row (its lines and sub) and gets a sheet line for the day; where the day after returns to that answer, its
    date is the day after's.
    Changes `tables` in place; returns {segment: {row key: the segment its date is read from}}."""
    out: Dict[int, Dict[str, int]] = {}
    if not A_ONE_DAY_STRICTER_ON_ROW or moments is not None:
        return out
    for j in range(1, len(starts) - 1):
        t, prev = tables[j], tables[j - 1]
        if t is None or prev is None or tables[j + 1] is None or starts[j + 1] - starts[j] != 1:
            continue
        before = {r["key"]: (b, r) for b in prev["boxes"] for r in b["rows"]}
        boxes = []
        for b in t["boxes"]:
            rows = []
            for ri, r in enumerate(b["rows"]):
                k = r["key"]
                a, now, nxt = year.answer(j - 1, k), year.answer(j, k), year.answer(j + 1, k)
                old = before.get(k)
                if (b["kind"] == "keep" and old is not None and old[0]["kind"] == "keep" and a is not None
                        and now is not None and TL._classify(a, now, k)[0] == "looser"
                        and _band_of(old[0], old[1]) == _band_of(b, r)):
                    o = old[1]
                    says = [x for x in r["lines"] if x not in o["lines"]] or list(r["lines"])
                    note = {"t": W.one_day(starts[j], says), "bold": False}
                    r = {**r, "lines": o["lines"], "sub": o["sub"],
                         "sheet": {**r["sheet"], "here": tuple(x for x in o["sheet"]["here"]) + (note,)}}
                    if nxt is not None and nxt.key == a.key:
                        out.setdefault(j, {})[k] = j + 1          # the row shown goes on: its date is the next one's
                rows.append(r)
            boxes.append({**b, "rows": tuple(rows)})
        tables[j] = {**t, "boxes": tuple(boxes)}
    return out


def _band_of(b: dict, r: dict) -> Optional[str]:
    ri = b["rows"].index(r)
    return next((x["text"] for x in b["bands"] if x["from"] <= ri <= x["to"]), None)


def _dates(year: TL.KeyYear, j: int, table: dict, after: Mapping[str, int] = {}) -> Tuple[Optional[dict], set]:
    """The segment's `DateSet` (contract 2.5): each row's next change (E), worded (G), placed at one level (E's
    `place_dates`); the sheet's coming-change sentence per row. `after`: rows whose date is read from a later segment
    (a one-day segment's row that shows the neighbours' answer). Also returns the keys of a closed table's rows that
    never reopen this year while the others share one reopening day (Q8: that day goes under the top line)."""
    keys = [r["key"] for b in table["boxes"] for r in b["rows"]]
    changes = {k: year.next_change(after.get(k, j), k) for k in keys}
    texts = {k: (W.date_line(c) if c is not None else None) for k, c in changes.items()}
    boxes = [TL.BoxRows(b["kind"], tuple(r["key"] for r in b["rows"]),
                        tuple((x["from"], x["to"], x["kind"]) for x in b["bands"])) for b in table["boxes"]]
    kinds = {b["kind"] for b in table["boxes"]}
    form = "closed" if kinds == {"closed"} else "release" if kinds == {"release"} else None
    placed = TL.place_dates(boxes, texts, form)
    all_year: set = set()
    if A_REOPENING_ONCE_UNDER_THE_TOP and form == "closed" and placed["top"] is None:
        said = {t for t in texts.values() if t is not None}
        never = {k for k, t in texts.items() if t is None}
        if len(said) == 1 and never and W.is_reopening(next(iter(said))):
            placed = {**placed, "top": next(iter(said)), "box": {}, "band": {}, "row": {}, "card": None}
            all_year = never
    sheet = {k: (W.sheet_sentence(c),) for k, c in changes.items() if c is not None}
    d = {**placed, "fold": placed["top"], "sheet": sheet}
    if not any(d[k] for k in ("top", "card", "box", "band", "row", "sheet")):
        return None, all_year
    return d, all_year
