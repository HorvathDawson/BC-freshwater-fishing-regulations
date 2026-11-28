"""Per part of a water, read off the export's decoded model (`codec.expand`): `Water`, a part's runs, place,
hint and label (`runs_label`, `part_place`, `part_label`, `part_labels`), its km, the picker, and the steelhead
line."""
from __future__ import annotations

import math
import re
from typing import Dict, List, Optional, Sequence, Tuple
from pipeline.common import entry_id as EID
from pipeline.deliver.answers.common import AnswersError, cap, clean, range_txt, when_dates
from pipeline.deliver.answers.display.rule import _js, cut_words, kind_of


def _win_txt(x: dict) -> str:
    w = when_dates(x.get("when"))
    return ", " + ", ".join(range_txt(a, b) for a, b in w) if w else ""


def _js_round(v: float) -> int:
    return int(math.floor(v + 0.5))


class Water:
    """One water of the export, as the page sees it: its parts with a rule set (parts outside
    B.C. carry none and are left out), in the page's order — most sections first, the export's
    order among equals — each part's rules its set's `reach` then `trib` ids, each sorted.

    `doc` is the decoded export (`codec.expand`); a rule's facts are read from `fields`,
    `provenance.rank`, `parts`, `label`, `verbatim`, `family`, `dimension`, `type`."""

    def __init__(self, water_id: str, w: dict, doc: dict):
        self.id, self.w, self.doc = water_id, w, doc
        self.export_ix = [i for i, p in enumerate(w["parts"]) if p.get("ruleset") is not None]
        self.export_ix.sort(key=lambda i: -w["parts"][i]["sections"])
        self.parts = [w["parts"][i] for i in self.export_ix]
        sets = doc["rulesets"]
        self.rules: List[List[str]] = []
        for p in self.parts:
            s = sets[p["ruleset"]]
            self.rules.append(sorted(s.get("reach") or []) + sorted(s.get("trib") or []))

    def r(self, k: str) -> dict:
        return self.doc["rules"][k]

    def f(self, k: str) -> dict:
        return self.r(k).get("fields") or {}

    def flat(self, k: str) -> dict:
        x = self.r(k)
        return {**(x.get("fields") or {}), "type": x.get("type"), "family": x.get("family"),
                "dimension": x.get("dimension")}

    def rank(self, k: str):
        return (self.r(k).get("provenance") or {}).get("rank")


def part_label(W: Water, i: int) -> str:
    """`partLabel` — what sets part i apart (consumer 3.1 step 3), from its rules' own words — with the ecological
    reserve's short form (`eco_reserve`), as every end word says it ("Within Skeena River Eco Reserve")."""
    return eco_reserve(_part_label(W, i))


def _part_label(W: Water, i: int) -> str:
    n = len(W.parts)
    if n == 1:
        return "Whole water"
    counts: Dict[str, int] = {}
    for ks in W.rules:
        for k in ks:
            counts[k] = counts.get(k, 0) + 1

    def own_ok(k):
        rk = W.rank(k)
        return (counts[k] < n and W.r(k).get("family") != "information"
                and ((rk is not None and 0 <= rk <= 2)
                     or (rk == -1 and "ALL_GAME_FISH" in (W.f(k).get("species") or []))))
    own = [k for k in W.rules[i] if own_ok(k)]
    if not own:
        return "The rest of the water"

    def score(k):
        x = W.flat(k)
        return (counts[k] * 10
                - (5 if kind_of(x) == "gate" and "ALL_GAME_FISH" in (x.get("species") or [])
                   else 0)
                - (-3 if x.get("dimension") == "lift" else 0)
                - (8 if _js((W.r(k).get("parts") or {}).get("where")) else 0))
    own.sort(key=score)
    pick = own[0]
    pp = W.r(pick).get("parts") or {}
    any_where = next((k for k in own if _js((W.r(k).get("parts") or {}).get("where"))), None)
    if _js(pp.get("where")) and _js(pp.get("what")):
        dated = _win_txt(W.f(pick)) if when_dates(W.f(pick).get("when")) and \
            not re.search(r"\d", pp["what"]) else ""
        base = f"{cap(pp['where'])}: {pp['what'].lower()}{dated}"
    elif any_where is not None:
        base = (f"{cap(W.r(any_where)['parts']['where'])}: "
                f"{clean(pp.get('what') or W.r(pick).get('label')).lower()}")
    else:
        base = "Stretch with: " + re.sub(r"^No Fishing", "no fishing",
                                         clean(W.r(pick).get("verbatim")))
    base = cut_words(clean(base), 130)

    def key(w):
        return re.sub(r"^(from|between|within|in) ", "", clean(w).lower())
    seen = {key(pp.get("where") or "")}
    extra: List[str] = []
    for k in own[1:]:
        rp = W.r(k).get("parts") or {}
        w = clean(rp.get("where") or "")
        t = re.sub(r"^within ", "", w) if w and key(w) not in seen else \
            clean(rp.get("what") or W.r(k).get("label")).lower()
        if t not in extra:
            extra.append(t)
        seen.add(key(w))
    if extra:
        more = f" (+{len(extra) - 1} more)" if len(extra) > 1 else ""
        return base + f" · also {cut_words(extra[0], 70)}{more}"
    return base


def _region(e: str) -> Optional[str]:
    """A water row's region (`r7:…` -> "7"); None for a table entry."""
    return EID.region_of(e) if EID.is_water_row(e) else None


#: (T801 item 5(1)) a part's field: the licensing set of each of its pieces that runs on the main stem (`_by_grp`
#: sets it on a rule-set group; a plain part's is its own, `stem_licensing`)
STEM_LICENSING = "stem_licensing"


def stem_licensing(parts: Sequence[dict]) -> List[Optional[str]]:
    """The licensing set (or None) of each part in `parts` with a main-stem run (a run that is not a `branch`): a
    part of side channels alone says nothing of where its water's designations stand."""
    return [p.get("licensing_set") for p in parts if any(not r.get("branch") for r in p.get("runs") or [])]


def _lic_entries(W: Water, j: int) -> List[str]:
    """(T801 item 5(1), owner 2026-11-16) the water rows whose licensing record is placed (`reach`) on EVERY main-stem
    piece of part j: the Skeena's "(mainstem only)" row, whose rules bind only its two weekend zones, still speaks on
    every part its Section 4 designation covers. None when the part has no main-stem piece."""
    p = W.parts[j]
    sets = p[STEM_LICENSING] if STEM_LICENSING in p else stem_licensing([p])
    if not sets:
        return []
    entries, lic = W.doc["entries"], W.doc.get("licensing") or {}

    def of(s):
        ls = W.doc["licensing_sets"].get(s) if s is not None else None
        return {lic[k]["entry_id"] for k in (ls or {}).get("reach") or []
                if (entries.get(lic[k]["entry_id"]) or {}).get("kind") == "water"}
    common = set.intersection(*(of(s) for s in sets))
    return sorted(common)


def part_place(W: Water, i: int) -> str:
    """`partPlace` — the entry a part comes from, when the water's parts come from two or more of
    its own entries (consumer 3.1 step 1). A water row speaks on a part by a rule it binds there (rank 0); a row whose
    place is its printed SCOPE ("(mainstem only)") and that speaks by a rule somewhere on the water speaks too on every
    part whose main stem its licensing record covers (T801 item 5(1): the Skeena's "Mainstem only" on every Skeena
    River 2 and Section 4 part, not only its two weekend zones). A region ("In Region 6") is where a part IS, read from
    the rules alone: a row's designation restated in another region moves no part's region (the Stellako)."""
    entries = W.doc["entries"]

    def scope(e):
        E = entries.get(e) or {}
        m = re.search(r"\(([^)]+)\)", E.get("full_name") or "")
        sc = re.sub(r"\.$", "", E.get("scope_note") or (m.group(1) if m else "") or "")
        return cap(sc) if sc and len(sc.split(" ")) > 1 else ""

    def by_rules(j):
        out = []
        for k in W.rules[j]:
            x = W.r(k)
            rk = W.rank(k)
            if rk is None:
                raise AnswersError(f"display: rule {k} ships no provenance rank")
            if rk == 0 and \
                    (entries.get(x["entry_id"]) or {}).get("kind") == "water" and \
                    x["entry_id"] not in out:
                out.append(x["entry_id"])
        return out
    ruled = {e for j in range(len(W.parts)) for e in by_rules(j)}

    def ents(j):
        out = by_rules(j)
        # only rows that speak by a rule elsewhere on the water: no water gains or loses a place
        return out + [e for e in _lic_entries(W, j) if e in ruled and e not in out and scope(e)]
    every: List[str] = []
    for j in range(len(W.parts)):
        for e in ents(j):
            if e not in every:
                every.append(e)
    if len(every) < 2:
        return ""

    def say(e):
        if scope(e):
            return scope(e)
        reg = _region(e)
        return f"In Region {reg.upper()}" if reg else ""
    regs = {_region(e) for e in every}
    mine: List[str] = []
    for t in (say(e) for e in ents(i)):
        if t and not (len(regs) < 2 and t.startswith("In Region ")) and t not in mine:
            mine.append(t)
    if len(mine) > 1 and all(t.startswith("In Region ") for t in mine):
        return "Where " + " and ".join(re.sub(r"^In ", "", t) for t in mine) + " meet"
    return " / ".join(mine)


_THE_LESS = re.compile(r"^\d|^[A-Z][\w.]* ?[\w.]*'s ", re.ASCII)


def begins_at(doc: dict, y: str) -> Optional[str]:
    """Where a stream BEGINS, as an end token: the upstream end (`from`) of its top main-stem run, when
    that is a cut of `splits` (the Vedder begins at Vedder Crossing Bridge); None where it begins at
    no cut (answers 2.8: then the place is said as "where it becomes the <name>")."""
    w = doc["waters"].get(y) or {}
    runs = [r for p in w.get("parts") or [] for r in p.get("runs") or []
            if not r.get("branch") and r.get("km_from") is not None]
    if not runs:
        return None
    top = max(runs, key=lambda r: r["km_from"]).get("from")
    return top if top in (doc.get("splits") or {}) else None


#: (T697) the end tokens of a lake that is not a named water (`bundle.spans`: the suffix dropped)
UNNAMED_LAKE = ("lake_inlet", "lake_outlet")


def _crossing_words(doc: dict, base: str, side: str) -> str:
    """A crossing said at its place: its own words — but an unnamed lake, either edge, is "unnamed lake" (owner
    2026-11-12: "unnamed lakes should be unnamed lake near x")."""
    return "unnamed lake" if base in UNNAMED_LAKE else end_name(doc, base, side)


#: (T1172, owner 2026-11-26: "inside Creston Valley WMA") the short form an area's kind takes in a part's name and subtitle
AREA_SHORT = ((re.compile(r"\bWildlife Management Area\b", re.I), "WMA"),)
#: (T1172, owner 2026-11-26: "it should be \"[name] eco reserve\" everywhere") every word a part ships says an ecological
#: reserve's short form — its name, its ends (`display.ends`, the map's marks), its subtitle, its label — applied by ONE
#: function (`eco_reserve`) at the two sources every such word is composed from (`end_name`, `part_label`); a national
#: park reserve and a park stay in full
ECO_RESERVE = (re.compile(r"\bEcological Reserve\b", re.I), "Eco Reserve")


def eco_reserve(words: str) -> str:
    """Words with an ecological reserve said in its short form (`ECO_RESERVE`): the one rule, at the two sources of a
    part's words — `end_name` (its ends, name, marks and subtitle) and `part_label` (its label, from its rules' words)."""
    return ECO_RESERVE[0].sub(ECO_RESERVE[1], words)


def area_short(words: str) -> str:
    """An area's name in its short form (`AREA_SHORT`): "Creston Valley Wildlife Management Area" → "Creston Valley WMA"."""
    for pat, short in AREA_SHORT:
        words = pat.sub(short, words)
    return words


#: a lake edge and another split id of one water this close (km, the export's own two decimals) stand at ONE place
#: (`end_name`'s "(km …)"): the lake edge is in no `same_place_as` group (`bundle.spans._clashes` skips edges)
ONE_PLACE_KM = 0.05

#: (T708 (b), owner 2026-11-13: "rename the end only … 'Mouth (at the signs)', no new cut, no change to which water the
#: rules cover") A WATER WHOSE MOUTH THE BOOK PLACES BY SIGNS, said with them: the Lakelse River ("Note: The mouth of the
#: river is designated by signs 1.7 km downstream of Lakelse River logging road bridge"). Its lowest part's lower end
#: ships the token `mouth:<item_id>` (`builders._names`) in place of `mouth`; only the words change, never a run, a cut
#: or a binding. Keyed by the durable item id (AGENTS 5).
MOUTH_WORDS: Dict[str, str] = {"gnis:4903": "at the signs"}
#: the prefix of that token
MOUTH_OF = "mouth:"


def end_name(doc: dict, e: Optional[str], side: str = "") -> str:
    """`endName` — a run end in words: THE ONE COMPOSER every part word goes through (`_end_short` / `end_words` /
    `display.ends`, `_run_txt` / the subtitle, `side_words`), with the ecological reserve's short form (`ECO_RESERVE`)."""
    return eco_reserve(_end_name(doc, e, side))


def _end_name(doc: dict, e: Optional[str], side: str = "") -> str:
    if not e:
        return ""
    if str(e).startswith(MOUTH_OF):
        iid = str(e)[len(MOUTH_OF):]
        if iid not in MOUTH_WORDS:
            raise AnswersError(f"run end {e!r}: no mouth words for {iid} (`part.MOUTH_WORDS`)")
        return f"the mouth ({MOUTH_WORDS[iid]})"
    if str(e).startswith(OWN):
        # T1140 PROPOSAL: a part of added city lines said by its own name and that name's source
        import json as _json
        nm, place = _json.loads(str(e)[len(OWN):])
        return f": {nm} ({place})"
    if str(e).startswith(AT):
        # T1140 (owner 2026-11-25, the Okanagan River Oxbows: "at Hayne's Lease Ecological Reserve"): its lines inside one
        # area while a polygon of it straddles the edge (`bundle.side_channels.inside_rows`), said "at <area>"
        return "at " + end_name(doc, INSIDE + str(e)[len(AT):], side)[len("inside "):]
    if str(e).startswith(INSIDE):
        # T1140 (owner 2026-11-25: "itshould have the real name … it will say eco reserve here"): a part with no main stem
        # lying at least 95 % inside one area, measured on the shapes (`bundle.side_channels.inside_rows`), said by the
        # area's name as every area end is: the export's split name less " boundary" where this water is cut at it,
        # else the catalogue's name cased by the one caser (`area_display`)
        # (T1172, owner 2026-11-26: "inside Creston Valley WMA") in the area's SHORT form (`AREA_SHORT`), the part's name
        # and its subtitle alike; the map's area labels keep the long one
        tok = str(e)[len(INSIDE):]
        if tok in (doc.get("splits") or {}):
            return "inside " + area_short(re.sub(r" boundary$", "", end_name(doc, tok, side)))
        from pipeline.deliver.bundle.place_names import area_display
        return "inside " + area_short(area_display(tok.split(":", 1)[1]))
    if str(e).startswith(SIDE):
        import json as _json
        w = side_words(doc, _json.loads(str(e)[len(SIDE):]))
        return w[:1].lower() + w[1:]
    if NEAR in str(e):
        # T517 (owner 2026-11-09): a crossing two parts of one water would otherwise name alike (a border, an area's
        # boundary, an unnamed lake, a river polygon), said near what stands there (`topology.crossing_near`)
        base, at = str(e).split(NEAR, 1)
        there = end_name(doc, at, side)
        return f"near {there}" if base == "polygon" else f"{_crossing_words(doc, base, side)} near {there}"
    if PLACE in str(e):
        # …or, with nothing named within reach, at its km from the mouth
        base, km = str(e).rsplit(PLACE, 1)
        return f"km {km}" if base == "polygon" else f"{_crossing_words(doc, base, side)} (km {km})"
    if str(e).startswith("lake:") and str(e).count(":") == 2:
        # (T520 derived) where the main line enters / leaves a lake: `lake:<wbk>:inlet|outlet`
        _, wbk, which = str(e).split(":")
        nm = (doc["waters"].get(f"wbk:{wbk}") or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: lake wbk:{wbk} has no name in the export")
        return f"the {nm} {which}"
    if str(e).startswith("trib:"):
        return end_name(doc, "confluence:" + str(e).split(":", 1)[1], side)       # (T520 derived) a confluence
    if str(e).startswith(("region:", "mu:")) and "|" in str(e):
        # (T520 derived) a region / management-unit boundary crossing, the downstream one first
        head, pair = str(e).split(":", 1)
        a, b = pair.split("|")
        return f"the Region {a} / Region {b} boundary" if head == "region" else f"the MU {a} / MU {b} boundary"
    if str(e).startswith("water:"):
        nm = (doc["waters"].get(str(e).split(":", 1)[1]) or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: that water has no name in the export")
        return nm                                       # another named water a part lies near (`topology.part_near`)
    if str(e).startswith("place:"):
        return str(e).split(":", 1)[1]                  # a B.C. town (`topology.crossing_near`)
    if e == "polygon":
        return "a river polygon"                        # never a part's end on its own: always said near or at km
    if e == "mouth":
        return "the mouth"
    if str(e).startswith("continues:"):
        # RULINGS P10: not a mouth — the water goes on under another name; the
        # place is where that water begins (Chilliwack: "Vedder Crossing Bridge")
        y = e.split(":", 1)[1]
        b = begins_at(doc, y)
        nm = (doc["waters"].get(y) or {}).get("name")
        if not nm:
            raise AnswersError(f"run end {e!r}: the water it goes on as has no name in the export")
        return end_name(doc, b, side) if b else f"where it becomes the {nm}"
    if e == "source":
        return "the top of the river" if side == "from" else "the source"
    if e == "bc_border":
        return "the B.C. border"
    if str(e).startswith("km:"):
        # T517: a place no named feature stands near, said by its km from the mouth (`_named_token`)
        return f"km {e.split(':', 1)[1]}"
    if str(e).startswith("near:"):
        # T517: a gauge cut said by the named feature nearest it on the ground (`_named_token`)
        return "near " + end_name(doc, e.split(":", 1)[1], side)
    splits = doc.get("splits") or {}
    sp = splits.get(e)
    if sp:
        # "(km …)" tells two PLACES of one water with one name apart — never two ids at ONE place (T691 item 3: the
        # Kootenay's navigation dolphin and the Main Body lake edge it aliases, km 122.7 both, read "… (km 123)";
        # a lake edge is no `same_place_as` member, `bundle.spans._clashes`)
        dup = bool(sp.get("water_id")) and sp.get("km") is not None and any(
            o is not sp and k != sp.get("same_place_as") and o.get("same_place_as") != e
            and o.get("water_id") == sp.get("water_id")
            and str(o.get("name")).lower() == str(sp.get("name")).lower()
            and not ("lake_edge" in (o.get("kind"), sp.get("kind")) and o.get("km") is not None
                     and abs(float(o["km"]) - float(sp["km"])) <= ONE_PLACE_KM)
            for k, o in splits.items())
        nm0 = re.sub(r"\bd/s\b", "downstream", re.sub(r"\bu/s\b", "upstream", sp["name"]))
        the = "" if re.match(r"^(area|region_line):", e) or _THE_LESS.search(nm0) else "the "
        km = f" (km {_js_round(sp['km'])})" if dup else ""
        # T780 (owner 2026-11-20): a part's end is said by the place's name alone — never with the cut's review
        # words ("…, approximate location (medium confidence)", T207). The cut's `confidence` still ships (the
        # export's splits, the bundle's `part_mark`) and the review app says it beside the cut's label.
        return the + re.sub(r"^the ", "", nm0, flags=re.I) + km
    head, _, ident = e.partition(":")
    nm = (doc["waters"].get(ident) or {}).get("name") if ident else ""
    if ident and not nm and head in ("confluence", "lake_inlet", "lake_outlet"):
        raise AnswersError(f"run end {e!r}: water {ident} has no name in the export")
    if head == "confluence":
        return f"the {nm} confluence" if nm else "a tributary confluence"
    if head == "lake_inlet":
        return nm if nm else "a lake"
    if head == "lake_outlet":
        return f"the {nm} outlet" if nm else "a lake outlet"
    if head == "area":
        return ident
    if head == "region_line":
        return f"the Region {ident} line"
    raise AnswersError(f"run end {e!r}: a token no rule names (T517: no fallback words)")


#: (T1199 PR 2, owner 2026-11-28: "do the recommended the app should auto join them later i think assuming same rules")
#: the words of a stretch between two crossings of an area's edge that lies OUTSIDE the area (it does not carry the
#: area's rule): the Beaverfoot River's two open stretches beside Yoho. ONE constant, the owner's pick.
OUTSIDE_BETWEEN = "Outside {area} (between its boundaries)"
def area_key(name: str) -> str:
    """(T1199 PR 2, review) THE ONE KEY an area is matched by, on both sides: the area name a part's end token carries
    (`area:<name>@…`) and the names the bundle writes (`part_area_held`, `cut_area`): Unicode NFC, spaces collapsed,
    case folded. Two spellings of one area never read as two areas, nor one area as "not carried"."""
    import unicodedata
    return " ".join(unicodedata.normalize("NFC", str(name)).split()).casefold()


def area_of_token(tok: str) -> str:
    """The area name an `area:<name>[@…]` end token stands for."""
    return str(tok)[len("area:"):].split("@")[0]


#: (T1199 PR 2) a part's field: the cut areas whose rule it carries (the bundle's `part_area_held`: the area's registry
#: item holds every section of it), set on every part by the answers step (`builders.held_facts`)
AREA_HELD = "area_held"


def _run_txt(doc: dict, r: dict, held: Optional[Sequence[str]] = None) -> str:
    if r.get("lakes"):
        n = r["lakes"]
        return _run_txt(doc, {**r, "lakes": 0}, held) + f" (through {n} lake{'s' if n > 1 else ''})"
    if _js(r.get("polygon")):
        return "" if r["polygon"] == "whole" else str(r["polygon"])
    if r.get("from") and r.get("from") == r.get("to") and str(r["from"]).startswith("area:"):
        # (T1199 PR 2, RULINGS E36) a stretch between two crossings of one area's edge is "within" it only when it
        # carries the area's rule; else it lies outside, between its boundaries. Never guessed: no facts, no words
        area = re.sub(r" boundary$", "", end_name(doc, r["from"]))
        if held is None:
            raise AnswersError(f"display: a stretch between two crossings of {area}: whether it lies inside is not "
                               f"given (`{AREA_HELD}`)")
        inside = area_key(area_of_token(r["from"])) in {area_key(h) for h in held}
        return ("Within " + area) if inside else OUTSIDE_BETWEEN.format(area=area)
    a, b = end_name(doc, r.get("from"), "from"), end_name(doc, r.get("to"), "to")
    t = f"From {b} up to {a}"
    return f"Side channel ({t[:1].lower() + t[1:]})" if r.get("branch") else t


def _from_mouth(side: List[dict]) -> str:
    """Where side channels meet their river, from their runs' `km_from` (the main stem's measure from the mouth): "4.1 km
    from the mouth", "1.1–5.5 km from the mouth", or "" when no run carries a measure. (T1172, owner 2026-11-26: "no verb
    for a leaving channel") NO VERB: a channel's measure is where it joins, where it leaves, or the main line's nearest
    point (`section_join.how`), and one shape says all three truthfully."""
    kms = sorted(round(float(r["km_from"]), 1) for r in side if r.get("km_from") is not None)
    if not kms:
        return ""
    lo, hi = kms[0], kms[-1]
    return f"{lo:g} km from the mouth" if lo == hi else f"{lo:g}–{hi:g} km from the mouth"


#: (T1172) a part's fields the answers step sets on a part of side channels only (`builders.side_facts`), read by
#: `runs_label`: whether its water has MORE THAN ONE such part (`SIDE_APART`: only then does a distance tell it from the
#: others), and the area it lies inside or at (`SIDE_AREA`: [catalogue name, how, cut], the bundle's `part_inside`)
SIDE_APART = "side_apart"
SIDE_AREA = "side_area"


def side_only(part: dict) -> bool:
    """A part of side channels only: runs, every one a side channel's (`branch`), a polygon of the whole water aside."""
    rs = [r for r in part.get("runs") or [] if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    return bool(rs) and all(r.get("branch") for r in rs)


def area_words(doc: dict, area: Sequence) -> str:
    """(T1172) The words of the area a part lies inside or at ("inside Creston Valley WMA", "at Hayne's Lease Eco
    Reserve"): its end token (`AREA_HEAD`) said by `end_words`, exactly as the part's name says it."""
    w = end_words(doc, f"{AREA_HEAD[area[1]]}area:{area[0]}", "from")
    return w[:1].lower() + w[1:]


def water_ends_km(w: dict) -> Tuple[Optional[float], Optional[float]]:
    """(T697) The km of a water's own lower and upper end: the lowest `km_to` and the highest `km_from` of the main-stem
    runs of ALL its parts (outside B.C. too) — where its stem begins and ends, cut or not. (None, None): no measured
    main-stem run."""
    main = [r for p in w.get("parts") or [] for r in p.get("runs") or []
            if not r.get("branch") and not _js(r.get("polygon"))]
    los = [float(r["km_to"]) for r in main if r.get("km_to") is not None]
    his = [float(r["km_from"]) for r in main if r.get("km_from") is not None]
    return (min(los) if los else None, max(his) if his else None)


def own_end(tok: Optional[str], km: Optional[float], w_km: Tuple[Optional[float], Optional[float]]) -> Optional[str]:
    """(T697, RULINGS P55) An UNNAMED LAKE at the water's OWN top or bottom is no cut: the water's own end, "source" /
    "mouth", as every water's is. THE ONE PLACE this is decided: the part's name (`builders._names`) and its header
    (`runs_label`) both read it (T711: the header said "up to a lake outlet" under a name reading "→ Source")."""
    lo_w, hi_w = w_km
    if km is None:
        return tok
    if tok == "lake_outlet" and hi_w is not None and abs(km - hi_w) <= 0.05:
        return "source"
    if tok == "lake_inlet" and lo_w is not None and abs(km - lo_w) <= 0.05:
        return "mouth"
    return tok


def runs_label(doc: dict, part: dict, w_km: Tuple[Optional[float], Optional[float]] = (None, None)) -> str:
    """`runsLabel` — where a part runs, said from the mouth upward (consumer 3.1 step 2). `w_km` is the water's own
    ends (`water_ends_km`): an unnamed lake there is said as the water's end (`own_end`), as the part's name says it."""
    rs = [r for r in part.get("runs") or []
          if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    rs = [r if r.get("branch") else {**r, "from": own_end(r.get("from"), r.get("km_from"), w_km),
                                     "to": own_end(r.get("to"), r.get("km_to"), w_km)} for r in rs]
    if not rs:
        return ""
    main0 = sorted((r for r in rs if not r.get("branch")),
                   key=lambda r: -(r.get("km_from") if r.get("km_from") is not None else 0))
    side = [r for r in rs if r.get("branch")]
    main: List[dict] = []
    for r in main0:
        last = main[-1] if main else None
        if last and str(last.get("to") or "").startswith("lake_inlet") and \
                str(r.get("from") or "").startswith("lake_outlet"):
            last["to"], last["km_to"] = r.get("to"), r.get("km_to")
            last["lakes"] = (last.get("lakes") or 0) + 1
        else:
            main.append(dict(r))
    held = part.get(AREA_HELD)
    bits = [b for b in (_run_txt(doc, r, held) for r in reversed(main)) if b]

    def low(s):
        return s[:1].lower() + s[1:]
    if not bits:
        t = ""
    elif len(bits) == 1:
        t = bits[0]
    elif len(bits) == 2:
        t = f"Two stretches: {low(bits[0])}, and {low(bits[1])}"
    else:
        t = f"{len(bits)} stretches, including {low(bits[0])}"
    if not t and side:
        # A PART OF SIDE CHANNELS ONLY (T1172, owner 2026-11-26, one rule for every water):
        # - "Side channel" / "N side channels";
        # - the area it lies inside or at, in the words its name uses ("2 side channels inside Creston Valley WMA"),
        #   for EVERY such part the bundle measures in one (`SIDE_AREA`), reviewed or not;
        # - its distance from the mouth ONLY where its water has more than one such part (`SIDE_APART`: "Distance only to
        #   tell parts apart"), with no verb (`_from_mouth`). Where no channel carries a measure there, a lone channel
        #   keeps its run's words ("Side channel (from … up to …)").
        area = part.get(SIDE_AREA)
        t = ("Side channel" if len(side) == 1 else f"{len(side)} side channels") + \
            (f" {area_words(doc, area)}" if area else "")
        if part.get(SIDE_APART):
            where = _from_mouth(side)
            if where:
                t += f", {where}"
            elif len(side) == 1 and not area:
                t = _run_txt(doc, side[0], part.get(AREA_HELD))
    # RULINGS P39: a part WITH a main run never mentions its side channels —
    # they are simply part of it ("… CNR Mile 66 Trestle and 1 side channel" confused the owner)
    return t


#: answers 2.7 (A4): a short part label's end names lose the sign-post words the full label keeps.
#: RULINGS P40: no label cuts a name mid-way ("Sikanni Chief River…" was scary) —
#: an end is said whole however long, and a part with no run shows its full label.
_SIGN_WORDS = re.compile(r"^(?:the )?(?:fishing )?(?:boundary )?signs? (?:at |near )?", re.I)
#: what is left after the sign words must name a place: nothing, a bare note "(200 m …)" or "its mouth" does not
_NO_PLACE = re.compile(r"^(?:$|\(|(?:its|their) )", re.I)


def _sign_less(t: str) -> str:
    """The end without its sign-post words — unless the signs ARE the place (T708 Gagnon: "fishing boundary signs
    (200 m upstream of the lake on the creek)" reads as the signs, never "(200 m …)" or "Its mouth")."""
    rest = _SIGN_WORDS.sub("", t)
    return t if _NO_PLACE.match(rest) else rest


#: T517: a regulation's cut named after the gauge beside it ("above Babine River (WSC gauge 08EB005)"): the gauge
#: reference is not the place's words (it stays an alternate, `end_variants`)
_GAUGE_REF = re.compile(r"\s*\((?:WSC |ECCC )?(?:hydrometric )?(?:gauge|station) \d{2}[A-Z]{2}\d{3}\)", re.I)


def _end_short(doc: dict, e: Optional[str], side: str) -> str:
    """A run end in a few words: "Mouth", "Source", "B.C. border", "Vedder Crossing Bridge"."""
    if e in ("mouth", "source"):
        return cap(e)
    if str(e).startswith("continues:") and begins_at(doc, e.split(":", 1)[1]) is None:
        nm = (doc["waters"].get(e.split(":", 1)[1]) or {}).get("name") or ""
        return cap(f"{nm} begins")
    t = end_name(doc, e, side)
    t = re.sub(r"^the ", "", t)
    t = _sign_less(t)
    t = re.sub(r" of Canada\b", "", t) or t
    t = _GAUGE_REF.sub("", t) or t
    return t if t.startswith("km ") else cap(t)       # a unit stays lower case ("km 121.9")


#: T517: a feature this close (metres, on the ground) to a gauge cut stands AT it ("Tanswanket Creek confluence");
#: farther, the end is said as near it ("near the Tanswanket Creek confluence")
SAME_PLACE_M = 200


def _named_token(doc: dict, e: Optional[str]) -> Optional[str]:
    """T517: the token a run end is SAID by. A gauge cut (`kind` gauge) is never the name of a place a regulation
    ends at (its code and station name stay an alternate: `end_variants`, the mark's `alt`): the regulation's own split
    at the same place (`same_place_as`), else the NEAREST NAMED FEATURE on the ground (`near`, the bundle's
    `topology.gauge_near`: a tributary's confluence or a named cut of the water, within `topology.NEAR_M`), said as
    near it beyond `SAME_PLACE_M`; else BY ITS KM from the mouth ("km 121.9": the place has no name — a deliberate
    rule, pinned in test_part_geo). A gauge cut at several places with neither stops the build."""
    splits = doc.get("splits") or {}
    sp = splits.get(e or "")
    if not sp or sp.get("kind") != "gauge":
        return e
    if sp.get("same_place_as") and sp["same_place_as"] in splits:
        return sp["same_place_as"]
    if sp.get("near"):
        return sp["near"] if sp["near_m"] <= SAME_PLACE_M else "near:" + sp["near"]
    if sp.get("km") is None:
        raise AnswersError(f"gauge cut {e!r}: no regulation split, no named feature near it and no one km to say it by")
    return f"km:{round(float(sp['km']), 1):g}"


def end_variants(doc: dict, e: Optional[str], side: str) -> List[str]:
    """T517 (owner 2026-11-08: a boundary that is both a gauge and a regulation's cut "can carry several name variations,
    and any can be used"): EVERY name of a run end, the one said first (`end_words`: the regulation's own), then the
    others it is also known by — the gauge's own name, a same-place cut's, an official spelling — none discarded."""
    first = end_words(doc, e, side)
    splits = doc.get("splits") or {}
    out = [first]
    for t in (e, (splits.get(e or "") or {}).get("same_place_as")):
        sp = splits.get(t or "")
        if not sp:
            continue
        for n in (_end_short(doc, t, side), cap(re.sub(r"\bd/s\b", "downstream", sp["name"])) if _GAUGE_REF.search(
                sp.get("name") or "") else None, sp.get("official_name")):
            if n and n not in out:
                out.append(n)
    return out


#: T517: the separator of an end token said at its place (`at_place`): "<token>@km<km>"
PLACE = "@km"
#: T517: the separator of an end token said near what stands there: "<token>@near:<token>"
NEAR = "@near:"
#: T1140: the end token of a part whose side channels lie wholly inside one area: "near:inside:<area token>", shipped as
#: BOTH its ends. The `near:` head is the one the app already composes as "<water> <words>" (`spanName`: lo = hi =
#: "near:…"), so a shipped app says "Squamish River inside Baynes Island Ecological Reserve" with no change of its own.
INSIDE = "near:inside:"
#: T1140: the same for a part whose lines lie in one area while a polygon of it straddles the area's edge: "near:at:…",
#: said "<water> at <Area>"
AT = "near:at:"
#: T1140 PROPOSAL: a part made only of added city lines with their own name (`bundle.side_channels.own_name_rows`):
#: "near:own:" + JSON [name, its municipality], said "<water>: <name> (<municipality>)" ("Noons Creek: Turner Creek (Port
#: Moody)"; the composers join a word beginning ":" with no space); the layer's full label stays the water's source
OWN = "near:own:"
#: the token head of each `part_inside.how`
AREA_HEAD = {"inside": INSIDE, "at": AT}


#: T1140 (owner 2026-11-25: "Named by its ends", "They should always have the cut names or distance it is impossible not
#: to") the end token of a part of side channels only that is named by its channels' NAMED ends (`side_channels`): shipped
#: as both its ends, "near:side:" + JSON [[lower end, upper end, km]…], each end a named token or "" and km the channel's
#: join where neither end is named. Said "Side channel from <lower> up to <upper>" (one channel) or "Side channels: one
#: …; one up to …" (`side_words`: every channel named, no distance, T1172); the `near:` head is what a shipped app composes
#: as "<water> <words>".
SIDE = "near:side:"
#: The run ends that name nothing on a side channel: its own two ends where it leaves and rejoins its river ("mouth",
#: "source" on a channel's run are the CHANNEL's), an unnamed lake, the water's own polygon.
UNNAMED_ENDS = ("mouth", "source", "polygon", "lake_inlet", "lake_outlet")
#: T1140: the parts the build may NAME "<water> inside <area>" / "<water> at <area>", each checked by a reviewer on its
#: picture (item id, the area's catalogue name, how): THE NAME'S GATE ONLY. (T1172, owner 2026-11-26: "universally")
#: the bundle measures every part in every area by one rule (`bundle.side_channels.inside_rows`: in a clean-cut area by
#: the cut, T1199 E30; else on its shapes) and the part's SUBTITLE
#: says its area whether or not it is listed here (`runs_label`). An unlisted area the atlas cuts at stops the build
#: until it is looked at and added here; an unlisted membership-only one (a park or WMA the atlas does not cut at) is
#: listed in the build log ("INSIDE candidate") and the name takes its next rung; a listed part no longer said inside
#: stops the build too (`builders.reviewed_still_said`).
REVIEWED_INSIDE = frozenset({("gnis:25671", "BAYNES ISLAND ECOLOGICAL RESERVE", "inside"),
                             ("gnis:18167", "Pacific Rim National Park Reserve of Canada", "inside"),
                             ("wsc:300-432687-459615", "HAYNE'S LEASE ECOLOGICAL RESERVE", "at"),
                             # (T1172, owner 2026-11-26: "Yes, but name it by the area") the Old Kootenay River
                             # Channel's side part: 99.40 % inside the WMA, which alone tells it from its main line
                             ("gnis:2123", "CRESTON VALLEY WILDLIFE MANAGEMENT AREA", "inside"),
                             # (T1172, owner 2026-11-26: "yes name it but this is something we need to fix generally")
                             # Thelwood Creek's side part: 100 % inside the park (membership-only)
                             ("gnis:21681", "STRATHCONA - WESTMIN PARK", "inside"),
                             # (T1199, RULINGS E35, owner 2026-11-28, shown its picture: "yes but we need to make sure
                             # the rule is consistent across the board here not just a ranom special case") the Skeena's
                             # side channel by the reserve: all its lines edge water the cut puts in the reserve (71.7 %
                             # by shape), "Side channel inside Skeena River Eco Reserve"
                             ("gnis:2936", "SKEENA RIVER ECOLOGICAL RESERVE", "inside")})


def named_end(tok: Optional[str]) -> Optional[str]:
    """A side channel's run end that names a place (a cut, an area's boundary, the B.C. border, a named lake or water), else
    None."""
    if not tok or tok in UNNAMED_ENDS or str(tok).startswith(("mouth:", "continues:")):
        return None
    return str(tok)


def side_channels(part: dict, flows_into: Optional[str] = None) -> Optional[list]:
    """T1140 THE CHANNELS OF A PART WITH NO MAIN STEM, as `SIDE` carries them, or None — the part is then said by its
    label, the plain count (`runs_label`) — unless EVERY channel has a named end (T1172 round 4, owner 2026-11-26:
    Snehumption's "one up to the B.C. border; one other" reads "2 side channels"). `flows_into`: the named water the
    part's lowest channel flows into where its run says only its own mouth (the bundle's flow pointers,
    `builders._flows_into`): that channel's lower end."""
    runs = sorted((r for r in part.get("runs") or [] if r.get("branch")),
                  key=lambda r: (r.get("km_from") is None, r.get("km_from") or 0, str(r.get("from")), str(r.get("to"))))
    if not runs:
        return None
    out = []
    for k, r in enumerate(runs):
        lo, hi = named_end(r.get("to")), named_end(r.get("from"))
        if lo is None and flows_into and k == 0 and r.get("to") == "mouth":
            lo = flows_into
        km = round(float(r["km_from"]), 1) if r.get("km_from") is not None else None
        out.append([lo or "", hi or "", km])
    return out if all(c[0] or c[1] for c in out) else None


def side_words(doc: dict, chans: list) -> str:
    """The words of a `SIDE` token: each channel by its named ends, never a distance (T1172, owner 2026-11-26: a `SIDE`
    part is the one such part of its water). Every channel has a named end (`side_channels`); one without STOPS."""
    def one(lo: str, hi: str) -> str:
        # (owner 2026-11-25: "The clearer one") a channel that flows into a lake says so: "into Olsen Lake"
        low = f"{'into' if lo.startswith('lake_inlet:') else 'from'} {end_name(doc, lo, 'to')}" if lo else ""
        return " ".join(x for x in (low, (f"up to {end_name(doc, hi, 'from')}" if hi else "")) if x)
    if not chans or any(not (c[0] or c[1]) for c in chans):
        raise AnswersError(f"display: a SIDE token with a channel of no named end ({chans}): `side_channels` gives none")
    if len(chans) == 1:
        return f"Side channel {one(chans[0][0], chans[0][1])}"
    # (owner 2026-11-25: "The clearer one") "<water> side channels: one from …; one up to …"
    return "Side channels: " + "; ".join(f"one {one(c[0], c[1])}" for c in chans)


def at_place(token: str, km: Optional[float]) -> str:
    """T517 (owner 2026-11-09) an end token said at its PLACE — its km from the mouth, one decimal. A part end with no
    km cannot be told from another one: the build stops."""
    if km is None:
        raise AnswersError(f"part end {token!r}: no km to tell it from another end of the same words")
    return f"{token}{PLACE}{round(float(km), 1):g}"


def end_table(doc: dict, extra: Sequence[str] = ()) -> Dict[str, List[str]]:
    """T517 THE ONE NAME SOURCE for a place a run can end at: {token: [words, other names…]} for EVERY split of the
    export (cut, area, region line, lake edge) and every end token a water's runs carry (confluence, lake inlet/outlet,
    mouth, source, a water that goes on as another) — what the part names, the marks and in-season "Where" say. The
    words are the `from` side's (`end_variants`); a token whose `to` side reads otherwise ("the source") carries it
    as `"to:" + words` last."""
    # every cut but a gauge (a gauge is a place's name only where a run ends at it: below), every run end token
    toks = {k for k, v in (doc.get("splits") or {}).items() if v.get("kind") != "gauge"}
    for w in (doc.get("waters") or {}).values():
        for p in w.get("parts") or []:
            for r in p.get("runs") or []:
                toks.update(t for t in (r.get("from"), r.get("to")) if t)
    toks.update(extra)                          # the part ends said at their place (`at_place`)
    out: Dict[str, List[str]] = {}
    for t in sorted(toks):
        v = end_variants(doc, t, "from")
        to = end_words(doc, t, "to")
        out[t] = v + ([f"to:{to}"] if to != v[0] else [])
    return out


def end_words(doc: dict, e: Optional[str], side: str) -> str:
    """T517: ONE end of a run, in the few words every name and mark uses (`_end_short`, the gauge rule above). Never
    empty: a token nothing can name stops the build."""
    t = _end_short(doc, _named_token(doc, e), side)
    if not t:
        raise AnswersError(f"run end {e!r} ({side}) has no words")
    return t


def run_ends(doc: dict, part: dict) -> Optional[tuple]:
    """T517 THE ONE NAME OF A PART: (lower end, upper end) in words, mouth first — its lowest main-stem run's
    downstream end and its highest run's upstream end. None: no main-stem run (a lake, side channels only) — the app says
    the water's name."""
    rs = [r for r in part.get("runs") or [] if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    main = [r for r in rs if not r.get("branch") and not _js(r.get("polygon"))]
    if not main:
        return None
    km = [(r.get("km_from") if r.get("km_from") is not None else 0) for r in main]
    lo, hi = main[km.index(min(km))], main[km.index(max(km))]
    return end_words(doc, lo.get("to"), "to"), end_words(doc, hi.get("from"), "from")


def end_tokens(doc: dict, part: dict) -> Optional[tuple]:
    """T517 (owner 2026-11-09) A PART SHIPS ITS TWO END TOKENS, never a composed name: (lower end, upper end, the
    lower end's km, the upper end's km), mouth first — its lowest main-stem run's downstream end and its highest
    run's upstream end. The words come from the release's `display.ends` (`end_table`), composed by one app function.
    None: no main-stem run (a lake, side channels only, a polygon) — the part keeps its `short`."""
    rs = [r for r in part.get("runs") or [] if not _js(r.get("polygon")) or r["polygon"] != "whole"]
    main = [r for r in rs if not r.get("branch") and not _js(r.get("polygon"))]
    if not main:
        return None
    if any(r.get("km_from") is None for r in main):
        raise AnswersError(f"part runs {[(r.get('from'), r.get('to')) for r in main]}: a main-stem run with no km")
    lo = min(main, key=lambda r: r["km_from"])
    hi = max(main, key=lambda r: r["km_from"])
    if not lo.get("to") or not hi.get("from"):
        raise AnswersError(f"part runs {[(r.get('from'), r.get('to')) for r in main]}: a main-stem end with no token")
    return lo["to"], hi["from"], lo.get("km_to"), hi["km_from"]


def branch_end_tokens(part: dict) -> Optional[tuple]:
    """T517 (owner 2026-11-09) the two end tokens of a part with NO main stem (side channels only), read off its
    side-channel runs the way `end_tokens` reads a main stem — only asked where one water has several such parts (a
    single one is said by its water's name). None: no run with ends (a lake's polygon)."""
    side = [r for r in part.get("runs") or [] if r.get("branch") and r.get("km_from") is not None
            and r.get("from") and r.get("to")]
    if not side:
        return None
    lo = min(side, key=lambda r: r["km_from"])
    hi = max(side, key=lambda r: r["km_from"])
    return lo["to"], hi["from"], lo.get("km_to"), hi["km_from"]


def end_name_of(doc: dict, lo: str, hi: str) -> str:
    """The words two end tokens stand for, as the app composes them from `display.ends` (`spanName`): "<lower> →
    <upper>" — every part, an area's or a border's included (owner 2026-11-09: no "Within")."""
    return f"{end_words(doc, lo, 'to')} → {end_words(doc, hi, 'from')}"





def part_km(part: dict) -> Optional[float]:
    """`partKm` — the picker's mouth-upward sort key: the highest `km_from` of the main-stem runs,
    else of the side channels (they sort where they join), else last."""
    def km(rs):
        return [r["km_from"] for r in rs if r.get("km_from") is not None]
    m = km([r for r in part.get("runs") or [] if not r.get("branch")])
    b = km([r for r in part.get("runs") or [] if r.get("branch")])
    return max(m) if m else max(b) if b else None          # None: no run carries a measure


def part_labels(W: Water) -> List[str]:
    """`partLabels` — each part's picker name, in the page's part order (consumer 3.1)."""
    rls = [runs_label(W.doc, p, water_ends_km(W.w)) for p in W.parts]
    out: List[str] = []
    for i in range(len(W.parts)):
        pl, lab, rl = part_place(W, i), part_label(W, i), rls[i]
        if rl:
            same = any(j != i and rls[j] == rl for j in range(len(W.parts)))
            hint = ""
            if same and not re.match(r"^(The rest of the water|Whole water)", lab):
                h = re.sub(r"^[^:·]{3,90}: ", "", re.sub(r"^Stretch with: ", "", lab))
                hint = h.split(" · also ")[0]                 # whole (RULINGS P40)
            out.append((pl + " · " if pl else "") + rl + (f" — {hint}" if hint else ""))
        elif not pl:
            out.append(lab)
        elif re.match(r"^(Stretch with: |The rest of the water)", lab):
            rest = re.sub(r"^The rest of the water", "the rest",
                          re.sub(r"^Stretch with: ", "", lab))
            out.append(f"{pl}: {rest}")
        else:
            out.append(f"{pl} · {lab}")

    def core(s):
        return re.sub(r" \(\+\d+ more\)$", "", s)
    for i in range(len(out)):
        j = next((k for k in range(i) if core(out[k]) == core(out[i])), None)
        if j is None:
            continue
        mine, theirs = W.rules[i], W.rules[j]
        diff = [k for k in dict.fromkeys(mine) if k not in set(theirs)]
        less = [k for k in dict.fromkeys(theirs) if k not in set(mine)]

        def say(k):
            return clean((W.r(k).get("parts") or {}).get("what") or W.r(k).get("label")).lower()
        if diff:
            tail = f" · plus {cut_words(say(diff[0]), 48)}"
        elif less:
            tail = f" · without {cut_words(say(less[0]), 48)}"
        else:
            tail = " · (same rules, a separate stretch)"
        out[i] = core(out[i]) + tail
    return out


def picker(W: Water, labels: Sequence[str], closed: Sequence[bool]) -> dict:
    """The picker (consumer 3.2): parts closed to every game fish all year are one choice when the
    water has two or more parts; choices sorted open before closed, then mouth upward (`part_km`);
    a heading per entry place when choices come from two or more. `labels` and `closed` are in the
    page's part order; the choices name the EXPORT's part indexes."""
    n = len(W.parts)
    shut = [i for i in range(n) if n > 1 and closed[i]]
    groups = [{"idx": [i], "closed": False} for i in range(n) if i not in shut]
    if shut:
        groups.append({"idx": shut, "closed": True})
    groups.sort(key=lambda g: g["idx"][0])
    # river order is `topology.order_parts` (one implementation): by km, a side channel with no km beside the
    # main-stem part it runs beside (T710), the other parts with no measure last
    from pipeline.deliver.bundle.topology import order_parts
    rank = {p["v"]: r for r, p in enumerate(order_parts(
        [{"v": i, "km": part_km(p), "runs": p.get("runs") or [], "touches": p.get("touches") or []}
         for i, p in enumerate(W.parts)]))}
    groups.sort(key=lambda g: (g["closed"], rank[g["idx"][0]]))

    def label(g):
        if g["closed"]:
            return (f"Closed all year · {len(g['idx'])} stretches" if len(g["idx"]) > 1
                    else f"Closed all year · {labels[g['idx'][0]]}")
        return labels[g["idx"][0]]
    out, heads = [], []
    for g in groups:
        pl = "" if g["closed"] else part_place(W, g["idx"][0])
        if pl not in heads:
            heads.append(pl)
        text = cap(re.sub(r"^( · |: )", "", label(g)[len(pl):])) if pl else label(g)
        out.append({"parts": [W.export_ix[i] for i in g["idx"]], "closed": g["closed"],
                    "sections": sum(W.parts[i]["sections"] for i in g["idx"]),
                    "heading": pl, "text": text})
    return {"choices": out, "headed": len(heads) >= 2}


def steelhead_line(presence: Optional[str], has_row: bool) -> Optional[str]:
    """Consumer 5.6's presence line, as a code: `possible` with a steelhead row ("Steelhead rules
    apply here; steelhead may not be present in this water"), `known` with one ("known to be in
    this water"), `known` without one ("recorded here, but no steelhead rule applies: treat any
    rainbow, however big, as a rainbow trout"); None otherwise. `has_row` already folds in
    `steelhead_rules: false` (ST is never asked there)."""
    if not presence:
        return None
    if presence == "possible":
        return "possible_with_rules" if has_row else None
    return "known_with_rules" if has_row else "known_no_rules"
