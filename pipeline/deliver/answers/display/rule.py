"""Per rule (consumer 2.3, 2.4, 6.3), from the rule's own fields: its `kind` (the page's 15-step `kindOf`),
whether it is a closure gate, its size `bands`, its `plain` sentence and `title`, and `rule_facts`, all of them
for one rule."""
from __future__ import annotations

import re
from typing import List, Optional, Sequence
from pipeline.deliver.answers.common import (
    AnswersError, band_words, cap, clean, cm_in, join, lc, m_ft, sp_name, when_words
)
from pipeline.deliver.bundle.rules import closure_grade

INF = float("inf")


def _js(v) -> bool:
    """JavaScript truthiness: an empty list or object is TRUE, 0 / "" / None are false. The page's
    tests read fields this way (`if (f.while)` holds for `while: []`)."""
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v != ""
    return True


def _num(v):
    """A number as JavaScript prints it: 50.0 -> 50."""
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


def _is(v, n) -> bool:
    """`v === n` for a number n (never true of a bool)."""
    return v is not None and not isinstance(v, bool) and isinstance(v, (int, float)) and v == n


def _gt0(v) -> bool:
    return v is not None and not isinstance(v, bool) and isinstance(v, (int, float)) and v > 0


def kind_of(x: dict) -> str:
    """The page's `kindOf`, test by test in its order (consumer 2.3)."""
    fam, t = x.get("family"), x.get("type")
    if fam == "gear_and_method":
        return "gear"
    if fam == "conduct":
        return "conduct"
    if fam == "vessel":
        return "vessel"
    if t == "angler_closure":
        return "anglerclosure"
    if t == "stop_fishing_after_quota":
        return "duty"
    if x.get("dimension") == "lift" or (_js(x.get("exempts")) and x.get("take") is None
                                        and not _js(x.get("unlimited"))
                                        and not _js(x.get("lengths"))):
        return "exempt"
    if _js(x.get("standing")):
        return "standing"
    if _js(x.get("while")):
        return "while"
    if _js(x.get("caught")):
        return "caught"
    if _js(x.get("per_daily")):
        return "possession"
    if _js(x.get("period")) and x.get("period") != "daily":
        if not _gt0(x.get("take")):
            return "duty"
        # rows F6, THE one place a possession limit with a number is told from a yearly one (the
        # page says "annual" for both: page bug d10)
        return "possession_cap" if x.get("period") == "possession" else "annual"
    n = len(x.get("lengths") or [])
    if _js(x.get("within")):
        return "sizecap" if n else "subcap"
    if _is(x.get("take"), 0) and not n:
        return "gate"
    if _gt0(x.get("take")) or _js(x.get("unlimited")):
        return "pool"
    if n:
        return "size"
    return "duty"


def is_closure_gate(x: dict, kind: Optional[str] = None) -> bool:
    """A gate that closes ("No fishing for …"), not one that releases."""
    return (kind or kind_of(x)) == "gate" and closure_grade(x) is not None


def cut_points(lengths: Optional[Sequence[dict]]) -> List[float]:
    """The lengths `bands` cuts at: 0 and every range's ends, ascending (before any merge of equal takes)."""
    pts = {0}
    for r in lengths or []:
        if r.get("min_cm") is not None:
            pts.add(r["min_cm"])
        if r.get("max_cm") is not None:
            pts.add(r["max_cm"])
    return sorted(pts)


def size_shape(lengths: Optional[Sequence[dict]]) -> dict:
    """T103 — THE ONE answer to "what size rule does this quota carry?", read off the same ranges `bands` cuts
    (every bound is one of its `cut_points`), each range named by its OPEN ENDS:

      min    a keeping range with a bottom and no top ("only 1 over 90 cm")              {min_cm}
      floor  a range keeping none, below a size ("none under 30 cm" / "30 cm or longer")  {max_cm, take 0}
      ceil   a range keeping none, above a size ("none over 50 cm")                      {min_cm, take 0}
      slot   a range keeping none between two sizes ("release any between 60 cm and 90 cm") {min_cm, max_cm, take 0}

    {min, floor, ceil: the bound in cm or None, slot: (from, to) or None, floor_closed: the floor's band holds
    its own bound (the book's "catch and release (X cm or less)"), ceil_closed: the ceiling's band holds its own
    bound (the book's "No cutthroat trout 40 cm or more")}. The first range of each shape counts.
    `_title` and `_plain` both render from it and keep no size logic of their own (`test_one_size_shape.py`)."""
    ln = list(lengths or [])
    lo = lambda r: r.get("min_cm")                                             # noqa: E731
    hi = lambda r: r.get("max_cm")                                             # noqa: E731
    mn = next((r for r in ln if lo(r) is not None and hi(r) is None and r.get("take") is None), None)
    floor = next((r for r in ln if hi(r) is not None and lo(r) is None and _is(r.get("take"), 0)), None)
    ceil = next((r for r in ln if lo(r) is not None and hi(r) is None and _is(r.get("take"), 0)), None)
    slot = next((r for r in ln if lo(r) is not None and hi(r) is not None and _is(r.get("take"), 0)), None)
    return {"min": None if mn is None else lo(mn), "floor": None if floor is None else hi(floor),
            "ceil": None if ceil is None else lo(ceil), "slot": None if slot is None else (lo(slot), hi(slot)),
            "floor_closed": bool(floor is not None and floor.get("closed")),
            "ceil_closed": bool(ceil is not None and ceil.get("closed"))}


def bands(x: dict) -> List[list]:
    """The page's `bands` (consumer 2.4): cut points at every min/max plus 0; each band's midpoint
    tested against the ranges in order, the first that holds gives the take (its own, else the
    rule's); a band no range covers takes 0 on a daily pool, nothing on a cap, clause or yearly
    limit; neighbours with the same take merge. [[from_cm, to_cm or None, take or None]]."""
    L = x.get("lengths") or []
    if not L:
        return []
    P = cut_points(L) + [INF]
    if _js(x.get("within")) or (_js(x.get("period")) and x.get("period") != "daily"):
        uncovered = None
    else:
        uncovered = 0 if (_gt0(x.get("take")) or _js(x.get("unlimited"))) else None
    segs: List[list] = []
    for a, b in zip(P, P[1:]):
        m = a + 1 if b == INF else (a + b) / 2
        hit = next((r for r in L if (r.get("min_cm") is None or m >= r["min_cm"])
                    and (r.get("max_cm") is None or m <= r["max_cm"])), None)
        if hit is not None:
            take = hit.get("take") if hit.get("take") is not None else x.get("take")
        else:
            take = uncovered
        if segs and segs[-1][2] == take:
            segs[-1][1] = b
        else:
            segs.append([a, b, take])
    return [[_num(a), None if b == INF else _num(b), _num(t)] for a, b, t in segs]


def written_name(x: dict, conj: str = " and ") -> str:
    """The rule's own fish in words (`writtenName`): "trout" for trout/char minus char (p.80:
    trout includes char unless char are excluded), else each code's name, else "fish"."""
    w = list(x.get("species") or [])
    if w == ["TROUT_CHAR"] and "CHAR" in (x.get("species_except") or []):
        return "trout"
    return join([lc(sp_name(c)) for c in w], conj) if w else "fish"


#: THE POSSESSION QUOTA'S EXCEPTION, in plain words — ONE source (RULINGS Q15:
#: "the possession quota … EXCEPT at place of ordinary residence", p.80, is part of the definition
#: of EVERY possession quota). Every possession line the answers say carries it (`plain`).
POSSESSION_HOME = "fish at home don’t count"


def _home() -> str:
    """RULINGS P43: the exception said as its own sentence after the possession line ("Possession: 2 days’ quota.
    Fish at home don’t count.")."""
    return f"{POSSESSION_HOME[:1].upper()}{POSSESSION_HOME[1:]}."

#: A CLOSURE PRINTED "UNLESS OPENED" (RULINGS Z12): a full closure carrying an
#: advisory proviso (`condition_of`) that names who may open it, said in the user's accepted
#: words, by the kind of area the closure holds in. A closure with a proviso and no words here is
#: refused (no fallback).
UNLESS_OPENED = {
    "national_parks": "Closed unless opened by Parks Canada — a national park fishing permit is "
                      "required.",
}


def caught_sentence(x: dict) -> str:
    """A duty for a fish CAUGHT some way (`caught`, RULINGS G6) in plain words: "Any fish
    snagged — even by accident — must be released." Only a release (`take: 0`) is a shape the
    book prints; anything else is refused."""
    from pipeline.regs.parsing.catalogue import CAUGHT_HOW, OPEN_SUBJECTS
    if x.get("type") != "retention_limit" or not _is(x.get("take"), 0) or x.get("lengths"):
        raise AnswersError(f"display: {x.get('entry')}::{x.get('rule')} holds for a fish caught "
                           f"{x.get('caught')} but is not a release: no words for it")
    sp = list(x.get("species") or [])
    fish = "fish" if (not sp or all(c in OPEN_SUBJECTS or c == "ALL_GAME_FISH" for c in sp)) \
        else re.sub(r"^all ", "", written_name(x, " or "))
    how = [CAUGHT_HOW[c] for c in x["caught"]]
    return (f"Any {fish} {' or '.join(h['as'] for h in how)} — "
            f"{', '.join(dict.fromkeys(h['even'] for h in how))} — must be released.")


def unless_opened(x: dict, rules: dict) -> Optional[str]:
    """The words of a closure printed "unless opened" (`UNLESS_OPENED`): a FULL closure whose entry
    holds an advisory proviso of it (`condition_of`); None for any other rule."""
    if closure_grade(x) != "full":
        return None
    e, r = x.get("entry"), x.get("rule")
    if not any(k[0] == e and y.get("condition_of") == r and y.get("type") == "advisory"
               for k, y in rules.items()):
        return None
    kinds = sorted({t.get("area_kind") for t in x.get("extents") or [] if t.get("area_kind")})
    words = [UNLESS_OPENED[k] for k in kinds if k in UNLESS_OPENED]
    if len(words) != 1:
        raise AnswersError(f"display: {e}::{r} is a closure printed with a proviso, in areas "
                           f"{kinds}: no words for it (`UNLESS_OPENED`)")
    return words[0]


_MANY = re.compile(r"^(trout|char|trout and char|game fish|fish|bass|whitefish|salmon)\b")


def plain(x: dict, names: Optional[str] = None) -> Optional[str]:
    """`_plain`, and every possession limit with a number ends with its exception (RULINGS
    Q15: "fish at home don’t count" is part of every possession quota)."""
    got = _plain(x, names)
    if got is not None and kind_of(x) == "possession_cap":
        got = f"{got} {_home()}"     # RULINGS P43: its own sentence
    return got


def _plain(x: dict, names: Optional[str] = None) -> Optional[str]:
    """The rule as a plain sentence (`sayRule`, consumer 6.3); None where the page writes none.

    `names` replaces the rule's fish word when a role covers only some of a row's fish (the page
    swaps them in from `species`); the shipped sentence is the rule's own (names=None)."""
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS
    t = x.get("type")
    if t == "stop_fishing_after_quota":
        o = f"{x['origin']} " if _js(x.get("origin")) else ""
        return (f"Once you’ve kept your daily limit of {o}{written_name(x, ' and ')}, stop "
                f"fishing this water for the rest of the day.")
    if t != "retention_limit":
        return None
    if _js(x.get("extent_text")) or _js(x.get("side")) or _js(x.get("standing")):
        return None

    def nm(conj: str) -> str:
        s = re.sub(r"^all ", "", written_name(x, conj))
        s = re.sub(r"^Dolly Varden/bull trout$", "Dolly Varden or bull trout", s)
        s = s.replace("Dolly Varden/bull trout", "Dolly Varden, bull trout")
        if conj == " or ":
            s = re.sub(r"^trout and char", "trout or char", s)
        return s

    species = list(x.get("species") or [])
    sp = names or nm(" and ")
    ex = [s for s in (x.get("species_except") or []) if not (s == "CHAR" and sp == "trout")]
    ex_txt = f" (not {join([lc(sp_name(s)) for s in ex], ' or ')})" if ex and not names else ""
    many = (len(species) > 1 or (bool(_MANY.match(sp)) and sp != "trout")
            or any(s in SPECIES_GROUPS for s in species))
    water = x.get("water")
    where = ("In streams" if water == "stream" else "In lakes" if water == "lake" else "") or \
        ("In tributaries" if _js(x.get("tributaries_only")) else "")
    when = when_words(x.get("when"), "plain")      # T104: dates, weekdays, hours, unparsed — said once
    org = f"{x['origin']} " if _js(x.get("origin")) else ""
    ln = x.get("lengths") or []
    z = size_shape(ln)                              # T103: the one size shape, `_title`'s too
    take = x.get("take")

    def wrap(s: str) -> str:
        y = f"{where}, {s[:1].lower() + s[1:]}" if where else s
        if when:
            y += f", {when}"
        return y + "."
    if _js(x.get("while")):
        return None
    if _js(x.get("caught")):
        return caught_sentence(x)
    if closure_grade(x) is not None:                # the page's `may_target === false`
        return wrap("No fishing" if re.search(r"game fish|^fish", sp) else f"No fishing for {sp}")
    if _js(x.get("per_daily")):
        # THE POSSESSION QUOTA (RULINGS Q15, P43: plain words): "2 days’ quota", and fish at home don't count
        # (`plain` adds that sentence)
        n = _num(x["per_daily"])
        times = "the daily quota" if n == 1 else f"{n} days’ quota"
        of = "" if re.search(r"game fish|^fish", sp) else f" for {sp}"
        return f"{wrap(f'Possession: {times}{of}')} {_home()}"
    over = f" over {cm_in(z['min'])}" if z["min"] is not None else ""
    # A SLOT (owner 2026-10-28, Q62): the book's "between", in the must-be / release style — "release any between 60 cm
    # (24 in) and 90 cm (35 in)", its own clause after a ";"
    slot = f"release any {band_words(z['slot'][0], z['slot'][1], False, False)}" if z["slot"] else ""
    if _js(x.get("record_retention")):
        return wrap(f"Write each {org}{sp}{over} on your licence right away")      # UR1 G
    if _js(x.get("unlimited")):
        return wrap(f"No daily limit for {sp}")
    if _is(take, 0):
        return wrap(f"Release every {org}{sp}")
    if take is None and z["floor"] is not None and z["floor_closed"] and len(ln) == 1:
        # the book's "catch and release (X cm or less)" (Q13: X itself released) — never "keep
        # only X cm or longer" (answers 2.5: Vedder r6, now the decided winner, rows F13)
        return wrap(f"Release every {org}{sp} {cm_in(z['floor'])} or less")
    if x.get("period") == "annual":
        return wrap(f"Keep up to {_num(take)} {sp}{over} a licence year"
                    f"{', all kinds together' if len(species) > 1 else ''}")
    # a possession limit with a number holds what you have with you, not a day's catch (rows F6)
    per = " in possession" if kind_of(x) == "possession_cap" else " a day"
    if take is not None and z["min"] is not None and z["floor"] is None:
        who = re.sub(r" and ([^,]+)$", r" or \1", names) if names else nm(" or ")
        return wrap(f"Only {_num(take)} {org}{who} over {cm_in(z['min'])}{per}{ex_txt}"
                    + (f"; {slot}" if slot else ""))
    if take is None:
        # A SIZE RULE WITH NO COUNT (owner 2026-10-28, Q63): the must-be / release style, the fish first — "Lake trout
        # must be 50 cm (20 in) or less; release any longer." — never "Keep only lake trout, none over 50 cm", which
        # reads as "only lake trout may be kept". The kept band's words hold their ends as the book does (RULINGS
        # P37): "No trout over 50 cm" keeps 50 cm ("or less"), "No cutthroat trout 40 cm or more" does not ("under").
        # A slot alone: "Release any lake trout between 60 cm (24 in) and 90 cm (35 in)."
        lo, hi = z["floor"], z["ceil"]
        if lo is None and hi is None:
            return wrap(f"Release any {org}{sp}{ex_txt} {slot[len('release any '):]}") if slot else None
        kept = band_words(lo or 0, hi, not z["floor_closed"], not z["ceil_closed"])
        rest = ("release any other size" if lo is not None and hi is not None
                else "release any shorter" if lo is not None else "release any longer")
        # the fish lead the sentence: capitalised alone, as written after "In streams, " ("Dolly Varden" keeps its own)
        who = f"{org}{sp}{ex_txt}"
        y = f"{where}, {who}" if where else who[:1].upper() + who[1:]
        y += f" must be {kept}; {rest}" + (f"; {slot}" if slot else "")
        return y + (f", {when}" if when else "") + "."
    sp += ex_txt
    bits = []
    if take is not None:
        # ONE fish of several kinds is "or" (any one of them), for a subset too (the owner:
        # "Keep up to 1 cutthroat trout, brown trout or rainbow trout a day"); more than one is "and"
        if _is(take, 1):
            fish = (re.sub(r" and ([^,]+)$", r" or \1", names) if names else nm(" or ")) + ex_txt
        else:
            fish = sp
        bits.append(f"{'Have no more than' if per != ' a day' else 'Keep up to'} {_num(take)} {fish}{per}"
                    f"{', all kinds together' if many and _gt0(take) and take > 1 else ''}")
    if z["floor"] is not None:
        bits.append(f"over {cm_in(50)}" if species == ["ST"] and _is(z["floor"], 50)
                    else f"{cm_in(z['floor'])} or longer")
    if z["ceil"] is not None:
        bits.append(f"none over {cm_in(z['ceil'])}")
    if _js(x.get("origin")):
        bits.append(f"{x['origin']} only")
    return wrap(", ".join(bits) + (f"; {slot}" if slot else ""))


_DASHED = re.compile(r"\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d+)-"
                     r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.? \d+|\d+)\b")


def title(x: dict) -> str:
    """RULINGS P27: EVERY rule's title, its effect in words, generated from its fields — never the raw
    list item the book prints under a region table ("ALL STEELHEAD" is "Release all steelhead"; "Lake trout,
    Oct 1-Nov 30" is "Release lake trout, Oct 1–Nov 30"; "Bull trout (Dolly Varden) from streams, Aug 1-Oct
    31" is "Release Dolly Varden/bull trout from streams, Aug 1–Oct 31"). The verbatim stays the rule's own
    words. A rule whose effect has no generated form (gear, boats, notices, lifts) takes its `label` — the
    ingest's effect words — dates with an en dash, cut to a title's length."""
    got = _title(x)
    if got is None:
        got = _DASHED.sub(lambda m: f"{m.group(1)}–{m.group(2)}", clean(x.get("label") or x.get("verbatim") or ""))
        got = cut_words(got, 110)
    return cap(got) if got else "Rule"


def _title(x: dict) -> Optional[str]:
    t = x.get("type")
    if t == "stop_fishing_after_quota":
        o = f"{x['origin']} " if _js(x.get("origin")) else ""
        return f"Stop fishing after your daily limit of {o}{written_name(x, ' and ')}"
    if t == "retention_limit" and kind_of(x) == "caught":
        return caught_sentence(x).rstrip(".")
    if t != "retention_limit" or kind_of(x) in ("exempt", "standing", "while"):
        return None
    sp = list(x.get("species") or [])

    def fish(conj: str) -> str:
        w = re.sub(r"^all ", "", written_name(x, conj))
        if conj == " or ":
            w = re.sub(r"^trout and char", "trout or char", w)
        ex = [s for s in (x.get("species_except") or []) if not (s == "CHAR" and w == "trout")]
        return w + (f" (not {join([lc(sp_name(e)) for e in ex], ' or ')})" if ex else "")
    whole = bool(re.search(r"game fish|^fish", written_name(x)))
    water = x.get("water")
    frm = f" from {water}s" if water else (" in tributaries" if _js(x.get("tributaries_only")) else "")
    when = when_words(x.get("when"), "title")      # T104: dates, weekdays, hours, unparsed — said once
    tail = f", {when}" if when else ""
    org = f"{x['origin']} " if _js(x.get("origin")) else ""
    ln = x.get("lengths") or []
    take = x.get("take")
    z = size_shape(ln)                              # T103: the one size shape, `_plain`'s too
    if closure_grade(x) is not None:
        where = f" in {water}s" if water else (" in tributaries" if _js(x.get("tributaries_only")) else "")
        return ("No fishing" if whole or not sp else f"No fishing for {fish(' and ')}") + where + tail
    if _js(x.get("per_daily")):
        n = _num(x["per_daily"])
        times = {1: "", 2: "twice ", 3: "three times "}.get(n, f"{n} times ")
        return f"Possession: {times}the daily quota" + ("" if whole else f" of {fish(' and ')}")
    if _js(x.get("record_retention")):
        over = f" over {cm_in(z['min'])}" if z["min"] is not None else ""
        return f"Write each {org}{fish(' or ')}{over} on your licence right away"     # UR1 G
    if _js(x.get("unlimited")):
        return f"No daily limit on {org}{fish(' and ')}{frm}{tail}"
    if _is(take, 0) and not ln:
        every = "all " if not when and not org and not water else ""
        return f"Release {every}{org}{fish(' and ')}{frm}{tail}"
    if take is None and z["floor"] is not None and z["floor_closed"] and len(ln) == 1:
        return f"Release {org}{fish(' and ')} {cm_in(z['floor'])} or less{frm}{tail}"
    sizes = []
    if z["floor"] is not None:
        sizes.append(f"none under {cm_in(z['floor'])}")
    if z["ceil"] is not None:
        sizes.append(f"none over {cm_in(z['ceil'])}")
    if z["slot"]:
        # RULINGS P34, P37: in cm (inches), and a slot keeps its bounds (the boundary standard, RULINGS P37)
        sizes.append(f"none {band_words(z['slot'][0], z['slot'][1], False, False)}")
    sz = (" (" + ", ".join(sizes) + ")") if sizes else ""
    k = kind_of(x)
    if k == "annual" and _gt0(take):
        over = f" over {cm_in(z['min'])}" if z["min"] is not None else ""
        return f"Up to {_num(take)} {org}{fish(' and ')}{over} a licence year{tail}"
    if k == "possession_cap" and _gt0(take):
        return f"Up to {_num(take)} {org}{fish(' and ')} in possession{tail}"
    if _gt0(take) and z["min"] is not None and z["floor"] is None:
        return f"At most {_num(take)} {org}{fish(' or ')} over {cm_in(z['min'])} a day{frm}{tail}"
    if _gt0(take):
        conj = " or " if _is(take, 1) else " and "
        verb = "At most" if k == "subcap" else "Keep up to"
        return f"{verb} {_num(take)} {org}{fish(conj)} a day{frm}{sz}{tail}"
    if take is None and z["slot"] and len(sizes) == 1:
        # owner 2026-10-28 (Q62): a slot is the book's "between", said as a release
        return f"Release {org}{fish(' and ')} {sizes[0].replace('none ', '')}{frm}{tail}"
    if take is None and sizes:
        return f"No {org}{fish(' or ')} " + " or ".join(s_.replace("none ", "") for s_ in sizes) + f"{frm}{tail}"
    return None


#: answers 2.8 (D2): the heading the fishway / fish-facility buffers ("No fishing within
#: 23 m downstream of … any fishway", "within 100 m of any government fish facility", p.9) show under —
#: on the KEEP side, beside the open/closed strip, never at the bottom of the gear page
CANT_FISH_TITLE = "Where you can’t fish"


def where_of(x: dict) -> Optional[str]:
    """Where the page shows a rule apart from the rows: "cant_fish" for a standing no-fishing place
    rule (a STANDING closure — a "no fishing" bound everywhere because no dataset places it), else
    None. The closure is `rules.closure_grade`, the one predicate (AGENTS 56, dataflow gate 2): a
    bundle rule carries `may_target` 0, the export's fields `false`, and both must say the same."""
    if kind_of(x) == "standing" and closure_grade(x) is not None:
        return "cant_fish"
    return None


def cant_fish_words(x: dict) -> Optional[str]:
    """UR3 2 (user panel: "how far is 23 m?"): a no-fishing place rule's sentence, the book's words with every distance
    in metres said in feet too — "No fishing within 23 m (75 ft) downstream of the lower entrance to any fishway,
    canal, obstacle or leap." (`common.m_ft`). None where the rule has no words."""
    w = clean(x.get("verbatim") or "").strip().rstrip(".")
    if not w:
        return None
    w = re.sub(r"\b(\d+(?:\.\d+)?) ?m\b(?! \()", lambda m: m_ft(m.group(1)), w)
    return f"No fishing {lc(w)}."


def rule_facts(x: dict, rules: Optional[dict] = None) -> dict:
    """{kind, closure?, bands?, plain?, unless_opened?, where?} for one rule — the per-rule display
    facts. `rules` (the bundle's, by (entry, rule)) lets a closure find its proviso (`unless_opened`);
    `where` the heading a place rule shows under on the Keep side (answers 2.8, D2)."""
    k = kind_of(x)
    out: dict = {"kind": k, "title": title(x)}
    if where_of(x) == "cant_fish":
        out["where"] = CANT_FISH_TITLE
    if is_closure_gate(x, k):
        out["closure"] = True
        u = unless_opened(x, rules) if rules is not None else None
        if u is not None:
            out["unless_opened"] = u
    b = bands(x)
    if b:
        out["bands"] = b
    p = plain(x)
    if out.get("where") == CANT_FISH_TITLE:
        p = cant_fish_words(x)
    if p is not None:
        out["plain"] = p
    return out


def cut_words(t: str, n: int) -> str:
    """`cutWords`: at most n characters, cut back to a word boundary, with an ellipsis."""
    if len(t) <= n:
        return t
    return re.sub(r"[\s,;:·–-]+\S*$", "", t[:n]) + "…"
