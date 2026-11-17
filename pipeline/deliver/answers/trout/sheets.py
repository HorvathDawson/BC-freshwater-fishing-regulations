"""THE FISH SHEET (trout table plan, T810 ticket H; spec 3.4, contract 2.3 `FishSheet`, Q-D6, Q-D10): what a tap on one
row of the trout table opens. Three parts on screen: "Here, today" (Python's `here` lines, below), "How to tell it"
(the app's pictures and FISH_TIPS: content, not law) and "Sources" (`src`, this fish's rules first, cut to 4 here; the
app never cuts or sorts a list). The coming change stays in the date set (`DateSet.sheet`), because it changes by
segment while the sheet does not.

EVERY SENTENCE IS G'S (`trout/words.py`), whole, and none says "today" (the app's heading does; contract Q-D2): the
stop duty reads "for the rest of the day".

THE "HERE" LINES, in order (spec 3.4):
  1. size          the row's size ("Size: 30 cm (12 in) or longer."; a steelhead row's "Here a steelhead is a rainbow
                   over 50 cm (20 in).")
  2. hatchery      "Hatchery only: wild ones go back." where the row or its box carries the label
  3. counts        "Counts toward the 5 in all, together with …" (a keep box of more than one fish); the row's band:
                   "This fish: only 3." / "Only 1 of these 4: this fish and …"
  4. the row's own lines that are not said above (a count it shares, a size cap, the weekday words, the wild quota)
  4b. together     the limit across two boxes or lakes on the rows it holds ("Only 2 wild trout and char in all, each
                   under 40 cm (16 in).")
  5. the box's      each size cap of the box's strip the fish can pass ("Only 1 over 50 cm (20 in)."), and "A kept one
     strip          is always over 50 cm (20 in), so it is that one." where the row says it uses it; the box's wild
                   quota on a row that may be wild
  6. possession    `card.possession_duty`'s line whole, with the home exception (the same function as the row's)
  7. quiet         the row's quiet lines (possession below the day's number, the steelhead lines)
  8. stop duty     bold: "Once you have kept 1 steelhead here, stop fishing this stream for the rest of the day: …"
  9. licence       "Licence: 10 a year · record each on your licence right away · <stamp>, only if …." — the yearly
                   limit and record duties (`card.licence_duty`, the card's own words) and the stamps the fish needs,
                   READ FROM THE POST-PASS VALUES (contract 2.2): a stamp is said only where `stamp_licence_documents`
                   put it on the fish's row AND the licence answer at the same key and segment, after
                   `drop_closed_steelhead_stamp`, still lists its document for that fish
(and the one-day note, `table._one_day`, appended last on a one-day segment: Adams River's Jul 31).

Mutation pins (each has a test in `test_trout_sheets.py` that fails with it off): `A_STAMP_FROM_THE_POST_PASS_LICENCE`,
`A_SOURCES_CUT_TO_FOUR`.
"""
from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.common import AnswersError
from pipeline.deliver.answers.trout import words as W

#: the stamps said are the ones the post-pass licence answer still lists for the fish. False = every stamp the row
#: carries, whatever the licence answer says (a stamp `drop_closed_steelhead_stamp` dropped would be said)
A_STAMP_FROM_THE_POST_PASS_LICENCE = True
#: Sources: this fish's rules first, at most 4 (Q-D6). False = the whole list
A_SOURCES_CUT_TO_FOUR = True

SOURCES_MAX = 4


class SheetError(AnswersError):
    """A sheet the table cannot say truthfully: the build stops."""


# ---------------------------------------------------------------------------------------------------- the inputs

def documents(licence: Optional[dict]) -> List[dict]:
    """The documents tied to some fish (`stamps.documents_of`) in the post-pass licence answer, for the angler the Keep
    tab answers for (`stamps.DEFAULT_PROFILE`)."""
    from pipeline.deliver.answers import licence as L
    from pipeline.deliver.answers import stamps as ST
    if licence is None:
        raise SheetError("trout sheet: no licence answer at this key and segment (the stamps are read from it)")
    profiles = licence.get("profiles") or []
    pix = L.profile_index(ST.DEFAULT_PROFILE)
    if pix >= len(profiles):
        if "tidal" in (licence.get("holds") or {}):
            return []
        raise SheetError(f"trout sheet: the licence answer has no profile {pix}")
    return ST.documents_of(profiles[pix])


def _kinds(nodes) -> Iterable[dict]:
    for n in nodes or ():
        if n.get("node") == "tier":
            yield from _kinds(n.get("children"))
        elif n.get("node") == "kind":
            yield n


def row_stamps(frame: dict, S: str) -> List[dict]:
    """Every stamp the post-pass rows frame carries for fish `S` (`stamps.stamp_licence_documents`): on the group kind
    or the released not-kept line holding it, or on a row of its own (`Row.stamps`). In card order, each document once."""
    got: Dict[str, dict] = {}
    for r in frame.get("rows") or ():
        g = r.get("group")
        if g:
            for k in _kinds(g.get("tree")):
                if S in k["species"]:
                    for s in k.get("stamps") or ():
                        got.setdefault(s["doc"], s)
            for x in g.get("not_kept") or ():
                if S in x["species"]:
                    for s in x.get("stamps") or ():
                        got.setdefault(s["doc"], s)
        if S in (r.get("members") or ()):
            for s in r.get("stamps") or ():
                got.setdefault(s["doc"], s)
    return list(got.values())


def stamps_for(frame: dict, S: str, docs: Sequence[dict]) -> List[str]:
    """The stamp parts of fish `S`'s licence line: a stamp the row carries whose document the licence answer lists for
    `S` (post-pass: `drop_closed_steelhead_stamp` already ran)."""
    held = {d["doc"] for d in docs if S in d["sp"]}
    out = []
    for s in row_stamps(frame, S):
        if A_STAMP_FROM_THE_POST_PASS_LICENCE and s["doc"] not in held:
            continue
        out.append(W.licence_stamp(s["name"], s["when"]))
    return out


def licence_duties(decided: Optional[dict], lo: float, hi: float) -> List[str]:
    """The yearly limit and record duties of one decided answer (its lines' rules resolved: `table.reading`), in the
    card's words (`card.licence_duty`), each once."""
    from pipeline.deliver.answers.card import licence_duty
    out: List[str] = []
    for ln in (decided or {}).get("lines") or ():
        w = licence_duty(ln, lo, hi)
        if w is not None and w not in out:
            out.append(w)
    return out


# ---------------------------------------------------------------------------------------------------- the sheet

def _name(x) -> str:
    """Another row's fish as a sheet names it: "brook trout", a twin's "wild brook trout"."""
    n = common.lc_names([x.fish])
    return f"{x.origin} {n}" if x.origin else n


def _others(rows: Sequence, r) -> str:
    names = list(dict.fromkeys(_name(x) for x in rows if x is not r and (x.fish != r.fish or x.origin != r.origin)))
    return common.join(names)


def _is_stop(t: str) -> bool:
    try:
        W.parse_stop_duty(t)
    except W.WordsError:
        return False
    return True


def here(box: dict, ri: int, r, *, stamps: Sequence[str], duties: Sequence[str],
         together: Optional[dict] = None) -> List[dict]:
    """The "Here" lines of row `ri` of `box` (a `table.Body` box: kind, n, label, parts, rows, bands)."""
    out: List[Tuple[str, bool]] = []
    add = lambda t, bold=False: out.append((t, bold)) if t not in [x for x, _ in out] else None  # noqa: E731
    lines = list(r.lines)
    poss_row = W.possession_row(r.poss[0]) if r.poss else None
    said = set()
    if box["kind"] == "keep":
        # 1. size
        if r.sub is not None:
            add(W.sheet_sub(r.sub))
        elif lines and r.size_text is not None and lines[0] == r.size_text:
            add(W.sheet_size(r.size_text))
            said.add(r.size_text)
        elif r.size is not None:
            add(W.sheet_size(r.size))
        # 2. hatchery only
        if r.label or box.get("label"):
            add(W.SHEET_HATCHERY_ONLY)
        # 3. the box and the row's band
        rows = box["rows"]
        if len({(x.fish, x.origin) for x in rows}) > 1:
            add(W.sheet_box(box["n"], common.lc_names(list(dict.fromkeys(x.fish for x in rows if x.fish != r.fish)))))
        band = next(b for b in box["bands"] if b["from"] <= ri <= b["to"])
        if band["kind"] == "own":
            add(W.sheet_own(W.parse_band(band["text"])[1]))
        elif band["kind"] == "shared":
            add(W.sheet_shared(band["text"], _others(rows[band["from"]:band["to"] + 1], r)))
        # 4. the row's own lines
        uses = None
        for t in lines:
            if t in said or t == poss_row or _is_stop(t):
                continue
            u = W.parse_uses(t)
            if u is not None:
                uses = u
                continue
            add(W.sentence(t))
        # 5. the box's strip
        for kind, text, cm in box.get("parts") or ():
            if kind == "size" and (cm is None or box["passes"](r, cm)):
                add(W.sentence(text))
                if uses is not None and uses[1] == cm:
                    add(W.sheet_uses(cm))
            elif kind == "wild" and r.origin != "hatchery" and not (r.label or box.get("label")):
                add(W.sentence(text))
        # the limit across two boxes or two lakes (`together`), on the rows it holds: every row of the table for the
        # lakes' count; the rows of its origin (or a row whose origins share one rule) for an origin's count
        if together is not None and (together["origin"] is None or r.origin == together["origin"]
                                     or (r.origin is None and not (r.label or box.get("label")))):
            add(W.sentence(together["text"]))
        # 6. possession
        if r.poss is not None:
            add(W.possession_sheet(r.poss[0]))
    else:
        for t in lines:
            add(W.sentence(t))
    # 7. quiet
    for t in r.quiet:
        add(W.sentence(t))
    # 8. the stop duty
    for d in r.duties:
        got = W.parse_duty_stop(d)
        if got is not None:
            add(W.sheet_stop(got[1], got[0], common.lc_names([r.fish])), True)
    # 9. the licence line
    parts = list(duties) + list(stamps)
    if parts:
        add(W.sheet_licence(parts))
    return [{"t": t, "bold": b} for t, b in out]


def head(box: dict, ri: int) -> str:
    if box["kind"] == "keep":
        band = next(b for b in box["bands"] if b["from"] <= ri <= b["to"])
        return W.sheet_keep(band["text"], box["n"])
    return W.SHEET_RELEASE if box["kind"] == "release" else W.SHEET_CLOSED


def sources(src: Sequence[int]) -> Tuple[int, ...]:
    """Q-D6: the sheet's `src`, this fish's rules first (the row's own order), cut to 4 by Python."""
    return tuple(src[:SOURCES_MAX]) if A_SOURCES_CUT_TO_FOUR else tuple(src)


def fish_sheet(box: dict, ri: int, r, *, stamps: Sequence[str], duties: Sequence[str],
               together: Optional[dict] = None) -> dict:
    """Row `ri` of `box`'s `FishSheet` (as a dict, validated with the table). `together`: the table's limit across
    boxes or lakes ({text, src, origin}), said on the rows it holds."""
    return {"head": head(box, ri), "here": tuple(here(box, ri, r, stamps=stamps, duties=duties, together=together)),
            "src": sources(r.src)}
