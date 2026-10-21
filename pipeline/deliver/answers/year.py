"""THE YEAR STRIP (rows 30) — every rows row says, for each of the 365 book days, how its fish stand that day:
`rows[].year` = run-length [[status, days]], status keep | release | closed | none, the days summing to 365.

This is the answer the app's year strip draws (T13 speciesYear reads it; the app decides nothing). It was page
v36's `dayStatus` / `strip` (ROUND14 5), ported to TypeScript in T08 and removed under the owner rule of
2026-10-21 (regulation answers are the pipeline's): a per-day status no single stored frame holds is a decision,
so it is made here, once, with THE PAGE'S RULE — the prototype's established policy, kept exactly:

  * A ROW'S FISH. The row's `members`; each answers with its LEADING origin answer (page `mainRes`): the
    hatchery answer if it keeps (keep / no_limit), else the wild one if it keeps, else the hatchery answer,
    else the wild one. A fish the frame does not ask about, or with neither answer, gives nothing.
  * A DAY'S MOMENTS. Every segment of the day's start (a start repeats once per moment group, answers 2.1)
    whose moment is not INSIDE an hours window: a weekday split day reads all its weekday classes (Kootenay
    Lake's Lower West Arm kokanee: release Mon–Fri, keep Sat–Sun — the strip is keep there, and `rows[].week`
    says the weekdays in words); an hours window never colours the day, either way (its outside-the-window
    answer does — as the status index never closes a day by its night, and as page v36 tapped `inside=false`).
  * THE DAY'S COLOUR. The most permissive answer over those fish and moments: keep (keep or no_limit) over
    release over closed; `none` where none of them has an answer (a closed row with no members, a fish
    nothing in scope speaks for).
  * THE BOOK'S DAYS. Jan 1 .. Dec 31 of a year without Feb 29 (the leap calendar's day 60 is skipped): a
    Feb 29 reads Feb 28's (P06), so its slot is Feb 28's run.

Rows with the same members in the same key carry the same strip on every segment's frame (the strip is the
key's year, not the frame's day). The page computed this for the 2026 calendar's weekdays; no key's hours
window is limited to some weekdays (every hours moment is the whole week), so no day's colour depends on the
year, and none is asked for here.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver import calendar as K
from pipeline.deliver.answers.common import AnswersError

#: The 365 book days, as leap-calendar days (Feb 29, day 60, is not one: it reads Feb 28's, P06).
BOOK_DAYS: Tuple[int, ...] = tuple(d for d in range(1, K.DAYS + 1) if d != K.LEAP)

#: The strip's statuses, and how permissive each stored status is (page RANKSTAT).
STATUSES = ("keep", "release", "closed", "none")
_RANK = {"keep": 3, "no_limit": 3, "release": 2, "closed": 1}
_SAYS = {"keep": "keep", "no_limit": "keep", "release": "release", "closed": "closed"}
_KEEPS = ("keep", "no_limit")


def leading_answer(answer: Optional[dict]) -> Optional[dict]:
    """page mainRes: of a fish's two stored origin answers ({hatchery, wild}), the one the strip reads — the
    keepable one, hatchery first; else hatchery, else wild; None when neither is answered."""
    if not answer:
        return None
    h, w = answer.get("hatchery"), answer.get("wild")
    if h is not None and h["status"] in _KEEPS:
        return h
    if w is not None and w["status"] in _KEEPS:
        return w
    return h if h is not None else w


def day_segments(starts: Sequence[int], moments: Optional[Sequence[dict]]) -> Dict[int, Tuple[int, ...]]:
    """Each leap-calendar day (1..366; the strip reads only the `BOOK_DAYS`) -> the segments whose answers colour
    it: the segments of the day's start, but those inside an hours window. `moments` per segment
    (`Moment.as_json`), None for a key with one moment."""
    if moments is None and len(set(starts)) != len(starts):
        raise AnswersError("answers: the year strip needs the moments of a key that repeats a start day")
    if moments is not None and len(moments) != len(starts):
        raise AnswersError(f"answers: the year strip has {len(moments)} moments for {len(starts)} segments")
    out: Dict[int, Tuple[int, ...]] = {}
    s = -1
    for d in range(1, K.DAYS + 1):
        while s + 1 < len(starts) and starts[s + 1] <= d:
            s += 1
        if s < 0:
            raise AnswersError(f"answers: no segment starts by day {d}")
        first = starts.index(starts[s])
        group = range(first, s + 1)
        if moments is None:
            out[d] = (s,)
            continue
        got = tuple(j for j in group if not (moments[j].get("hours") or {}).get("in"))
        if not got:
            raise AnswersError(f"answers: day {d} has no moment outside an hours window")
        out[d] = got
    return out


def _runs(per_day: List[str]) -> List[list]:
    out: List[list] = []
    for st in per_day:
        if out and out[-1][0] == st:
            out[-1][1] += 1
        else:
            out.append([st, 1])
    return out


def stamp_years(frames: Sequence[dict], starts: Sequence[int],
                moments: Optional[Sequence[dict]]) -> List[dict]:
    """One key's rows frames (one per segment, `starts` and `moments` the key's) -> new frames whose every row
    carries `year`. The inputs are not changed (frames are shared between keys and segments)."""
    if len(frames) != len(starts):
        raise AnswersError(f"answers: the year strip has {len(frames)} rows frames for {len(starts)} segments")
    days = day_segments(starts, moments)
    # each segment: each fish's stored status (its leading origin answer), or absent
    status: List[Dict[str, str]] = []
    for f in frames:
        got = {}
        for S, a in (f.get("fish") or {}).items():
            m = leading_answer(a)
            if m is not None:
                got[S] = m["status"]
        status.append(got)
    memo: Dict[Tuple[str, ...], List[list]] = {}

    def strip(members: Tuple[str, ...]) -> List[list]:
        if members not in memo:
            per_seg = []
            for st in status:
                best, bv = "none", 0
                for S in members:
                    v = st.get(S)
                    if v is not None and _RANK[v] > bv:
                        best, bv = _SAYS[v], _RANK[v]
                per_seg.append((best, bv))
            per_day = []
            for d in BOOK_DAYS:
                best, bv = "none", 0
                for j in days[d]:
                    if per_seg[j][1] > bv:
                        best, bv = per_seg[j]
                per_day.append(best)
            memo[members] = _runs(per_day)
        return [list(r) for r in memo[members]]

    out = []
    for f in frames:
        rows = []
        for r in f.get("rows") or []:
            if "year" in r:
                raise AnswersError("answers: a rows row already carries its year strip")
            rows.append({**r, "year": strip(tuple(r.get("members") or ()))})
        out.append({**f, "rows": rows})
    return out
