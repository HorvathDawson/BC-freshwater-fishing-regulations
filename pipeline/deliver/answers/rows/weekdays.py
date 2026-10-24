"""A part that reads differently by weekday: the days in words (`weekdays_words`, `weekdays_label`) and
one line per kind of day (`say_weekdays`)."""
from __future__ import annotations

import json
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.deliver.answers import common
from pipeline.deliver.answers.rows.rule import _js
from pipeline.deliver.answers.rows.wire import _num
from pipeline.deliver.answers.rows.groups import _names


def weekdays_words(days: Sequence[str]) -> str:
    """ROUND14 5: a set of weekdays in words — "Monday to Friday", "on Saturdays and Sundays" (T104: said by
    `common.when_words`, plain style)."""
    return common.when_words({"weekdays": list(days)}, "plain")


def weekdays_short(days: Sequence[str]) -> str:
    """ROUND15 3: a set of weekdays in a short label — "Sat–Sun", "Mon–Fri", "Mon, Wed" (`when_words`, title)."""
    return common.when_words({"weekdays": list(days)}, "title")


def weekdays_label(days: Sequence[str]) -> str:
    """ROUND15 3: a set of weekdays as the status names it — "Weekends", "Weekdays", else the short label
    (`when_words`, status)."""
    return common.when_words({"weekdays": list(days)}, "status")


def weekdays_when(days: Sequence[str]) -> str:
    """ROUND15 3: the days a part holds in words — "Monday to Friday", "Saturday and Sunday" (`when_words`, days)."""
    return common.when_words({"weekdays": list(days)}, "days")


def _week_answer(frame: dict, S: str) -> Optional[Tuple[str, Optional[float], str]]:
    """(status, daily, winner) of one fish in a rows frame: its leading origin's, the main row's (T120:
    `common.lead_origin`; it used to take the most permissive keeper, so a weekday row could say a bigger quota)."""
    v = (frame.get("fish") or {}).get(S) or {}
    best = common.lead_origin(v.get("hatchery"), v.get("wild"))
    if best is None:
        return None
    return best["status"], best.get("daily"), best["win"]


_WEEK_RANK = {"no_limit": 3, "keep": 2, "release": 1, "closed": 0}


def say_weekdays(frames: Sequence[dict], moments: Sequence[Optional[dict]]) -> List[dict]:
    """ROUND14 5 (owner): A WEEKDAY WINDOW SAID AS WEEKDAYS. `frames` are one day's rows frames, one per
    moment (`moments`: `calendar.Moment.as_json`, weekdays and no hours window). Every row of every frame
    whose fish answer differently by weekday carries `week`: Kootenay Lake's Lower West Arm kokanee —
      says    "Keep up to 5 on Saturdays and Sundays; release Monday to Friday" (one sentence);
      status  (ROUND15 3) "Weekends: keep 5 · Weekdays: release" — the row's status, never one moment's;
      parts   (ROUND15 3) [{days "Sat–Sun", when "Saturday and Sunday", weekdays, status, n, says "keep up to
              5", rules [the deciding rule], today}] — how it was decided, each day-part with its rule; `today`
              marks the frame's own moment —
    the same on the Saturday frame and the Monday one, so no moment's card contradicts another. A row's
    `restates` notes (the combined quota with the Upper West Arm) are carried to every moment's frame.
    Returns new frames (the inputs are not changed); frames cut by an hours window are returned as they are."""
    import copy
    if len(frames) < 2 or any(m is None or m.get("hours") for m in moments):
        return list(frames)
    fish = sorted({S for f in frames for S in (f.get("fish") or {})})
    split: Dict[str, List[Tuple[Tuple[str, Optional[float]], List[str], List[str]]]] = {}
    for S in fish:
        by: Dict[tuple, Tuple[List[str], List[str]]] = {}
        for f, m in zip(frames, moments):
            a = _week_answer(f, S)
            if a is None:
                break
            days, rules = by.setdefault(a[:2], ([], []))
            days.extend(m["weekdays"])
            if a[2] not in rules:
                rules.append(a[2])
        else:
            if len(by) > 1:
                split[S] = sorted(((k, d, r) for k, (d, r) in by.items()),
                                  key=lambda x: (-_WEEK_RANK.get(x[0][0], -1), x[1]))
    if not split:
        return list(frames)

    def verb(status, daily, short=False) -> str:
        if status == "no_limit":
            return "no limit" if short else "keep any number"
        if status == "keep":
            return f"keep {_js(daily)}" if short else f"keep up to {_js(daily)}"
        if status == "release":
            return "release" if short else "release every one"
        return "closed" if short else "no fishing for them"

    def words(pattern) -> str:
        out = []
        for (status, daily), days, _r in pattern:
            what = ("keep any number" if status == "no_limit" else
                    f"keep up to {_js(daily)}" if status == "keep" else
                    "release" if status == "release" else "no fishing for them")
            out.append(f"{what} {weekdays_words(days)}")
        return common.cap("; ".join(out))

    restated: Dict[tuple, List[dict]] = {}
    for f in frames:
        for r in f.get("rows") or []:
            for x in r.get("restates") or []:
                got = restated.setdefault(tuple(r.get("all_members") or []), [])
                if x not in got:
                    got.append(x)

    got = []
    for f, m in zip(frames, moments):
        f2 = copy.deepcopy(f)
        for r in f2.get("rows") or []:
            mine = [S for S in r.get("all_members") or [] if S in split]
            if not mine:
                continue
            pats = sorted({json.dumps(split[S]) for S in mine})
            named = len(pats) > 1 or len(mine) < len(r["all_members"])
            says_l, status_l, parts = [], [], []
            for p in pats:
                pat = json.loads(p)
                w = words(pat)
                who = [S for S in mine if json.dumps(split[S]) == p]
                lead = f"{common.cap(_names(who, ' and '))}: " if named else ""
                # several fish of the row: each named ("Kokanee: keep up to 5 …")
                says_l.append(f"{lead}{w[:1].lower()}{w[1:]}" if named else w)
                status_l.append(lead + " · ".join(f"{weekdays_label(d)}: {verb(st, n, True)}"
                                                  for (st, n), d, _r in pat))
                for (st, n), d, rules in pat:
                    parts.append({"species": who, "days": weekdays_short(d), "when": weekdays_when(d),
                                  "weekdays": d, "status": st, "n": _num(n) if st == "keep" else None,
                                  "says": verb(st, n), "rules": rules,
                                  "today": bool(set(m["weekdays"]) & set(d))})
            r["week"] = {"species": mine, "says": "; ".join(says_l), "status": "; ".join(status_l),
                         "parts": parts}
            notes = restated.get(tuple(r.get("all_members") or []))
            if notes:
                r["restates"] = notes
        got.append(f2)
    return got
