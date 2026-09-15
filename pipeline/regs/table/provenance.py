"""EVERY CELL, BACK TO THE SENTENCE THAT PUT IT THERE — as data, for one section on one day.

The tables are only trustworthy if a number can be traced to the book. `trace` prints that for a
person reading a terminal; this emits it, so a page can let a reader point at a row and see the
rules that produced it, the order they were weighed in, and which of them are speaking on the
date being viewed.

Three things it carries that the terminal view does not:

    THE DATE. A rung's window decides whether it is the answer TODAY. The pipeline cannot know
    the day, so every rung ships with its window as data and the answer is resolved per date
    here, which is what a date control on the page will do.

    THE ENTRY. A rule's `verbatim` is one sentence; the reader wants the passage it came from,
    which is the entry's own words. Both ship, so a row can be shown against the paragraph a
    person would find in the synopsis.

    THE IDENTITY. `entry::rule` — because a bare rule id is shared by up to nine different rules
    and anything keyed on it merges them.
"""
from __future__ import annotations
import json
from typing import Optional

from pipeline.regs.table.build import (build, section_rules, section_regions,
                                       section_label, name, D)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.resolve import head_of, competes


def _win(w) -> Optional[dict]:
    if not isinstance(w, dict):
        return None
    f, t = w.get("from") or {}, w.get("to") or {}
    return {"from": [f.get("month"), f.get("day")], "to": [t.get("month"), t.get("day")]}


#: Every (month, day) of a year, Feb 29 included — the calendar a row's answer is walked over.
_DAYS = [(m, d) for m, n in enumerate((31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31), 1)
         for d in range(1, n + 1)]


def _in_window(win: dict, month: int, day: int) -> bool:
    (fm, fd), (tm, td) = win["from"], win["to"]
    if None in (fm, fd, tm, td):
        return True
    here, lo, hi = (month, day), (fm, fd), (tm, td)
    return lo <= here <= hi if lo <= hi else (here >= lo or here <= hi)


def section(water: str, run: int = 0, on: Optional[tuple] = None) -> dict:
    """One section's table with its whole chain of custody, optionally as of (month, day)."""
    kind = "lake" if (D[water].get("kind") == "lake") else "stream"
    rules = section_rules(water, run)
    rows = build(rules, kind, section_regions(water, run), section_label(water, run))
    src = {rid(x): x for x in all_rules()}

    def rule_of(key: str) -> dict:
        x = src.get(key) or {}
        return {"id": key,
                "entry": key.split("::")[0],
                "label": x.get("label") or "",
                "verbatim": x.get("verbatim") or "",
                "windows": [w for w in (_win(o) for o in (x.get("windows") or [])) if w],
                # The dates can be when the rule does NOT apply (`windows_are: excepts`).
                "unless": str(x.get("windows_are") or "") == "excepts",
                # A CONDITION INSIDE THE DAY. "One hour after sunset to one hour before
                # sunrise" and "Saturday and Sunday only" are windows too, and a (month, day)
                # cannot settle them. Such a rung is never the answer FOR A DATE; it rides.
                "within_day": bool(x.get("from_time") or x.get("to_time") or x.get("weekdays")),
                "extent": x.get("extent_text") or "",
                "type": x.get("type") or ""}

    def live_on(rung, day) -> bool:
        x = rule_of(rung.rule_id)
        wins = x["windows"]
        if rung.applies.kind == "window":
            inside = not wins or any(_in_window(w, *day) for w in wins)
            return not inside if x["unless"] else inside
        if x["unless"] and wins:
            return not any(_in_window(w, *day) for w in wins)
        return True

    def live(rung) -> bool:
        return on is None or live_on(rung, on)

    def for_a_date(rung) -> bool:
        """Can this rung be THE ANSWER on a date? Not if it left the running (lifted, or
        replaced by its own clause), and not if its window is a time of day or a weekday.
        The Harrison's dusk-to-dawn closure was live on every date, so every date read
        closed; the Lower West Arm's weekday kokanee release did the same to its weekend 5."""
        return competes(rung) and not rule_of(rung.rule_id)["within_day"]

    # WHICH ROW SITS UNDER WHICH. "Bull trout" and "Steelhead" are inside "Trout and char", and
    # listed flat they read as four unrelated answers when three of them are the group's own
    # members. The narrowest row that covers another is its parent.
    def parent_of(r):
        cands = [o for o in rows if o is not r and o.subject.covers(r.subject)
                 and not r.subject.covers(o.subject)]
        if not cands:
            return None
        narrow = min(cands, key=lambda o: sum(1 for x in cands if x.subject.covers(o.subject)))
        return narrow

    def key_of(r):
        w, q = r.subject.words(name, is_release=(r.outcome.kind == "release"))
        return w + "||" + q

    out = []
    for r in rows:
        who, qual = r.subject.words(name, is_release=(r.outcome.kind == "release"))
        par = parent_of(r)
        chain = [{"rule": c.rule_id, "authority": c.authority, "rank": c.rank,
                  "outcome": c.outcome.word(), "why": c.status,
                  "when": c.applies.detail,
                  "about": c.subject.words(name)[0],
                  "live": live(c)}
                 for c in r.chain]
        # ON A DATE, THE ANSWER IS RE-DECIDED, NOT RE-FILTERED.
        #
        # The chain leads with the YEAR-ROUND answer, because that is the one the pipeline can
        # know. Taking the first LIVE rung therefore always hands back that same head, and a
        # seasonal closure that outranks it never takes over — the Fording read "keep 2" on
        # 15 September with its own Sep 1 – Oct 31 closure sitting live two lines below.
        #
        # So the live rungs are re-weighed by the same rule that weighed them in the first
        # place: one winner per subject, then `head_of`. The page does not re-implement the
        # ladder; it hands the day back to the ladder.
        # ...AND ONLY BY THE RUNGS THAT SPEAK FOR THE WHOLE ROW. A chain carries rungs about
        # narrower fish — absorbed rows, orphans the sweep placed — and `resolve` never let
        # one of those decide a subject it does not cover. Re-weighed as if it could, the
        # Chilliwack's "hatchery rainbow trout: 4" answered for every hatchery trout on the
        # river, and Region 6's May 15 – Jun 15 steelhead closure shut the Skeena's trout.
        # A joined row ("release · wild and hatchery") is covered by neither of its halves and
        # is decided by both, so where nothing covers the row every live rung is weighed.
        def answer_on(day):
            live_rungs = [c for c in r.chain if for_a_date(c) and live_on(c, day)]
            whole = [c for c in live_rungs if c.subject.covers(r.subject)]
            live_rungs = whole or live_rungs
            best = {}
            for rg in live_rungs:
                cur = best.get(rg.subject)
                if cur is None or (rg.rank, rg.outcome.rank) < (cur.rank, cur.outcome.rank):
                    best[rg.subject] = rg
            return head_of(list(best.values())) if best else None
        if on is None:
            answer = chain[0] if chain else None
        else:
            won = answer_on(on)
            answer = next((d for c, d in zip(r.chain, chain) if c is won), None)
        # THE YEAR, AS THE ANSWER CHANGES. The headline is the year-round rule, and on the
        # Skeena the year-round rule for a trout is never the answer on any day: "1 trout
        # from streams, Jul 1 – Oct 31" and "trout of any size from streams — release, Nov 1
        # – Jun 30" cover the calendar between them, and "Trout/char: 5" reads above both.
        # The calendar is the honest headline — every stretch of days with its answer and
        # the rule that set it — and `year_round` says whether the headline ever holds.
        calendar, cur = [], None
        for day in _DAYS:
            won = answer_on(day)
            key = (won.outcome.word(), won.rule_id) if won is not None else (None, None)
            if cur is not None and cur[0] == key:
                cur[2] = day
            else:
                cur = [key, day, day]; calendar.append(cur)
        if len(calendar) > 1 and calendar[0][0] == calendar[-1][0]:
            # Dec 31 wraps into Jan 1: one stretch, not two.
            calendar[0][1] = calendar[-1][1]; calendar.pop()
        calendar = [{"from": list(a), "to": list(b), "keep": k[0], "rule": k[1]}
                    for k, a, b in calendar]
        year_round = any(seg["rule"] == (chain[0]["rule"] if chain else None)
                         for seg in calendar)
        out.append({
            "fish": who, "qualifier": qual,
            "key": who + "||" + qual,
            "under": key_of(par) if par is not None else None,
            "pooled": bool(r.outcome.pooled),
            "members": sorted(name(c) for c in r.subject.effective())[:24],
            "keep": r.outcome.word(), "means": r.outcome.sentence(),
            # The headline's own season, where it has one — a row nothing year-round speaks to.
            "season": r.governs.applies.detail if not r.governs.applies.always else "",
            "answer_today": (answer or {}).get("outcome"),
            "set_by": (answer or {}).get("rule"),
            "calendar": calendar,
            "year_round": year_round,
            "chain": chain,
            # A SUB-LIMIT IS PART OF A NUMBER, so it ships with the pieces a reader needs to
            # see it that way: which fish it caps, how many, and whether that cap is shared.
            "of_which": [{"rule": l.rule_id, "says": l.sentence(name),
                          "n": l.n, "pooled": l.pooled,
                          "fish": l.subject.words(name)[0],
                          "qualifier": l.subject.words(name)[1]}
                         for l in (r.limits or [])],
            "also": [{"rule": c.rule_id, "outcome": c.outcome.word(),
                      "per": c.outcome.period,
                      "fish": c.subject.words(name)[0],
                      "qualifier": c.subject.words(name)[1]} for c in (r.ceilings or [])],
            "somewhere": [{"rule": c.rule_id, "where": c.applies.detail}
                          for c in (r.caveats or [])],
            # SIZE GATES AND DUTIES ARE NOT COMPETITORS EITHER. A count and a size bound are
            # both true at once — "5 per day" and "none under 30 cm" do not argue — so they
            # ride on the row rather than winning or losing a chain. They were shipped nowhere
            # and rendered nowhere, which is how a size limit disappears off a page. A gate
            # ships with its authority and its status: one the closer authority replaced, or
            # one that is moot because nothing may be kept, is still shown, marked.
            "gates": [{"rule": g.rule_id, "says": g.sentence(name, r.subject), "kind": "size",
                       "bound": g.size.bound, "authority": g.authority, "when": g.when,
                       "why": g.status, "fish": g.subject.words(name)[0]}
                      for g in (r.gates or [])]
                     + [{"rule": q.rule_id, "says": q.sentence(name), "kind": q.kind}
                        for q in (r.quals or [])],
            "duties": [{"rule": q.rule_id, "says": q.sentence(name)} for q in (r.duties or [])],
            "except": list(r.exemptions or []),
        })

    # A CLOSURE IS NOT ONLY ABOUT FISH. "Do not place any fishing gear in any water during a No
    # Fishing period" bites whenever a No Fishing rule does, and it is a method rule, so it goes
    # to the gear table and never appears beside the closure it belongs to.
    #
    # Matched on the rule's own sentence, which is the honest way to say it: the schema has no
    # field for "conditional on another rule closing the water", so there is nothing structural
    # to match on. That is a gap in the catalogue, not a licence to guess — the sentence names
    # the condition explicitly, and the page says it was matched that way.
    while_closed = [rule_of(rid(x)) for x in rules
                    if x.get("permitted") is False
                    and "no fishing period" in (x.get("verbatim") or "").lower()]

    # EVERY rule the page can name has to be in here, or it renders an empty quotation mark.
    # The annual ceiling did exactly that: cited on the row, absent from the dictionary.
    used = sorted({c["rule"] for row in out for c in row["chain"]}
                  | {l["rule"] for row in out for l in row["of_which"]}
                  | {c["rule"] for row in out for c in row["also"]}
                  | {g["rule"] for row in out for g in row["gates"]}
                  | {d["rule"] for row in out for d in row["duties"]}
                  | {c["rule"] for row in out for c in row["somewhere"]})
    return {"water": water, "stretch": run + 1,
            "label": section_label(water, run) or f"stretch {run + 1}",
            "kind": kind, "regions": sorted(section_regions(water, run)),
            "on": list(on) if on else None,
            "rules_in": len(rules), "rows_out": len(out),
            "rows": out, "while_closed": while_closed,
            "rules": {k: rule_of(k) for k in used}}


if __name__ == "__main__":
    import sys
    w = sys.argv[1] if len(sys.argv) > 1 else "Fraser River"
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    on = tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None
    print(json.dumps(section(w, r, on), indent=1))
