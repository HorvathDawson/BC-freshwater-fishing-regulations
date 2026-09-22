"""EVERY CELL, BACK TO THE SENTENCE THAT PUT IT THERE — as data, for one section.

The tables are only trustworthy if a number can be traced to the book. This emits, for one
stretch, the derived rows with every counter on them, each counter with its provenance on both
axes (who wrote it, what it binds to), the rule it came from, the dates it is live, and — where
it does not bind — why. A page renders it; it computes nothing.

    THE STAGE. Every counter says whether it is part of the region's standing table (`base`)
    or an override this section carries on top of it.

    THE DATE. A counter's window ships as data, so the page can answer for any day, and each
    row carries its calendar — every stretch of the year with the headline in force.

    THE IDENTITY. `entry::rule`, because a bare rule id is shared by up to nine rules.
"""
from __future__ import annotations
import json
from typing import Optional

from pipeline.regs.table.build import (ledger, base, section_rules, section_regions,
                                       section_label, section_kind, name, D)
from pipeline.regs.table.corpus import rid, rules as all_rules
from pipeline.regs.table.ledger import Allowance, Ledger
from pipeline.regs.table.rows import rows, Row, HIDDEN
from pipeline.regs.table.subject import Origin


def _win(w) -> Optional[dict]:
    if not isinstance(w, dict):
        return None
    f, t = w.get("from") or {}, w.get("to") or {}
    return {"from": [f.get("month"), f.get("day")], "to": [t.get("month"), t.get("day")]}


def counter_json(L: Ledger, a: Allowance, row: Optional[Row] = None, status: str = "",
                 on: Optional[tuple] = None) -> dict:
    who, qual = a.scope.words(name)
    # THE FISH THIS COUNTER STILL REACHES, from where this row stands — not the fish its
    # sentence names. Region 4's "1 bull trout (Dolly Varden)" no longer reaches bull trout
    # on Kootenay Lake, and the Dolly Varden's line must not say the cap is shared with it.
    o = (row.origin if row and row.origin is not Origin.both else Origin.wild)
    reaches = sorted(name(sp) for sp in a.scope.effective() if L.reaches(a, sp, o)) if row else []
    return {
        "rule": a.rule_id,
        "stage": "base" if a.source.is_base else "override",
        "kind": a.kind, "keep": a.word(), "n": a.n, "period": a.period, "pooled": a.pooled,
        "fish": who, "qualifier": qual, "size": a.scope.size.words(),
        "says": a.sentence(name),
        "plain": plain_words(a),
        "source": src_json(a.source),
        "when": a.applies.detail, "windows": [{"from": list(f), "to": list(t)}
                                              for f, t in a.applies.windows],
        "unless": a.applies.unless, "within_day": a.applies.within_day,
        "somewhere": a.applies.kind == "somewhere",
        "within": a.within,
        "derived_from": a.derived_from.rule_id if a.derived_from else None,
        "multiplier": a.multiplier,
        "multiplied_by": a.multiplied_by.rule_id if a.multiplied_by else None,
        "carves": [{"rule": c.rule_id, "words": c.source.words(), "fish": c.scope.words(name)[0],
                    "when": c.applies.detail} for c in L.carves.get(a, [])],
        "status": status or L.status.get(a, ""),
        "moot": bool(row and row.moot(a, on)),
        "reaches": reaches,
        "bc_wide": a.source.scope.value == "region" and a.source.authority.value == "province",
    }


def src_json(s) -> dict:
    return {"authority": s.authority.value, "scope": s.scope.value, "region": s.region,
            "place": s.place, "who": s.who, "tag": s.tag, "words": s.words(), "rank": s.rank,
            "rule": s.rule_id, "verbatim": s.verbatim.strip()}


def plain_words(a: Allowance) -> str:
    """What a counter says, for a person at the water. Not notation."""
    if a.kind == "gate":
        return a.scope.size.plain()
    if a.kind == "closed":
        return "closed — no fishing for it"
    if a.kind == "release":
        return "release — let it go"
    if a.kind == "unlimited":
        return "no limit"
    if a.within and a.scope.size.is_any:
        return f"only {a.n} of those"
    if not a.scope.size.is_any:
        return f"only {a.n} {a.scope.size.plain()}"
    return f"up to {a.n}" + (" between them" if a.pooled else "")


def in_force(c: dict, on: Optional[tuple] = None) -> bool:
    """A counter this table may state as its answer.

    WITH NO DATE — the standing table — a counter is in force only if it is in force EVERY day
    of the year: a window, a time of day, or a place nobody can draw all make it seasonal, true
    sometimes and not always, so it may not be folded into a number that claims to stand.

    WITH A DATE, it is in force if it is in force that day. The two readings are one function
    because a table for a date and a table for the year are the same table asked a different
    question — a second rule for dates would be the fourth ladder this model exists to delete.
    """
    if c["within_day"] or c["somewhere"]:
        return False
    if on is None:
        return not c["windows"]
    return _live(c, on)


def steady(c: dict) -> bool:
    """A counter in force every day of the year."""
    return in_force(c, None)


def combine(out: list, by_key: dict, on: Optional[tuple] = None) -> list:
    """A COMBINED QUOTA IS THE GROUPING; A SEASON IS NOT A FISH OF ITS OWN.

    Region 3 prints ONE line for bull trout, Dolly Varden and lake trout — "only 1 of these"
    inside the trout-and-char 5 — and this pass used to draw it as two rows, identical to the
    last digit, because the lake trout carries a release from Oct 15 and the bull trout does
    not. An entry was keyed by its WHOLE counter set, so one seasonal counter split a group
    the book treats as one, and the shared cap printed twice as if it were two separate caps.

    Two entries become one when, **ignoring the counters that are seasonal**, their counters
    and their size statements are identical AND they share a POOLED COMBINED QUOTA that names
    both of them. Both halves are load-bearing:

      · counter equality alone is not a group — two fish that merely happen to carry the same
        numbers are two fish, and joining them would invent a pool the book never wrote;
      · the pooled counter must not be the BAND they already sit under. The band is what puts
        every trout and char in one place; the thing that makes these three ONE LINE is the
        narrower number they share inside it. (It must also be a count of fish, not a cap on
        a size class: Region 3's "1 over 50 cm between them" reaches every trout and char and
        would merge the whole band into a single row.)

    What differs is exactly the seasons, and every one of them is carried onto the entry —
    `combined.seasons` for the ones every member has, `combined.members[].seasons` for the
    ones a member has alone, each labelled with the fish that counter ITSELF reaches. A
    season shown against a fish it does not name would close a legal fishery on the page.
    """
    def lone(e):
        if len(e["lines"]) != 1 or e["lines"][0]["origin"] != "either":
            return None                      # wild and hatchery already split it; leave it
        return by_key[e["lines"][0]["row"]]

    def key(e, r):
        return (e["band"], e["outside"], r["keep"],
                frozenset((c["rule"], c["period"], c["n"], c["kind"], c["within"])
                          for c in r["counters"] if in_force(c, on)),
                frozenset((x["rule"], x["kind"], x["n"], x["plain"]) for x in r["size"]))

    groups, order = {}, []
    for i, e in enumerate(out):
        r = lone(e)
        k = key(e, r) if r is not None else ("alone", i)
        if k not in groups:
            groups[k] = []; order.append(k)
        groups[k].append(e)

    res = []
    for k in order:
        g = groups[k]
        if len(g) < 2:
            res.extend(g); continue
        r0 = by_key[g[0]["lines"][0]["row"]]
        pooled = next((c for c in r0["counters"]
                       if in_force(c, on) and c["pooled"] and c["kind"] == "quota"
                       and c["period"] == "daily" and not c["size"]
                       and c["rule"] != g[0]["band"]
                       and all(set(c["reaches"]) & set(e["members"]) for e in g)), None)
        if pooled is None:
            res.extend(g); continue           # same numbers, no shared pool: not one group
        res.append(one_entry(g, by_key, pooled, on))
    return res


def season_json(c: dict, within: list) -> dict:
    """One seasonal counter as a line beneath a group reads it.

    `fish` is WHAT THIS COUNTER REACHES INSIDE `within` — never the line's own name, which
    may be wider (a closure that names six trout must not read as if it named all nine), and
    never the counter's whole subject, which may be wider still (Region 3's spring closure
    names every game fish in the region; on this line it speaks for these fish only). A
    season shown against a fish it does not name closes a legal fishery on the page.

    `subject` keeps the counter's own words, so the line can say where the rule came from."""
    here = sorted(set(c["reaches"]) & set(within))
    return {"rule": c["rule"], "kind": c["kind"], "keep": c["keep"], "n": c["n"],
            "period": c["period"], "plain": c["plain"], "says": c["says"],
            "windows": c["windows"], "unless": c["unless"],
            "subject": c["fish"], "fish": here or sorted(within)}


def one_entry(g: list, by_key: dict, pooled: dict, on: Optional[tuple] = None) -> dict:
    """The merged entry: the group's name and its ONE set of numbers, with each member's own
    seasons kept beside the member that owns them."""
    from pipeline.regs.table.rows import heading
    rs = [by_key[e["lines"][0]["row"]] for e in g]
    seasonal = [[c for c in r["counters"] if not in_force(c, on) and not c["moot"]] for r in rs]
    everywhere = set.intersection(*[{c["rule"] for c in s} for s in seasonal])
    fish = sorted({f for e in g for f in e["fish"]})
    all_members = sorted({m for e in g for m in e["members"]})
    return {
        "fish": fish, "heading": heading(frozenset(fish), name),
        "members": all_members,
        "lines": [l for e in g for l in e["lines"]],
        "band": g[0]["band"], "outside": g[0]["outside"],
        "province_only": all(e["province_only"] for e in g),
        "combined": {
            "pooled": pooled["rule"],
            "cells": rs[0]["key"],
            "seasons": [season_json(c, all_members) for c in seasonal[0]
                        if c["rule"] in everywhere],
            "members": [{"name": e["heading"], "fish": e["fish"], "row": r["key"],
                         "members": e["members"],
                         "seasons": [season_json(c, e["members"]) for c in s
                                     if c["rule"] not in everywhere]}
                        for e, r, s in zip(g, rs, seasonal)],
        },
    }


def present(d: dict, on: Optional[tuple] = None) -> dict:
    """THE TABLE A READER SEES, from the rows: one entry per kind of fish, never two rows for
    one species — where wild and hatchery differ they are two LINES under the one name — and
    a shared number as a BAND above its members, carrying once whatever is true of every
    member (a bound, a cap, an origin). Only what differs stays on the member's line.

    A COMBINED QUOTA IS ITSELF A GROUPING: fish that share one pooled number are ONE entry
    even when their seasons differ, and each member's season rides on that member's own line
    (`combine`). Without that, one seasonal counter split a group the book prints as one line.

    Pure structure: which rows make up which entry, which entries sit under which band, and
    which counters are hoisted. Cells are still computed from the counters by whoever draws
    them. A test proves a hoisted statement is true of every line.

    `on` ASKS THE SAME QUESTION OF ONE DAY. With no date this is the table of the year, and a
    seasonal counter can never be folded into it. With a date it is the table of that day, and
    a season IS the answer: Region 3's bull trout and lake trout share one number and stop
    sharing it on Oct 15, when the lake trout goes back and the char does not, so on that day
    they are two entries and on Nov 5 the lake trout stands alone. The grouping is settled
    here, from counters in force, rather than annotated onto a table built for another day.
    """
    rows_ = d["rows"]
    by_key = {r["key"]: r for r in rows_}
    species = sorted({sp for r in rows_ for sp in r["fish"]})
    def row_for(sp, origin):
        for r in rows_:
            if sp in r["fish"] and r["origin"] in ("both", origin):
                return r
        return None
    entries, order = {}, []
    for sp in species:
        w, h = row_for(sp, "wild"), row_for(sp, "hatchery")
        key = (w["key"] if w else None, h["key"] if h else None)
        if key not in entries:
            entries[key] = {"fish": [], "wild": w, "hatchery": h}; order.append(key)
        entries[key]["fish"].append(sp)
    from pipeline.regs.table.rows import heading
    out, bands = [], {}
    for key in order:
        e = entries[key]
        w, h = e["wild"], e["hatchery"]
        fish = sorted(e["fish"])
        lines = ([{"origin": "either", "row": w["key"]}] if w is h or (w and h and w["key"] == h["key"])
                 else [x for x in ({"origin": "wild", "row": w["key"]} if w else None,
                                   {"origin": "hatchery", "row": h["key"]} if h else None) if x])
        band = next((by_key[l["row"]]["group"] for l in lines if by_key[l["row"]]["group"]), None)
        for l in lines:
            l["in_band"] = bool(band) and by_key[l["row"]]["group"] == band
        # A FISH THAT STANDS OUTSIDE THE SHARED NUMBER IS STILL DRAWN UNDER ITS GROUP. Kootenay's
        # rainbow (10, on top of the 5) is a trout, and a reader scanning TROUT AND CHAR must
        # find it there — with a line saying it does not come out of the shared number.
        outside = None
        if band is None:
            for l in lines:
                for o in by_key[l["row"]].get("outside_of") or []:
                    if o["rule"] in {r["group"] for r in rows_ if r["group"]}:
                        outside = o["rule"]; break
                if outside: break
        entry = {"fish": fish, "heading": heading(frozenset(fish), name),
                 "members": sorted(name(c) for c in fish if c not in HIDDEN), "lines": lines,
                 "band": band or outside, "outside": bool(outside and not band),
                 "province_only": all(by_key[l["row"]]["province_only"] for l in lines)}
        out.append(entry)
    # FISH THAT SHARE ONE COMBINED NUMBER ARE ONE ENTRY, whatever their seasons. This runs
    # before the bands are assembled so a band holds the merged entry, once, not its halves.
    out = combine(out, by_key, on)
    for entry in out:
        if entry["band"]:
            bands.setdefault(entry["band"], {"id": entry["band"], "entries": []})["entries"].append(entry)
    # HOIST WHAT IS TRUE OF THE WHOLE BAND. A counter every in-band line carries is stated
    # once on the band; the band's own number and its possession are the band itself.
    for bid, b in list(bands.items()):
        in_band = [by_key[l["row"]] for e in b["entries"] for l in e["lines"] if l["in_band"]]
        if not in_band:
            continue
        # A BAND WHOSE OWN NUMBER IS NOT IN FORCE IS NOT A BAND. On a date, the shared quota
        # may be carved away — a closure, a seasonal release — and the rows underneath no
        # longer count against anything. Drawing the band anyway prints a heading that says
        # "one shared number for every fish below" above no number at all. The entries stand
        # on their own for that stretch of the year.
        counter = next((c for c in in_band[0]["counters"] if c["rule"] == bid), None)
        if counter is None:
            del bands[bid]
            for e in b["entries"]:
                e["band"], e["outside"] = None, False
                for l in e["lines"]:
                    l["in_band"] = False
            continue
        poss = next((c for c in in_band[0]["counters"] if c["derived_from"] == bid), None)
        common = set.intersection(*[{c["rule"] for c in r["counters"]} for r in in_band]) if in_band else set()
        common -= {bid}
        if poss: common.discard(poss["rule"])
        # a derived possession of a hoisted daily counter is hoisted with it, silently
        b["counter"] = counter; b["possession"] = poss
        b["hoisted"] = sorted(c["rule"] for c in in_band[0]["counters"]
                              if c["rule"] in common and c["derived_from"] is None)
        b["hoisted_all"] = sorted(common)
        quals = {r["qualifier"] for r in in_band}
        b["origin"] = quals.pop() if len(quals) == 1 else ""
        b["province_only"] = all(e["province_only"] for e in b["entries"])
        # A SHARED NUMBER NOTHING MAY BE KEPT AGAINST IS NOT A NUMBER. Under Region 3's
        # spring closure every row beneath the trout-and-char band reads "No fishing" and the
        # band went on offering four a day and eight in possession — a shut river printing a
        # limit, which is the worst direction for this page to be wrong in. The counter is
        # already marked `moot` on each of those rows; a band is moot when it is moot on
        # EVERY row under it (one released fish among five must not empty the other four),
        # and then the band says what they all say.
        mine = [c for r in in_band for c in r["counters"] if c["rule"] == bid]
        b["moot"] = bool(mine) and all(c["moot"] for c in mine)
        keeps = {r["keep"] for r in in_band}
        b["answer"] = keeps.pop() if len(keeps) == 1 else None
    return {"entries": out, "bands": bands}


def _live(c: dict, on: tuple) -> bool:
    """The page's own test, from the JSON fields alone: is this counter drawn as in force?"""
    if c["somewhere"] or c["within_day"]:
        return False
    if not c["windows"]:
        return True
    def inside(w):
        a, b = tuple(w["from"]), tuple(w["to"])
        return a <= tuple(on) <= b if a <= b else (tuple(on) >= a or tuple(on) <= b)
    ins = any(inside(w) for w in c["windows"])
    return (not ins) if c["unless"] else ins


def visible(d: dict, on: tuple) -> dict:
    """{row key: the rule ids a reader sees on that row, on that date} — computed from the
    emitted JSON the way the page computes it, not from the objects `rows()` built. A counter
    drawn on the band above a row is visible on the row; a stopped line shows its stop and
    its seasons, nothing else counts."""
    out = {}
    pr = d["present"]
    for r in d["rows"]:
        shown = {c["rule"] for c in r["counters"] if _live(c, on)}
        shown |= {x["rule"] for x in r["size"] if x["rule"]}
        out[r["key"]] = shown
    for e in pr["entries"]:
        b = pr["bands"].get(e["band"]) if e["band"] else None
        for l in e["lines"]:
            if b and l["in_band"]:
                out[l["row"]] |= {b["id"]} | set(b["hoisted_all"])
    return out


def row_json(L: Ledger, r: Row, on: Optional[tuple] = None) -> dict:
    """One row: the fish, the size statement, and every counter — the shape a page lays out
    as Fish · Size · Daily · Annual · Possession, and a text table prints the same way."""
    head = r.headline()
    today = r.headline(on) if on else head
    cal = r.calendar()
    counters = [counter_json(L, a, r, on=on) for a in r.counters]
    behind = [counter_json(L, a, r, st, on) for a, st in r.behind]
    # A SHARED NUMBER THIS FISH WAS TAKEN OUT OF. Kootenay's bull trout has its own 1 and does
    # not count against the trout-and-char 5; the row says so in words, since it is drawn
    # beside a band it is not part of.
    outside = [{"rule": a.rule_id, "fish": a.scope.words(name)[0], "keep": a.word()}
               for a, st in r.behind if st.startswith("replaced for these fish") and a.pooled
               and a.period == "daily" and a.derived_from is None and not a.within
               and a.scope.size.is_any and head is not None and not head.is_zero]
    size = [{"says": x["says"], "plain": x["plain"], "kind": x["kind"], "rule": x["rule"],
             "n": x["n"], "shared": x["shared"],
             "source": src_json(x["source"]) if x["source"] else None}
            for x in r.size(on, name)]
    # THE WIDEST SHARED NUMBER THIS FISH COUNTS INSIDE IS ITS BAND — whether or not it is the
    # headline. Kootenay's bull trout has its own 1 and still counts inside Region 4's shared
    # 5, so it is drawn under the trout-and-char band with its 1 on its own line; five rows
    # never read as five fives, and a fish inside a shared number is never drawn outside it.
    o = r.origin if r.origin is not Origin.both else Origin.wild
    shared = [a for a in r.counters if a.period == "daily" and a.outcome.kind == "quota"
              and a.pooled and not a.within and a.scope.size.is_any
              and len(a.scope.effective()) > len(r.fish)
              and (a.applies.always or a.applies.unless)
              and L.binds(a, r.species, o, None, None)]
    group = max(shared, key=lambda a: len(a.scope.effective())).rule_id if shared else None
    return {
        "fish": sorted(r.fish), "members": sorted(name(c) for c in r.fish if c not in HIDDEN),
        "heading": r.heading(name), "qualifier": r.qualifier(), "origin": r.origin.value,
        "key": r.heading(name) + "||" + r.qualifier(),
        "keep": head.word() if head else None,
        "set_by": head.rule_id if head else None,
        "group": group,
        "province_only": r.province_only,
        "size": size,
        "outside_of": outside,
        "means": head.outcome.sentence() if head else "no standing number — see the calendar",
        "answer_today": today.word() if today else None,
        "today_by": today.rule_id if today else None,
        "live_today": [a.rule_id for a in r.live(on)] if on else None,
        "calendar": cal,
        "year_round": any(seg["rule"] == (head.rule_id if head else None) for seg in cal),
        "counters": counters, "behind": behind,
    }


def as_of(L: Ledger, r: Row, on: tuple) -> dict:
    """ONE ROW AS A DAY FINDS IT — the answer on that date, not the year-round one.

    `row_json` answers for the YEAR: `keep` is the number that stands all twelve months, and a
    season rides beside it as a line the reader has to apply themselves. That is the right
    answer to "what does this region say", and the wrong one to "what may I keep today". Here
    the season IS the number: a counter not in force on this day is not on the row at all, so
    nothing downstream — the grouping, the bands, the size classes, the possession twin — can
    read a rule that is not in force today as if it were.

    Everything is still settled by the ledger. This only chooses WHICH settled counters the
    day can see, and re-reads the headline from them; there is no second resolution here.
    """
    d = row_json(L, r, on)
    live = set(r.live(on))
    d["counters"] = [c for a, c in zip(r.counters, d["counters"]) if a in live]
    shown = {c["rule"] for c in d["counters"]}
    h = r.headline(on)
    d["dated"] = list(on)
    d["keep"] = h.word() if h is not None else None
    d["set_by"] = h.rule_id if h is not None else None
    d["means"] = (h.outcome.sentence() if h is not None
                  else "no number reaches this fish on this date")
    # A BAND IS A NUMBER THIS FISH COUNTS INSIDE, and on a day the number may not be there.
    if d["group"] not in shown:
        d["group"] = None
    # ...AND SO MAY THE NUMBER A FISH WAS TAKEN OUT OF. "Does not come out of the shared 5"
    # says nothing on a day when there is no shared 5. That rule sits in `behind`, never in
    # `counters`, so its own dates are what decide it.
    back = {a.rule_id: a for a, _ in r.behind}
    d["outside_of"] = [o for o in d["outside_of"]
                       if h is not None and not h.is_zero
                       and back[o["rule"]].applies.live(*on)]
    return d


def section(water: str, run: int = 0, on: Optional[tuple] = None) -> dict:
    """One section's table with its whole chain of custody, optionally as of (month, day)."""
    kind = section_kind(water)
    rules = section_rules(water, run)
    L = ledger(rules, kind, section_regions(water, run), section_label(water, run))
    B = base(rules, kind)
    src = {rid(x): x for x in all_rules()}

    def rule_of(key: str) -> dict:
        x = src.get(key) or {}
        return {"id": key, "entry": key.split("::")[0], "label": x.get("label") or "",
                "verbatim": x.get("verbatim") or "",
                "windows": [w for w in (_win(o) for o in (x.get("windows") or [])) if w],
                "unless": str(x.get("windows_are") or "") == "excepts",
                "within_day": bool(x.get("from_time") or x.get("to_time") or x.get("weekdays")),
                "extent": x.get("extent_text") or "", "type": x.get("type") or ""}

    out, used = [], set()
    for r in rows(L, name):
        head = r.headline()
        d = row_json(L, r, on)
        for c in d["counters"] + d["behind"]:
            used.add(c["rule"])
            if c["multiplied_by"]: used.add(c["multiplied_by"])
        d["duties"] = [{"rule": s.rule_id, "says": text, "source": s.words(), "tag": s.tag}
                       for subj, s, text in L.duties
                       if head is not None and not head.is_zero
                       and (subj.covers(r_subject(r)) or any(subj.contains(f, r.origin if r.origin.value != "both" else None) for f in r.fish))]
        out.append(d)
    somewhere = [counter_json(L, a) for a in L.allowances if a.applies.kind == "somewhere"]
    used |= {c["rule"] for c in somewhere}
    while_closed = [rule_of(rid(x)) for x in rules
                    if x.get("permitted") is False
                    and "no fishing period" in (x.get("verbatim") or "").lower()]
    d = {"water": water, "stretch": run + 1,
            "label": section_label(water, run) or f"stretch {run + 1}",
            "kind": kind, "regions": sorted(section_regions(water, run)),
            "on": list(on) if on else None,
            "rules_in": len(rules), "rows_out": len(out),
            "base": {"rules": sorted(a.rule_id for a in B.allowances if a.derived_from is None),
                     "label": f"{region_label(section_regions(water, run))} · {kind}s"},
            "rows": out,
            "somewhere": somewhere,
            "exemptions": [dict(e, says=(src.get(e.get("lifter")) or {}).get("label") or
                                (src.get(e.get("lifter")) or {}).get("verbatim") or "")
                           for e in L.exemptions],
            "while_closed": while_closed,
            "rules": {k: rule_of(k) for k in sorted(used)}}
    d["present"] = present(d)
    return d


def r_subject(r: Row):
    from pipeline.regs.table.subject import Subject
    return Subject(r.fish, r.origin)


def region_label(regions) -> str:
    from pipeline.regs.table.authority import region_words
    return region_words(regions) if regions else "?"


def base_table(water: str, run: int) -> dict:
    """STAGE 1, printable: the standing table a section draws on, as the book prints it — one
    row per fish the region's rules treat alike."""
    kind, rules = section_kind(water), section_rules(water, run)
    regions = section_regions(water, run)
    B = base(rules, kind)
    out = [row_json(B, r) for r in rows(B, name)]
    d = {"regions": sorted(regions), "kind": kind,
         "label": f"{region_label(regions)} · {kind}s",
         "rules": sorted(a.rule_id for a in B.allowances if a.derived_from is None),
         "rows": out, "from": (water, run)}
    d["present"] = present(d)
    return d


def provincial_table(kind: str) -> dict:
    """The province's own table for a kind of water — the rules every region inherits."""
    from pipeline.regs.table.build import allowances
    from pipeline.regs.table.corpus import rules as all_rules
    from pipeline.regs.table.authority import source_of
    rs = [x for x in all_rules() if x["entry"].startswith("zp:") and source_of(x).is_base]
    alw, lifted, fam, mults, duties, unresolved = allowances(rs, kind)
    P = Ledger(alw, lifted=lifted, family=fam, multiples=mults, duties=duties,
               exemptions=unresolved, water_kind=kind)
    out = [row_json(P, r) for r in rows(P, name)]
    d = {"regions": [], "kind": kind, "label": f"Provincial · {kind}s",
         "rules": sorted(a.rule_id for a in P.allowances if a.derived_from is None),
         "rows": out, "from": ("Provincial", 0), "sections": 102}
    d["present"] = present(d)
    return d


def base_tables() -> list:
    """Every distinct standing table the shipped sections draw on — keyed by the RULE SET,
    not the region id: Haida Gwaii is administratively Region 1 and draws on a different
    table from the rest of it, and a boundary stretch draws on two regions at once."""
    seen, out = set(), []
    for w in D:
        if w.startswith("_"): continue
        for run in range(len(D[w].get("runs") or [])):
            rules = section_rules(w, run)
            if not rules: continue
            key = (frozenset(a.rule_id for a in base(rules, section_kind(w)).allowances),
                   section_kind(w))
            if key in seen: continue
            seen.add(key)
            t = base_table(w, run)
            t["sections"] = sum(1 for w2 in D if not w2.startswith("_")
                                for r2 in range(len(D[w2].get("runs") or []))
                                if section_rules(w2, r2) and section_kind(w2) == key[1]
                                and frozenset(a.rule_id for a in base(section_rules(w2, r2), key[1]).allowances) == key[0])
            out.append(t)
    out.sort(key=lambda t: (t["regions"], t["kind"], t["from"]))
    return [provincial_table("lake"), provincial_table("stream")] + out


if __name__ == "__main__":
    import sys
    w = sys.argv[1] if len(sys.argv) > 1 else "Fraser River"
    r = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    on = tuple(int(x) for x in sys.argv[3].split("-")) if len(sys.argv) > 3 else None
    print(json.dumps(section(w, r, on), indent=1))
