"""Emit the standing (base) tables and their print checks as one JSON payload.

The base tables are the foundation everything else is validated against: a section's table
is its region's base plus named overrides, so a base that is wrong is a defect on every
water in the region at once. They were reachable only through a `<select>` underneath the
section table on each artifact — hidden beneath the thing they are meant to validate, and
split across two artifacts with different layouts, so reviewing one region meant reading
two pages.

This writes what a review surface needs, per region and kind, in one file:

    {"regions": [{key, region, kind, title, quota: {...}, gear: {...}}], "meta": {...}}

Both halves carry the TABLE (what we would show) and the CHECKS (each printed line beside
the line we computed from it, with its citation). Nothing here resolves anything — it is
`provenance.base_tables`, `method_provenance.base_tables`, `quota_print.all_panels` and
`method_print.all_panels` put side by side.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.tools.emit_base_tables -o out.json

The two print modules name their fields inversely — a quota `Check.line` is the PRINTED
line and `.found` is ours; a gear `Check.line` is OURS and `.sentence` is printed. They are
normalised here to `printed` / `ours` so one renderer reads both.
"""
from __future__ import annotations

import argparse
import json
import sys

from pipeline.regs.table import provenance, method_provenance
from pipeline.regs.table import quota_print as QP, method_print as MP
from pipeline.regs.table import state as ST
from pipeline.regs.table.corpus import rid
from pipeline.regs.table.authority import source_of


def log(*a):
    print(*a, file=sys.stderr)


def _check(c, printed_attr: str, ours_attr: str) -> dict:
    """One line of a print diff, normalised. `why` carries the reason a tick rests on an
    argument rather than a found sentence — a ✓ with neither is refused upstream."""
    return {
        "ok": bool(c.ok),
        "printed": (getattr(c, printed_attr, "") or "").strip(),
        "ours": (getattr(c, ours_attr, "") or "").strip(),
        "why": (getattr(c, "why", "") or "").strip(),
        "label": (getattr(c, "label", "") or "").strip(),
        "rule": (getattr(c, "rule", "") or "").strip(),
    }


def _panel(p, printed_attr: str, ours_attr: str) -> dict:
    checks = [_check(c, printed_attr, ours_attr) for c in p.checks]
    return {
        "title": getattr(p, "title", ""),
        "source": getattr(p, "source", ""),
        "md5": getattr(p, "md5", ""),
        "gov": getattr(p, "gov", ""),
        "checks": checks,
        "ok": sum(1 for c in checks if c["ok"]),
        "n": len(checks),
    }


def _quota_table(region: str, kind: str) -> dict:
    """The rows AND the presentation pass over them.

    `provenance.present` is what makes the table a reader's table rather than a dump: one
    entry per kind of fish, **never two rows for one species** — where wild and hatchery
    differ they become two LINES under the one name — a shared number as a BAND above its
    members carrying once whatever is true of all of them, and a fish that stands outside the
    shared number still drawn under its group. Rendering the raw rows instead loses every one
    of those and splits a species across the table.
    """
    d = _table(region, kind)
    d["always"] = _always(region, kind)
    return d


def _table(region: str, kind: str, area=None) -> dict:
    """ONE TABLE, UNDER ONE SET OF CONDITIONS — rendered from `state.state`, which is the only
    thing in this file that decides what a table is made of. `area=None` is anywhere in the
    region no area rule reaches; each named area is the same builder with that area's rules
    as an extra input, so a reviewer reading the page and a test walking the conditions are
    looking at the same function."""
    st = ST.state(region, kind, area)
    rows = [provenance.row_json(st.ledger, r) for r in st.rows]
    segs = st.schedule()
    terms = list(getattr(st.gear, "terms", ()) or ())
    against = {"rows": rows, "gear": None}
    return {"rows": rows,
            "present": provenance.present({"rows": rows}) if rows else {"entries": [], "bands": {}},
            "views": _views(st, segs, any(t.applies.windows for t in terms), against)}


def _views(st, segs, seasonal_gear: bool, against=None) -> list:
    """THE YEAR, AS TABLES A READER CAN PICK FROM.

    A standing table is the table of a year: where a rule is seasonal it can state no number,
    and the season rides beside the row as a line the reader applies themselves. That is honest
    and it is not usable — the question at the water is "what may I keep TODAY".

    So the year is cut into the stretches over which one table holds and each is emitted as a
    whole table settled for a day inside it. A table with no seasons has one stretch and
    carries no rows of its own — it IS the year-round table, and a copy would be a second
    thing to keep true. A per-stretch GEAR table is emitted only where a gear term really
    carries a window, because everywhere else it would be the same table repeated.
    """
    extra = list(st.area.rule_dicts) if st.area is not None else []
    # THE BASELINE A SEASON IS MEASURED AGAINST MUST INCLUDE ITS GEAR. Comparing a dated gear
    # table against nothing made every line on it read as newly in force — a bait ban arriving
    # on Nov 1 alongside the nine provincial rules that were there all along.
    if against is not None and seasonal_gear:
        against = dict(against,
                       gear=method_provenance.base_table(st.region, st.kind, None, extra))
    out = []
    for seg in segs:
        v = dict(seg)
        on = tuple(seg["from"])
        if not seg["whole_year"]:
            vr = [_slim(provenance.as_of(st.ledger, r, on)) for r in st.rows]
            v["rows"] = vr
            v["present"] = provenance.present({"rows": vr}, on)
        else:
            v["same"] = True
        if seasonal_gear:
            v["gear"] = method_provenance.base_table(st.region, st.kind, on, extra)
        if against is not None and not seg["whole_year"]:
            v["delta"] = _delta(against, {"rows": v["rows"], "gear": v.get("gear")})
        out.append(v)
    return out


_MON = ("", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _when(applies) -> str:
    if not applies.windows:
        return ""
    w = ", ".join(f"{_MON[f[0]]} {f[1]} – {_MON[t[0]]} {t[1]}" for f, t in applies.windows)
    return ("every day but " + w) if applies.unless else w


def conditional(region: str, kind: str) -> list:
    """EVERY RULE HERE THAT DEPENDS ON WHERE OR WHEN YOU ARE, in one list.

    The states are all reachable — and a reviewer still has to guess which chip to press to
    find a given sentence, which is no way to check a chapter against a book. Region 1's
    stream rules are four lines in the synopsis: a bait ban and a hook rule that hold all
    year, a summer closure in six management units, and a bait ban in two more. Three of the
    four are conditional, and until now nothing on the page said so in one place.

    So: every rule with a season, and every rule that belongs to a named area, with the place
    and the dates it answers to and the words the book uses. The page hangs a button on each
    one that goes to the state where it bites, which is the difference between "the state
    exists" and "a reviewer can find it".
    """
    out, seen = [], set()

    def add(src, applies, where, side, needs_when=False, rank=0):
        if src.rule_id in seen:
            return
        when = _when(applies)
        # A REGION'S OWN RULES ARE NOT A CONDITION OF BEING IN IT. Haida Gwaii IS Management
        # Units 6-12 and 6-13, so every rule in its base names that place; listing all nine
        # buries the one line that actually turns on something — the bait ban, Nov 1 – Apr 30.
        # A base rule earns a place here by having a season; an AREA rule earns it by being
        # narrower than the table you are looking at.
        if needs_when and not when:
            return
        if not when and not where:
            return
        seen.add(src.rule_id)
        out.append({"rule": src.rule_id, "verbatim": (src.verbatim or "").strip(),
                    "who": src.who, "where": where, "when": when, "side": side, "rank": rank,
                    "on": [applies.windows[0][0][0], applies.windows[0][0][1]]
                          if applies.windows and not applies.unless else None})

    # an area's rules first — they are the ones a reader cannot find by scrolling
    for area in ST.areas(region, kind):
        # A PARK IS EVERY REGION'S AREA; A MANAGEMENT UNIT IS THIS ONE'S. The province's
        # closures are true here and identical on the other ten chapters, so they go last —
        # a reviewer checking Region 1 against its page is looking for Region 1's sentences.
        rs = area.rule_dicts
        own = 0 if rs and ST.chapter(rs[0]["entry"]) == region else 1
        for x in rs:
            add(source_of(x), _applies_of(x), area.name, "area", rank=own)
    st = ST.state(region, kind)
    # ...then whatever the region's own table only says sometimes. A base rule can still name
    # a place — Haida Gwaii IS Management Units 6-12 and 6-13, so its bait ban is an area rule
    # that happens to cover the whole of it, and saying so is how a reader recognises the
    # sentence in the book.
    for a in st.ledger.allowances:
        if a.derived_from is None:
            add(a.source, a.applies, _place(a.source, region), "quota", needs_when=True)
    for t in getattr(st.gear, "terms", ()) or ():
        add(t.source, t.applies, _place(t.source, region), "gear", needs_when=True)
    return sorted(out, key=lambda r: (r["rank"], not r["when"], r["rule"]))


def _place(src, region: str) -> str:
    """The place a rule names, where that is narrower than the whole region."""
    p = (src.place or "").strip()
    return p if p.startswith("MU") or "Park" in p or "Reserve" in p else ""


def _applies_of(x: dict):
    from pipeline.regs.table.build import _applies
    return _applies(x, "")


def _answers(rows: list) -> dict:
    """{species → what you may do with it}. Keyed by SPECIES, not by row, because a state can
    regroup the rows themselves: Region 3's bull trout, Dolly Varden and lake trout are one
    row until the lake trout is released, and then they are two. A row-to-row diff would call
    that "one row gone, one row new" and say nothing about the fish."""
    out = {}
    for r in rows:
        for sp in r["fish"]:
            out[sp] = r["keep"]
    return out


def _delta(base: dict, now: dict) -> dict:
    """WHAT THIS STATE CHANGES, against the region's year-round table.

    The whole model rests on "a table is its base plus named overrides, and every difference is
    attributable to one of them". The page could show a state and leave the reader to spot the
    difference by eye across two screens — which is how a wrong number survives a review. This
    says it: these fish, from this to that, and this gear line came or went.
    """
    a, b = _answers(base["rows"]), _answers(now["rows"])
    by = {}
    for sp in sorted(set(a) | set(b)):
        was, is_ = a.get(sp), b.get(sp)
        if was == is_:
            continue
        by.setdefault((was, is_), []).append(sp)
    from pipeline.regs.table.build import name as fish_name
    from pipeline.regs.table.rows import HIDDEN
    fish = [{"was": k[0], "now": k[1],
             "fish": sorted(fish_name(c) for c in v if c not in HIDDEN)}
            for k, v in by.items()]
    fish = [f for f in fish if f["fish"]]

    def rig(t, steady=False):
        """`steady` keeps only the lines in force EVERY day. That is what a stretch has to be
        measured against: the year-round gear table lists every seasonal rule too, each with
        its dates, so comparing a stretch to it showed no difference at all — the bait ban is
        on both, and the whole point is that it only bites on one."""
        out = set()
        def take(ts, topic):
            for x in ts:
                if steady and x.get("windows"):
                    continue
                out.add((topic, x.get("plain") or x.get("text") or ""))
        for row in ((t or {}).get("rows") or []):
            for topic, ts in (row.get("rig") or {}).items():
                take(ts, topic)
        for topic, ts in (((t or {}).get("band") or {}).get("rig") or {}).items():
            take(ts, topic)
        return {x for x in out if x[1]}

    ga, gb = rig(base.get("gear"), steady=True), rig(now.get("gear"))
    gear = ([{"how": "now", "topic": t, "says": w} for t, w in sorted(gb - ga)] +
            [{"how": "gone", "topic": t, "says": w} for t, w in sorted(ga - gb)])
    return {"fish": fish, "gear": gear}


def _seen(d: dict) -> str:
    """WHAT A READER ACTUALLY SEES, as one string. Two states are the same state when this is
    the same — not when their JSON is. The park-reserve rule binds nothing and shows up only
    as one more line in `behind`, the chain of custody; by raw JSON that is a different table,
    and it would put twenty identical copies of every region on the page, each claiming to be
    a state of its own."""
    rows = [(r["key"], r["keep"], [x["plain"] for x in r["size"]], r["group"],
             sorted((c["rule"], c["n"], c["kind"], c["period"], c["moot"])
                    for c in r["counters"]))
            for r in d["rows"]]
    return json.dumps([rows, d["present"], [v.get("label") for v in d["views"]]],
                      sort_keys=True, separators=(",", ":"))


def _shut(d: dict) -> bool:
    """A place where nothing may be taken at all. It needs no table — twelve rows all reading
    "No fishing" is the same sentence twelve times — and the rules that shut it are the whole
    answer."""
    return bool(d["rows"]) and all(r["keep"] in ("0", "closed") for r in d["rows"])


def _slim(row: dict) -> dict:
    """A dated row without its chain of custody.

    `behind` — what else names these fish and why it does not bind — and each counter's own
    source sentence are how a line is traced back to the book, and they are 1.8 MB of the 52
    dated tables. They are not dropped from the page: the STANDING table carries all of it,
    unchanged, and it is the table the printed synopsis is checked against. A dated view
    answers "what may I keep on Oct 20"; the custody of every one of its numbers is one click
    away on the year-round table, where the check itself lives.
    """
    row.pop("behind", None)
    for c in row["counters"]:
        c.pop("carves", None)
        c.pop("source", None)
    return row


def _always(region: str, kind: str) -> list:
    """The rules that are true on every water and can never be drawn on a map.

    "Within 23 m downstream of any fishway, canal, obstacle or leap" and "within 100 m of any
    government counting, passing or rearing facility" bind everywhere, but because nothing can
    place them they were filed as "somewhere" caveats and rendered as the faintest text on the
    page, under a collapsed disclosure. A man standing below a fishway reads the big number and
    never sees the rule that governs him. They are general rules and belong at the top.
    """
    from pipeline.regs.table import rows as R
    L = QP.base_ledger(region, kind)

    # WHAT A ROW ALREADY ANSWERS DOES NOT BELONG UP HERE. Three rules reach this list only
    # because their `extent_text` is prose rather than a place — Region 4's "illegal to fish
    # for bass, perch, pike or walleye in the Kootenay Region", Region 5's bass notice, and
    # Region 2's protected-species list. Each of those fish already has its own row saying 0,
    # so repeating the notice at the top is the same answer twice, and it crowds out the two
    # rules that genuinely cannot be drawn: the 23 m and 100 m buffers, which say 0 in a spot
    # nobody can map while the rows say 5.
    answered = {}
    for r in R.rows(L):
        h = r.headline()
        if h is None:
            continue
        for sp in r.fish:
            answered[sp] = h.word()

    out, seen = [], set()
    for a in L.allowances:
        if getattr(a.applies, "kind", "") != "somewhere":
            continue
        rid_ = a.source.rule_id
        if rid_ in seen:
            continue
        fish = a.scope.effective()
        word = a.word()
        if fish and all(answered.get(sp) == word for sp in fish):
            continue                      # every fish it names already reads this on its row
        seen.add(rid_)
        out.append({"rule": rid_, "where": a.applies.detail or a.source.place,
                    "verbatim": a.source.verbatim, "who": a.source.who, "says": word})
    return out


def _quota_rows(region: str, kind: str) -> list:
    """The base table's own rows.

    NOT `provenance.base_tables()`: that list is derived from the sections that ship, so it
    is keyed by label, omits Haida Gwaii and Region 7B entirely, and carries composites
    ("Regions 3 and 5 · streams") that no single region owns. The print panels are the
    complete spine — one per region per kind — so the rows are built from the same ledger
    the panel checked, and every region gets a table whether a section uses it or not.
    """
    L = QP.base_ledger(region, kind)
    return [provenance.row_json(L, r) for r in rows_of(L)]


def rows_of(L):
    from pipeline.regs.table import rows as R
    return R.rows(L)


def collect() -> dict:
    # -- the print diffs, keyed (region, kind) -------------------------------------------
    # THE TWO SIDES NAME THE PROVINCE DIFFERENTLY — quota's panel is region "p", kind "any"
    # (it stands for both kinds); gear's is region "province", one per kind. Left alone they
    # produce three provincial entries, none of them whole.
    def reg_key(r):
        return "province" if str(r) in ("p", "province") else str(r)

    qp = {}
    for p in QP.all_panels():
        qp[(reg_key(p.region), p.kind)] = _panel(p, "line", "found")
    mp = {}
    for reg, kind, p in MP.all_panels():
        mp[(reg_key(reg), kind)] = _panel(p, "sentence", "line")

    keys = sorted({(r, k) for r, k in set(qp) | set(mp) if k in ("lake", "stream")},
                  key=lambda k: (str(k[0]), str(k[1])))
    out = []
    for region, kind in keys:
        q_panel = qp.get((region, kind)) or qp.get((region, "any"))
        m_panel = mp.get((region, kind)) or mp.get((region, "any"))
        try:
            q = _quota_table(region, kind) if kind in ("lake", "stream") else {}
        except Exception as e:                      # a region with no base of that kind
            log(f"  no quota base for {region}·{kind}: {e}")
            q = {}
        try:
            g_table = method_provenance.base_table(region, kind) if kind in ("lake", "stream") else None
        except Exception as e:
            log(f"  no gear base for {region}·{kind}: {e}")
            g_table = None
        # EVERY OTHER STATE THIS REGION HAS. One entry per named area inside it, each a whole
        # table — its own year schedule included, because an area rule can carry dates of its
        # own (Region 1's summer closure runs Jul 15 – Aug 31 inside six management units).
        pl = []
        if kind in ("lake", "stream") and region not in ("province", "p") and q and g_table:
            base_state = {"rows": q["rows"], "present": q["present"], "views": q["views"]}
            g_base = g_table
            for area in ST.areas(region, kind):
                try:
                    st = _table(region, kind, area)
                except Exception as e:
                    log(f"  no state for {region}·{kind}·{area.name}: {e}")
                    continue
                entry = {
                    "name": area.name,
                    "why": [{"rule": rid(x), "verbatim": (x.get("verbatim") or "").strip(),
                             "type": x.get("type") or "", "who": source_of(x).who}
                            for x in area.rule_dicts],
                }
                # A STATE THAT IS NOT DIFFERENT IS NOT A STATE. Three of the five areas change
                # no line a reader reads — the park-reserve rule binds nothing, the Creston
                # permit is a duty and not a limit — and a place that shuts everything needs
                # its rules, not twelve rows saying "No fishing". Only a place that really
                # draws a different table carries one; the rest carry why they are listed.
                if _shut(st):
                    entry["shut"] = True
                elif _seen(st) == _seen(base_state):
                    entry["same"] = True
                else:
                    entry["quota"] = {"rows": [_slim(r) for r in st["rows"]],
                                      "present": st["present"], "views": st["views"]}
                g = method_provenance.base_table(region, kind, None, area.rule_dicts)
                if json.dumps(g, sort_keys=True) != json.dumps(g_base, sort_keys=True):
                    entry["gear"] = {"table": g}
                entry["delta"] = _delta({"rows": q["rows"], "gear": g_base},
                                        {"rows": st["rows"], "gear": g})
                pl.append(entry)
        out.append({
            "places": pl,
            "conditional": (conditional(region, kind)
                            if kind in ("lake", "stream") and region not in ("province", "p")
                            else []),
            "key": f"{region}:{kind}",
            "region": region,
            "kind": kind,
            "title": (q_panel or m_panel or {}).get("title") or f"{region} · {kind}",
            "quota": {"rows": q.get("rows") or [], "present": q.get("present") or {},
                      "views": q.get("views") or [], "print": q_panel},
            "gear": {"table": g_table, "print": m_panel},
            "always": (q.get("always") or []),
        })

    def tally(side):
        ok = sum(r[side]["print"]["ok"] for r in out if r[side]["print"])
        n = sum(r[side]["print"]["n"] for r in out if r[side]["print"])
        return {"ok": ok, "n": n}

    # A READER LOOKS FOR A FAMILY FIRST. "My fish is a char" should find one place on every
    # region's table, whether that region writes "Trout/char: 5" as one number (Region 2) or
    # writes "Trout: 4" and releases char separately (Region 1). The page groups by this.
    from pipeline.regs.parsing.catalogue import SPECIES_GROUPS, DEFINITIONAL_SIZE
    fams = {"TROUT_CHAR": sorted(SPECIES_GROUPS["TROUT_CHAR"]),
            "TROUT": sorted(SPECIES_GROUPS["TROUT"]),
            "CHAR": sorted(SPECIES_GROUPS["CHAR"])}
    return {"regions": out,
            "meta": {"quota": tally("quota"), "gear": tally("gear"), "families": fams,
                     "definitions": DEFINITIONAL_SIZE}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-o", "--out", help="write here (default: stdout)")
    a = ap.parse_args()
    d = collect()
    log(f"{len(d['regions'])} region tables · "
        f"quota {d['meta']['quota']['ok']}/{d['meta']['quota']['n']} · "
        f"gear {d['meta']['gear']['ok']}/{d['meta']['gear']['n']}")
    text = json.dumps(d, separators=(",", ":"))
    if a.out:
        open(a.out, "w").write(text)
        log(f"wrote {a.out} ({len(text)/1e6:.1f} MB)")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
