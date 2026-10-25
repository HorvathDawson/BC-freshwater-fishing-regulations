"""The phase-0 parity oracle: for every golden state, what the Python reference decoder says the app's tap must resolve.

    .venv/bin/python app/design/regs-v11/port/taps.py [--release data/release/r-…] [--golden app/fixtures/golden/golden.json]
    -> app/fixtures/golden/taps.json

Per state: the water's item id (page v36's export ids, by the snapshot's water name), the part index, the date (2026, the
prototype's year), the weekday, then the store's answer: item_ord, akey, the segment starts, the moments, the segment index
(pipeline.deliver.answers.encode.segment_index, inside=False), the cell's five frame ids, and the per-day display frame id
over the leap calendar (1..366) so the TS year strip can be checked frame by frame. Plus a sample of unnamed section handles
(handle -> akey, place, the unnamed name). Read-only; no LLM, no network. The store is the RELEASE's (P20: no regs.sqlite),
put back together by `pipeline.deliver.store.release.province_store` and held to its store_digest when the release
carries one (`store_meta`; older releases: `meta.store.store_digest` is null and `answers_sha256` pairs the oracles).

And `leap`: the Feb 29 cases (owner, P06/D31: Feb 29 READS Feb 28's day, 59, on its OWN real weekday), each tapped as
`encode.tap_key` does (`calendar.reading_day_of` + `calendar_spec.weekday_of`): the Zymoetz below Limonite Creek
(`zymoetz_river.r10`, "Fridays, Saturdays, and Sundays") on Fri 2036-02-29 beside its Thu 2036-02-28, and Tue
2028-02-29 beside Mon 2028-02-28.

PART IDENTITY. A golden state names a part by its index in the PROTOTYPE's water; a release that re-cuts a water's parts
(two parts merge when their rows become the same) moves or removes that index. Every state whose golden part label is
readable (its `sub` minus the kind and section count; not "closed all year", which the prototype merges) is held to the
store's part at that index: same label (a "…"-truncated golden label as a prefix) and same section count. A state that
fails is an ERROR (exit 1) unless it is declared below — never a silent `akey: null` nor a tap on whatever water now has
that index. Declared states are themselves checked, so an entry that no longer describes the store fails too:
- PART_MOVED: the golden part is the store's part `ix` now, unchanged (label and section count equal there).
- PART_GROWN: the golden part is at the same index with the same rows, but its extent took in merged sections (label
  and/or count differ). Tapped and compared (`grown` on the state); the parity excludes only the words of the extent
  (the header's part label and the subtitle).
- every water with a PART_MOVED / PART_GROWN / PART_GONE state is listed in taps.json `recut` (item -> reason): its
  list of parts is not the golden's, so the parity excludes the Parts sheet on its states.
- PART_RECOUNTED: the golden part is the store's part `ix` (the same index, or a renumbered one) with the same label
  and rows, but the atlas added or removed sections under it (T67: braid cuts, a ditch leaving its river, moved
  length cuts), so only its section COUNT differs. Tapped and compared whole (`recounted` on the state: the reason and
  the store's count); the parity holds the subtitle to the golden's with the count replaced.
- A "closed all year" golden part (no label to check) is held to the store's part at its index being closed all year
  (T67: a renumbering put an open part at a closed state's index); one that is not must be declared.
- PART_GONE: the golden part's water merged into the store's part `now_in` (or, T67, was split or recounted so
  the page's merged closed list is not the golden's); no store part has the golden label and count. Written to taps.json `gone` (not `states`), and the parity excludes the state with this reason
  (conformance/src/regs/exclusions.ts PART_GONE_IDS).
"""
import argparse, datetime, json, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.release import province_store  # noqa: E402
from pipeline.deliver.answers import encode as E           # noqa: E402
from pipeline.deliver.calendar import day_of, reading_day_of  # noqa: E402
from pipeline.common.calendar_spec import weekday_of        # noqa: E402

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_D0 = datetime.date(2026, 1, 1)
WD_OF_DAY = {day_of(((_D0 + datetime.timedelta(i)).month, (_D0 + datetime.timedelta(i)).day)): WEEKDAYS[(_D0 + datetime.timedelta(i)).weekday()] for i in range(365)}


# (T115, r-941dcc149b17) owner decision AGENTS 54 / tickets/T115-one-region-membership.md: a stream piece straddling a
# region line is its HOME region's only. The four Fraser pieces that carried two regions' rows (and so were parts of their
# own: golden x15p14/15/17/18) now carry their home region's rows and merge with the neighbouring part; the Fraser has 15
# parts (0..14), not 19. Measured old r-b518a2b507fa -> new by part_section: old 0..13 -> same index, 14 -> 4,
# 15 -> 6, 16 -> 14, 17 -> 1, 18 -> 0.
T183 = "T183 (owner 2026-10-24, tickets/T183): a straddling stream piece carries both regions' rules again (reverses T115)"
T115 = "T115 (owner decision AGENTS 54, tickets/T115-one-region-membership.md): a straddling stream piece is its home region's only"
T67 = "T67 (owner 2026-10-24, decisions.md \"braids accepted\"; RULINGS E17, AGENTS 12)"
_LEN = "the automatic length cuts moved off braid-spanned junctions (owner Q2)"
_GAUGE = "the old perpendicular sweep's gauge cuts are gone"
PART_MOVED = {"x15p16_1010": 15,
              # (T67, then T183) the Skeena's Ecological Reserve is two parts (PART_GONE x19p5: 5 below the Exchamsiks cut,
              # 6 the host channel above it), which renumbers the parts after them
              "x19p6_1010": 7, "x19p7_1010": 8}
              # (T187) x22p6 (the Peace's Dinosaur Lake part) is back to its golden place and size: part 6, 1 section
#: gone states whose water's list of parts is still the golden's (labels and order the Parts sheet draws): not `recut`
GONE_NOT_RECUT: set[str] = set()
#: the reason a water is `recut` (its list of parts is not the golden's), by the state that shows it
RECUT_WHY = {"x22p2_1010": "T67 (owner 2026-10-24): the Peace's braids are cut at Site C, the 1,200 m signs and the "
                           "Halfway, so its parts are renumbered (part order follows rule-set ids)",
             "x19p6_1010": "T67 (owner 2026-10-24) and T183 (owner 2026-10-24: Skeena section 2 starts at the Exchamsiks "
                           "cut): the Ecological Reserve's Exchamsiks host channel above the cut, 360216952:1017, keeps the "
                           "section-2 designation the rest of the Reserve lost, and is a part of its own (8 -> 9 parts)",
             # (T183) the straddling pieces are parts of their own again (T115's merge undone, two regions' rules)
             "x15p16_1010": f"{T183}: the Fraser's straddling pieces are parts of their own again (15 -> 17 parts; the "
                            f"golden has 19)",
             "x27p0_1010": f"{T183}: the West Road's straddling pieces are parts of their own (3 -> 5 parts)"}
PART_GROWN = {
    "x27p0_1010": f"{T183}: the West Road's pieces on the Region 5 / 6 / 7A lines are parts of their own; part 0 is 79 sections",
    "x27p2_1010": f"{T183}: the West Road's side channel above Charleson Creek (Region 5 and 7A) is part 3 now; part 2 is the mouth stretch",
    "x19p0_1010": f"{T67}: the Exchamsiks confluence cut runs straight across and {_LEN}; then T183 (owner 2026-10-24): "
                  f"the mainstem from the Ecological Reserve up to the Exchamsiks left section 2 and joined this part "
                  f"(3 stretches, 44 -> 48 sections)",
    "x19p1_1010": "T183 (owner 2026-10-24): Skeena section 2 starts at the Exchamsiks confluence, not the Ecological Reserve boundary (61 -> 59 sections; the extent changed, the rows did not)",
}
PART_GONE = {
    "x27p1_1010": (0, f"{T183}: the West Road's Region 6 stretch below Carnlick Creek shares its rules with part 0 now"),
    "x15p17_1010": (16, f"{T183}: the golden part (356364114:674465, Region 5 and Region 7A) shares its rules with 699253 and is in part 16 now"),
    "x15p18_1010": (16, f"{T183}: the golden part (356364114:699253, Region 5 and Region 7A) shares its rules with 674465 and is in part 16 now"),
    "x15p15_1010": (14, f"{T183}: the golden part (356364114:434930, Region 3 and Region 5) has the same rules as 418405 and is in part 14 now"),
    # not PART_GROWN: the rows differ too. The prototype's two-region page on 418405 drew Region 3's zone and r5's Fraser
    # rows r2-r4; T183 binds Region 5's zone rules (p.42) and r5 r1 / r6 there as well (tickets/T183 "so 14, not 12")
    "x15p14_1010": (14, f"{T183}: the golden part (356364114:418405, Region 3 and Region 5) is in part 14 with :434930, "
                        f"which also carries Region 5's zone rules and the Region 5 Fraser rows r1 and r6 the prototype's "
                        f"page did not draw, so its rows are not the golden's"),
    # (T67, r-0ed3c2b2a0a7) two closed states whose golden page counts sections the release re-cut: the app's merged
    # "Closed all year on N stretches (M sections)" list is the store's, the golden's is the prototype's
    "x19p5_1010": (5, "T67 (owner 2026-10-24): the Exchamsiks cut split the Skeena's Ecological Reserve part, and T183 "
                      "(owner 2026-10-24) started Skeena section 2 at the cut — below it, 6 sections (the side channel "
                      "piece 360216962:96 among them) in part 5; above it, the Exchamsiks host channel 360216952:1017, "
                      "still section 2, in part 6; both closed all year, so the page merges two closed parts where the "
                      "golden had one"),
    "x22p2_1010": (3, "T67 (owner 2026-10-24): the Peace's closed parts hold 7 sections, not 5 — the Site C closure took "
                      "the braid pieces 359013560:165 and (T187) 359005110:3454 — and the first is renumbered 2 -> 3 "
                      "(part order follows rule-set ids)"),
}
# (T67, r-0ed3c2b2a0a7: braids cut at build, ditch-only lines leave their river, length cuts skip braid-spanned
# junctions, the old perpendicular gauge sweep removed). Measured against r-bb4331929428 by section id (registry of
# atlas full.prev vs the bundle of atlas full): every part below keeps its label and rows; only its section count moved.
# {state: (store part_ix, why)} — the golden part is the store's part `ix`, same label, another section count
PART_RECOUNTED = {
    "x15p0_1010": (0, f"{T183}: a Fraser piece at the Region 5 / 7A line carries both regions' rules and joined part 16: 109 -> 108 sections"),
    "x9p1_1010": (1, f"{T67}: the Kootenay's ditch-only line 356365735:0 (1,982.6 m) left the river item (owner Q1, "
                     f"DITCHES_LEAVE_RIVER_ITEMS); 3 length cuts on 356570348 moved: 52 -> 51 sections"),
    "x13p3_1010": (3, f"{T67}: the Dean's side channel 360227736 is cut at the 0.5 km sign (421 m): 2 -> 3 sections"),
    "x15p2_1010": (2, f"{T67}: on the Fraser mainstem {_GAUGE} (94,834 / 108,135 / 111,137 / 122,096 / 132,550 / "
                      f"148,097 m) and {_LEN} (93,822 / 116,370 / 135,999 m): 40 -> 36 sections"),
    "x15p3_1010": (3, f"{T67}: {_GAUGE} on the Fraser's side channels 355992485 and 355994872, and 08MH028's cut "
                      f"moved 578 -> 545 m: 30 -> 29 sections"),
    "x18p0_1010": (0, f"{T67}: on the Columbia (356570372) {_LEN}: 4 cuts -> 5, 132 -> 133 sections"),
    "x19p3_1010": (3, f"{T67}: the Cedarvale cut takes the braid 360243437 (1,069 m): 51 -> 52 sections"),
    "x20p2_1010": (2, f"{T67}: the Thompson's side channel 355995300 is cut at the boundary signs (187 m): 7 -> 8 sections"),
    "x22p0_1010": (0, f"{T67}: the Peace's braids are cut at Site C, the 1,200 m signs and the Halfway (+5 km), "
                      f"07FD002's cut moved 959 -> 836 m, and {_LEN} on 359572348: 54 -> 56 sections"),
    "x22p1_1010": (1, f"{T67}: the Peace braids 359005110 / 359013560 cut at Site C dam, and {_LEN}: 29 -> 30 sections"),
    "x22p3_1010": (4, f"{T67}: the Halfway braid 359012853 is cut at 3,358 m: 6 -> 7 sections; renumbered 3 -> 4 "
                      f"(part order follows rule-set ids)"),
}
_SUB = re.compile(r"^(.*?) · (\d+) sections?(?: · (.*))?$")


def golden_part(st):
    """(label or None, section count) of the golden state's part, from its `sub`."""
    m = _SUB.match(st["sub"])
    if not m:
        raise SystemExit(f"taps: golden sub {st['sub']!r} is not 'kind · N sections[ · label]'")
    return m.group(3), int(m.group(2))


def same_part(golden, store):
    """Is the store's part (label, sections) the golden part? None when the golden label cannot tell (no label, or the
    prototype's merged 'closed all year')."""
    lab, n = golden
    if lab is None or lab == "closed all year":
        return None
    slab, sn = store
    return sn == n and (slab is not None and (slab.startswith(lab[:-1]) if lab.endswith("…") else slab == lab))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--release", default=None, help="the release folder whose regs store to read (default: data/release/LATEST)")
    ap.add_argument("--golden", default=str(REPO / "app/fixtures/golden/golden.json"))
    ap.add_argument("--page", default=str(REPO / "app/design/regs-v11/reference/page_v36.html"))
    ap.add_argument("--out", default=str(REPO / "app/fixtures/golden/taps.json"))
    ap.add_argument("--unnamed", type=int, default=20)
    a = ap.parse_args()

    html = Path(a.page).read_text()
    D = json.loads(re.search(r'<script[^>]*id="data"[^>]*>([\s\S]*?)</script>', html).group(1))
    by_name = {w["name"]: w["id"] for w in D["waters"]}
    G = json.loads(Path(a.golden).read_text())
    s = province_store(Path(a.release) if a.release else None)   # the release IS the store (P20: no regs.sqlite)
    c = s.con
    meta = dict(c.execute("select k, v from meta").fetchall())
    M = s.static("answers.moments")
    ord_of = {}
    for o, sid in c.execute("select item_ord, item_id from item"):
        ord_of[s.s(sid)] = o

    def store_parts(item):
        return {ix: (s.s(l) if l is not None else None, n) for ix, l, n in
                c.execute("select part_ix, label_s, sections from part where item_ord=?", (ord_of[item],))}

    def resolve(sid, st, item, part_ix):
        """-> (part_ix to tap, None) or (None, gone record); raises SystemExit on an undeclared mismatch."""
        if item not in ord_of:
            return part_ix, None
        parts, g = store_parts(item), golden_part(st)
        closed = dict(c.execute("select part_ix, closed_all_year from part where item_ord=?", (ord_of[item],)))
        if sid in PART_MOVED:
            ix = PART_MOVED[sid]
            if same_part(g, parts.get(ix, (None, -1))) is not True or same_part(g, parts.get(part_ix, (None, -1))):
                raise SystemExit(f"taps: PART_MOVED {sid} -> {ix} does not describe the store: golden {g}, store {parts.get(part_ix)} at {part_ix}, {parts.get(ix)} at {ix}")
            return ix, None
        if sid in PART_GONE:
            now_in, why = PART_GONE[sid]
            hit = [ix for ix, p in parts.items() if same_part(g, p)]
            if hit or now_in not in parts:
                raise SystemExit(f"taps: PART_GONE {sid} does not describe the store: golden {g} is store part {hit}, now_in {now_in} in {sorted(parts)}")
            return None, {"item": item, "partIx": part_ix, "nowIn": now_in, "reason": why}
        if sid in PART_RECOUNTED:
            ix, _ = PART_RECOUNTED[sid]
            lab, n = g
            got = parts.get(ix, (None, -1))
            if lab is None or got[1] == n or same_part((lab, got[1]), got) is not True \
                    or (ix != part_ix and same_part(g, parts.get(part_ix, (None, -1)))):
                raise SystemExit(f"taps: PART_RECOUNTED {sid} -> {ix} does not describe the store: golden {g}, store {got} at {ix}")
            return ix, None
        if part_ix not in parts:
            raise SystemExit(f"taps: {sid} names part {part_ix} of {item}, the store has parts {min(parts)}..{max(parts)}: "
                             "the release re-cut the water — declare the state in PART_MOVED / PART_GROWN / PART_GONE")
        ok = same_part(g, parts[part_ix])
        if sid in PART_GROWN:
            if ok is not False:
                raise SystemExit(f"taps: PART_GROWN {sid} does not describe the store: part {part_ix} is the golden part {g} unchanged")
            return part_ix, None
        if ok is False:
            raise SystemExit(f"taps: {sid}'s golden part {g} is not the store's part {part_ix} {parts[part_ix]}: "
                             "the release re-cut the water — declare the state in PART_MOVED / PART_GROWN / PART_GONE")
        if g[0] == "closed all year" and closed.get(part_ix) != 1:
            # the label cannot tell, the closure can: a renumbering put an open part at a closed state's index (T67)
            raise SystemExit(f"taps: {sid}'s golden part is closed all year, the store's part {part_ix} {parts[part_ix]} is "
                             "not: the release renumbered the water — declare the state in PART_GONE")
        return part_ix, None

    def tap(item, part_ix, month, day, weekday):
        o = ord_of.get(item)
        if o is None:
            return {"error": "no item"}
        row = c.execute("select akey from part where item_ord=? and part_ix=?", (o, part_ix)).fetchone()
        akey = row[0] if row else None
        if akey is None:
            raise SystemExit(f"taps: part {part_ix} of {item} has no answer key")
        seg, mom, nseg = c.execute("select seg, mom, nseg from akey where akey=?", (akey,)).fetchone()
        starts = json.loads(c.execute("select starts from seglist where seg=?", (seg,)).fetchone()[0])
        moms = None
        if mom is not None:
            moms = [M[i] for i in json.loads(c.execute("select ix from segmom where mom=?", (mom,)).fetchone()[0])]
        si = E.segment_index(starts, day_of((month, day)), moms, weekday, False)
        cell = c.execute("select ladder, rows, gear, licence, display from cell where akey=? and s=?", (akey, si)).fetchone()
        # the display frame of every day of the leap calendar, each at ITS OWN weekday in 2026 (page v36 segOf(w, md) =
        # segAt(w, md, weekdayOf(md), false), the snapshot's runsOf); day 60 (Feb 29, absent in 2026) at its first moment
        days = []
        for d in range(1, 367):
            if moms is None:
                sd = E.segment_index(starts, d, None, None, False)
            elif d == 60:
                sd = _first_moment(starts, d)
            else:
                sd = E.segment_index(starts, d, moms, WD_OF_DAY[d], False)
            days.append(c.execute("select display from cell where akey=? and s=?", (akey, sd)).fetchone()[0])
        return {"item_ord": o, "akey": akey, "seg": seg, "mom": mom, "nseg": nseg, "starts": starts, "moments": moms,
                "s": si, "cell": dict(zip(("ladder", "rows", "gear", "licence", "display"), cell)), "displayByDay": days}

    def leap(cid, item, part_ix, year, month, day):
        wd = weekday_of(year, month, day)
        akey = c.execute("select akey from part where item_ord=? and part_ix=?", (ord_of[item], part_ix)).fetchone()[0]
        seg, mom = c.execute("select seg, mom from akey where akey=?", (akey,)).fetchone()
        starts = json.loads(c.execute("select starts from seglist where seg=?", (seg,)).fetchone()[0])
        moms = None if mom is None else [M[i] for i in json.loads(c.execute("select ix from segmom where mom=?", (mom,)).fetchone()[0])]
        rd = reading_day_of((month, day))
        si = E.segment_index(starts, rd, moms, wd, False)
        cell = c.execute("select ladder, rows, gear, licence, display from cell where akey=? and s=?", (akey, si)).fetchone()
        return {"id": cid, "item": item, "partIx": part_ix, "date": f"{year}-{month:02d}-{day:02d}", "weekday": wd,
                "readingDay": rd, "akey": akey, "starts": starts, "moments": moms, "s": si,
                "cell": dict(zip(("ladder", "rows", "gear", "licence", "display"), cell))}

    out = {"meta": {"generator": "app/design/regs-v11/port/taps.py", "store": {k: meta.get(k) for k in ("format", "section_handles", "reach_digest", "rule_ids_sha256", "answers_sha256", "store_digest", "layout")},
                    "golden": G["meta"].get("sha"), "version": G["meta"].get("version"), "year": 2026}, "states": {}, "unnamed": [],
           "leap": [leap("zymoetz_20360229", "gnis:3137", 0, 2036, 2, 29), leap("zymoetz_20360228", "gnis:3137", 0, 2036, 2, 28),
                    leap("zymoetz_20280229", "gnis:3137", 0, 2028, 2, 29), leap("zymoetz_20280228", "gnis:3137", 0, 2028, 2, 28)],
           "gone": {}, "recut": {}}
    for sid, st in G["states"].items():
        item = by_name.get(st["water"])
        part_ix = int(st["part"]) if st.get("part") not in (None, "") else 0
        md = st["md"]; month, day = md // 100, md % 100
        wd = WEEKDAYS[datetime.date(2026, month, day).weekday()]
        ix, gone = resolve(sid, st, item, part_ix) if item else (part_ix, None)
        if gone is not None:
            out["gone"][sid] = {**gone, "md": md, "date": f"2026-{month:02d}-{day:02d}", "weekday": wd}
            continue
        out["states"][sid] = {"item": item, "partIx": ix, "md": md, "date": f"2026-{month:02d}-{day:02d}", "weekday": wd,
                              **({"goldenPartIx": part_ix} if ix != part_ix else {}),
                              **({"grown": PART_GROWN[sid]} if sid in PART_GROWN else {}),
                              **({"recounted": {"reason": PART_RECOUNTED[sid][1], "sections": store_parts(item)[ix][1]}}
                                 if sid in PART_RECOUNTED else {}),
                              **(tap(item, ix, month, day, wd) if item else {"error": "no water"})}
    # the waters whose parts the release re-cut: every state of one sees a different list of parts than the golden's
    for sid in (*PART_MOVED, *PART_GROWN, *PART_GONE):
        if sid in GONE_NOT_RECUT:
            continue
        w = by_name[G["states"][sid]["water"]]
        out["recut"][w] = RECUT_WHY.get(sid) or out["recut"].get(w) or T115
    stale = sorted((set(PART_MOVED) | set(PART_GROWN) | set(PART_GONE) | set(PART_RECOUNTED)) - set(G["states"]))
    if stale:
        raise SystemExit(f"taps: declared states not in the golden: {stale}")
    # unnamed: an evenly spaced sample of handles whose akey is past the named range, half streams and half the rest
    # (wetlands outnumber streams at the top of the handle range; T183's sample held 3 streams, none on its region's base)
    akeys = s.section_akeys()
    first = s.static("answers.unnamed")["first"]
    hs = sorted(h for h, k in akeys.items() if k and k - 1 >= first)
    pool = hs[::max(1, len(hs) // max(1, a.unnamed * 20))]
    named = [(h, s.unnamed_name(h)) for h in pool]
    streams = [p for p in named if p[1].startswith("Unnamed stream")]
    rest = [p for p in named if not p[1].startswith("Unnamed stream")]
    half = a.unnamed // 2
    pick = [g[::max(1, len(g) // max(1, n))][:n] for g, n in ((streams, half), (rest, a.unnamed - half))]
    for h, name in sorted(pick[0] + pick[1]):
        out["unnamed"].append({"handle": h, "akey": s.akey_of(h), "place": s.place_of(h), "name": name})
    Path(a.out).write_text(json.dumps(out, indent=1))
    bad = [k for k, v in out["states"].items() if "error" in v]
    print(f"taps: {len(out['states'])} states ({len(bad)} unresolved: {bad[:5]}), {len(out['unnamed'])} unnamed, {len(out['leap'])} leap -> {a.out}")
    for sid, v in out["states"].items():
        if "goldenPartIx" in v:
            how = "PART_MOVED" if sid in PART_MOVED else "PART_RECOUNTED"
            print(f"taps: {sid} golden part {v['goldenPartIx']} -> store part {v['partIx']} ({how})")
    for sid in PART_GROWN:
        print(f"taps: {sid} part {out['states'][sid]['partIx']} grew (PART_GROWN): {PART_GROWN[sid]}")
    for sid in PART_RECOUNTED:
        print(f"taps: {sid} part {out['states'][sid]['partIx']} recounted (PART_RECOUNTED): {PART_RECOUNTED[sid][1]}")
    for sid, g in out["gone"].items():
        print(f"taps: {sid} GONE (golden part {g['partIx']}, now in part {g['nowIn']}): {g['reason']}")
    if bad:
        raise SystemExit(f"taps: {len(bad)} states unresolved: {bad}")


def _first_moment(starts, d):
    import bisect
    i = bisect.bisect_right(starts, d) - 1
    return starts.index(starts[i])


if __name__ == "__main__":
    main()
