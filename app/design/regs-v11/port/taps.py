"""The phase-0 parity oracle: for every golden state, what the Python reference decoder says the app's tap must resolve.

    .venv/bin/python app/design/regs-v11/port/taps.py [--store data/generated/bundle/regs.sqlite] [--golden app/design/regs-v11/port/golden/golden.json]
    -> app/design/regs-v11/port/golden/taps.json

Per state: the water's item id (page v36's export ids, by the snapshot's water name), the part index, the date (2026, the
prototype's year), the weekday, then the store's answer: item_ord, akey, the segment starts, the moments, the segment index
(pipeline.deliver.answers.encode.segment_index, inside=False), the cell's five frame ids, and the per-day display frame id
over the leap calendar (1..366) so the TS year strip can be checked frame by frame. Plus a sample of unnamed section handles
(handle -> akey, place, the unnamed name). Read-only; no LLM, no network. The store's full digest is not verified (fast).

And `leap`: the Feb 29 cases (owner, P06/D31: Feb 29 READS Feb 28's day, 59, on its OWN real weekday), each tapped as
`encode.tap_key` does (`calendar.reading_day_of` + `calendar_spec.weekday_of`): the Zymoetz below Limonite Creek
(`zymoetz_river.r10`, "Fridays, Saturdays, and Sundays") on Fri 2036-02-29 beside its Thu 2036-02-28, and Tue
2028-02-29 beside Mon 2028-02-28.
"""
import argparse, datetime, json, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.decode import Store            # noqa: E402
from pipeline.deliver.answers import encode as E           # noqa: E402
from pipeline.deliver.calendar import day_of, reading_day_of  # noqa: E402
from pipeline.common.calendar_spec import weekday_of        # noqa: E402

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_D0 = datetime.date(2026, 1, 1)
WD_OF_DAY = {day_of(((_D0 + datetime.timedelta(i)).month, (_D0 + datetime.timedelta(i)).day)): WEEKDAYS[(_D0 + datetime.timedelta(i)).weekday()] for i in range(365)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", default=str(REPO / "data/generated/bundle/regs.sqlite"))
    ap.add_argument("--golden", default=str(REPO / "app/design/regs-v11/port/golden/golden.json"))
    ap.add_argument("--page", default=str(REPO / "pipeline/deliver/answers/reference/page_v36.html"))
    ap.add_argument("--out", default=str(REPO / "app/design/regs-v11/port/golden/taps.json"))
    ap.add_argument("--unnamed", type=int, default=20)
    a = ap.parse_args()

    html = Path(a.page).read_text()
    D = json.loads(re.search(r'<script[^>]*id="data"[^>]*>([\s\S]*?)</script>', html).group(1))
    by_name = {w["name"]: w["id"] for w in D["waters"]}
    G = json.loads(Path(a.golden).read_text())
    s = Store(a.store, verify=False)
    c = s.con
    meta = dict(c.execute("select k, v from meta").fetchall())
    M = s.static("answers.moments")
    ord_of = {}
    for o, sid in c.execute("select item_ord, item_id from item"):
        ord_of[s.s(sid)] = o

    def tap(item, part_ix, month, day, weekday):
        o = ord_of.get(item)
        if o is None:
            return {"error": "no item"}
        row = c.execute("select akey from part where item_ord=? and part_ix=?", (o, part_ix)).fetchone()
        akey = row[0] if row else None
        if akey is None:
            return {"item_ord": o, "akey": None}
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

    out = {"meta": {"generator": "app/design/regs-v11/port/taps.py", "store": {k: meta.get(k) for k in ("format", "section_handles", "reach_digest", "rule_ids_sha256", "store_digest", "layout")},
                    "golden": G["meta"].get("sha"), "version": G["meta"].get("version"), "year": 2026}, "states": {}, "unnamed": [],
           "leap": [leap("zymoetz_20360229", "gnis:3137", 0, 2036, 2, 29), leap("zymoetz_20360228", "gnis:3137", 0, 2036, 2, 28),
                    leap("zymoetz_20280229", "gnis:3137", 0, 2028, 2, 29), leap("zymoetz_20280228", "gnis:3137", 0, 2028, 2, 28)]}
    for sid, st in G["states"].items():
        item = by_name.get(st["water"])
        part_ix = int(st["part"]) if st.get("part") not in (None, "") else 0
        md = st["md"]; month, day = md // 100, md % 100
        wd = WEEKDAYS[datetime.date(2026, month, day).weekday()]
        out["states"][sid] = {"item": item, "partIx": part_ix, "md": md, "date": f"2026-{month:02d}-{day:02d}", "weekday": wd,
                              **(tap(item, part_ix, month, day, wd) if item else {"error": "no water"})}
    # unnamed: an evenly spaced sample of handles whose akey is past the named range
    akeys = s.section_akeys()
    first = s.static("answers.unnamed")["first"]
    hs = sorted(h for h, k in akeys.items() if k and k - 1 >= first)
    step = max(1, len(hs) // max(1, a.unnamed))
    for h in hs[::step][: a.unnamed]:
        out["unnamed"].append({"handle": h, "akey": s.akey_of(h), "place": s.place_of(h), "name": s.unnamed_name(h)})
    Path(a.out).write_text(json.dumps(out, indent=1))
    bad = [k for k, v in out["states"].items() if "error" in v]
    print(f"taps: {len(out['states'])} states ({len(bad)} unresolved: {bad[:5]}), {len(out['unnamed'])} unnamed, {len(out['leap'])} leap -> {a.out}")


def _first_moment(starts, d):
    import bisect
    i = bisect.bisect_right(starts, d) - 1
    return starts.index(starts[i])


if __name__ == "__main__":
    main()
