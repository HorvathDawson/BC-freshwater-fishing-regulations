"""Read-only: the shipped panels (the live bundle) against each variant from build_variants.py, under one feed.

For every section and quantity (Flow = discharge, Level = level, horizon 0) the estimate is a port of the app:
`contributionsOf` (app/packages/data/src/panel.ts — every member REPORTING the quantity) into `estimate`
(app/packages/core/src/trust.ts), with the pipeline's own mirrors of the shared arithmetic (`weight_of`, `error_for`,
`leader_of`, `lead_shares`). Prints, per variant: sections whose panel changes; the outcome bins (as in
handoff/PLAN/app-data/own-gauge/REPORT.md), by sections and km; robustness (answers that survive their heaviest
reporting member going silent); the paint file's gzip size against the gate; and the stations ECCC lists today
against the roster.

    .venv/bin/python handoff/PLAN/app-data/live-panels/scripts/evaluate.py VARIANT_DIR [STATIONLIST.csv] [INDEX.json]
"""
import collections
import gzip
import json
import math
import pickle
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, ".")
from pipeline.atlas.gauges.panel import CORE_MEMBERS, Contribution, answering, weight_of  # noqa: E402
from pipeline.deliver.release import build as RB  # noqa: E402
from pipeline.deliver.release import paint as PAINT  # noqa: E402
from pipeline.gauges.feed.publish import listed_stations, roster_drift  # noqa: E402

MAIN = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
BUNDLE = MAIN / "data/generated/bundle/bundle.sqlite"
STATUS = MAIN / "data/generated/bundle/status_index.bin"
INDEX = MAIN / "data/generated/gauges/feeds/index.json"
ROSTER = MAIN / "data/source/bc_hydrometric_stations.json"
LIVE_PAINT_GZ = 933_549          # r-81d40ad73176 manifest, files.paint.gz


def reading(row, q):
    if not row:
        return None
    if row.get(q) is not None:
        return row[q]
    return row.get("percentile") if (row.get("parameter") or "discharge") == q else None


# THE APP'S ANSWER, imported — `answering` in pipeline/atlas/gauges/panel.py, which app/tools/one-formula.test.ts
# runs beside `answering` in app/packages/core/src/trust.ts (T125 item 7). This script used to port it by hand.
def estimate(members, target, idx, q, drop=None):
    """`(percentile 0-100 | None, why, [(station, weight)])` — the app's answer at horizon 0 (`answering`): members
    past `CORE_MEMBERS` (panel order) are reserve that stands back, lowest weight first, rather than leave the answer
    too uncertain while a core member reports."""
    cs, sts = [], []
    for rank, (st, role, area, years, _reg, same, lead) in enumerate(members):
        p = reading(idx.get(st), q)
        if st == drop or p is None:
            continue
        cs.append(Contribution(p, role, area, years, bool(same), lead, rank >= CORE_MEMBERS))
        sts.append(st)
    got, _stood = answering(target, cs)
    if not got["ok"]:
        return None, got["why"], []
    return got["value"]["percentile"] * 100, "ok", [
        (sts[i], weight_of(min(target, cs[i].area_km2) / max(target, cs[i].area_km2), cs[i].role, cs[i].years,
                           cs[i].same_river)) for i in sorted(got["value"]["shares"])]


ORDER = ["unchanged", "moves under 5 points", "moves 5 to 10 points", "moves 10 to 25 points",
         "moves 25 points or more", "gains an answer", "loses its answer", "silent before and after"]


def binned(a, b):
    if a is None and b is None:
        return "silent before and after"
    if a is None:
        return "gains an answer"
    if b is None:
        return "loses its answer"
    d = abs(a - b)
    return ("moves 25 points or more" if d >= 25 else "moves 10 to 25 points" if d >= 10 else
            "moves 5 to 10 points" if d >= 5 else "moves under 5 points" if d > 0.05 else "unchanged")


def baseline():
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    by_sid = {sid: (pid, a) for sid, pid, a in db.execute("SELECT sid, panel_id, area_km2 FROM section_panel")}
    members = collections.defaultdict(list)
    for pid, st, role, a, y, reg, same, lead in db.execute(
            "SELECT panel_id, station, role, area_km2, years, regulated, same_river, lead FROM panel_member "
            "ORDER BY panel_id, ord"):
        members[pid].append((st, role, a, y, reg, same, lead))
    return {"by_sid": by_sid, "members": {k: tuple(v) for k, v in members.items()}}


def paint_gz(variant):
    """The paint file this variant's panels would ship as, gzip -9 as the release writes it."""
    src = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    mem = sqlite3.connect(":memory:")
    mem.execute("CREATE TABLE gauge (station TEXT PRIMARY KEY)")
    mem.executemany("INSERT INTO gauge VALUES (?)", src.execute("SELECT station FROM gauge"))
    mem.execute("CREATE TABLE section_panel (sid INTEGER PRIMARY KEY, panel_id INTEGER, area_km2 REAL)")
    mem.executemany("INSERT INTO section_panel VALUES (?,?,?)",
                    ((sid, pid, a) for sid, (pid, a) in variant["by_sid"].items()))
    mem.execute("CREATE TABLE panel_member (panel_id, ord, station, role, area_km2, years, regulated, same_river, "
                "lead)")
    mem.executemany("INSERT INTO panel_member VALUES (?,?,?,?,?,?,?,?,?)",
                    ((pid, i, *m) for pid, ms in variant["members"].items() for i, m in enumerate(ms)))
    mem.execute("CREATE TABLE basin_member (basin_id, station, area_km2, years)")
    mem.executemany("INSERT INTO basin_member VALUES (?,?,?,?)",
                    src.execute("SELECT basin_id, station, area_km2, years FROM basin_member"))
    raw = PAINT.encode(STATUS.read_bytes(), mem)
    return len(raw), len(gzip.compress(raw, compresslevel=9, mtime=0))


def members_of(v, sid):
    got = v["by_sid"].get(sid)
    return v["members"][got[0]] if got else ()


def main(vdir, stationlist=None, index=INDEX):
    vdir = Path(vdir)
    idx = json.loads(Path(index).read_text())["stations"]
    lengths = pickle.loads((vdir / "lengths.pkl").read_bytes())
    base = baseline()
    roster = json.loads(ROSTER.read_text())
    listed = listed_stations(Path(stationlist).read_text()) if stationlist else None
    drift = roster_drift(roster, listed) if listed is not None else None
    reporting = {s for s, r in idx.items() if reading(r, "discharge") is not None or reading(r, "level") is not None}
    print(f"feed {json.loads(Path(index).read_text()).get('fetchedAt')}: {len(reporting)} stations reporting")
    if drift:
        print(f"ECCC list today vs roster: joined {drift['joined']}; left {drift['left']}")
    braw, bgz = paint_gz(base)
    print(f"paint, shipped panels re-encoded: {braw:,} B raw, {bgz:,} B gz (live release {LIVE_PAINT_GZ:,}; "
          f"gate {RB.SIZE_GATES['paint']:,})")
    for path in sorted(vdir.glob("panels_K*_L*.pkl")):
        v = pickle.loads(path.read_bytes())
        print(f"\n===== {path.stem}")
        nm = sum(len(ms) for ms in v["members"].values())
        sizes = collections.Counter(len(members_of(v, s)) for s in v["by_sid"])
        print(f"panels {len(base['members']):,} -> {len(v['members']):,}; member rows "
              f"{sum(len(m) for m in base['members'].values()):,} -> {nm:,}; sections with a panel "
              f"{len(base['by_sid']):,} -> {len(v['by_sid']):,}")
        print("members per section: " + ", ".join(f"{k}: {sizes[k]:,}" for k in sorted(sizes)))
        raw, gz = paint_gz(v)
        print(f"paint {raw:,} B raw, {gz:,} B gz: {gz - bgz:+,} B against the shipped panels; "
              f"{RB.SIZE_GATES['paint'] - gz:+,} B under the paint gate")
        sids = set(base["by_sid"]) | set(v["by_sid"])
        changed = [s for s in sids if {(m[0], m[6]) for m in members_of(base, s)}
                   != {(m[0], m[6]) for m in members_of(v, s)}]
        print(f"sections whose panel changes (station or lead): {len(changed):,} "
              f"({sum(lengths.get(s, 0) for s in changed) / 1000:,.0f} km)")
        for q, label in (("discharge", "Flow"), ("level", "Level")):
            c, km = collections.Counter(), collections.Counter()
            robust_b = robust_v = ans_b = ans_v = 0
            for s in sids:
                mb, mv = members_of(base, s), members_of(v, s)
                tb = base["by_sid"].get(s, (None, None))[1]
                tv = v["by_sid"].get(s, (None, None))[1]
                a, _w, wb = estimate(mb, tb, idx, q)
                b, _w2, wv = estimate(mv, tv, idx, q)
                k = binned(a, b)
                c[k] += 1
                km[k] += lengths.get(s, 0) / 1000
                # ROBUSTNESS: does the answer survive its heaviest reporting member going silent?
                if a is not None:
                    ans_b += 1
                    robust_b += estimate(mb, tb, idx, q, drop=max(wb, key=lambda x: x[1])[0])[0] is not None
                if b is not None:
                    ans_v += 1
                    robust_v += estimate(mv, tv, idx, q, drop=max(wv, key=lambda x: x[1])[0])[0] is not None
            print(f"-- {label}: answers {ans_b:,} -> {ans_v:,}; survive losing their heaviest reporting member "
                  f"{robust_b:,} ({100 * robust_b / max(ans_b, 1):.1f}%) -> {robust_v:,} "
                  f"({100 * robust_v / max(ans_v, 1):.1f}%)")
            for k in ORDER:
                if c[k]:
                    print(f"  | {k} | {c[k]:,} | {km[k]:,.0f} |")
        for st in ("08EG012", "08LG028"):
            led_b = [s for s in base["by_sid"] if any(m[0] == st and m[6] for m in members_of(base, s))]
            led_v = [s for s in v["by_sid"] if any(m[0] == st and m[6] for m in members_of(v, s))]
            print(f"{st} (roster live, not transmitting): lead candidate on {len(led_b):,} -> {len(led_v):,} sections")
        if drift:
            for st in drift["joined"]:
                nb = sum(1 for s in base["by_sid"] if any(m[0] == st for m in members_of(base, s)))
                nv = sum(1 for s in v["by_sid"] if any(m[0] == st for m in members_of(v, s)))
                print(f"joined {st}: a member of {nb:,} -> {nv:,} sections' panels")


if __name__ == "__main__":
    main(*sys.argv[1:])
