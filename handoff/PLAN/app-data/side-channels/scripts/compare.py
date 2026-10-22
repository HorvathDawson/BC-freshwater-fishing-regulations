"""Read-only: prove prototype.py's baseline IS the live bundle, then measure each variant against it on TODAY'S feed.

The estimate is a line-for-line port of `estimate()` in app/packages/core/src/trust.ts (discharge, horizon 0, the
`reading()` rule from app/packages/data/src/panel.ts), so "today" means the feed index on disk at run time.

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/compare.py SCRATCH_DIR GRAPH.pkl
"""
import collections
import json
import math
import pickle
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, ".")
from pipeline.atlas.gauges.panel import ERROR_BY_RATIO, MAX_USEFUL_SPREAD, MIN_TOTAL_WEIGHT, weight_of  # noqa: E402

MAIN = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
BUNDLE = MAIN / "data/generated/bundle/bundle.sqlite"
INDEX = MAIN / "data/generated/gauges/feeds/index.json"


def phi(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def probit(p):
    q = min(1 - 1e-9, max(1e-9, p))
    lo, hi = -9.0, 9.0
    for _ in range(80):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if phi(mid) < q else (lo, mid)
    return (lo + hi) / 2


def plus_minus(ratio):
    for limit, _n, err in ERROR_BY_RATIO:
        if ratio <= limit:
            return err
    return ERROR_BY_RATIO[-1][2]


def reading(row):
    if not row:
        return None
    if row.get("discharge") is not None:
        return row["discharge"]
    return row.get("percentile") if (row.get("parameter") or "discharge") == "discharge" else None


def estimate(members, target, idx):
    """-> (percentile 0..100 | None, why, [(station, share of answer)])."""
    if not members:
        return None, "no-station", []
    if not target or target <= 0:
        return None, "no-station", []
    votes = []
    for st, role, area, years, _reg, same in members:
        p = reading(idx.get(st))
        if p is None or not 0 < p < 1 or not area or area <= 0:
            continue
        ratio = max(target, area) / min(target, area)
        w = weight_of(min(target, area) / max(target, area), role, years, bool(same))
        if w > 0:
            votes.append((st, probit(p), w, ratio, bool(same)))
    if not votes:
        return None, "no-record", []
    tot = sum(v[2] for v in votes)
    if tot < MIN_TOTAL_WEIGHT:
        return None, "too-uncertain", []
    mean = sum(v[1] * v[2] for v in votes) / tot
    sd = math.sqrt(sum(v[2] * (v[1] - mean) ** 2 for v in votes) / tot)
    spread = (phi(mean + sd) - phi(mean - sd)) * 100
    pm = sum(plus_minus(v[3]) * v[2] for v in votes) / tot
    pc = phi(mean) * 100
    half = max(pm, spread / 2)
    lo, hi = max(0, pc - half), min(100, pc + half)
    if not any(v[4] for v in votes) and hi - lo > MAX_USEFUL_SPREAD:
        return None, "too-uncertain", []
    return pc, "ok", [(v[0], v[2] / tot) for v in votes]


def load_bundle():
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    members = collections.defaultdict(list)
    for pid, _o, st, role, area, yrs, reg, same in db.execute(
            "SELECT panel_id, ord, station, role, area_km2, years, regulated, same_river FROM panel_member "
            "ORDER BY panel_id, ord"):
        members[pid].append((st, role, round(area, 3), yrs, reg, same))
    live = {sid: (tuple(members[pid]), None if a is None else round(a, 3))
            for sid, pid, a in db.execute("SELECT sid, panel_id, area_km2 FROM section_panel")}
    names = {}
    for sid, name in db.execute("SELECT s.sid, i.name FROM item_section s JOIN item i USING(ord) "
                                "WHERE i.kind='stream'"):
        names.setdefault(sid, name)
    gname = dict(db.execute("SELECT station, name FROM gauge"))
    return live, names, gname


def main(scratch, graph_pkl):
    scratch = Path(scratch)
    live, names, _gname = load_bundle()
    idx = json.loads(INDEX.read_text())["stations"]
    base = pickle.loads((scratch / "panels_baseline.pkl").read_bytes())
    # A panel is a SET: the bundle interns it in the order its first section met the donors, so compare sets.
    canon = lambda e: None if e is None else (frozenset(e[0]), e[1])  # noqa: E731
    same = sum(1 for s, v in live.items() if canon(base.get(s)) == canon(v))
    print(f"baseline vs live bundle: {len(live):,} live sections, {len(base):,} rebuilt, "
          f"{same:,} identical (member set + area), {len(live) - same:,} differ")
    with open(graph_pkl, "rb") as fh:
        g = pickle.load(fh)
    length = {v["sid"]: v["length"] for v in g["nodes"].values()}
    side = {v["sid"] for v in g["nodes"].values() if v["side"] and v["kind"] == "stream"}
    jumped = {g["nodes"][c]["sid"] for c, p in g["parent"].items() if g["nodes"][c]["wsc"] != g["nodes"][p]["wsc"]}
    print(f"side-channel stream sections: {len(side):,}; adopted a reach on ANOTHER watershed code: {len(jumped):,}")

    cache = {}

    def est(entry):
        if entry is None:
            return None, "no-panel", []
        if entry not in cache:
            cache[entry] = estimate(entry[0], entry[1], idx)
        return cache[entry]

    rows = {}
    loaded = {"baseline": base}
    for variant in ("same_wsc", "same_wsc_ns", "nested"):
        f = scratch / f"panels_{variant}.pkl"
        if f.exists():
            loaded[variant] = pickle.loads(f.read_bytes())
    # `nested` is measured against `same_wsc` too, so its own effect is not confused with the side-channel fix's
    for variant, against in (("same_wsc", "baseline"), ("same_wsc_ns", "baseline"), ("nested", "baseline"),
                             ("nested", "same_wsc")):
        if variant not in loaded or against not in loaded:
            continue
        var, base = loaded[variant], loaded[against]
        changed = [s for s in set(base) | set(var) if canon(base.get(s)) != canon(var.get(s))]
        km = sum(length.get(s, 0) for s in changed) / 1000
        moved = collections.Counter()
        for s in changed:
            a, b = est(base.get(s))[0], est(var.get(s))[0]
            if a is None and b is None:
                moved["both silent"] += 1
            elif a is None:
                moved["gains an answer"] += 1
            elif b is None:
                moved["loses its answer"] += 1
            elif abs(a - b) >= 25:
                moved[">=25 points"] += 1
            elif abs(a - b) >= 10:
                moved["10-25 points"] += 1
            else:
                moved["<10 points"] += 1
        print(f"\n{variant} vs {against}: {len(changed):,} sections ({km:,.0f} km) change panel "
              f"({sum(1 for s in changed if s in side):,} of them side channels); today: {dict(moved)}")
        rows.setdefault(variant, (var, changed))
    base = loaded["baseline"]

    # NAMED examples: side channels that jumped, with today's answer now vs same_wsc, biggest moves first
    if "same_wsc" in rows:
        var, _ = rows["same_wsc"]
        ex = collections.defaultdict(list)
        for c, p in g["parent"].items():
            cs = g["nodes"][c]["sid"]
            if cs not in jumped:
                continue
            nm = names.get(cs) or "(unnamed)"
            a, b = est(base.get(cs))[0], est(var.get(cs))[0]
            ex[nm].append((cs, g["nodes"][p]["name"] or "(unnamed)", a, b))
        scored = []
        for nm, xs in ex.items():
            d = max((abs(a - b) if a is not None and b is not None else (50 if (a is None) != (b is None) else 0))
                    for _s, _p, a, b in xs)
            scored.append((d, nm, xs))
        scored.sort(key=lambda t: -t[0])
        print(f"\n{len(ex):,} waters hold a jumped side channel ({sum(1 for n in ex if n != '(unnamed)'):,} named)")
        fmt = lambda v: "none" if v is None else f"{v:.0f}"  # noqa: E731
        for d, nm, xs in scored[:40]:
            if nm == "(unnamed)":
                continue
            s, pn, a, b = xs[0]
            print(f"  {nm[:30]:30} {len(xs):3} pieces, e.g. sid {s}: adopts {pn[:22]:22} now {fmt(a):>4} -> "
                  f"same_wsc {fmt(b):>4}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
