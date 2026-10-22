"""Read-only: today's answer on every section, before (option A as merged) and after (the own-gauge build), in Flow and
in Level, binned as the side-channels report bins them; the panel dictionary before and after; named examples.

The estimate is `estimate()` of app/packages/core/src/trust.ts at horizon 0, ported line for line, with the lead taken
from the pipeline's OWN `leader_of` / `lead_shares` (held equal to the app's by app/tools/one-formula.test.ts) and the
feed rule of `reading()` in app/packages/data/src/panel.ts — so a level-only gauge is absent from Flow.

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/compare_own.py BEFORE.pkl AFTER.pkl GRAPH_SMALL.pkl
"""
import collections
import json
import math
import pickle
import sqlite3
import sys

sys.path.insert(0, ".")
from pipeline.atlas.gauges.panel import (  # noqa: E402
    ERROR_BY_RATIO, MAX_USEFUL_SPREAD, MIN_TOTAL_WEIGHT, lead_shares, leader_of, weight_of,
)

MAIN = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations"
BUNDLE = MAIN + "/data/generated/bundle/bundle.sqlite"
INDEX = MAIN + "/data/generated/gauges/feeds/index.json"


def phi(z):
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def probit(p):
    q = min(1 - 1e-9, max(1e-9, p))
    lo, hi = -9.0, 9.0
    for _ in range(80):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if phi(mid) < q else (lo, mid)
    return (lo + hi) / 2


def err(ratio):
    for limit, _n, e in ERROR_BY_RATIO:
        if ratio <= limit:
            return e
    return ERROR_BY_RATIO[-1][2]


def reading(row, q):
    if not row:
        return None
    if row.get(q) is not None:
        return row[q]
    return row.get("percentile") if (row.get("parameter") or "discharge") == q else None


def estimate(entry, idx, q):
    """-> (percentile 0..100 | None, why, [(station, share)])."""
    if entry is None:
        return None, "no-panel", []
    members, target = entry
    if not members or not target or target <= 0:
        return None, "no-station", []
    votes = []
    for m in members:
        st, role, area, years, _reg, same = m[:6]
        lead = m[6] if len(m) > 6 else 0
        p = reading(idx.get(st), q)
        if p is None or not 0 < p < 1 or not area or area <= 0:
            continue
        w = weight_of(min(target, area) / max(target, area), role, years, bool(same))
        if w > 0:
            votes.append((st, probit(p), w, max(target, area) / min(target, area), bool(same), lead))
    if not votes:
        return None, "no-record", []
    if sum(v[2] for v in votes) < MIN_TOTAL_WEIGHT:
        return None, "too-uncertain", []
    leader = leader_of([v[5] for v in votes], [v[2] for v in votes])
    sh = lead_shares([v[2] for v in votes], leader)
    mean = sum(v[1] * s for v, s in zip(votes, sh))
    sd = math.sqrt(sum(s * (v[1] - mean) ** 2 for v, s in zip(votes, sh)))
    spread = (phi(mean + sd) - phi(mean - sd)) * 100
    pm = sum(err(v[3]) * s for v, s in zip(votes, sh))
    pc = phi(mean) * 100
    half = max(pm, spread / 2)
    lo, hi = max(0, pc - half), min(100, pc + half)
    if not (any(v[4] for v in votes) or leader is not None) and hi - lo > MAX_USEFUL_SPREAD:
        return None, "too-uncertain", []
    return pc, "ok", [(v[0], s) for v, s in zip(votes, sh)]


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
            "moves 5 to 10 points" if d >= 5 else "moves under 5 points" if d >= 0.5 else "unchanged")


def main(before_pkl, after_pkl, graph_pkl, *names):
    before = pickle.loads(open(before_pkl, "rb").read())
    after = pickle.loads(open(after_pkl, "rb").read())
    g = pickle.loads(open(graph_pkl, "rb").read())
    length = {v["sid"]: v["length"] or 0.0 for v in g["nodes"].values() if v["sid"] is not None}
    idx = json.loads(open(INDEX).read())["stations"]
    strip = lambda e: None if e is None else (frozenset(m[:6] for m in e[0]), e[1])  # noqa: E731

    print(f"sections with a panel: before {len(before):,}, after {len(after):,}")
    print(f"distinct panels (member set): before {len({frozenset(e[0]) for e in before.values()}):,}, "
          f"after {len({frozenset(e[0]) for e in after.values()}):,}")
    same_set = sum(1 for s in before if strip(before.get(s)) == strip(after.get(s)))
    print(f"same donor set + area, lead aside: {same_set:,}; different: {len(set(before) | set(after)) - same_set:,} "
          "(the cap keeping an own gauge that may lead)")
    led = [s for s, e in after.items() if any(len(m) > 6 and m[6] for m in e[0])]
    print(f"sections holding an own gauge that may lead: {len(led):,} ({sum(length.get(s, 0) for s in led) / 1000:,.0f} km)")

    for q, label in (("discharge", "Flow"), ("level", "Level")):
        c, km = collections.Counter(), collections.Counter()
        cache = {}
        for s in set(before) | set(after):
            kb, ka = before.get(s), after.get(s)
            key = (kb, ka)
            if key not in cache:
                cache[key] = (estimate(kb, idx, q)[0], estimate(ka, idx, q)[0])
            a, b = cache[key]
            k = binned(a, b)
            c[k] += 1
            km[k] += length.get(s, 0) / 1000
        print(f"\n{label} (feed {json.loads(open(INDEX).read())['fetchedAt']}): every panelled section")
        print("| effect | sections | km |\n|---|---|---|")
        for k in ORDER:
            if c[k]:
                print(f"| {k} | {c[k]:,} | {km[k]:,.0f} |")

    if names:
        db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
        fmt = lambda e: e[1] if e[0] is None else f"{e[0]:.0f}th: " + ", ".join(  # noqa: E731
            f"{st} {w * 100:.0f}%" for st, w in sorted(e[2], key=lambda t: -t[1]))
        for spec in names:
            name, _, only = spec.partition("@")
            want = {int(x) for x in only.split(",")} if only else None
            for (ordn, item) in db.execute("SELECT ord, item_id FROM item WHERE name=? AND kind='stream'", (name,)):
                print(f"\n=== {name} ({item})")
                seen = set()
                for (sid,) in db.execute("SELECT sid FROM item_section WHERE ord=? ORDER BY sid", (ordn,)):
                    if want is not None and sid not in want:
                        continue
                    b, a = before.get(sid), after.get(sid)
                    if a is None and b is None:
                        continue
                    lines = []
                    for q, label in (("discharge", "Flow"), ("level", "Level")):
                        lines.append(f"    {label:5} before {fmt(estimate(b, idx, q))}\n"
                                     f"    {'':5} after  {fmt(estimate(a, idx, q))}")
                    leads = ", ".join(f"{m[0]} rank {m[6]}" for m in (a[0] if a else ()) if m[6]) or "none"
                    key = (leads, "\n".join(lines))
                    if key in seen and want is None:
                        continue
                    seen.add(key)
                    print(f"  sid {sid} ({(a or b)[1]:.0f} km2); may lead: {leads}")
                    print("\n".join(lines))


if __name__ == "__main__":
    main(*sys.argv[1:])
