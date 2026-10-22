"""Read-only: named waters, section by section — today's Flow and Level answer before and after the own-gauge rule,
which own gauge leads (and why the others do not), and every donor's share.

    .venv/bin/python handoff/PLAN/app-data/own-gauge/scripts/examples.py OWN.pkl PANELS.pkl "Pitt River" ...
    (a name may be `Name@sid,sid` to print only those sections)
"""
import json
import pickle
import sqlite3
import sys

sys.path.insert(0, "handoff/PLAN/app-data/own-gauge/scripts")
from evaluate import BUNDLE, INDEX, estimate, lead_of  # noqa: E402

FAR_KM, SMALLER, SHARE = 50.0, 10.0, 0.7


def fmt(e):
    p, why, sh = e
    if p is None:
        return why
    return f"{p:4.0f}th  " + ", ".join(f"{s} {w * 100:.0f}%" for s, w in sorted(sh, key=lambda t: -t[1]))


def main(own_pkl, panels_pkl, *names):
    rows = pickle.loads(open(own_pkl, "rb").read())["rows"]
    panels = pickle.loads(open(panels_pkl, "rb").read())
    idx = json.loads(open(INDEX).read())["stations"]
    db = sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)
    for spec in names:
        name, _, only = spec.partition("@")
        want = {int(s) for s in only.split(",")} if only else None
        for (ordn, item) in db.execute("SELECT ord, item_id FROM item WHERE name=? AND kind='stream'", (name,)):
            sids = [s for (s,) in db.execute("SELECT sid FROM item_section WHERE ord=? ORDER BY sid", (ordn,))]
            print(f"\n=== {name} ({item}): {len(sids)} sections")
            last = None
            for sid in sids:
                if want and sid not in want:
                    continue
                if sid not in panels:
                    continue
                ms, target = panels[sid]
                r = rows.get(sid, {"own": [], "target": target})
                lf, ll = lead_of(r, "discharge", FAR_KM, SMALLER), lead_of(r, "level", FAR_KM, SMALLER)
                out = []
                for q, lead in (("discharge", lf), ("level", ll)):
                    a = estimate(ms, target, idx, q)
                    b = estimate(ms, target, idx, q, lead, SHARE)
                    out.append(f"    {('Flow' if q == 'discharge' else 'Level'):5} lead {lead or '-':8} before {fmt(a)}\n"
                               f"    {'':5} {'':13} after  {fmt(b)}")
                own = "; ".join(f"{o['station']} {o['direction']} {o['distance_m'] / 1000:.1f} km, "
                                f"gauge {o['area']:.0f} km2 ({(target or 0) / o['area']:.2f}x), {'/'.join(o['measures'])}"
                                for o in r["own"]) or "none"
                key = (ms, round(target or 0), lf, ll, "\n".join(out))
                if key == last and not want:
                    continue
                last = key
                print(f"  sid {sid} target {target:.0f} km2; own: {own}")
                print("\n".join(out))


if __name__ == "__main__":
    main(*sys.argv[1:])
