"""Read-only: list each section of a named water with its donor panel, the app's weights, and the target area.

Reads the LIVE bundle (data/generated/bundle/bundle.sqlite) read-only; `check_release.py` proves the release's
paint carries the same panels. Weights are the app's (`weightFor` in app/packages/core/src/trust.ts, mirrored by
pipeline/gauges/consume/panel.py `weight_of`).

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/panels_of.py "Widgeon Slough" "Pitt River"
"""
import collections
import sqlite3
import sys

sys.path.insert(0, ".")
from pipeline.gauges.consume.panel import share_of, weight_of  # noqa: E402

BUNDLE = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/bundle/bundle.sqlite"


def connect():
    return sqlite3.connect(f"file:{BUNDLE}?mode=ro", uri=True)


def panel(c, pid):
    return c.execute("SELECT station, role, area_km2, years, regulated, same_river FROM panel_member "
                     "WHERE panel_id=? ORDER BY ord", (pid,)).fetchall()


_NAMES = {}


def gauge_name(c, s):
    if not _NAMES:
        _NAMES.update(dict(c.execute("SELECT station, name FROM gauge").fetchall()))
    return _NAMES.get(s, "?")


def weights(c, pid, target):
    """[(station, name, role, area, years, regulated, same_river, share of the answer)] — every donor reporting."""
    rows = panel(c, pid)
    ws = [weight_of(share_of(target, a), role, yrs, bool(same)) if target else 0.0
          for _s, role, a, yrs, _r, same in rows]
    tot = sum(ws) or 1.0
    return [(s, gauge_name(c, s), role, a, yrs, reg, same, w / tot)
            for (s, role, a, yrs, reg, same), w in zip(rows, ws)]


def sections_of(c, name, kind="stream"):
    out = []
    for (o,) in c.execute("SELECT ord FROM item WHERE name=? AND kind=?", (name, kind)).fetchall():
        out.append((o, [r[0] for r in c.execute("SELECT sid FROM item_section WHERE ord=? ORDER BY sid", (o,))]))
    return out


def describe(c, name):
    for o, sids in sections_of(c, name):
        print(f"== {name} (ord {o}): {len(sids)} sections")
        groups = collections.OrderedDict()
        for sid in sids:
            r = c.execute("SELECT panel_id, area_km2 FROM section_panel WHERE sid=?", (sid,)).fetchone()
            key = (r[0], round(r[1] or 0, 1)) if r else (None, None)
            groups.setdefault(key, []).append(sid)
        for (pid, area), ss in groups.items():
            print(f"  {len(ss)} sections (sid {ss[0]}..{ss[-1]}): panel {pid}, target area {area} km2")
            if pid is None:
                continue
            for s, nm, role, a, yrs, reg, same, w in weights(c, pid, area):
                print(f"     {s:8} {nm[:46]:46} {role:4} {a:>10.1f} km2 {yrs:3}y "
                      f"reg={reg} same={same} -> {w * 100:5.1f}%")


if __name__ == "__main__":
    conn = connect()
    for n in sys.argv[1:]:
        describe(conn, n)
