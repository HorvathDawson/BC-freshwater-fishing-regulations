"""Sanity of the spec's data -> view mapping, before any TypeScript exists: for every golden state, decode the store's
frames the way contracts.md says the app will, and check the golden (what the prototype drew) agrees.

    .venv/bin/python app/design/regs-v11/port/verify_map.py

Checks (each prints passed / states):
  strip.runs     per-day display_frame.strip over 2026 (taps.json displayByDay, Feb 29 dropped), run-length == golden keep.strip.bar[].flex
  strip.status   closed iff the day's display_frame.status == "closed" == golden keep.strip.statusClass
  groups         rows with group.headline among the rows page v36 draws in full (TROUTISH split; none on a closed day) == golden keep.groups
  otherFish      the non-trout rows when >= 2 (and not closed) == golden keep.otherFish
  tiers / kinds  TreeTier.says == the golden tier line (minus a trailing ⓘ / Rule book link); TreeKind.says starts the golden kind sentence
"""
import json, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.decode import Store  # noqa: E402

TROUTISH = set("RB CT EB GB LT DV ST BT CT_ws CT_c".split())   # page_v36.js rowsHtml


def main():
    G = json.loads((REPO / "app/design/regs-v11/port/golden/golden.json").read_text())
    T = json.loads((REPO / "app/design/regs-v11/port/golden/taps.json").read_text())
    s = Store(str(REPO / "data/generated/bundle/regs.sqlite"), verify=False)
    c, cache = s.con, {}

    def val(t, i):
        if (t, i) not in cache:
            b, = c.execute(f"select j from {t} where id=?", (i,)).fetchone()
            cache[(t, i)] = s.val(t, b)
        return cache[(t, i)]

    R = {k: 0 for k in ("strip.runs", "strip.status", "groups", "otherFish")}
    tiers = [0, 0]; kinds = [0, 0]; n = 0; bad = []
    for sid, t in T["states"].items():
        n += 1
        g = G["states"][sid]["keep"]
        days = [val("display_frame", f).get("strip") for i, f in enumerate(t["displayByDay"]) if i + 1 != 60]
        runs = []
        for b in days:
            if runs and runs[-1][0] == b: runs[-1][1] += 1
            else: runs.append([b, 1])
        gold = [x["flex"] for x in g["strip"]["bar"]] if g["strip"] else None
        ok = gold == [k for _, k in runs] or (gold is None and runs == [[None, 365]])
        R["strip.runs"] += ok; ok or bad.append(("strip.runs", sid))
        closed = val("display_frame", t["cell"]["display"]).get("status") == "closed"
        R["strip.status"] += (not g["strip"]) or ((g["strip"]["statusClass"] == "closed") == closed)
        spp, fs, rs, line = val("rows_frame", t["cell"]["rows"])
        rows = [val("rows_row", i) for i in rs]
        main_ = [r for r in rows if any(m in TROUTISH for m in (r.get("all_members") or r["members"]))]
        rest = [r for r in rows if r not in main_]
        split = bool(main_) and len(rest) >= 2
        shown = [] if closed else (main_ if split else rows)
        want = [r for r in shown if r.get("group") and r["group"].get("headline")]
        ok = len(want) == len(g["groups"]); R["groups"] += ok; ok or bad.append(("groups", sid))
        of = g["otherFish"]
        ok = (of is None and (closed or not split)) or (of is not None and not closed and split and len(of) == len(rest))
        R["otherFish"] += ok; ok or bad.append(("otherFish", sid))
        for r, gg in zip(want, g["groups"]):
            tree = r["group"].get("tree")
            if not tree or not gg["tree"]: continue
            root = tree[0] if isinstance(tree, list) else tree
            nodes = root.get("children", []) if gg["head"] and gg["head"]["rootTier"] == root.get("id") else (tree if isinstance(tree, list) else [tree])
            walk(nodes, gg["tree"], tiers, kinds, bad, sid)
    for k, v in R.items():
        print(f"{k:13} {v} / {n}")
    print(f"{'tiers':13} {tiers[0]} / {tiers[1]}\n{'kinds':13} {kinds[0]} / {kinds[1]}")
    if bad: print("differ:", bad[:10])


def walk(ns, gs, tiers, kinds, bad, sid):
    for nd, x in zip(ns, gs):
        if nd["node"] == "tier":
            tiers[1] += 1
            line = re.sub(r"\s*(ⓘ|What these words mean)?\s*(Rule book p\.\d+ ↗\s*)*$", "", x["line"] or "")
            if line == nd["says"]: tiers[0] += 1
            else: bad.append(("tier", sid, nd["says"], x["line"]))
            walk(nd.get("children", []), x["children"], tiers, kinds, bad, sid)
        else:
            kinds[1] += 1
            if nd.get("folded") or (x["says"] or "").startswith(nd["says"]): kinds[0] += 1
            else: bad.append(("kind", sid, nd["says"], x["says"]))


if __name__ == "__main__":
    main()
