"""Sanity of the spec's data -> view mapping, before any TypeScript exists: for every golden state, decode the store's
frames the way contracts.md says the app will, and check the golden (what the prototype drew) agrees.

    .venv/bin/python app/design/regs-v11/port/verify_map.py      # REGS_RELEASE=<data/release/r-…> to read another release

The store is the release's (P20: no regs.sqlite), put back together by `pipeline.deliver.store.release.province_store`.

Checks (each prints passed / states):
  strip.runs     per-day display_frame.strip over 2026 (taps.json displayByDay, Feb 29 dropped), run-length == golden keep.strip.bar[].flex
  strip.status   closed iff the day's display_frame.status == "closed" == golden keep.strip.statusClass
  groups         rows with group.headline among the rows page v36 draws in full (TROUTISH split; none on a closed day) == golden keep.groups
  otherFish      the non-trout rows when >= 2 (and not closed) == golden keep.otherFish
  tiers / kinds  TreeTier.says == the golden tier line (minus a trailing ⓘ / Rule book link); TreeKind.says starts the golden kind sentence.
                 A FOLDED share (golden `folded`, the page's data-share-folded: its words add nothing to the head's lead, int1) draws
                 no line: it passes when it is a share and the head's lead shows the group headline's number, which is the share's.
                 An own tier's `relation` (rows 28) == the golden relation line.
"""
import json, os, re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO))
from pipeline.deliver.store.release import province_store  # noqa: E402

# OWNER RULING 2026-10-21 (a) (pipeline/docs/GOTCHAS.md): on the Kitimat the store's group tree differs from page v36's by
# design (app/conformance/src/regs/exclusions.ts KITIMAT_RULED_IDS); their trees are not walked here, every other check runs.
KITIMAT_RULED = {"x14p0_1010", "x14p1_1010", "x14p2_1010"}
# T203 (owner 2026-10-28, ANSWERS-SPEC T203; exclusions.ts T203_SIZE_BOX_IDS): Region 6's shared 1 over 50 cm is one size tier
# (the count tier it crosses split) where page v36 said it on each kind; their trees are not walked here either.
KITIMAT_RULED |= set(["x17p0_1010", "x17p1_1010", "x17p2_1010", "x17p3_1010", "x17p4_1010", "x17p7_1010", "x19p0_1010", "x19p1_1010", "x19p2_1010", "x19p3_1010", "x19p4_1010", "x19p6_1010", "x19p7_1010"])
# decisions D19 (a), AS BUILT (T80 item 3; app/conformance/src/regs/exclusions.ts D19_GROUPS): the store ships klmb_1010's
# 10 rainbow trout as a one-kind GROUP, where the golden (page v36's legacy card path) draws a plain article, so the store has
# exactly one group more than the golden there; asserted as exactly +1, and the extra group is not walked.
D19_EXTRA = {"klmb_1010": 1}
D19_ROW = 2
TROUTISH = set("RB CT EB GB LT DV ST BT CT_ws CT_c".split())   # page_v36.js rowsHtml


def main():
    G = json.loads((REPO / "app/fixtures/golden/golden.json").read_text())
    T = json.loads((REPO / "app/fixtures/golden/taps.json").read_text())
    rel = os.environ.get("REGS_RELEASE")
    s = province_store(Path(rel) if rel else None)
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
        spp, fs, rs, line, _strips = val("rows_frame", t["cell"]["rows"])   # rows 30: + each row's strip
        rows = [val("rows_row", i) for i in rs]
        main_ = [r for r in rows if any(m in TROUTISH for m in (r.get("all_members") or r["members"]))]
        rest = [r for r in rows if r not in main_]
        split = bool(main_) and len(rest) >= 2
        shown = [] if closed else (main_ if split else rows)
        want = [r for r in shown if r.get("group") and r["group"].get("headline")]
        ok = len(want) == len(g["groups"]) + D19_EXTRA.get(sid, 0); R["groups"] += ok; ok or bad.append(("groups", sid))
        of = g["otherFish"]
        ok = (of is None and (closed or not split)) or (of is not None and not closed and split and len(of) == len(rest))
        R["otherFish"] += ok; ok or bad.append(("otherFish", sid))
        cmp_ = want if sid not in D19_EXTRA else [r for r in want if r is not rows[D19_ROW]]   # the extra group is the legacy-path row's
        for r, gg in zip(cmp_, g["groups"]):
            if sid in KITIMAT_RULED: continue
            tree = r["group"].get("tree")
            if not tree or not gg["tree"]: continue
            root = tree[0] if isinstance(tree, list) else tree
            nodes = root.get("children", []) if gg["head"] and gg["head"]["rootTier"] == root.get("id") else (tree if isinstance(tree, list) else [tree])
            walk(nodes, gg["tree"], tiers, kinds, bad, sid, r["group"].get("headline"), gg["head"])
    for k, v in R.items():
        print(f"{k:13} {v} / {n}")
    print(f"{'tiers':13} {tiers[0]} / {tiers[1]}\n{'kinds':13} {kinds[0]} / {kinds[1]}")
    print(f"{'(not walked)':13} {len(KITIMAT_RULED)} ruled states' trees (Kitimat 2026-10-21 (a), T203 size box)")
    if bad:
        print("differ:", bad[:10]); sys.exit(1)


# T128 (owner 2026-10-22): the store's kind line names its total, "Of your 2 trout and char a day in streams, up to 2 may
# be X"; page v36 (the golden) says the old forms. conformance/src/regs/subcapTotal.ts holds the full rewrite against the
# golden; here the kind is compared by what the sentence SAYS (its cap K, its fish X, the wild-steelhead clause, and the
# total T where the golden names one), whichever of the two wordings carries it.
_NEW = re.compile(r"^Of (?:your|the) (?P<t>\d+) [^,]*?, (?:up to|only) (?P<k>\d+) may be (?P<x>.+)$")
_OLD = [re.compile(r"^(?:Up to|Only) (?P<k>\d+) (?:of the (?P<t>\d+)(?: from streams)? )?may be (?P<x>.+)$"),
        re.compile(r"^Up to (?P<k>\d+) (?P<x>.+) \(part of the (?P<t>\d+) from streams\)$")]


def _kind_facts(says):
    s = re.sub(r"\s*How to tell ›$", "", says.strip())
    m = _NEW.match(s) or next((m for m in (r.match(s) for r in _OLD) if m), None)
    if not m: return None
    x, wild = m["x"], False
    for w in (" only — wild steelhead go back", " — wild steelhead go back"):
        if x.endswith(w): x, wild = x[:-len(w)], True
    return m["k"], x, wild, m["t"]


def kind_same(store, golden):
    if golden.startswith(store): return True    # kinds the owner's wording never touched
    a, b = _kind_facts(store), _kind_facts(golden)
    return bool(a and b and a[:3] == b[:3] and (b[3] is None or a[3] == b[3]))


def walk(ns, gs, tiers, kinds, bad, sid, headline=None, head=None):
    for nd, x in zip(ns, gs):
        if x.get("t", nd["node"]) != nd["node"]:   # v38 golden nodes carry `t`; a different shape is a difference, not a crash
            (tiers if nd["node"] == "tier" else kinds)[1] += 1
            bad.append(("shape", sid, nd["node"], x.get("t"))); continue
        if nd["node"] == "tier":
            tiers[1] += 1
            line = re.sub(r"\s*(ⓘ|What these words mean)?\s*(Rule book p\.\d+ ↗\s*)*$", "", x["line"] or "")
            if x.get("folded"):
                ok = (nd["tier"] == "share" and not x["line"] and headline is not None and nd.get("n") == headline.get("n")
                      and head is not None and head.get("leadN") == str(nd.get("n")))
            else:
                ok = line == nd["says"] and (x.get("relation") or None) == nd.get("relation")
            if ok: tiers[0] += 1
            else: bad.append(("tier", sid, nd["says"], x["line"], nd.get("relation"), x.get("relation")))
            walk(nd.get("children", []), x.get("children") or [], tiers, kinds, bad, sid, headline, head)
        else:
            kinds[1] += 1
            if nd.get("folded") or kind_same(nd["says"], x["says"] or ""): kinds[0] += 1
            else: bad.append(("kind", sid, nd["says"], x["says"]))


if __name__ == "__main__":
    main()
