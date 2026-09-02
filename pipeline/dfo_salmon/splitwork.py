"""Per-water split dossiers: every DFO locator that names a place, beside the cut-points that exist.

`dossier.py` answers "which registry item is this water"; this answers "which cut-points does its
regulation need, and which are already curated". Matching is done — 160 of 161 waters are bound — so
what is left is 147 locators that name a bridge, a sign, a dam or a road and have no coordinate.

Grouped BY WATER on purpose. A river's locators reference each other ("from the signs 200 m above the
bridge down to the cable car 200 m below it"), its existing cuts are the vocabulary the next one
should reuse, and a curator opening a map is opening it once per river, not once per rule.

An anchor is classified by what it would take to resolve it:

    satisfied   an existing cut-point's label already appears in the locator text
    confluence  the locator names a tributary whose watershed code is a CHILD of this water's —
                derivable from FWA topology, no coordinate needed
    lake        the locator names a lake and an auto lake boundary exists
    coordinate  a bridge / sign / road / dam — a human has to place it on a map

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.dfo_salmon.splitwork            # the worklist
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.dfo_salmon.splitwork "Morice"   # one water
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from pipeline.dfo_salmon.dossier import ENTRIES, item_pin, osm_link

SKIP_OPS = {"whole_water", "tributaries_only", "described", "tributary_set", "named_tributaries"}
COORD_ANCHORS = {"bridge", "boundary_sign", "road", "dam_or_hatchery", "point", "place_name"}
TRIB_RE = re.compile(r"\b([A-Z][A-Za-z'\-]*(?:\s+[A-Z][A-Za-z'\-]*)*\s+(?:River|Creek))\b")


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def _registry(path=None):
    from pipeline.registry import default_registry_path, load_registry
    p = Path(path) if path else Path("output/v2/full/registry.json")
    return load_registry(p if p.exists() else default_registry_path())


def waters(reg) -> list[dict]:
    """One row per DFO water that has at least one place-naming locator."""
    by_stream_name: dict[str, list] = {}
    for it in reg.values():
        if it.kind == "stream":
            by_stream_name.setdefault(it.name.lower(), []).append(it)

    def wscs(it):
        return [x.split(":", 1)[1] for x in it.ref_ids if x.startswith("wsc:")]

    out: list[dict] = []
    for p in sorted(ENTRIES.glob("region-*.json")):
        doc = json.loads(p.read_text(encoding="utf-8"))
        by_id = {w["water_id"]: w for w in doc["waters"]}
        locs: dict[str, list] = {}
        for loc in doc["locations"]:
            st = loc.get("source_text") or {}
            if (st.get("op") or "") in SKIP_OPS:
                continue
            if loc.get("water_id") in by_id:
                locs.setdefault(loc["water_id"], []).append(loc)
        for wid, ls in locs.items():
            w = by_id[wid]
            items = [reg[i] for i in (w.get("item_ids") or []) if i in reg]
            cuts = [(b.id, b.label or "", b.kind) for it in items for b in it.boundaries]
            mine = {c for it in items for c in wscs(it)}
            rows = []
            for loc in ls:
                st = loc["source_text"]
                text = st.get("specific_area") or ""
                hit = next((c for c in cuts if len(_norm(c[1])) > 4 and _norm(c[1]) in _norm(text)), None)
                kind, detail = "coordinate", ""
                if hit:
                    kind, detail = "satisfied", hit[0]
                else:
                    a = set(st.get("anchor_types") or [])
                    tribs = []
                    for mm in TRIB_RE.finditer(text):
                        for cand in by_stream_name.get(mm.group(1).lower(), []):
                            if any(t.startswith(m + "-") for t in wscs(cand) for m in mine):
                                tribs.append((mm.group(1), cand.id, wscs(cand)[0]))
                    if tribs and not (a & COORD_ANCHORS):
                        kind = "confluence"
                        detail = "; ".join("%s %s (%s)" % t for t in tribs)
                    elif a & {"lake_end"} and not (a & COORD_ANCHORS):
                        kind, detail = "lake", "auto lake boundary"
                rows.append({"op": st.get("op"), "anchors": st.get("anchor_types") or [],
                             "text": text, "kind": kind, "detail": detail,
                             "loc": loc["location_id"]})
            out.append({"slug": p.stem.split("region-")[1], "water": w["name"],
                        "region": w.get("region_number"), "sections": w.get("sections") or [],
                        "items": items, "cuts": cuts, "locators": rows})
    return out


def render(w: dict) -> str:
    L = ["=" * 96,
         "%s    [region %s%s]" % (w["water"], w["region"],
                                  (" · DFO scope " + ", ".join(w["sections"])) if w["sections"] else ""),
         "=" * 96]
    L.append("  BOUND TO")
    for it in w["items"] or []:
        pin = item_pin(it.id)
        L.append("    %-18s %-30s kind=%-6s mus=%s sections=%d"
                 % (it.id, repr(it.name), it.kind, list(it.mus), len(it.section_ids)))
        if pin:
            L.append("        %s" % osm_link(*pin))
    if not w["items"]:
        L.append("    (nothing — this water is still unmatched)")

    need = [r for r in w["locators"] if r["kind"] != "satisfied"]
    L.append("\n  EXISTING CUT-POINTS (%d)" % len(w["cuts"]))
    for cid, label, kind in w["cuts"]:
        L.append("    · %-52s %-42s [%s]" % (cid, repr(label), kind))
    if not w["cuts"]:
        L.append("    (none)")

    L.append("\n  DFO LOCATORS — %d, of which %d still need a cut" % (len(w["locators"]), len(need)))
    for r in w["locators"]:
        tag = {"satisfied": "OK ", "confluence": "TRIB", "lake": "LAKE", "coordinate": "MAP "}[r["kind"]]
        L.append("    [%s] %-16s anchors=%s" % (tag, r["op"], r["anchors"]))
        L.append("         %s" % r["text"])
        if r["detail"]:
            L.append("         -> %s" % r["detail"])
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="Per-water DFO split dossiers.")
    ap.add_argument("name", nargs="?", help="water name (substring); omit for the worklist")
    ap.add_argument("--registry")
    ap.add_argument("--all", action="store_true", help="render every water needing a cut")
    args = ap.parse_args()

    reg = _registry(args.registry)
    ws = waters(reg)
    if args.name and not args.all:
        hits = [w for w in ws if args.name.casefold() in w["water"].casefold()]
        if not hits:
            print("no water matching %r with place-naming locators" % args.name)
            return
        for w in hits:
            print(render(w))
        return

    ranked = sorted(ws, key=lambda w: (-sum(1 for r in w["locators"] if r["kind"] != "satisfied"),
                                       w["water"]))
    todo = [w for w in ranked if any(r["kind"] != "satisfied" for r in w["locators"])]
    if args.all:
        for w in todo:
            print(render(w) + "\n")
        return
    from collections import Counter
    tot = Counter(r["kind"] for w in ws for r in w["locators"])
    print("%d waters carry place-naming locators; %d still need at least one cut" % (len(ws), len(todo)))
    print("locators: " + " · ".join("%s %d" % (k, v) for k, v in tot.most_common()) + "\n")
    print("%-34s %-6s %5s %5s %5s %5s  %s" % ("WATER", "REGION", "need", "MAP", "TRIB", "LAKE", "cuts"))
    for w in todo:
        c = Counter(r["kind"] for r in w["locators"])
        print("%-34s r%-5s %5d %5d %5d %5d  %d"
              % (w["water"][:34], w["region"], sum(v for k, v in c.items() if k != "satisfied"),
                 c["coordinate"], c["confluence"], c["lake"], len(w["cuts"])))


if __name__ == "__main__":
    main()
