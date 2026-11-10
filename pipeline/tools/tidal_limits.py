"""T326: WRITE THE TIDAL LIMITS (`data/curated/waters/tidal_limits.json`) from the Tidal-tab marks — the tidal domain's
write-through writer (`review.apply.WRITERS["tidal"]`), run in the request like sloughs and names.

Each Place y tidal mark in force (`proposals._tidal_decides`) becomes the row of its water (its blue line):

  the point is the mouth, or the atlas line's own lower end      -> limit `mouth` (an alias at the mouth, no cut)
  the mark names an existing cut, or its point IS a splits.json  -> limit `split` (that cut is the tidal limit)
  cut (same anchor, or within `proposals.TIDAL_REUSE_M`)
  the point is a located words point / a named place, new        -> limit `cut` (its point; the atlas mints the id)
  the owner's "the whole water is tidal" (Place y, call whole)  -> limit `whole` (the candidate's one registry item)
  anything else (polygon point, off point, unsure, wrong)        -> nothing written: a follow-up, listed with why
                                                                     (the review app's Tidal follow-ups, T359)

Rows WITHOUT a mark are the owner's own calls (the whole Fraser delta channels) and are kept, unless a LATER
decision on the same water replaces them (a mark made the same day or after the call's `decided` date). The atlas reads the file (`atlas.splits.tidal`); nothing in the build reads the marks.

    python -m pipeline.tools.tidal_limits            # write the file
    python -m pipeline.tools.tidal_limits --check    # exit 1 when the file is not what the marks say
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from pipeline.atlas.splits import tidal as T


def _blk(m, n, cand) -> str:
    got = str(n.blk or cand.get("blk") or "")
    if not got and str(m.target).startswith("tidal:"):
        got = str(m.target).split(":", 1)[1]
    return got


#: T359 item 4 — an OWNER DECISION, open: the writer does not cut at a DEM point (`proposals.tidal_point` knows the
#: mouth, the line's end, located words and a named place only); the follow-up asks the question instead
DEM_QUESTION = ("owner decision (T359): does a confirmed DEM point become a cut? Until answered, no tidal limit")


def _dem_point(n) -> bool:
    """Was this tidal cut the DEM limit the owner switched to (the Tidal tab's `altSplit` note)?"""
    return "chosen point: dem" in (n.note or "").lower()


def build(decided, src=None, existing: Optional[list] = None) -> tuple[list, list[dict]]:
    """(rows, skipped [{target, why, followup?}]) — the file's rows from the decided data (`review.export.Decided`), the curated
    splits and the atlas (`proposals.Sources`), keeping the owner's own rows of `existing` (no mark)."""
    from pipeline.regs.review import proposals as P
    from pipeline.regs.review import tidal as TC
    if src is None:
        src = P.Sources.live()
    if existing is None:
        try:
            existing = T.load()
        except SystemExit:
            existing = []
    splits_doc = src.doc(src.splits_path)
    have = {sp["id"]: sp for wb in splits_doc.get("waterbodies") or [] for sp in wb.get("splits") or [] if sp.get("id")}
    latest = decided.inforce
    deciding = [m for m in latest if P._tidal_decides(m)]
    wholes = [m for m in latest if P._tidal_whole(m)]
    starts = P._line_starts(src.build, {str(m.new_splits[0].blk or "") for m in deciding})
    rows: dict[str, T.Limit] = {}
    skipped: list[dict] = []
    for m in sorted(deciding, key=lambda m: m.target):
        n = m.new_splits[0]
        cand = TC.candidate(m.target) or {}
        blk = _blk(m, n, cand)
        water = cand.get("water") or ((src.item(n.on_item) or {}).get("name") if n.on_item else None) \
            or (n.name or "").partition(":")[0].strip()
        prov = (decided.provenance.get(m.key) or {}).get("mark")
        base = {"blk": blk, "name": water or "", "decided": m.date, "mark": prov}
        if not blk.isdigit():
            skipped.append({"target": m.target, "why": "the mark names no blue line"})
            continue
        if n.anchor == "split" and not n.offset_m:
            if n.split_id not in have:
                skipped.append({"target": m.target, "why": f"splits.json has no split {n.split_id}"})
                continue
            rows[blk] = T.Limit(limit="split", split=n.split_id, why="the owner named this cut", **base)
            continue
        point = P.tidal_point(n, cand, starts.get(blk))
        if point is None:
            skipped.append({"target": m.target, "why": (
                f"a DEM point ({n.measure} m above the mouth): {DEM_QUESTION}" if _dem_point(n) else
                f"the point ({n.measure} m above the mouth) is neither a located words point nor the mouth — "
                "mark it again")})
            continue
        if point in ("mouth", "line_end"):
            why = "the mouth" if point == "mouth" else (
                f"the atlas line's own lower end (it starts {starts.get(blk)} m above the FWA mouth): a cut there "
                "would break both halves")
            rows[blk] = T.Limit(limit="mouth", why=why, **base)
            continue
        got = P.place_groups([("t", n, True, water)], src, splits_doc, have)["t"]
        if got["dup"]:
            rows[blk] = T.Limit(limit="split", split=got["dup"], why=f"{point}: the cut already there", **base)
            continue
        if got["problems"] or n.coord is None:
            skipped.append({"target": m.target, "why": "; ".join(got["problems"]) or "the new cut has no point"})
            continue
        at = {"coord": [round(float(c), 6) for c in n.coord], "measure": n.measure, "label": got["split"].name}
        rows[blk] = T.Limit(limit="cut", at=at, why=f"{point}: a new cut", **base)
    for m in sorted(wholes, key=lambda m: m.target):
        # T359: "the whole water is tidal" — a `whole` row on the candidate's one registry item
        cand = TC.candidate(m.target) or {}
        blk = str(cand.get("blk") or str(m.target).partition(":")[2])
        items = list(cand.get("items") or [])
        if not blk.isdigit() or len(items) != 1:
            skipped.append({"target": m.target, "why": "the whole water is tidal, but the candidate has "
                            + (f"{len(items)} registry items" if blk.isdigit() else "no blue line")
                            + " — one item is needed"})
            continue
        prov = (decided.provenance.get(m.key) or {}).get("mark")
        rows[blk] = T.Limit(blk=blk, limit="whole", item=items[0], name=cand.get("water") or "", decided=m.date,
                            mark=prov, why="the owner: the whole water is tidal")
    for r in existing or ():
        # the owner's own call (no mark) is kept — over a decision on the same water only when it is the later one
        # (Noons Creek: auto-accepted at the mouth 10-28, then the railway bridge in the owner's words 10-29)
        if r.mark is None and (r.blk not in rows or r.decided > rows[r.blk].decided):
            rows[r.blk] = r
    for m in latest:
        if P._tidal_followup(m) or (m.scope == "tidal" and not P._tidal_decides(m) and not P._tidal_whole(m)):
            why = f"a follow-up ({m.verdict}): no tidal limit until a Place y mark"
            held = rows.get(str(m.target).partition(":")[2])
            if held is not None and held.mark is None:
                # the owner's own row already decides the water: the mark is newer words, not a decision
                why += (f" — tidal.json already holds the owner's own `{held.limit}` row ({held.decided}); "
                        "re-mark to confirm it")
            skipped.append({"target": m.target, "why": why, "followup": True})   # decides nothing: nothing to land
    return sorted(rows.values(), key=lambda r: int(r.blk)), skipped


def document(rows) -> str:
    return T.document(rows)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.tools.tidal_limits", description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="exit 1 when the file is not what the marks say")
    a = ap.parse_args(argv)
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import export as E
    rows, skipped = build(E.load())
    text = document(rows)
    path = Path(CURATED.waters.tidal)
    counts = {k: sum(1 for r in rows if r.limit == k) for k in T.LIMITS}
    print(f"tidal: {counts}; {len(skipped)} not written (follow-ups)")
    if a.check:
        same = path.exists() and path.read_text(encoding="utf-8") == text
        print("current" if same else f"{path} is NOT what the marks say")
        return 0 if same else 1
    from pipeline.regs.parsing.io import atomic_write
    atomic_write(path, text)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
