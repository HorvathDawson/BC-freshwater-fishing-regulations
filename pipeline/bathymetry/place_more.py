"""Place EVERY vector depth file the program can, beyond the first pass's 318 one-sheet-one-lake codes (T634 third
pass, PROTOTYPE). Measures only: every placing is a PROPOSAL for the owner's tick beside the PDF.

LICENCE: the zips are linked only from the "Access Only" sheet layer; owner says OGL (2026-11-11, link owed). Output
carries LICENCE_FLAG. Confirm before any production publish.

Per zip (all 510 fetched, the 6 compressed ones now read by `e00.decompress`), per line coverage in it:
  shore     `place.shore` at the 300 m inlet gap; if nothing closes, again at 1,000 m (`gap_m` says which)
  lakes     the candidates are the lakes `propose` put under the zip's code(s): the one named lake (clean or review),
            each named lake of a two_named code, each unnamed lake of an unnamed code; the best IoU is proposed and the
            runner-up kept beside it
  fit       `place.fit` (rigid: turn + shift, never a stretch)
  grade     strict (IoU >= 0.9, median <= 25 m, area 0.8-1.25), loose (IoU >= 0.8, area 0.8-1.25), poor (fitted, below
            loose), and the failures (no_shore, no_candidate, unreadable) with their reason
Units per file are what its own words say (`e00.units_stated` over META + annotation), else `not stated`.

    scripts/heavy.sh place 50 -- python -m pipeline.bathymetry.place_more     # 4 workers; ~/data-offgit/…/placing_all.json
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

LICENCE_FLAG = "owner says OGL, catalogue says Access Only, unconfirmed"
GAPS = (300.0, 1000.0)


def vec_dir() -> Path:
    return Path.home() / "data-offgit" / "bc-bathymetry-vectors"


def grade(f: dict) -> str:
    ar = f["area_ratio"]
    if f["iou"] >= 0.9 and f["median_m"] <= 25 and 0.8 <= ar <= 1.25:
        return "strict"
    if f["iou"] >= 0.8 and 0.8 <= ar <= 1.25:
        return "loose"
    return "poor"


def candidates(codes, props: Dict[str, dict]) -> List[int]:
    out = []
    for c in sorted(codes):
        p = props.get(c)
        if not p:
            continue
        ev = p.get("evidence", {})
        if p.get("wbks"):
            out += [int(w.split(":")[1]) for w in p["wbks"]]
        out += [int(l["wbk"].split(":")[1]) for l in ev.get("named_lakes", [])]
        out += [int(l["wbk"].split(":")[1]) for l in ev.get("unnamed_lakes", [])]
    return sorted(set(out))


def _one(job):
    zname, codes, cands, geoms = job
    from shapely import wkb
    from pipeline.bathymetry import e00, place
    path = vec_dir() / "zips" / zname
    rows = []
    try:
        sheets = e00.read_zip_all(path)
        words = e00.annotation_words(path)
    except Exception as e:  # reported, never dropped
        return [{"zip": zname, "codes": sorted(codes), "state": "unreadable", "error": f"{type(e).__name__}: {e}"}]
    seen = set()
    lakes = {w: wkb.loads(g) for w, g in geoms.items()}
    for sh in sheets:
        key = (len(sh.lines.arcs), sh.lines.arcs[0].points[:2] if sh.lines.arcs else None)
        row = {"zip": zname, "coverage": sh.lines.name, "codes": sorted(codes), "licence": LICENCE_FLAG,
               "units": e00.units_stated([sh.text()] + words) or "not stated"}
        if str(key) in seen:
            row["state"] = "duplicate_coverage"
            rows.append(row)
            continue
        seen.add(str(key))
        try:
            cont, snd = sh.contours(), sh.soundings()
        except Exception as e:
            rows.append(dict(row, state="unreadable", error=f"{type(e).__name__}: {e}"))
            continue
        shore = None
        for gap in GAPS:
            shore = place.shore(cont, snd, max_gap=gap)
            if shore is not None and shore.area > 0:
                row["gap_m"] = gap
                break
        if shore is None or shore.area <= 0:
            rows.append(dict(row, state="no_shore"))
            continue
        if not lakes:
            rows.append(dict(row, state="no_candidate", sheet_ha=round(shore.area / 1e4, 2)))
            continue
        fits = []
        for w in sorted(lakes):
            f = place.fit(shore, lakes[w])
            f.update(wbk=f"wbk:{w}", lake_ha=round(lakes[w].area / 1e4, 2))
            fits.append(f)
        fits.sort(key=lambda f: -f["iou"])
        best = fits[0]
        row.update(state="fitted", sheet_ha=round(shore.area / 1e4, 2), fit=best, grade=grade(best),
                   mirror_beats=best["mirror_iou"] > best["iou"], candidates=len(fits))
        if len(fits) > 1:
            row["runner_up"] = {k: fits[1][k] for k in ("wbk", "iou", "area_ratio")}
        rows.append(row)
    return rows


def main(argv=None) -> int:
    from multiprocessing import Pool
    from pipeline.bathymetry.contours import fwa_lakes
    from pipeline.common.curated import GENERATED
    ap = argparse.ArgumentParser(description="place every vector depth file (T634 prototype)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args(argv)
    d = vec_dir()
    sheets = json.loads((d / "sheets.json").read_text())
    z2c = defaultdict(set)
    for s in sheets:
        z2c[s["image_zip"]].add(s["wbid"])
    zips = sorted(p.name for p in (d / "zips").glob("*.zip"))
    if a.only:
        zips = [z for z in zips if z in set(a.only)]
    props = {p["chart_id"]: p for p in
             json.loads((GENERATED.base / "bathymetry" / "proposals.json").read_text())["proposals"]}
    want = {z: candidates(z2c[z], props) for z in zips}
    allw = sorted({w for v in want.values() for w in v})
    geom = {}
    for k in range(0, len(allw), 500):
        for l in fwa_lakes(f"WATERBODY_KEY IN ({','.join(map(str, allw[k:k + 500]))})"):
            g = geom.get(l["wbk"])
            geom[l["wbk"]] = l["geometry"] if g is None else g.union(l["geometry"])
    jobs = [(z, z2c[z], want[z], {w: geom[w].wkb for w in want[z] if w in geom}) for z in zips]
    with Pool(min(a.workers, 4)) as pool:
        rows = [r for rs in pool.imap(_one, jobs, chunksize=2) for r in rs]
    rows.sort(key=lambda r: (r["zip"], r.get("coverage", "")))
    st = Counter(r.get("grade", r["state"]) for r in rows)
    units = Counter(r["units"] for r in rows if r.get("units") and r["state"] != "duplicate_coverage")
    out = {"_about": "Every vector depth sheet placed by program (pipeline.bathymetry.place_more, T634). PROPOSALS: "
                     "each placing needs the owner's tick beside the PDF.", "licence": LICENCE_FLAG,
           "counts": dict(sorted(st.items())), "units": dict(sorted(units.items())),
           "feet": sorted({r["zip"] for r in rows if r.get("units") == "feet"}), "rows": rows}
    dest = d / ("placing_all.json" if not a.only else "placing_only.json")
    tmp = dest.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, indent=1, sort_keys=True))
    os.replace(tmp, dest)
    print(json.dumps({k: out[k] for k in ("counts", "units")}), f"rows {len(rows)}; wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
