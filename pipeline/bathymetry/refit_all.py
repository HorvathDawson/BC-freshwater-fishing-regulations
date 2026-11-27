"""Run the wider fitter (`refit`, T1188) over every vector depth sheet. MEASURES ONLY: the rows are proposals for the
owner's eye beside the PDF; nothing here is accepted, curated or shipped (depth does not ship).

Per zip: the baseline is the repo's fitter UNCHANGED (`place_more._one`: shore, candidate lakes, `place.fit`, grade).
Every sheet it grades loose or poor then goes through `refit.refit_sheet` (rigid wider, two lakes, wrong-lake code,
small stretch; the bigger stretch only listed). Strict sheets and the failures (no shore, no candidate, unreadable,
duplicate coverage) are carried as they are.

Inputs: the 2001 vector zips, outside git (`--vec-dir`, its `zips/` as `fetch_vectors` writes them), the sheet list
(`fetch_vectors.sheets(SOURCE.fwa_gpkg)`: zip -> codes, titles), the chart proposals (`GENERATED.bathymetry /
proposals.json`: code -> candidate lakes) and the FWA lakes (`SOURCE.fwa_gpkg`).
Outputs, under `GENERATED.bathymetry` (machine output, rewritten freely):
    refit.json         the counts after each step, the lists, one row per sheet
    refit.csv          one row per sheet: classes before / after, rotation, mirror, scale, IoU, flags
    refit_shapes.json  the outlines (WKT) behind the review pictures: lakes, the placing before / after / stretched /
                       refused (the best lift a step found but did not adopt)

    scripts/heavy.sh refit 30 -- python -m pipeline.bathymetry.refit_all     # 4 workers
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

from pipeline.bathymetry.place_more import LICENCE_FLAG

STEP_LABELS = {"baseline": "the repo's fitter", "rigid": "rigid, wider", "two_lake": "two-lake fit",
               "other_lake": "wrong-lake code", "small_stretch": "small stretch (<= 5 %)"}


#: the steps whose found lift can be refused (ambiguous, a name that differs, a lake the sheet does not draw)
REFUSABLE = ("rigid", "two_lake", "other_lake", "small_stretch")


def near_lakes_from(gpkg):
    """(bounds, pad) -> wbk -> {"geom", "name", "layer"}: the FWA lakes (lakes + manmade layers) in the padded box,
    each lake's parts joined."""
    def near(bounds, pad):
        import pyogrio
        from shapely.ops import unary_union
        minx, miny, maxx, maxy = bounds
        bb = (minx - pad, miny - pad, maxx + pad, maxy + pad)
        got: Dict[int, dict] = {}
        for layer in ("lakes", "manmade"):
            df = pyogrio.read_dataframe(gpkg, layer=layer, bbox=bb, columns=["WATERBODY_KEY", "GNIS_NAME_1"])
            for r in df.itertuples():
                e = got.setdefault(int(r.WATERBODY_KEY), {"layer": layer, "g": [],
                                                          "name": r.GNIS_NAME_1 if isinstance(r.GNIS_NAME_1, str) else None})
                e["g"].append(r.geometry)
        return {w: {"geom": unary_union(e.pop("g")), **e} for w, e in sorted(got.items())}
    return near


def _wkt(g, tol: float) -> str:
    from shapely import wkt
    return wkt.dumps(g.simplify(tol), rounding_precision=1)


def _sheet_of(zpath: str, coverage: str, gap_m: float):
    from shapely.geometry import LineString
    from pipeline.bathymetry import e00, place
    sh = {s.lines.name: s for s in e00.read_zip_all(zpath)}[coverage]
    cont, snd = sh.contours(), sh.soundings()
    return place.shore(cont, snd, max_gap=gap_m), [(d, LineString(p)) for d, _fc, p in cont if len(p) >= 2]


def job(j):
    """One zip: the baseline rows, then every loose or poor sheet refitted (with its outlines for the pictures)."""
    zpath, codes, cands, geoms, info, titles = j
    from shapely import wkb
    from pipeline.bathymetry import place, place_more, refit
    from pipeline.common.curated import SOURCE
    rows = place_more._one((zpath, codes, cands, geoms))
    lakes = {w: wkb.loads(g) for w, g in geoms.items()}
    near = near_lakes_from(SOURCE.fwa_gpkg)
    shapes = {}
    for r in rows:
        if r.get("state") != "fitted" or r["grade"] == "strict":
            continue
        sheet, lines = _sheet_of(zpath, r["coverage"], r["gap_m"])
        coded = {w: {"geom": lakes[w], **info.get(w, {})} for w in sorted(lakes)}
        got = refit.refit_sheet(sheet, lines, coded, near, base_fit=r["fit"], units=r["units"], titles=titles)
        geoms_used = got.pop("_geoms")
        r["refit"] = got
        o = tuple(got["origin"])
        tol = max(1.0, (sheet.area ** 0.5) / 400)
        base = place._place(sheet, r["fit"]["angle_deg"], r["fit"]["dx"], r["fit"]["dy"], False, tuple(r["fit"]["origin"]))
        sh = {"lakes": {f"wbk:{w}": _wkt(g, tol) for w, g in sorted(geoms_used.items())},
              "before": _wkt(base, tol),
              "after": _wkt(refit.apply(sheet, refit.pose_of(got["after"]["fit"]), o), tol)}
        if got["would_help"]:
            sh["stretched"] = _wkt(refit.apply(sheet, refit.pose_of(got["would_help"]["fit"]), o), tol)
        refused = sorted((refit.RANK[st["fit"]["grade"]], -st["fit"]["iou"], name) for name, st in got["steps"].items()
                         if name in REFUSABLE and st.get("lifts") and not st.get("adopted"))
        if refused and refit.better(got["steps"][refused[0][2]]["fit"]["grade"], got["after"]["grade"]):
            st = got["steps"][refused[0][2]]
            sh["refused"] = {"step": refused[0][2], "target": st["target"],
                             "outline": _wkt(refit.apply(sheet, refit.pose_of(st["fit"]), o), tol)}
        shapes[f"{r['zip']}/{r['coverage']}"] = sh
    return rows, shapes


def _cls(r: dict, step: str) -> str:
    """the class of a row after `step` (a failure state when it was never fitted); a small stretch is its own class"""
    if r.get("state") != "fitted":
        return r["state"]
    if "refit" not in r:
        return r["grade"]
    g = r["refit"]["classes"][step]
    if step == "small_stretch" and r["refit"]["steps"]["small_stretch"].get("adopted"):
        return f"{g} (stretch <= 5 %)"
    return g


def summarise(rows: List[dict]) -> dict:
    """The counts after each step (cumulative) and the review lists."""
    from pipeline.bathymetry.refit import RANK, STEPS
    counts = {s: dict(sorted(Counter(_cls(r, s) for r in rows).items())) for s in STEPS}
    lists = defaultdict(list)
    for r in rows:
        f = r.get("refit")
        if not f:
            continue
        k = f"{r['zip']}/{r['coverage']}"
        if f["after"]["grade"] != r["grade"]:
            lists["changed_class"].append(k)
        st = f["steps"]
        if st["two_lake"].get("adopted"):
            lists["two_lake"].append(k)
        if st["other_lake"].get("adopted"):
            lists["other_lake"].append(k)
        if st["small_stretch"].get("adopted"):
            lists["small_stretch"].append(k)
        if f["would_help"]:
            lists["bigger_stretch_would_help"].append(k)
        # a lift a step found but refused (ambiguous, a name that differs, a lake the sheet does not draw), listed
        # only when no later step reached that class anyway
        refused = [st[s]["fit"]["grade"] for s in REFUSABLE
                   if st.get(s, {}).get("lifts") and not st[s].get("adopted")]
        if any(RANK[g] < RANK[f["after"]["grade"]] for g in refused):
            lists["lift_not_adopted"].append(k)
    return {"counts": counts, "lists": {k: sorted(v) for k, v in sorted(lists.items())},
            "list_sizes": {k: len(v) for k, v in sorted(lists.items())}}


CSV_COLS = ["zip", "coverage", "title", "codes", "units", "class_before", "class_after", "changed_by", "lake_before",
            "lake_after", "lake_names_after", "rotation_deg", "mirror", "scale", "iou_before", "iou_after",
            "median_m_before", "median_m_after", "area_ratio_after", "flags", "stretch_would_change",
            "stretch_scale", "stretch_iou", "stretch_class"]


def csv_rows(rows: List[dict], titles: Dict[str, List[str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_COLS, lineterminator="\n")
    w.writeheader()
    for r in rows:
        fit = r.get("fit") or {}
        f = r.get("refit")
        after = f["after"] if f else {"grade": r.get("grade", r["state"]), "step": "baseline",
                                      "target": fit.get("wbk", ""), "fit": dict(fit, mirror=False, scale=1.0)}
        af = after["fit"] or {}
        flags = list(f["flags"]) if f else []
        names = []
        if f:
            names = [f["lakes"].get(k, {}).get("name") or "unnamed" for k in after["target"].split("+")]
            if "other_lake" in flags:
                flags.append(f"name_{f['steps']['other_lake']['name_check']}")
        wh = f["would_help"] if f else None
        w.writerow({"zip": r["zip"], "coverage": r.get("coverage", ""), "title": "; ".join(titles.get(r["zip"], [])),
                    "codes": " ".join(r.get("codes", [])), "units": r.get("units", ""),
                    "class_before": r.get("grade", r["state"]), "class_after": _cls(r, "small_stretch"),
                    "changed_by": after["step"] if f and after["grade"] != r["grade"] else "",
                    "lake_before": fit.get("wbk", ""), "lake_after": after["target"], "lake_names_after": "; ".join(names),
                    "rotation_deg": af.get("angle_deg", ""), "mirror": af.get("mirror", "") if af else "",
                    "scale": af.get("scale", "") if af else "", "iou_before": fit.get("iou", ""),
                    "iou_after": af.get("iou", ""), "median_m_before": fit.get("median_m", ""),
                    "median_m_after": af.get("median_m", ""), "area_ratio_after": af.get("area_ratio", ""),
                    "flags": " ".join(sorted(flags)), "stretch_would_change": wh["change"] if wh else "",
                    "stretch_scale": wh["fit"]["scale"] if wh else "", "stretch_iou": wh["fit"]["iou"] if wh else "",
                    "stretch_class": wh["fit"]["grade"] if wh else ""})
    return buf.getvalue()


def _write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def main(argv=None) -> int:
    from multiprocessing import Pool
    from pipeline.bathymetry import accept, place_more
    from pipeline.bathymetry.contours import fwa_lakes
    from pipeline.bathymetry.fetch_vectors import sheets as sheet_list
    from pipeline.common.curated import GENERATED, SOURCE
    ap = argparse.ArgumentParser(description="the wider fitter over every vector depth sheet (T1188)")
    ap.add_argument("--vec-dir", type=Path, default=place_more.vec_dir(),
                    help="the vector files' directory outside git (its zips/ as fetch_vectors writes them)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--only", nargs="*", help="zip names")
    a = ap.parse_args(argv)
    zdir = a.vec_dir.expanduser() / "zips"
    if not zdir.is_dir():
        raise SystemExit(f"no vector zips at {zdir}: fetch them with "
                         f"`python -m pipeline.bathymetry.fetch_vectors --out {a.vec_dir}`")
    z2c, z2t = defaultdict(set), defaultdict(set)
    for s in sheet_list(SOURCE.fwa_gpkg):
        z2c[s["image_zip"]].add(s["wbid"])
        if s.get("title"):
            z2t[s["image_zip"]].add(s["title"])
    zips = sorted(p.name for p in zdir.glob("*.zip"))
    if a.only:
        missing = sorted(set(a.only) - set(zips))
        if missing:
            raise SystemExit(f"--only names zips not in {zdir}: {missing}")
        zips = [z for z in zips if z in set(a.only)]
    props = {p["chart_id"]: p for p in json.loads(accept.proposals_path("charts").read_text())["proposals"]}
    want = {z: place_more.candidates(z2c[z], props) for z in zips}
    allw = sorted({w for v in want.values() for w in v})
    geom, info = {}, {}
    for k in range(0, len(allw), 500):
        for lk in fwa_lakes(f"WATERBODY_KEY IN ({','.join(map(str, allw[k:k + 500]))})"):
            g = geom.get(lk["wbk"])
            geom[lk["wbk"]] = lk["geometry"] if g is None else g.union(lk["geometry"])
            info.setdefault(lk["wbk"], {"name": (lk["names"] or [None])[0], "layer": lk["layer"]})
    jobs = [(str(zdir / z), z2c[z], want[z], {w: geom[w].wkb for w in want[z] if w in geom},
             {w: info[w] for w in want[z] if w in info}, sorted(z2t.get(z, ()))) for z in zips]
    with Pool(max(1, min(a.workers, 4))) as pool:
        got = list(pool.imap(job, jobs, chunksize=1))
    rows = sorted((r for rs, _s in got for r in rs), key=lambda r: (r["zip"], r.get("coverage", "")))
    shapes = {k: v for _rs, s in got for k, v in s.items()}
    titles = {z: sorted(t) for z, t in z2t.items()}
    summ = summarise(rows)
    out = {"_about": "The wider fitter over every vector depth sheet (pipeline.bathymetry.refit_all, T1188). PROPOSALS: "
                     "each placing needs the owner's tick beside the PDF; a bigger stretch is never adopted.",
           "licence": LICENCE_FLAG, "steps": STEP_LABELS, **summ, "rows": rows}
    dest = Path(GENERATED.bathymetry)
    dest.mkdir(parents=True, exist_ok=True)
    name = "refit" if not a.only else "refit_only"
    _write(dest / f"{name}.json", json.dumps(out, indent=1, sort_keys=True))
    _write(dest / f"{name}.csv", csv_rows(rows, titles))
    _write(dest / f"{name}_shapes.json", json.dumps(dict(sorted(shapes.items())), indent=0, sort_keys=True))
    print(json.dumps({"counts": summ["counts"], "list_sizes": summ["list_sizes"]}, indent=1),
          f"\nrows {len(rows)}; wrote {dest / name}.json / .csv / _shapes.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
