"""Run the wider fitter (`refit`, T1188) over every vector depth sheet. MEASURES ONLY: the rows are proposals for the
owner's eye beside the PDF; nothing here is accepted, curated or shipped (depth does not ship).

Per zip: the baseline is the repo's fitter UNCHANGED (`place_more._one`: shore, candidate lakes, `place.fit`, grade).
Every sheet it grades loose or poor then goes through `refit.refit_sheet` (rigid wider, two lakes, wrong-lake code,
small stretch; the bigger stretch only listed). The failures (no shore, no candidate, unreadable, duplicate coverage)
are carried as they are. Then EVERY placed sheet, strict ones too, gets the trust checks (`check`): the best
neighbouring lake and its gap (`refit.neighbour_check`), a strict baseline's other turns and mirror
(`refit.orientation_check`), the shape's complexity (`refit.self_similarity`), the depth lines as a second witness
(`depth_on_land_pct`: the share more than ON_LAND_TOL_M outside our lake at the adopted placing; over
ON_LAND_FLAG_PCT it is flagged, a warning, never a tie-breaker); and its `confidence` (`confidence`). The shore's own
flags (`place.shore_flags`: a straight join over `place.JOIN_TRUSTED_M`, depth lines outside the shore) and a placing
on a depth line (`refit.depth_line_step`) are always review.

A lake is free (`claims`): a wrong-lake find that lands on a lake another sheet ends on is refused (that sheet is rerun
with the lake taken, until nothing changes); one that lands on a lake only coded for another sheet which itself moved
off is kept, flagged `takes_a_coded_lake`. Two sheets on one lake are legitimate only when it is coded for both (the
same chart code: Carp Lake and Mahood's two surveys today).

Inputs: the 2001 vector zips, outside git (`--vec-dir`, its `zips/` as `fetch_vectors` writes them), the sheet list
(`fetch_vectors.sheets(SOURCE.fwa_gpkg)`: zip -> codes, titles), the chart proposals (`GENERATED.bathymetry /
proposals.json`: code -> candidate lakes) and the FWA lakes (`SOURCE.fwa_gpkg`).
Outputs, under `GENERATED.bathymetry` (machine output, rewritten freely):
    refit.json         the counts after each step, the lists, one row per sheet
    refit.csv          one row per sheet: classes before / after, rotation, mirror, scale, IoU, flags
    refit_shapes.json  the outlines (WKT) behind the review pictures: lakes, the placing before / after / stretched /
                       refused (the best lift a step found but did not adopt)

    scripts/heavy.sh refit 30 -- python -m pipeline.bathymetry.refit_all     # 4 workers
    python -m pipeline.bathymetry.refit_all --resummarise    # the counts, lists, confidence and CSV again from
                                                             # refit.json's rows (no fitting)
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.bathymetry.place_more import LICENCE_FLAG

STEP_LABELS = {"baseline": "the repo's fitter", "rigid": "rigid, wider", "two_lake": "two-lake fit",
               "other_lake": "wrong-lake code", "small_stretch": "small stretch (<= 5 %)",
               "depth_line": "a depth line as the outline (safe rule)"}


#: the steps whose found lift can be refused (ambiguous, a name that differs, a lake the sheet does not draw)
REFUSABLE = ("rigid", "two_lake", "other_lake", "small_stretch")

#: below this complexity (`refit.self_similarity`) a shore is simple (round, oval): its strict fit is not high
#: confidence unless its lake is the only one of its size within the neighbour radius. Chosen from the distribution
#: over every placed sheet (T1188 rerun 2026-11-27; see the ticket).
SIMPLE_BELOW = 0.10
#: flags that make a placing ambiguous (another turn, the mirror or another lake within the margin)
AMBIGUOUS = ("rigid_ambiguous", "two_lake_ambiguous", "other_lake_ambiguous", "small_stretch_ambiguous",
             "orientation_ambiguous")
#: why a placing needs the owner's look, in the order a contact sheet groups it (the first that applies)
REASONS = ("fitted_on_depth_line", "shore_joined_straight", "depth_lines_outside_shore", "depth_lines_on_land",
           "neighbour_rival", "ambiguous", "takes_a_coded_lake", "other_lake", "two_lake", "rigid_wider", "mirror",
           "simple_shape", "stretch_at_limit", "loose", "poor")
SETTLE_ROUNDS = 6


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


def zip_names(zdir: Path, vec_dir: Path) -> List[str]:
    """The zips to run, sorted. A missing or empty folder raises, naming it and the fetch command (an empty folder
    must never overwrite good results with a 0-row refit.json)."""
    how = f"fetch them with `python -m pipeline.bathymetry.fetch_vectors --out {vec_dir}`"
    if not zdir.is_dir():
        raise SystemExit(f"no vector zips at {zdir}: {how}")
    zips = sorted(p.name for p in zdir.glob("*.zip"))
    if not zips:
        raise SystemExit(f"no vector zips in {zdir} (the folder is empty): {how}")
    return zips


def _sheet_of(zpath: str, coverage: str, gap_m: float):
    """the sheet's water (`place.shore`), its arcs as (depth, LineString), its depth lines (`place.depth_lines`)"""
    from shapely.geometry import LineString
    from pipeline.bathymetry import e00, place
    sh = {s.lines.name: s for s in e00.read_zip_all(zpath)}[coverage]
    cont, snd = sh.contours(), sh.soundings()
    return (place.shore(cont, snd, max_gap=gap_m), [(d, LineString(p)) for d, _fc, p in cont if len(p) >= 2],
            place.depth_lines(cont))


#: T1188 v5, the depth lines as a second witness: a depth line more than this far (m) outside our lake is on land
ON_LAND_TOL_M = 10.0
#: over this share (percent) of the depth-line length on land, the placing is flagged `depth_lines_on_land` (a
#: warning, never a tie-breaker: it favours the bigger lake). The 2026-11-28 experiment: 45 placings over it, all
#: already review; the worst high one 7.6 %.
ON_LAND_FLAG_PCT = 10.0


def depth_on_land_pct(deep, place_fn, target) -> Optional[float]:
    """percent of the depth lines' length more than ON_LAND_TOL_M outside `target` once placed by `place_fn`"""
    from pipeline.bathymetry import place
    if not deep:
        return None
    v = place.outside_share([place_fn(l) for l in deep], target, ON_LAND_TOL_M)
    return None if v is None else round(100 * v, 2)


def job(j):
    """One zip: the baseline rows, then every loose or poor sheet refitted (with its outlines for the pictures)."""
    zpath, codes, cands, geoms, info, titles, taken = j
    from shapely import wkb
    from shapely.ops import unary_union
    from pipeline.bathymetry import place, place_more, refit
    from pipeline.common.curated import SOURCE
    rows = place_more._one((zpath, codes, cands, geoms))
    lakes = {w: wkb.loads(g) for w, g in geoms.items()}
    near = near_lakes_from(SOURCE.fwa_gpkg)
    shapes = {}
    for r in rows:
        if r.get("state") != "fitted":
            continue
        sheet, lines, deep = _sheet_of(zpath, r["coverage"], r["gap_m"])
        c = sheet.centroid
        o = (c.x, c.y)
        tol = max(1.0, (sheet.area ** 0.5) / 400)
        base = place._place(sheet, r["fit"]["angle_deg"], r["fit"]["dx"], r["fit"]["dy"], False, tuple(r["fit"]["origin"]))
        bw = int(r["fit"]["wbk"].split(":")[1])
        check: Dict[str, object] = {"orientation": None}
        outline, line_placed = sheet, None   # the outline fitted: the shore, or a depth line (refit step 5)
        if r["grade"] == "strict":       # the repo's fitter placed it strict: checked, never refitted
            got, geoms_used = None, {bw: lakes[bw]}
            final_fit, members, placed = r["fit"], [bw], base
            check["orientation"] = refit.orientation_check(sheet, lakes[bw], o, r["fit"])
            fo = tuple(r["fit"]["origin"])

            def place_fn(g, f=r["fit"], fo=fo):
                return place._place(g, f["angle_deg"], f["dx"], f["dy"], False, fo)
        else:
            coded = {w: {"geom": lakes[w], **info.get(w, {})} for w in sorted(lakes)}
            got = refit.refit_sheet(sheet, lines, coded, near, base_fit=r["fit"], units=r["units"], titles=titles,
                                    taken=taken)
            geoms_used = got.pop("_geoms")
            outline = got.pop("_outline")
            r["refit"] = got
            o = tuple(got["origin"])
            final_fit, members = got["after"]["fit"], refit._members(got["after"]["target"])
            placed = refit.apply(sheet, refit.pose_of(final_fit), o)

            def place_fn(g, f=final_fit, o=o):
                return refit.apply(g, refit.pose_of(f), o)
        target = unary_union([geoms_used[w] for w in members])
        dl = (got or {}).get("steps", {}).get("depth_line", {})
        r["fit_line"] = dl.get("line", "shore") if dl.get("adopted") else "shore"
        if dl.get("adopted"):
            r["fit_line_depth"] = dl["depth"]
            line_placed = place_fn(outline)
        r["shore_over_lake"] = round(sheet.area / target.area, 3)
        r["depth_on_land_pct"] = depth_on_land_pct(deep, place_fn, target)
        if dl.get("adopted"):
            # the depth line's own neighbour check (its shape and size, its unstretched IoU) is the placing's
            nb, nb_geom = dl["neighbour"], None
            if nb.get("lake"):
                nb_geom = near(target.bounds, refit.NEIGHBOUR_M)[int(nb["lake"].split(":")[1])]["geom"]
        else:
            # the neighbours are fitted without a stretch: the gap is from the placing before its stretch
            own = float(final_fit["iou"]) if got is None else refit.unstretched_iou(got)
            nb, nb_geom = refit.neighbour_check(sheet, o, own, target, members, near)
        check["neighbour"] = nb
        check["shape"] = refit.self_similarity(outline, o)
        check["lakes"] = {f"wbk:{w}": {"name": (info.get(w) or {}).get("name")} for w in members if w in lakes}
        r["check"] = check
        sh = {"lakes": {f"wbk:{w}": _wkt(g, tol) for w, g in sorted(geoms_used.items())},
              "before": _wkt(base, tol), "after": _wkt(placed, tol)}
        if line_placed is not None:
            sh["after_line"] = _wkt(line_placed, tol)
        if nb_geom is not None:
            sh["lakes"][nb["lake"]] = _wkt(nb_geom, tol)
            sh["neighbour"] = _wkt(refit.apply(outline, refit.pose_of(nb["fit"]), o), tol)
        if got is None:
            shapes[f"{r['zip']}/{r['coverage']}"] = sh
            continue
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


def _key(r: dict) -> str:
    return f"{r['zip']}/{r.get('coverage', '')}"


def final_lakes(r: dict) -> List[int]:
    """the lake or lakes a placed row ends on"""
    t = r["refit"]["after"]["target"] if "refit" in r else r["fit"]["wbk"]
    return [int(k.split(":")[1]) for k in t.split("+")]


def claims(rows: List[dict], coded_by: Dict[int, List[str]]):
    """Is every found lake free? A found lake: a wrong-lake find, or a neighbour pulled into a two-lake union (a
    member not coded for the sheet). `coded_by`: wbk -> the zips whose code names it. Returns (refuse, takes):
    refuse[row key] = {wbk: the other rows that end on it} (the find must be refused); takes[row key] = {wbk: the zips
    it is coded for} (kept: those sheets moved off it; flagged for the owner). Two rows on one lake are legitimate
    only when it is coded for both."""
    on: Dict[int, List[str]] = defaultdict(list)
    for r in rows:
        if r.get("state") == "fitted":
            for w in final_lakes(r):
                on[w].append(_key(r))
    refuse, takes = {}, {}
    for r in rows:
        st = r.get("refit", {}).get("steps", {}) if r.get("state") == "fitted" else {}
        if not (st.get("other_lake", {}).get("adopted") or st.get("two_lake", {}).get("adopted")):
            continue
        k = _key(r)
        for w in final_lakes(r):
            if r["zip"] in coded_by.get(w, ()):
                continue                      # its own coded lake
            others = sorted(x for x in on[w] if x != k)
            coded = sorted(z for z in coded_by.get(w, ()) if z != r["zip"])
            if others:
                refuse.setdefault(k, {})[w] = others
            elif coded:
                takes.setdefault(k, {})[w] = coded
    return dict(sorted(refuse.items())), dict(sorted(takes.items()))


def settle(results: Dict[str, tuple], coded_by: Dict[int, List[str]], rerun) -> Dict[str, Dict[int, List[str]]]:
    """Rerun the zips whose wrong-lake find takes a lake another sheet ends on, with that lake taken, until no find
    does (`claims`); then flag the finds that take a lake coded for a sheet that moved off. `results`: zip -> (rows,
    shapes), updated in place; `rerun(zips, taken)` -> [(rows, shapes)] in that order. Returns the taken lakes per
    zip. Raises when it does not settle in SETTLE_ROUNDS."""
    taken: Dict[str, Dict[int, List[str]]] = defaultdict(dict)
    for rnd in range(SETTLE_ROUNDS + 1):
        rows = [r for z in sorted(results) for r in results[z][0]]
        refuse, takes = claims(rows, coded_by)
        if not refuse:
            break
        if rnd == SETTLE_ROUNDS:
            raise RuntimeError(f"the free-lake check (refit_all.claims) did not settle in {SETTLE_ROUNDS} rounds: "
                               f"still refused {sorted(refuse)}; rerun `python -m pipeline.bathymetry.refit_all "
                               f"--only {' '.join(sorted({k.split('/')[0] for k in refuse}))}` to look")
        for k, lk in refuse.items():
            z = k.split("/")[0]
            for w, who in lk.items():
                taken[z][w] = sorted(set(taken[z].get(w, [])) | set(who))
        redo = sorted({k.split("/")[0] for k in refuse})
        for z, got in zip(redo, rerun(redo, {z: dict(taken[z]) for z in redo})):
            results[z] = got
    for r in (r for z in sorted(results) for r in results[z][0]):
        lk = takes.get(_key(r))
        if lk:
            r["refit"]["takes_a_coded_lake"] = {f"wbk:{w}": v for w, v in sorted(lk.items())}
            r["refit"]["flags"] = sorted(set(r["refit"]["flags"]) | {"takes_a_coded_lake"})
    return {z: v for z, v in sorted(taken.items())}


def all_flags(r: dict) -> List[str]:
    """the row's refit flags, its shore's flags (`place.shore_flags`: `shore_joined_straight`,
    `depth_lines_outside_shore`) and its check flags (`neighbour_rival`, `orientation_ambiguous`,
    `depth_lines_on_land`)"""
    fl = set(r.get("refit", {}).get("flags", []))
    fl |= set((r.get("shore") or {}).get("flags", []))
    if float(r.get("depth_on_land_pct") or 0) > ON_LAND_FLAG_PCT:
        fl.add("depth_lines_on_land")
    ch = r.get("check") or {}
    if (ch.get("neighbour") or {}).get("rival"):
        fl.add("neighbour_rival")
    if (ch.get("orientation") or {}).get("ambiguous"):
        fl.add("orientation_ambiguous")
    return sorted(fl)


def confidence(r: dict) -> Tuple[str, List[str]]:
    """(`high` | `high_stretched` | `review`, why). high: strict on its own coded lake, no stretch, no flag that it is
    ambiguous, has a neighbour rival, is a wrong-lake or two-lake find, was moved by the wider rigid search
    (`rigid_wider`: the owner has not seen that placing), takes a coded lake or is mirrored, and a shore
    that is not simple (`SIMPLE_BELOW`) unless its lake is the only one of its size within the radius. high_stretched:
    the same with a stretch of 5 % or less, not at the limit. Everything else, the unplaced too: review."""
    if r.get("state") != "fitted":
        return "review", ["not_placed"]
    f = r.get("refit")
    after = f["after"] if f else {"grade": r["grade"], "step": "baseline", "fit": dict(r["fit"], mirror=False)}
    fl = all_flags(r)
    why = []
    for x in ("fitted_on_depth_line", "shore_joined_straight", "depth_lines_outside_shore", "depth_lines_on_land"):
        if x in fl:                                   # T1188 v5: the shore or the depth lines say look
            why.append(x)
    if "neighbour_rival" in fl:
        why.append("neighbour_rival")
    if any(x in fl for x in AMBIGUOUS):
        why.append("ambiguous")
    if "takes_a_coded_lake" in fl:
        why.append("takes_a_coded_lake")
    for step in ("other_lake", "two_lake"):          # from the flags: a later stretch overwrites after["step"]
        if step in fl:
            why.append(step)
    if "rigid_wider" in fl:                           # a placing the wider search changed: he has never seen it
        why.append("rigid_wider")
    if after["fit"].get("mirror") or "mirror" in fl:
        why.append("mirror")
    ch = r.get("check") or {}
    cx = (ch.get("shape") or {}).get("complexity")
    if cx is not None and cx < SIMPLE_BELOW and (ch.get("neighbour") or {}).get("similar_size", 1) > 0:
        why.append("simple_shape")
    stretched = _stretched(r)
    if "small_stretch_at_limit" in fl:
        why.append("stretch_at_limit")
    if after["grade"] != "strict":
        why.append(after["grade"])
    if why:
        return "review", why
    return ("high_stretched" if stretched else "high"), []


def _stretched(r: dict, step: str = "depth_line") -> bool:
    """is the row's placing after `step` stretched (a small stretch, or a depth line fitted with one)"""
    f = r.get("refit")
    if not f or step not in ("small_stretch", "depth_line"):
        return False
    dl = f["steps"].get("depth_line", {})
    if step == "depth_line" and dl.get("adopted"):
        return abs(float(dl["fit"].get("scale", 1.0)) - 1) > 1e-9
    return bool(f["steps"]["small_stretch"].get("adopted"))


def _cls(r: dict, step: str) -> str:
    """the class of a row after `step` (a failure state when it was never fitted); a small stretch is its own class"""
    if r.get("state") != "fitted":
        return r["state"]
    if "refit" not in r:
        return r["grade"]
    g = r["refit"]["classes"][step]
    return f"{g} (stretch <= 5 %)" if _stretched(r, step) else g


def summarise(rows: List[dict]) -> dict:
    """The counts after each step (cumulative) and the review lists."""
    from pipeline.bathymetry.refit import RANK, STEPS
    counts = {s: dict(sorted(Counter(_cls(r, s) for r in rows).items())) for s in STEPS}
    lists = defaultdict(list)
    conf = Counter()
    for r in rows:
        c, why = confidence(r)
        r["confidence"], r["review_reasons"] = c, why
        conf[c] += 1
        if c == "high_stretched":
            lists["high_stretched"].append(_key(r))
        elif c == "review" and why != ["not_placed"]:
            lists[f"review_{why[0]}"].append(_key(r))
        for x in ("neighbour_rival", "shore_joined_straight", "depth_lines_outside_shore", "depth_lines_on_land"):
            if x in all_flags(r):
                lists[x].append(_key(r))
        if any(j["how"] == "arcs" for j in (r.get("shore") or {}).get("joins", [])):
            lists["shore_completed_with_arcs"].append(_key(r))
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
        if st.get("depth_line", {}).get("adopted"):
            lists["fitted_on_depth_line"].append(k)
        if f["would_help"]:
            lists["bigger_stretch_would_help"].append(k)
        # a lift a step found but refused (ambiguous, a name that differs, a lake the sheet does not draw), listed
        # only when no later step reached that class anyway
        refused = [st[s]["fit"]["grade"] for s in REFUSABLE
                   if st.get(s, {}).get("lifts") and not st[s].get("adopted")]
        if any(RANK[g] < RANK[f["after"]["grade"]] for g in refused):
            lists["lift_not_adopted"].append(k)
        if f.get("takes_a_coded_lake"):
            lists["takes_a_coded_lake"].append(k)
        if st["other_lake"].get("taken_by"):
            lists["other_lake_refused_taken"].append(k)
        if st["two_lake"].get("taken_by"):
            lists["two_lake_refused_taken"].append(k)
    cx = sorted(r["check"]["shape"]["complexity"] for r in rows if r.get("check"))
    shape = {"simple_below": SIMPLE_BELOW, "placed": len(cx), "simple": sum(1 for v in cx if v < SIMPLE_BELOW),
             "min": cx[0] if cx else None, "median": cx[len(cx) // 2] if cx else None, "max": cx[-1] if cx else None}
    return {"counts": counts, "confidence": dict(sorted(conf.items())), "complexity": shape,
            "lists": {k: sorted(v) for k, v in sorted(lists.items())},
            "list_sizes": {k: len(v) for k, v in sorted(lists.items())}}


CSV_COLS = ["zip", "coverage", "title", "codes", "units", "class_before", "class_after", "changed_by", "lake_before",
            "lake_after", "lake_names_after", "rotation_deg", "mirror", "scale", "iou_before", "iou_after",
            "median_m_before", "median_m_after", "area_ratio_after", "flags", "stretch_would_change",
            "stretch_scale", "stretch_iou", "stretch_class", "neighbour_lake", "neighbour_name", "neighbour_iou",
            "neighbour_gap", "similar_size_lakes", "complexity", "confidence", "review_reasons", "fit_line",
            "fit_line_depth", "shore_over_lake", "depth_on_land_pct", "depth_lines_outside_shore_pct",
            "longest_straight_join_m", "shore_joins"]


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
        flags = all_flags(r) if r.get("state") == "fitted" else []
        ch = r.get("check") or {}
        nb = ch.get("neighbour") or {}
        sh = r.get("shore") or {}
        conf, why = confidence(r)
        names = []
        if f:
            names = [f["lakes"].get(k, {}).get("name") or "unnamed" for k in after["target"].split("+")]
            if "other_lake" in flags:
                flags.append(f"name_{f['steps']['other_lake']['name_check']}")
        wh = f["would_help"] if f else None
        w.writerow({"zip": r["zip"], "coverage": r.get("coverage", ""), "title": "; ".join(titles.get(r["zip"], [])),
                    "codes": " ".join(r.get("codes", [])), "units": r.get("units", ""),
                    "class_before": r.get("grade", r["state"]), "class_after": _cls(r, "depth_line"),
                    "changed_by": after["step"] if f and after["grade"] != r["grade"] else "",
                    "lake_before": fit.get("wbk", ""), "lake_after": after["target"], "lake_names_after": "; ".join(names),
                    "rotation_deg": af.get("angle_deg", ""), "mirror": af.get("mirror", "") if af else "",
                    "scale": af.get("scale", "") if af else "", "iou_before": fit.get("iou", ""),
                    "iou_after": af.get("iou", ""), "median_m_before": fit.get("median_m", ""),
                    "median_m_after": af.get("median_m", ""), "area_ratio_after": af.get("area_ratio", ""),
                    "flags": " ".join(sorted(flags)), "stretch_would_change": wh["change"] if wh else "",
                    "stretch_scale": wh["fit"]["scale"] if wh else "", "stretch_iou": wh["fit"]["iou"] if wh else "",
                    "stretch_class": wh["fit"]["grade"] if wh else "",
                    "neighbour_lake": nb.get("lake") or "", "neighbour_name": nb.get("name") or "",
                    "neighbour_iou": nb.get("iou", ""), "neighbour_gap": nb.get("gap", ""),
                    "similar_size_lakes": nb.get("similar_size", ""),
                    "complexity": (ch.get("shape") or {}).get("complexity", ""), "confidence": conf,
                    "review_reasons": " ".join(why), "fit_line": r.get("fit_line", ""),
                    "fit_line_depth": r.get("fit_line_depth", ""), "shore_over_lake": r.get("shore_over_lake", ""),
                    "depth_on_land_pct": _blank(r.get("depth_on_land_pct")),
                    "depth_lines_outside_shore_pct": _blank(sh.get("depth_lines_outside_shore_pct")),
                    "longest_straight_join_m": _blank(sh.get("longest_straight_join_m")),
                    "shore_joins": " ".join(f"{j['m']:g}:{j['how']}" + (f"({'/'.join(j['codes'])} {j['arcs_m']:g})"
                                                                         if j["how"] == "arcs" else "")
                                            for j in sh.get("joins", []))})
    return buf.getvalue()


def _blank(v) -> object:
    return "" if v is None else v


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
    ap.add_argument("--resummarise", action="store_true",
                    help="no fitting: the counts, lists, confidence and CSV again from refit.json's rows")
    a = ap.parse_args(argv)
    if a.resummarise:
        return resummarise()
    zdir = a.vec_dir.expanduser() / "zips"
    zips = zip_names(zdir, a.vec_dir)
    z2c, z2t = defaultdict(set), defaultdict(set)
    for s in sheet_list(SOURCE.fwa_gpkg):
        z2c[s["image_zip"]].add(s["wbid"])
        if s.get("title"):
            z2t[s["image_zip"]].add(s["title"])
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
    coded_by: Dict[int, List[str]] = defaultdict(list)
    for z in zips:
        for w in want[z]:
            coded_by[w].append(z)

    def jobs_for(zs, taken):
        return [(str(zdir / z), z2c[z], want[z], {w: geom[w].wkb for w in want[z] if w in geom},
                 {w: info[w] for w in want[z] if w in info}, sorted(z2t.get(z, ())), taken.get(z, {})) for z in zs]

    with Pool(max(1, min(a.workers, 4))) as pool:
        results = dict(zip(zips, pool.imap(job, jobs_for(zips, {}), chunksize=1)))
        taken = settle(results, coded_by, lambda zs, tk: list(pool.imap(job, jobs_for(zs, tk), chunksize=1)))
    got = [results[z] for z in sorted(results)]
    rows = sorted((r for rs, _s in got for r in rs), key=lambda r: (r["zip"], r.get("coverage", "")))
    shapes = {k: v for _rs, s in got for k, v in s.items()}
    titles = {z: sorted(t) for z, t in z2t.items()}
    name = "refit" if not a.only else "refit_only"
    return _publish(name, rows, titles, shapes, {z: {f"wbk:{w}": v for w, v in tk.items()} for z, tk in taken.items()})


ABOUT = ("The wider fitter over every vector depth sheet (pipeline.bathymetry.refit_all, T1188). PROPOSALS: each "
         "placing needs the owner's tick beside the PDF; a bigger stretch is never adopted; `confidence` only says "
         "which ones need a closer look.")


def _publish(name: str, rows: List[dict], titles: Dict[str, List[str]], shapes, taken) -> int:
    from pipeline.common.curated import GENERATED
    summ = summarise(rows)
    out = {"_about": ABOUT, "licence": LICENCE_FLAG, "steps": STEP_LABELS, **summ, "taken": taken,
           "titles": titles, "rows": rows}
    dest = Path(GENERATED.bathymetry)
    dest.mkdir(parents=True, exist_ok=True)
    _write(dest / f"{name}.json", json.dumps(out, indent=1, sort_keys=True))
    _write(dest / f"{name}.csv", csv_rows(rows, titles))
    if shapes is not None:
        _write(dest / f"{name}_shapes.json", json.dumps(dict(sorted(shapes.items())), indent=0, sort_keys=True))
    print(json.dumps({"counts": summ["counts"], "confidence": summ["confidence"], "complexity": summ["complexity"],
                      "list_sizes": summ["list_sizes"]}, indent=1),
          f"\nrows {len(rows)}; wrote {dest / name}.json / .csv" + (" / _shapes.json" if shapes is not None else ""))
    return 0


def resummarise() -> int:
    """refit.json's rows summarised again (a changed SIMPLE_BELOW or list): no fitting, the shapes kept."""
    from pipeline.common.curated import GENERATED
    p = Path(GENERATED.bathymetry) / "refit.json"
    if not p.exists():
        raise SystemExit(f"no {p} to summarise: run `python -m pipeline.bathymetry.refit_all` first")
    d = json.loads(p.read_text())
    if any("refit" in r and "depth_line" not in r["refit"]["classes"] for r in d["rows"]):
        raise SystemExit(f"{p} predates step 5 (no depth_line class, T1188 v5): rerun "
                         f"`python -m pipeline.bathymetry.refit_all`")
    if "titles" not in d:
        raise SystemExit(f"{p} predates --resummarise (no titles): rerun `python -m pipeline.bathymetry.refit_all`")
    return _publish("refit", d["rows"], d["titles"], None, d.get("taken", {}))


if __name__ == "__main__":
    raise SystemExit(main())
