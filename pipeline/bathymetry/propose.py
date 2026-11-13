"""Chart -> lake PROPOSALS under the owner's rules of 2026-11-11 (T634 item 5). Nothing here accepts anything.

THE ONE MATCHER. Built on `pipeline.deliver.lake_facts.core` (the name normaliser `bind` uses) and the deleted
`bathymetry/generate/audit.py`'s two-name test (printed title vs gazetted column, `git show 971f40575^:…`), plus the
position + size test of the T608 D audit. It replaces nothing that was sound: `lake_facts.bind` stays the binding rule
for the lakes file; `pipeline/stocking/generate/match.py` (2,000 m same-name fallback, substring names, nearest pick,
node ids) is NOT reused.

THE RULES, in the order tried; every chart id ends in exactly one:
  no_lake        the code is on no polygon of the FWA lakes layer (reservoir / wetland polygons carrying it are OFFERED)
  two_named      the code covers two or more NAMED lakes: the owner picks (owner 2026-11-11, A2)
  unnamed        the code's lakes are all unnamed: our name may have come from this chart, so agreement proves nothing
  name_disagrees no name on the chart agrees with the lake (refused until the owner accepts)
  gazetted_only  only the gazetted column agrees; the printed title does not (the column was filled FROM the code)
  variant_circular the title agrees only through a curated variant whose source is `bathymetry` or `stocking`: those
                 were written by the v1 matchers from these very rows, so they cannot confirm them. NOT clean (owner
                 2026-11-11): on the owner's review list with the rest, never in the bulk-acceptable set
  no_outline     no sheet outline to test position and size (open-table-only ids)
  position_or_size the outline's box centre is more than 100 m outside the lake's box, or its area is under half or
                 over double the lake's
  not_in_app     clean, but the lake is not an item of the release: HELD and listed, never dropped
  clean          one named lake, the title agrees, position and size agree
VERDICTS: `clean` (the only rule `accept --rule` may take in bulk: BULK_ACCEPTABLE), `held` (not_in_app), `review`
(every other rule: the owner's review list, `review` in the output, accepted one id at a time with `--ids`).
A name never picks a lake: the CODE picks; names, position and size only confirm or refuse. A code that also covers
unnamed ponds binds ONLY its named lake (owner 2026-11-11, A2); the ponds are listed in the evidence.

NAME AGREEMENT (`name_evidence`), strongest first: `equal` (core(title) == core(gazetted FWA name)), `word_order`
("Lake Maxwell" == "Maxwell Lake": a leading lake/L. moved), `variant` (a CURATED variant from
`CURATED.waters.name_variants` whose source is independent of the old matchers), `contains` (the lake's name as whole
words inside a longer title: "WILSON L. LITTLE WILSON L."). Sourced words are kept as sourced: titles and names are
stored unchanged beside the normalised forms.

The watershed code is EVIDENCE only, never a gate: the deleted stocking audit found the two sources encode it
differently and comparing them invented mismatches (Jessie, Twin).

OUTPUT: `GENERATED.base/bathymetry/proposals.json` (machine output, rewritten freely). The frozen file is written only
by `pipeline.bathymetry.accept`, on the owner's word.

    python -m pipeline.bathymetry.propose            # heavy-ish: reads the gpkg (via scripts/heavy.sh)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from pipeline.deliver.lake_facts import core

NEAR_M = 100.0
AREA_MIN, AREA_MAX = 0.5, 2.0
CIRCULAR_SOURCES = frozenset({"bathymetry", "stocking"})
#: (T634 third pass, owner 2026-11-11: build now, licence link owed by the owner) every row that draws on the sheet
#: layer (its outline, title or code) carries this flag; confirm before any production publish. The first pass's
#: `in_open_table: false` HOLD is lifted: those ids take the normal rules.
LICENCE_FLAG = "owner says OGL, catalogue says Access Only, unconfirmed"
_LEADING = re.compile(r"^(lake|l)\s+")


# ---- inputs, as plain data ------------------------------------------------------------------------------------------

@dataclass
class SheetRow:
    code: Optional[str]
    image: str                     # MAP_IMAGE_FILENAME (sheet layer) or the PDF name (open table)
    title: str
    gazetted: str
    sheet_no: Optional[int]
    in_layer: bool                 # in the sheet layer (Access Only)
    in_open: bool                  # in the open table (Open Government Licence)
    vectorized: Optional[str] = None
    wsc: Optional[str] = None
    outline: Optional[object] = None   # shapely geometry, EPSG:3005


@dataclass
class LakePoly:
    layer: str                     # lakes | manmade | wetlands
    code: str
    wbk: int
    gnis: Tuple[Tuple[Optional[int], Optional[str]], ...]
    area_m2: float
    bounds: Tuple[float, float, float, float]
    wsc50k: Optional[str] = None


@dataclass
class Lake:
    wbk: int
    layer: str
    gnis: List[Tuple[int, str]]
    area_m2: float
    bounds: Tuple[float, float, float, float]
    wsc50k: Set[str] = field(default_factory=set)

    @property
    def named(self) -> bool:
        return bool(self.gnis)


def lakes_by_code(polys: Iterable[LakePoly]) -> Dict[str, Dict[str, Dict[int, Lake]]]:
    """{code: {layer: {wbk: Lake}}} — polygons of one waterbody key folded into one lake (area summed, box unioned)."""
    out: Dict[str, Dict[str, Dict[int, Lake]]] = defaultdict(lambda: defaultdict(dict))
    for p in polys:
        cur = out[p.code][p.layer].get(p.wbk)
        names = [(int(g), n) for g, n in p.gnis if n]
        if cur is None:
            out[p.code][p.layer][p.wbk] = Lake(p.wbk, p.layer, names, p.area_m2, p.bounds,
                                              {p.wsc50k} if p.wsc50k else set())
        else:
            for nm in names:
                if nm not in cur.gnis:
                    cur.gnis.append(nm)
            cur.area_m2 += p.area_m2
            b = cur.bounds
            cur.bounds = (min(b[0], p.bounds[0]), min(b[1], p.bounds[1]), max(b[2], p.bounds[2]), max(b[3], p.bounds[3]))
            if p.wsc50k:
                cur.wsc50k.add(p.wsc50k)
    return out


def variants_by_lake(rows: Sequence[Mapping]) -> Tuple[Dict[int, List[Tuple[str, str]]], Dict[int, List[Tuple[str, str]]]]:
    """({wbk: [(name, source)]}, {gnis_id: [(name, source)]}) from the curated name variants file."""
    by_wbk: Dict[int, List[Tuple[str, str]]] = defaultdict(list)
    by_gnis: Dict[int, List[Tuple[str, str]]] = defaultdict(list)
    for r in rows:
        t = r.get("target") or {}
        names = [(n["name"], n.get("source") or "") for n in r.get("names", []) if n.get("name")]
        for w in t.get("wbks", []) or []:
            by_wbk[int(w)].extend(names)
        for g in t.get("gnis_ids", []) or []:
            by_gnis[int(g)].extend(names)
    return by_wbk, by_gnis


# ---- names ----------------------------------------------------------------------------------------------------------

def word_order(n: str) -> str:
    """core() with a LEADING lake / L. dropped: "lake maxwell" -> "maxwell" (core already drops a trailing one)."""
    return _LEADING.sub("", core(n))


def _contains(title: str, name: str) -> bool:
    t, n = f" {core(title)} ", core(name)
    return bool(n) and f" {n} " in t


def name_evidence(title: str, lake_names: Sequence[str], variants: Sequence[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """How the chart's printed title agrees with the lake, strongest first: (kind, the name it agreed with).
    kinds: equal | word_order | variant | variant_circular | contains. None when nothing agrees."""
    if not title or not core(title):
        return None
    ct, wt = core(title), word_order(title)
    for n in lake_names:
        if core(n) == ct:
            return "equal", n
    for n in lake_names:
        if word_order(n) == wt and wt:
            return "word_order", n
    indep = [(n, s) for n, s in variants if s not in CIRCULAR_SOURCES]
    circ = [(n, s) for n, s in variants if s in CIRCULAR_SOURCES]
    for n, s in indep:
        if core(n) == ct or (word_order(n) == wt and wt):
            return "variant", f"{n} ({s})"
    for n in lake_names:
        if _contains(title, n):
            return "contains", n
    for n, s in circ:
        if core(n) == ct or (word_order(n) == wt and wt):
            return "variant_circular", f"{n} ({s})"
    return None


# ---- position and size ----------------------------------------------------------------------------------------------

def position_size(outline, lake: Lake) -> Dict[str, float]:
    """The audit's test: the outline's box centre against the lake's box (metres outside it; 0 inside), and the
    outline's area over the lake's."""
    x0, y0, x1, y1 = outline.bounds
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    bx0, by0, bx1, by1 = lake.bounds
    dx = max(bx0 - cx, 0.0, cx - bx1)
    dy = max(by0 - cy, 0.0, cy - by1)
    return {"centre_outside_m": round((dx * dx + dy * dy) ** 0.5, 1),
            "area_ratio": round(outline.area / lake.area_m2, 4) if lake.area_m2 > 0 else float("inf")}


# ---- one chart id ---------------------------------------------------------------------------------------------------

def propose_one(code: str, sheets: Sequence[SheetRow], layers: Mapping[str, Mapping[int, Lake]],
                var_wbk: Mapping[int, List[Tuple[str, str]]], var_gnis: Mapping[int, List[Tuple[str, str]]],
                app: Mapping[str, Mapping]) -> dict:
    from shapely.ops import unary_union

    titles = sorted({s.title for s in sheets if s.title})
    gazetted = sorted({s.gazetted for s in sheets if s.gazetted})
    row = {"chart_id": code, "titles": titles, "gazetted": gazetted,
           "sheets": [{"image": s.image, "sheet_no": s.sheet_no, "title": s.title, "in_open_table": s.in_open,
                       "in_sheet_layer": s.in_layer, "vectorized": s.vectorized} for s in
                      sorted(sheets, key=lambda s: (s.sheet_no or 0, s.image))],
           "in_open_table": any(s.in_open for s in sheets), "wbks": [], "evidence": {}}
    if any(s.in_layer for s in sheets):
        row["licence"] = LICENCE_FLAG
    ev = row["evidence"]
    lakes = dict(layers.get("lakes", {}))
    if not lakes:
        offers = [{"wbk": f"wbk:{l.wbk}", "layer": l.layer, "names": [n for _, n in l.gnis]}
                  for layer in ("manmade", "wetlands") for l in layers.get(layer, {}).values()]
        ev["offered"] = offers
        return _done(row, "no_lake")
    named = [l for l in lakes.values() if l.named]
    unnamed = [l for l in lakes.values() if not l.named]
    if len(named) >= 2:
        ev["named_lakes"] = [{"wbk": f"wbk:{l.wbk}", "names": [n for _, n in l.gnis],
                              "area_ha": round(l.area_m2 / 1e4, 2)} for l in sorted(named, key=lambda l: l.wbk)]
        return _done(row, "two_named")
    if not named:
        ev["unnamed_lakes"] = [{"wbk": f"wbk:{l.wbk}", "area_ha": round(l.area_m2 / 1e4, 2)}
                               for l in sorted(unnamed, key=lambda l: l.wbk)]
        return _done(row, "unnamed")
    lake = named[0]
    row["wbks"] = [f"wbk:{lake.wbk}"]
    row["lake_names"] = [n for _, n in lake.gnis]
    row["gnis_id"] = lake.gnis[0][0]
    if unnamed:
        ev["unnamed_left_out"] = [{"wbk": f"wbk:{l.wbk}", "area_ha": round(l.area_m2 / 1e4, 2)}
                                  for l in sorted(unnamed, key=lambda l: l.wbk)]
    item = app.get(f"wbk:{lake.wbk}")
    row["in_app"] = item is not None
    row["has_rows"] = bool(item and item.get("has_rows"))
    variants = list(var_wbk.get(lake.wbk, []))
    for g, _ in lake.gnis:
        variants.extend(var_gnis.get(g, []))
    lake_names = [n for _, n in lake.gnis]
    best = None
    for t in titles:
        e = name_evidence(t, lake_names, variants)
        if e and (best is None or _RANK[e[0]] < _RANK[best[0]]):
            best = e
    gaz = None
    for g in gazetted:
        e = name_evidence(g, lake_names, [])
        if e and e[0] in ("equal", "word_order"):
            gaz = e
    ev["name_evidence"] = best[0] if best else None
    ev["name_agreed_with"] = best[1] if best else None
    ev["gazetted_agrees"] = gaz is not None
    # the sheet writes "129-360400-…", the lakes layer "129360400…": the same code, compared without the hyphens
    wsc_sheet = {s.wsc.replace("-", "") for s in sheets if s.wsc}
    wsc_lake = {w.replace("-", "") for w in lake.wsc50k}
    ev["wsc_agrees"] = (bool(wsc_sheet & wsc_lake) if wsc_sheet and wsc_lake else None)
    outlines = [s.outline for s in sheets if s.outline is not None]
    if outlines:
        ps = position_size(unary_union(outlines), lake)
        ev.update(ps)
        ev["position_size_ok"] = ps["centre_outside_m"] <= NEAR_M and AREA_MIN <= ps["area_ratio"] <= AREA_MAX
    if best is None:
        return _done(row, "gazetted_only" if gaz else "name_disagrees")
    if best[0] == "variant_circular":
        return _done(row, "variant_circular")
    if not outlines:
        return _done(row, "no_outline")
    if ev["centre_outside_m"] > NEAR_M or not (AREA_MIN <= ev["area_ratio"] <= AREA_MAX):
        return _done(row, "position_or_size")
    if not row["in_app"]:
        return _done(row, "not_in_app")
    return _done(row, "clean")


_RANK = {"equal": 0, "word_order": 1, "variant": 2, "contains": 3, "variant_circular": 4}
VERDICT = {"clean": "clean", "not_in_app": "held"}
BULK_ACCEPTABLE = frozenset({"clean"})
REVIEW_RULES = ("two_named", "unnamed", "no_lake", "name_disagrees", "gazetted_only", "variant_circular",
                "no_outline", "position_or_size")


def _done(row: dict, rule: str) -> dict:
    row["rule"] = rule
    row["verdict"] = VERDICT.get(rule, "review")
    return row


def propose(sheets: Sequence[SheetRow], polys: Iterable[LakePoly], variant_rows: Sequence[Mapping],
            app: Mapping[str, Mapping]) -> dict:
    by_code = lakes_by_code(polys)
    var_wbk, var_gnis = variants_by_lake(variant_rows)
    grouped: Dict[str, List[SheetRow]] = defaultdict(list)
    no_code = []
    for s in sheets:
        if s.code:
            grouped[s.code].append(s)
        else:
            no_code.append({"image": s.image, "title": s.title, "in_open_table": s.in_open,
                            "rule": "no_code", "verdict": "review"})
    rows = [propose_one(c, grouped[c], by_code.get(c, {}), var_wbk, var_gnis, app) for c in sorted(grouped)]
    counts = Counter(r["rule"] for r in rows)
    clean_by = Counter(r["evidence"].get("name_evidence") for r in rows if r["rule"] == "clean")
    review: Dict[str, List[str]] = {k: [] for k in REVIEW_RULES}
    for r in rows:
        if r["verdict"] == "review":
            review[r["rule"]].append(r["chart_id"])
    review["no_code"] = [s["image"] for s in no_code]
    return {"_about": "Chart -> lake PROPOSALS (pipeline.bathymetry.propose, T634). Nothing here is accepted; only "
                      "`python -m pipeline.bathymetry.accept` writes the frozen match file, on the owner's word.",
            "rules": {"near_m": NEAR_M, "area": [AREA_MIN, AREA_MAX], "circular_variant_sources": sorted(CIRCULAR_SOURCES)},
            "counts": dict(sorted(counts.items())),
            "counts_by_verdict": dict(sorted(Counter(r["verdict"] for r in rows).items())),
            "bulk_acceptable_rules": sorted(BULK_ACCEPTABLE), "review": review,
            "clean_by_name_evidence": dict(sorted((str(k), v) for k, v in clean_by.items())),
            "no_code_sheets": no_code, "proposals": rows}


# ---- reading the real inputs ----------------------------------------------------------------------------------------

def _sheet_no(text: Optional[str]) -> Optional[int]:
    """The open table writes "1", or "1 OF 1": the sheet is the leading number. Anything else raises."""
    t = (text or "").strip()
    if not t:
        return None
    m = re.match(r"^(\d+)(?:\s+OF\s+\d+)?$", t, re.I)
    if not m:
        raise ValueError(f"open table SHEET_NO {text!r} is not a sheet number")
    return int(m.group(1))


def read_inputs(gpkg: Path, open_csv: Path):
    """Sheets (layer + open table) and the FWA polygons carrying any chart code. Reads the 10 GB gpkg: minutes."""
    import fiona
    from shapely.geometry import shape

    sheets: List[SheetRow] = []
    with fiona.open(gpkg, layer="bathymetry_polygons") as src:
        for f in src:
            p = f["properties"]
            sheets.append(SheetRow(code=(p["WATERBODY_IDENTIFIER"] or "").strip() or None,
                                   image=p["MAP_IMAGE_FILENAME"] or "", title=p["MAP_TITLE"] or "",
                                   gazetted=p["GAZETTED_NAME"] or "", sheet_no=p["SHEET_NO"], in_layer=True,
                                   in_open=False, vectorized=p["VECTORIZED_FLAG"], wsc=p["NEW_WATERSHED_CODE"],
                                   outline=shape(f["geometry"]) if f["geometry"] else None))
    # one sheet file can be the chart of two codes ("WILSON L. LITTLE WILSON L."): every layer row of it is marked
    by_image: Dict[str, List[SheetRow]] = defaultdict(list)
    for s in sheets:
        by_image[s.image.rsplit(".", 1)[0].upper()].append(s)
    with open(open_csv, encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            stem = (r["MAP_IMAGE_FILENAME_PDF"] or "").rsplit(".", 1)[0].upper()
            code = (r["WATERBODY_IDENTIFIER_WSA_50K"] or "").strip() or None
            same = [s for s in by_image.get(stem, []) if s.code == code]
            if same:
                for s in same:
                    s.in_open = True
                continue
            sheets.append(SheetRow(code=code,
                                   image=r["MAP_IMAGE_FILENAME_PDF"], title=r["MAP_TITLE"] or "",
                                   gazetted=r["GAZETTED_NAME"] or "",
                                   sheet_no=_sheet_no(r["SHEET_NO"]),
                                   in_layer=False, in_open=True, wsc=r["WATERSHED_CODE_WSA_50K"] or None))
    codes = {s.code for s in sheets if s.code}
    polys: List[LakePoly] = []
    for layer in ("lakes", "manmade", "wetlands"):
        with fiona.open(gpkg, layer=layer) as src:
            for f in src:
                p = f["properties"]
                code = (p["WATERBODY_KEY_GROUP_CODE_50K"] or "").strip()
                if code not in codes:
                    continue
                g = shape(f["geometry"])
                polys.append(LakePoly(layer, code, int(p["WATERBODY_KEY"]),
                                      tuple((p[f"GNIS_ID_{k}"], p[f"GNIS_NAME_{k}"]) for k in (1, 2, 3)),
                                      g.area, g.bounds, p["WATERSHED_CODE_50K"]))
    return sheets, polys


def read_app(core_sqlite: Path) -> Dict[str, dict]:
    import sqlite3
    with sqlite3.connect(f"file:{core_sqlite}?mode=ro", uri=True) as db:
        return {r[0]: {"name": r[1], "has_rows": r[2]} for r in
                db.execute("select item_id, name, has_rows from item where item_id like 'wbk:%'")}


def main(argv=None) -> int:
    import gzip
    import shutil
    import tempfile

    from pipeline.common.curated import CURATED, GENERATED, SOURCE
    from project_config import get_config

    ap = argparse.ArgumentParser(description="chart -> lake proposals (T634)")
    ap.add_argument("--release", type=Path, help="a release directory (default: the one data/release/LATEST names)")
    a = ap.parse_args(argv)
    gpkg = Path(get_config().get_path("data", "fetch", "output_gpkg"))
    from pipeline.deliver.release.manifest import live
    rel = a.release or live(GENERATED.release)
    cores = sorted(p for p in Path(rel).glob("core-*.sqlite.gz") if p.name.count("-") == 1)
    if len(cores) != 1:
        raise SystemExit(f"{rel}: want one core-<hash>.sqlite.gz, found {[p.name for p in cores]}")
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "core.sqlite"
        with gzip.open(cores[0]) as src, open(db, "wb") as dst:
            shutil.copyfileobj(src, dst)
        app = read_app(db)
    variants = json.loads(Path(CURATED.waters.name_variants).read_text())
    sheets, polys = read_inputs(gpkg, SOURCE / "wsa_bathymetry_maps.csv")
    out = propose(sheets, polys, variants, app)
    out["inputs"] = {"release": Path(rel).name, "gpkg": gpkg.name, "open_table": "wsa_bathymetry_maps.csv"}
    dest = GENERATED.base / "bathymetry" / "proposals.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, indent=1, sort_keys=True, ensure_ascii=False))
    os.replace(tmp, dest)
    print(json.dumps({"counts": out["counts"], "counts_by_verdict": out["counts_by_verdict"],
                      "review": {k: len(v) for k, v in out["review"].items()}, "clean_by_name_evidence": out["clean_by_name_evidence"],
                      "no_code_sheets": len(out["no_code_sheets"])}, indent=1))
    print(f"wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
