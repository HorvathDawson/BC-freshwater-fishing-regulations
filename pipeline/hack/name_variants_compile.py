"""One-off compiler (docs/13): merge the current static name sources into ONE name_variants.json.

    .venv/bin/python -m pipeline.hack.name_variants_compile --out pipeline/name_variants.json

Sources: feature_display_names.json, overrides.json, and the stocking/bathy names in
the archived anglerinfo matches (wbk_names). FWA gazette stays live at build,
NOT here. Future stocking/bathy/gauge formats get their own appenders; this is the bootstrap.

Every entry is {"target": {blk|wbk|gnis_id|wsc: "..."}, "reach"?: {from_m,to_m}, "names":
[{name, source, note}]}. Skipped/unresolved inputs are logged (never silently dropped).
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from pipeline.atlas.fwa import FWADataAccessor
from pipeline.common.utils.wsc import trim_wsc
from pipeline.common.curated import CURATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]   # pipeline/oneoff/ -> repo root
_FDN = _ROOT / "archive" / "pipeline" / "matching" / "feature_display_names.json"
_OVR = _ROOT / "archive" / "pipeline" / "matching" / "overrides.json"
#: The archived anglerinfo matches. `add_anglerinfo` skips a file that is not there,
#: which is why this pointed at `output/pipeline/anglerinfo/` — a directory that never
#: existed — for as long as it did without anyone noticing the source was silently absent.
_ANG = _ROOT / "archive" / "pipeline" / "recurring" / "anglerinfo" / "anglerinfo_matches.json"
_GPKG = str(SOURCE / "bc_fisheries_data.gpkg")


def _clean(s) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return "" if s.lower() in ("", "nan", "none") else s


class _Compiler:
    def __init__(self, fwa: FWADataAccessor):
        self.fwa = fwa
        self.entries: list[dict] = []
        self.log: list[str] = []

    def emit(self, target: dict, names: list[dict], reach: Optional[dict] = None):
        names = [n for n in names if n.get("name")]
        target = {k: v for k, v in target.items() if v}   # drop empty id lists
        if target and names:
            e = {"target": target, "names": names}
            if reach:
                e["reach"] = reach
            self.entries.append(e)

    # ---- FWA resolvers (one-off, cached) ----
    def fids_to_blk_reach(self, fids: list[str]):
        g = self.fwa.get_features_by_attribute("streams", "LINEAR_FEATURE_ID", list(fids),
                                               ignore_geom=True)
        if g.empty:
            return None
        blks = set(g["BLUE_LINE_KEY"])
        if len(blks) != 1:
            self.log.append(f"fid-target spans {len(blks)} blks {sorted(blks)} — cannot use blk+reach")
            return None
        blk = next(iter(blks))
        dm = [float(x) for x in g["DOWNSTREAM_ROUTE_MEASURE"]]
        um = [float(x) for x in g["UPSTREAM_ROUTE_MEASURE"]]
        spans = sorted(zip(dm, um))
        for (d0, u0), (d1, _u1) in zip(spans, spans[1:]):
            if d1 - u0 > 1.0:
                self.log.append(f"fid-target on blk {blk} non-contiguous (gap at {u0:.0f}->{d1:.0f})")
        return blk, {"from_m": round(min(dm), 1), "to_m": round(max(um), 1)}

    def polys_to_wbks(self, poly_ids: list[str]) -> list[str]:
        if not poly_ids:
            return []
        out: list[str] = []
        for layer in ("lakes", "manmade", "wetlands"):
            if layer not in self.fwa.layer_names:
                continue
            g = self.fwa.get_features_by_attribute(layer, "WATERBODY_POLY_ID", list(poly_ids),
                                                   ignore_geom=True)
            out += [str(w) for w in g.get("WATERBODY_KEY", []) if w]
        if not out:
            self.log.append(f"poly_ids {poly_ids} did not resolve to any wbk")
        return sorted(set(out))

    def gazette_to_gnis(self, name: str) -> Optional[str]:
        g = self.fwa.get_features_by_attribute("streams", "GNIS_NAME", name, ignore_geom=True)
        gnis = sorted({str(x) for x in g.get("GNIS_ID", []) if x})
        if len(gnis) == 1:
            return gnis[0]
        self.log.append(f"gazette name {name!r} -> {len(gnis)} gnis (ambiguous/none); not resolved")
        return None

    # ---- source extractors ----
    def add_feature_display_names(self, path: Path):
        for e in json.loads(path.read_text()):
            note = _clean(e.get("note"))
            # provenance: a gauge-derived name (note says so) is `gauge`; the rest are manual
            # names added for regulation matching -> `regulation`.
            source = "gauge" if "gauge" in note.lower() else "regulation"
            names = []
            dn = _clean(e.get("display_name"))
            if dn:                                   # the authored DISPLAY name -> beats gazette
                names.append({"name": dn, "source": source, "note": note, "display": True})
            names += [{"name": _clean(v), "source": source, "note": note}
                      for v in e.get("name_variants", []) or []]
            blks = [str(b) for b in e.get("blue_line_keys", []) or []]
            wbks = [str(w) for w in e.get("waterbody_keys", []) or []]
            if blks:                                 # one entry, all blks (multi-blk side channels)
                self.emit({"blks": blks}, names)
            if wbks:
                self.emit({"wbks": wbks}, names)
            fids = e.get("linear_feature_ids", []) or []
            if fids:
                res = self.fids_to_blk_reach([str(f) for f in fids])
                if res:
                    blk, reach = res
                    self.emit({"blks": [blk]}, names, reach=reach)

    def _entry_ids(self, e: dict) -> list[tuple]:
        """The structured target id(s) of an override entry as (plural_key, id), before 1:1 gating."""
        ids: list[tuple] = []
        ids += [("gnis_ids", str(g)) for g in e.get("gnis_ids", []) or []]
        ids += [("wbks", str(w)) for w in e.get("waterbody_keys", []) or []]
        ids += [("wbks", w) for w in self.polys_to_wbks([str(p) for p in e.get("waterbody_poly_ids", []) or []])]
        ids += [("wscs", trim_wsc(str(w))) for w in e.get("fwa_watershed_codes", []) or []]
        ids += [("blks", str(b)) for b in e.get("blue_line_keys", []) or []]
        return ids

    def add_overrides(self, path: Path):
        data = json.loads(path.read_text())
        # index for variant_of resolution: (name_verbatim, region) -> entry
        by_crit: dict[tuple, dict] = {}
        for e in data:
            c = e.get("criteria", {})
            by_crit[(c.get("name_verbatim", ""), c.get("region", ""))] = e

        for e in data:
            c = e.get("criteria", {})
            region = c.get("region", "")
            mus = c.get("mus", []) or []
            note = "; ".join(x for x in [_clean(e.get("note")) or _clean(e.get("skip_reason")),
                                         region, ("MU " + ",".join(mus)) if mus else ""] if x)

            # variant_of (skipped in-season corrections): attach the variant name to the CANONICAL
            vo = e.get("variant_of")
            if vo:
                canon = by_crit.get((vo.get("name_verbatim", ""), vo.get("region", "")))
                ids = self._entry_ids(canon) if canon else []
                if not ids:                                   # e.g. Heber River — gazette lookup
                    gnis = self.gazette_to_gnis(vo.get("name_verbatim", "").title())
                    ids = [("gnis_ids", gnis)] if gnis else []
                variant_name = _clean(c.get("name_verbatim"))
                for key, val in ids:
                    self.emit({key: [val]}, [{"name": variant_name, "source": "regulation",
                                              "note": f"variant_of {vo.get('name_verbatim')}; {note}"}])
                if not ids:
                    self.log.append(f"variant_of unresolved: {c.get('name_verbatim')} -> {vo.get('name_verbatim')}")
                continue

            ids = self._entry_ids(e)
            if len(ids) != 1:
                if ids:
                    self.log.append(f"multi-id override not harvested (compound/group): "
                                    f"{c.get('name_verbatim')!r} -> {len(ids)} ids")
                continue                                       # 1:1 gate (Q6)
            names = [{"name": _clean(c.get("name_verbatim")), "source": "regulation", "note": note}]
            if _clean(e.get("canonical_name")):
                names.append({"name": _clean(e["canonical_name"]), "source": "regulation", "note": note})
            names += [{"name": _clean(v), "source": "regulation", "note": note}
                      for v in e.get("name_variants", []) or []]
            key, val = ids[0]
            self.emit({key: [val]}, names)

    def add_anglerinfo(self, path: Path):
        if not path.exists():
            self.log.append(f"anglerinfo matches not found at {path} — no stocking/bathy names")
            return
        wbk_names = json.loads(path.read_text()).get("wbk_names", {})
        for wbk, names in wbk_names.items():
            self.emit({"wbks": [str(wbk)]},
                      [{"name": _clean(n.get("name")), "source": n.get("source", "stocking"),
                        "note": ""} for n in names])


# Grounded manual entries that no source file yet carries — names inferred while authoring the
# curated splits (docs/04). Kept here so a regen preserves them; each records WHY (a `concern`
# elsewhere). Add sparingly and always with a note.
_MANUAL: list[dict] = [
    {"target": {"blks": ["360844922"]},
     "names": [{"name": "Sitkatapa Creek", "source": "regulation",
                "note": "INFERRED: FWA-unnamed direct tributary of Burnt Bridge Creek "
                        "(WSC 910-275583-777225-504013); named for Sitkatapa Lake up its upper "
                        "fork (blk 360855126). Backs the burnt_bridge_at_sitkatapa split; low "
                        "confidence, no alternative."}]},
    {"target": {"blks": ["360886970"]},
     "reach": {"from_m": 108816.1, "to_m": 111116.3},
     "names": [{"name": "Rainbow Alley", "source": "regulation",
                "note": "Local name for the flowing-water reach of the Babine River between "
                        "Babine Lake (wbk 329026676, upstream) and Nilkitkwa Lake (wbk 329026685, "
                        "downstream) on BLK 360886970 (connecting waterbody 329705328, route measure "
                        "108816.1-111116.3). Regulation locator 'between Babine and Nilkitkwa Lakes' / "
                        "'Rainbow Alley'. FWA gazettes this reach as Babine River, so this is a "
                        "searchable/matchable variant (does not override the display name). "
                        "Reach-scoped so it hits only the between-lakes pieces, not all of Babine "
                        "River or either lake. NOTE: an overrides.json entry cannot express a reach "
                        "window (1:1 id gate, no from_m/to_m), so _MANUAL is the correct home."}]},
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpkg", default=_GPKG)
    ap.add_argument("--out", default=str(_ROOT / "pipeline" / "name_variants.json"))
    args = ap.parse_args()

    c = _Compiler(FWADataAccessor(args.gpkg))
    c.add_feature_display_names(_FDN)
    c.add_overrides(_OVR)
    c.add_anglerinfo(_ANG)
    for m in _MANUAL:                      # grounded split-inferred names (docs/04)
        c.emit(m["target"], m["names"], m.get("reach"))

    Path(args.out).write_text(json.dumps(c.entries, indent=1))
    print(f"wrote {len(c.entries)} name-variant entries -> {args.out}")
    print(f"skipped/unresolved ({len(c.log)}):")
    for line in c.log[:40]:
        print("  -", line)


if __name__ == "__main__":
    main()
