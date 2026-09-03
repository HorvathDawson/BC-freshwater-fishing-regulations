"""Lossless dedup + repair of the compiled ``name_variants.json`` (operates on the EXISTING file).

Why a transform and not a recompile: the compiler (`name_variants_compile.py`) reads
`feature_display_names.json`, which no longer exists in the archive — so a recompile would both crash
and drop the FDN-derived regulation names. The existing `name_variants.json` already holds every name,
so we repair it in place, never dropping a distinct name (asserted at the end).

Three repairs, all identity-safe (authoritative FWA joins, never name matching):

1. **gnis→wbk for lakes.** A `gnis_ids` target never lands on a lake node (lake nodes carry no gnis —
   they're keyed by WATERBODY_KEY), so a regulation-sourced lake name curated by GNIS (e.g. Ballon
   Lake `gnis:18257`) was silently lost at graph-apply time. We resolve each `gnis_ids` target through
   the FWA lakes/manmade `GNIS_ID_{1,2,3} → WATERBODY_KEY` map; if it's a lake, the target becomes
   `wbks` (original gnis kept in `_gnis` for provenance). Stream GNIS (no lake hit) is left untouched —
   stream nodes DO carry gnis.
2. **Merge same-target groups.** After (1), the regulation `wbk` group and the stocking/bathymetry
   `wbk` group for one lake collide on the same target; merge them into one (union names, dedup by
   (name, source), keep any `display` flag, keep distinct notes). This is the Ballon/Baillon fix.
3. **Fold in dropped single-feature name overrides.** Archive overrides whose only target is
   `linear_feature_ids` (Sicamous Narrows, "Diana" Creek) were dropped by the compiler's `_entry_ids`.
   Resolve the fids → one blk + route-measure reach and add them as reach-scoped blk groups.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.hack.name_variants_dedup --dry-run
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.hack.name_variants_dedup            # writes
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

from data.data_extractor import FWADataAccessor
from pipeline.common.curated import CURATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]
_NV = _ROOT / "pipeline" / "name_variants.json"
_OVR = _ROOT / "archive" / "pipeline" / "matching" / "overrides.json"
_GPKG = str(SOURCE / "bc_fisheries_data.gpkg")


def build_gnis_to_wbk(fwa: FWADataAccessor) -> dict[str, set[str]]:
    """Authoritative GNIS_ID → {WATERBODY_KEY} from the FWA lake/manmade polygons (up to 3 GNIS each)."""
    g2w: dict[str, set[str]] = defaultdict(set)
    for layer in ("lakes", "manmade"):
        if layer not in fwa.layer_names:
            continue
        df = fwa.get_layer(layer, columns=["WATERBODY_KEY", "GNIS_ID_1", "GNIS_ID_2", "GNIS_ID_3"])
        for _, r in df.iterrows():
            wbk = r["WATERBODY_KEY"]
            if wbk is None:
                continue
            for c in ("GNIS_ID_1", "GNIS_ID_2", "GNIS_ID_3"):
                g = r[c]
                if g is not None and str(g).strip() and str(g) != "nan" and int(g) != 0:
                    g2w[str(int(g))].add(str(int(wbk)))
    return g2w


def _target_key(target: dict, reach) -> tuple:
    """Canonical, order-independent identity of a group's target (+reach) for merging."""
    parts = []
    for k in ("blks", "wbks", "gnis_ids", "wscs"):
        for v in sorted(str(x) for x in (target.get(k) or [])):
            parts.append((k, v))
    r = (round(reach["from_m"], 1), round(reach["to_m"], 1)) if reach else None
    return (tuple(sorted(parts)), r)


def _merge_names(dest: list[dict], src: list[dict]) -> None:
    """Append src names into dest, deduped by (name, source); keep any display flag; keep distinct notes."""
    index = {(n["name"], n.get("source", "")): n for n in dest}
    for n in src:
        key = (n["name"], n.get("source", ""))
        if key in index:
            cur = index[key]
            if n.get("display"):
                cur["display"] = True
            if n.get("note") and n["note"] not in (cur.get("note") or ""):
                cur["note"] = "; ".join(x for x in [cur.get("note", ""), n["note"]] if x)
        else:
            dest.append(dict(n))
            index[key] = dest[-1]


def fids_to_blk_reach(fwa: FWADataAccessor, fids: list[str]):
    g = fwa.get_features_by_attribute("streams", "LINEAR_FEATURE_ID", list(fids), ignore_geom=True)
    if g.empty:
        return None
    blks = set(g["BLUE_LINE_KEY"])
    if len(blks) != 1:
        return None
    dm = [float(x) for x in g["DOWNSTREAM_ROUTE_MEASURE"]]
    um = [float(x) for x in g["UPSTREAM_ROUTE_MEASURE"]]
    return str(int(next(iter(blks)))), {"from_m": round(min(dm), 1), "to_m": round(max(um), 1)}


def dropped_fid_overrides(fwa: FWADataAccessor) -> list[dict]:
    """The archive overrides whose ONLY structured target is linear_feature_ids (compiler drops them),
    resolved to reach-scoped blk name-variant groups."""
    out: list[dict] = []
    for e in json.loads(_OVR.read_text()):
        c = e.get("criteria", {})
        fids = e.get("linear_feature_ids") or []
        other = any(e.get(k) for k in ("gnis_ids", "waterbody_keys", "waterbody_poly_ids",
                                       "fwa_watershed_codes", "blue_line_keys"))
        if not fids or other:
            continue
        res = fids_to_blk_reach(fwa, [str(f) for f in fids])
        if not res:
            print(f"  ! fid override {c.get('name_verbatim')!r} did not resolve to one blk — skipped")
            continue
        blk, reach = res
        mus = c.get("mus", []) or []
        note = "; ".join(x for x in [e.get("note", ""), c.get("region", ""),
                                     ("MU " + ",".join(mus)) if mus else ""] if x)
        out.append({"target": {"blks": [blk]}, "reach": reach,
                    "names": [{"name": c.get("name_verbatim", ""), "source": "regulation",
                               "note": note, "display": True}]})
    return out


def run(dry: bool) -> None:
    nv = json.loads(_NV.read_text())
    before_names = {(n["name"], n.get("source", "")) for e in nv for n in e["names"]}
    fwa = FWADataAccessor(_GPKG)
    g2w = build_gnis_to_wbk(fwa)

    # 1. gnis-lake targets -> wbk
    n_to_wbk = n_multi = 0
    for e in nv:
        t = e["target"]
        gids = [str(g) for g in (t.get("gnis_ids") or [])]
        if not gids or t.get("wbks") or t.get("blks") or t.get("wscs"):
            continue
        wbks: set[str] = set()
        for g in gids:
            wbks |= g2w.get(g, set())
        if not wbks:
            continue                                  # stream gnis: leave as-is
        e["target"] = {"wbks": sorted(wbks)}
        e["_gnis"] = gids                             # provenance: the id the override curated
        n_to_wbk += 1
        if len(wbks) > 1:
            n_multi += 1

    # 2. merge same-target groups
    merged: dict[tuple, dict] = {}
    order: list[tuple] = []
    for e in nv:
        key = _target_key(e["target"], e.get("reach"))
        if key not in merged:
            merged[key] = e
            order.append(key)
        else:
            _merge_names(merged[key]["names"], e["names"])
            if e.get("_gnis"):
                merged[key].setdefault("_gnis", [])
                merged[key]["_gnis"] = sorted(set(merged[key]["_gnis"]) | set(e["_gnis"]))
    deduped = [merged[k] for k in order]

    # 3. fold in dropped fid overrides (only if that target isn't already present)
    present = {_target_key(e["target"], e.get("reach")) for e in deduped}
    added = 0
    for grp in dropped_fid_overrides(fwa):
        key = _target_key(grp["target"], grp.get("reach"))
        if key in present:
            _merge_names(merged[key]["names"], grp["names"])
        else:
            deduped.append(grp)
            present.add(key)
            added += 1

    after_names = {(n["name"], n.get("source", "")) for e in deduped for n in e["names"]}
    lost = before_names - after_names
    assert not lost, f"LOSSY! dropped names: {sorted(lost)[:20]}"

    print(f"groups: {len(nv)} -> {len(deduped)}  (merged {len(nv) - len(deduped) + added} away, +{added} fid)")
    print(f"gnis->wbk retargeted: {n_to_wbk} ({n_multi} to multiple wbks)")
    print(f"distinct (name,source): {len(before_names)} -> {len(after_names)}  (lossless: {not lost})")
    if dry:
        print("[dry-run] not written")
        return
    _NV.write_text(json.dumps(deduped, indent=1))
    print(f"wrote {_NV}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(args.dry_run)


if __name__ == "__main__":
    main()
