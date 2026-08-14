"""Build pipeline/splits.json (by-waterbody) from the curated pipeline/docs/waterbody-splits.json.

The curation is the source of truth; this is a deterministic, re-runnable conversion:
  - keep only 'curated' + 'manual' rows;
  - map each row's anchor_kind -> a SplitAnchor (confluence/point/lake/area_boundary/line);
  - clean LABELS (the landmark, not the reach description) so location_identifier reads right;
  - DEDUP rows that are the same physical cut (same coord+offset / tributary / wbk / area);
  - human-readable, globally-unique IDs (the authored id when the note records one, else a slug);
  - carry `_coord` (curated ground-truth) + `_offset_m` + `_note` for review/audit.

Audit-driven fixes (from the old->new accuracy audit, 2026-08-14) live in the override maps below,
each with a one-line reason. Run:  python -m pipeline.oneoff.build_splits [--dry-run]
Verify with:  python -m pipeline.oneoff.audit_splits
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

from project_config import get_config
from pipeline.utils.wsc import trim_wsc
from pipeline.oneoff.waterbody_splits import load_curation

ROOT = get_config().project_root

KEEP_STATUSES = {"curated", "manual"}   # rows converted into splits; all other statuses are dropped


# --- FWA-resolved tail (resolver subagent 2026-08-13) --------------------------------------------
MANUAL_TARGET = {
    "whiteswan-lake-s-inlet-outlet-streams-89ed48": {"blk": "356560775"},
    "atnarko-bella-coola-rivers-includes-tributari-985001": {"blk": "360836756"},
    "lynn-creek-d5e0fb-a": {"blk": "360860175"}, "lynn-creek-d5e0fb-b": {"blk": "360860175"},
}
# area_boundary rows given a WSC target so wsc_descendants actually fires (a gnis target ignores it)
PARK_FIX = {
    "wood-river-495c0d": ("HAMBER PARK", "300-829106"),
    "atnarko-bella-coola-rivers-includes-tributari-8142a5": ("TWEEDSMUIR PARK", "910-275583"),
    "atnarko-bella-coola-rivers-includes-tributari-01c9b0": ("TWEEDSMUIR PARK", "910-275583"),
    "baker-creek-b9b499": ("PINNACLES PARK", "100-458476"),
    "baker-creek-aaf7ac": ("PINNACLES PARK", "100-458476"),
    "baker-creek-b57f9e": ("PINNACLES PARK", "100-458476"),
    "pitt-river-16d11f": ("GARIBALDI PARK", "100-025956"),   # gnis-scoped -> descendants ignored; reg covers park tributaries
}

# --- audit-driven fixes (old->new audit, 2026-08-14) ---------------------------------------------
DROP_ROWS: set[str] = set()
# re-home splits filed under the wrong card to the correct entry (both Campbell dams -> CAMPBELL RIVER 39532)
REHOME = {"john-hart-lake-s-tributaries-36c501": "2d6a28ef",         # Ladore Dam
          "lower-campbell-lake-s-tributaries-190ad5": "2d6a28ef"}    # Strathcona Dam (dam point; +100m d/s already there)
# point rows with no curated target: scope to the channel the coord sits on (FWA, wsc preferred over blk)
POINT_TARGET = {
    "bear-mahood-creek-55c67c": {"wsc": "900-005473-486949"},           # Mahood Creek (152nd St; card's 1st gnis is a diff. Bear Ck)
    "north-alouette-river-e3810a": {"wsc": "100-025956-057184-074570"},  # North Alouette (216th St; mis-filed under main Alouette)
    "coal-creek-downstream-of-old-mf-m-railway-bri-347452": {"wsc": "300-625474-584724-253915"},  # Coal Creek
    "elk-river-s-tributaries-see-exceptions-e09244": {"wsc": "300-625474-584724-253915"},          # Coal Creek (dup coord)
}
# confluences whose curated tributary_wsc was wrong (bare trunk / wrong region) -> the correct FWA WSC.
# The three self-mouth cases use the CARD river's own WSC (its mouth into a larger river); saunders is a
# genuine tributary recorded in the wrong region (200 NE-BC) whose real VI code is 930-508366-243451-240268.
TRIB_WSC = {
    "babine-river-912f94-b": "400-536025",                              # Babine mouth -> Skeena
    "babine-river-fdcd21-b": "400-536025",
    "kootenay-river-downstream-of-idaho-border-1e5f9d-b": "300-625474", # Kootenay mouth -> Columbia
    "kemess-creek-e5aa68-a": "200-948755-999851-889551-178842",         # Kemess mouth -> Attichika
    "heber-river-ddafd5": "930-508366-243451-240268",                   # Saunders Creek (real VI tributary)
}
# self-mouth confluences: trib == card river, so the parent/target is the river itself (not a trimmed level)
SELF_MOUTH = {"babine-river-912f94-b", "babine-river-fdcd21-b",
              "kootenay-river-downstream-of-idaho-border-1e5f9d-b", "kemess-creek-e5aa68-a"}


# --- match table + overrides -> (name, region, mus) -> applies_to identifier ----------------------
def _strip_paren(nv: str) -> str:
    return re.sub(r"\s*\(.*$", "", nv or "").strip()


def _mkey(nv, region, mus):
    return (nv or "", region or "", frozenset(mus or []))


def _ident(e: dict) -> Optional[dict]:
    if e.get("gnis_ids"):
        return {"gnis_id": e["gnis_ids"][0]} if len(e["gnis_ids"]) == 1 else {"gnis_ids": e["gnis_ids"]}
    if e.get("waterbody_keys"): return {"wbk": str(e["waterbody_keys"][0])}
    if e.get("blue_line_keys"): return {"blk": str(e["blue_line_keys"][0])}
    if e.get("fwa_watershed_codes"): return {"wsc": e["fwa_watershed_codes"][0]}
    return None


def _load_match_index() -> dict:
    mt = json.loads((ROOT / "output/pipeline/matching/match_table.json").read_text())
    try:
        ov = json.loads((ROOT / "archive/pipeline/matching/overrides.json").read_text())
    except Exception:
        ov = []
    match: dict = {}
    for e in list(mt) + list(ov):
        c = e.get("criteria", {})
        idt = _ident(e)
        if not idt:
            continue
        for nm in {c.get("name_verbatim"), _strip_paren(c.get("name_verbatim"))}:
            match[_mkey(nm, c.get("region"), c.get("mus"))] = idt
    return match


def _resolve_applies_to(match: dict, nv, region, mus) -> Optional[dict]:
    for nm in (nv, _strip_paren(nv)):
        for key in (_mkey(nm, region, mus), _mkey(nm, region, []), _mkey(nm, None, mus), _mkey(nm, None, [])):
            if key in match:
                return match[key]
    return None


# --- helpers ------------------------------------------------------------------------------------
def _slug(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")
    return re.sub(r"_+", "_", s) or "split"


def _landmark(row: dict) -> str:
    off = row.get("offset") or {}
    base = off.get("anchor_label") or row.get("label") or row.get("locator_text") or ""
    base = re.sub(r"(?i)^\s*(upstream of|downstream of|upstream edge of|downstream edge of|from|between)\s+", "", base)
    base = re.sub(r"\s*\(.*$", "", base).strip()            # drop trailing parenthetical
    base = re.sub(r"(?i)\s+confluence$", "", base).strip()  # "Bannon Creek confluence" -> "Bannon Creek"
    return base or (row.get("locator_text") or row["id"])


def _to_anchor(r: dict) -> Optional[dict]:
    kind = r.get("anchor_kind"); tgt = r.get("target") or {}; coord = r.get("coord")
    off = r.get("offset") or {}
    trib_wsc = TRIB_WSC.get(r.get("id")) or tgt.get("wsc")            # corrected WSC wins over the curated one
    # bare-trunk confluence with no known real tributary -> fall back to a point at the curated coord
    if kind == "confluence" and coord and trib_wsc and "-" not in trim_wsc(trib_wsc):
        a = {"type": "point", "coord": coord, "is_lonlat": True}
        if off.get("m"):
            a["offset_m"] = off["m"]; a["offset_dir"] = off.get("dir", "downstream")
        return a
    if kind == "confluence" and trib_wsc:
        a = {"type": "confluence", "tributary_wsc": trib_wsc}
    elif kind == "confluence" and tgt.get("blk"):
        a = {"type": "confluence", "tributary_blk": str(tgt["blk"])}
    elif kind in ("lake", "lake_outlet", "lake_io") and off.get("m") and coord:
        a = {"type": "point", "coord": coord, "is_lonlat": True}   # a lake anchor can't carry an offset
    elif kind in ("lake", "lake_outlet", "lake_io") and tgt.get("wbk"):
        a = {"type": "lake", "wbk": str(tgt["wbk"])}
    elif kind == "area_boundary" and tgt.get("area_name"):
        a = {"type": "area_boundary", "area_layer": tgt.get("area_layer", "parks_bc"), "area_name": tgt["area_name"]}
        if tgt.get("wsc_descendants"):
            a["wsc_descendants"] = True
    elif kind == "line" and r.get("coords"):
        a = {"type": "line", "coords": r["coords"], "is_lonlat": True}
    elif coord:
        a = {"type": "point", "coord": coord, "is_lonlat": True}
    else:
        return None
    if off.get("m") and a["type"] in ("point", "confluence"):
        a["offset_m"] = off["m"]; a["offset_dir"] = off.get("dir", "downstream")
    return a


def _dedup_key(a: dict):
    """Same physical cut => same key (coord+offset, or tributary, or wbk, or area)."""
    t = a["type"]
    off = (round(a.get("offset_m", 0), 1), a.get("offset_dir", ""))
    if t == "point" and a.get("coord"):
        return ("pt", round(a["coord"][0], 5), round(a["coord"][1], 5), off)
    if t == "confluence":
        return ("cf", a.get("tributary_wsc") or a.get("tributary_blk"), off)
    if t == "lake":
        return ("lk", a.get("wbk"))
    if t == "area_boundary":
        return ("ar", a.get("area_name"))
    if t == "line":
        return ("ln", tuple(map(tuple, a.get("coords", []))))
    return (t, id(a))


def build_waterbodies(rows: list[dict]) -> tuple[list[dict], dict]:
    """Convert curation rows -> the by-waterbody `waterbodies` list. Returns (waterbodies, stats)."""
    match = _load_match_index()
    by_entry: dict = defaultdict(lambda: {"name": None, "nv": None, "region": None, "mus": None, "raw": []})
    skipped: Counter = Counter(); noanchor: list = []

    for r in rows:
        if r.get("status") not in KEEP_STATUSES:
            skipped[r.get("status")] += 1; continue
        if r.get("id") in DROP_ROWS:
            continue
        a = _to_anchor(r)
        if a is None:
            noanchor.append((r.get("name_verbatim"), r.get("id"))); continue
        eid = REHOME.get(r.get("id")) or r.get("entry_id") or r.get("name_verbatim")
        g = by_entry[eid]
        if r.get("id") not in REHOME:        # re-homed rows contribute only their split, never the group identity
            g["name"] = g["name"] or r.get("name_verbatim"); g["nv"] = g["nv"] or r.get("name_verbatim")
            g["region"] = g["region"] or r.get("region"); g["mus"] = g["mus"] or r.get("mus")
        g["raw"].append((r, a))

    used_ids: set = set()
    waterbodies: list = []; kinds: Counter = Counter(); merged = 0
    prov: dict = {}          # split_id -> [source curation row ids] (incl. dedup-merged rows)
    for eid, g in by_entry.items():
        wb_slug = _slug(_strip_paren(g["name"]))
        seen: dict = {}; splits: list = []
        for r, a in g["raw"]:
            extra: dict = {}
            if r["id"] in PARK_FIX:
                area_name, wsc = PARK_FIX[r["id"]]
                a = {"type": "area_boundary", "area_layer": "parks_bc", "area_name": area_name,
                     "area_name_field": "PROTECTED_LANDS_NAME", "wsc_descendants": True}
                extra["wsc"] = wsc   # _coord is added below from r["coord"]; no separate cache needed
            elif r["id"] in MANUAL_TARGET:
                extra.update(MANUAL_TARGET[r["id"]])
            if a["type"] == "point" and not extra:              # scope a point to its own channel
                if r["id"] in POINT_TARGET:
                    extra.update(POINT_TARGET[r["id"]])
                elif r.get("target") and r["id"] not in TRIB_WSC:  # corrected-trib rows: don't reuse the bogus row target
                    rt = r["target"]
                    w = trim_wsc(rt["wsc"]) if rt.get("wsc") else ""
                    if w and "-" in w:
                        extra["wsc"] = w                        # never scope to a bare trunk (e.g. "300")
                    elif rt.get("blk"):
                        extra["blk"] = str(rt["blk"])
            if a["type"] == "confluence" and a.get("tributary_wsc"):
                if r["id"] in SELF_MOUTH:
                    extra["wsc"] = trim_wsc(a["tributary_wsc"])         # river's own mouth: parent = the river itself
                else:
                    segs = trim_wsc(a["tributary_wsc"]).split("-")
                    if len(segs) > 1:
                        extra["wsc"] = "-".join(segs[:-1])
            k = _dedup_key(a)
            if k in seen:                      # same physical cut -> merge
                merged += 1; prov.setdefault(seen[k], []).append(r.get("id")); continue
            lm = _landmark(r)
            if a["type"] == "confluence":
                base = _slug(lm)                              # consistent id format for ALL confluences
                if not base.endswith("_confluence"):
                    base += "_confluence"
            else:
                m = re.search(r"authored split: ([a-z0-9_]+)", r.get("notes", "") or "")
                base = m.group(1) if m else _slug(lm)
            sid = base
            if sid in used_ids:
                sid = f"{wb_slug}__{base}"
            i = 2
            while sid in used_ids:
                sid = f"{base}_{i}"; i += 1
            used_ids.add(sid)
            split = {"id": sid, "label": lm, "kind": r.get("anchor_kind"), "anchor": a,
                     "note": "", "_note": r.get("notes", "")}
            if r.get("coord"):
                split["_coord"] = r["coord"]                   # curated ground-truth cut location (review/audit aid)
            _off = r.get("offset") or {}
            if _off.get("m"):
                split["_offset_m"] = _off["m"]                 # note: resolved cut = _coord shifted by this
            tgt = {k: extra.pop(k) for k in ("wsc", "blk", "gnis_id") if k in extra}
            if tgt:
                split["applies_to"] = tgt                      # per-split target override (same shape as waterbody applies_to)
            split.update(extra)                                # any non-target extras
            if r.get("polygon"):
                split["polygon"] = r["polygon"]
            seen[k] = sid; splits.append(split); kinds[a["type"]] += 1
            prov.setdefault(sid, []).append(r.get("id"))
        if not splits:
            continue
        waterbodies.append({"name": g["name"],
                            "applies_to": _resolve_applies_to(match, g["nv"], g["region"], g["mus"]),
                            "_entry_id": eid, "splits": splits})

    stats = {"waterbodies": len(waterbodies),
             "splits": sum(len(w["splits"]) for w in waterbodies),
             "merged": merged, "skipped": dict(skipped), "no_anchor": len(noanchor),
             "anchor_types": dict(kinds),
             "provenance": prov,          # split_id -> [source row ids]
             "no_anchor_rows": noanchor}  # [(name, row_id)] kept rows that yielded no anchor
    return waterbodies, stats


def build(out_path: Optional[Path] = None, dry_run: bool = False) -> dict:
    rows = load_curation()
    waterbodies, stats = build_waterbodies(rows)
    out = {"_about": "Curated stream splits, organized by waterbody. Built from waterbody-splits.json "
                     "by pipeline.oneoff.build_splits.", "waterbodies": waterbodies}
    dest = out_path or (ROOT / "pipeline/splits.json")
    if not dry_run:
        dest.write_text(json.dumps(out, indent=1))
    return {"dest": str(dest), "dry_run": dry_run, **stats}


def main() -> None:
    ap = argparse.ArgumentParser(description="Build pipeline/splits.json from curated waterbody-splits.json")
    ap.add_argument("--dry-run", action="store_true", help="build + report but don't write")
    ap.add_argument("--out", help="output path (default pipeline/splits.json)")
    args = ap.parse_args()
    r = build(out_path=Path(args.out) if args.out else None, dry_run=args.dry_run)
    print(f"{'(dry-run) ' if r['dry_run'] else ''}wrote {r['dest']}")
    print(f"  waterbodies={r['waterbodies']} splits={r['splits']} merged_dups={r['merged']} "
          f"no_anchor={r['no_anchor']}")
    print(f"  skipped={r['skipped']}")
    print(f"  anchor_types={r['anchor_types']}")


if __name__ == "__main__":
    main()
