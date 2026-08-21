"""Data layer for the curation-review app — **reuses pipeline code, reimplements nothing.**

- Matching an entry -> registry item is done by `pipeline.matching.matcher` (the exact matcher the
  pipeline uses), so the review tool resolves geometry/boundaries the same way the build does.
- Validation on save is `pipeline.parsing.entry_models.Entry` + `validate_entry_splits`.
- "Unused curated splits" reuses `entry_models.unused_splits`, restricted to curated (`ref="split:*"`)
  boundaries so lake/outlet/headwaters auto-boundaries don't count.

Write model: the PARSER owns `pipeline/parsing/entries/region-*.json` (append-only as parsing
progresses). This app never writes there — curator decisions go to a SEPARATE overlay
`pipeline/parsing/entries/reviewed/region-*.json`, so the app can run against a half-finished parse
while parsing keeps adding entries. The presented entry = parser entry overlaid by its reviewed copy.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from pipeline.matching.matcher import (
    MatchResult, build_id_index, build_name_index, build_override_index, load_overrides, match_row,
)
from pipeline.parsing.entry_models import Entry, unused_splits, validate_entry_splits
from pipeline.parsing.rows import load_synopsis_rows
from pipeline.registry import load_registry

_ROOT = Path(__file__).resolve().parents[2]
ENTRIES_DIR = _ROOT / "pipeline" / "parsing" / "entries"
REVIEWED_DIR = ENTRIES_DIR / "reviewed"
REGISTRY_PATH = _ROOT / "output" / "v2" / "full" / "registry.json"
OVERRIDES_PATH = _ROOT / "pipeline" / "matching" / "overrides.json"
SPLITS_RESOLVED_PATH = _ROOT / "output" / "v2" / "full" / "splits.resolved.json"
GRAPH_GPKG_PATH = _ROOT / "output" / "v2" / "full" / "graph.gpkg"
BASEMAP_PMTILES = _ROOT / "data" / "bc.pmtiles"          # the webapp's basemap (web-mercator)
SPLITS_JSON_PATH = _ROOT / "pipeline" / "splits.json"    # THE hand-curated split source (editable here)
ROW_IMAGES_DIR = _ROOT / "output" / "pipeline" / "extraction" / "row_images"  # source synopsis row crops


# --------------------------------------------------------------------------- #
# Registry + matcher indices (expensive — built once)
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=1)
def _registry() -> dict:
    return load_registry(REGISTRY_PATH)


@lru_cache(maxsize=1)
def _indices():
    reg = _registry()
    overrides = load_overrides(OVERRIDES_PATH if OVERRIDES_PATH.exists() else None)
    return build_name_index(reg), build_id_index(reg), build_override_index(overrides)


@lru_cache(maxsize=1)
def _splits_meta() -> dict[str, dict]:
    """split_id -> {label, anchor_type, blk, route_measure, ...} from splits.resolved.json."""
    if not SPLITS_RESOLVED_PATH.exists():
        return {}
    rows = json.loads(SPLITS_RESOLVED_PATH.read_text(encoding="utf-8"))
    return {r["split_id"]: r for r in rows}


def _clean_val(v):
    """None-out pandas NaN so it serializes to JSON null."""
    try:
        if v is None or (isinstance(v, float) and v != v):
            return None
    except Exception:  # noqa: BLE001
        pass
    return v


@lru_cache(maxsize=1)
def _split_points_attrs() -> dict[str, dict]:
    """split_id -> the split_points layer's attributes (concern, offset_m, picked_up, anchor_type,
    route_measure, ...) read ONCE without geometry. Extra provenance for the curator beyond
    splits.resolved.json — this is where a mis-anchored split (wrong falls vs bridge) shows up."""
    try:
        import pyogrio
        df = pyogrio.read_dataframe(GRAPH_GPKG_PATH, layer="split_points", read_geometry=False)
    except Exception:  # noqa: BLE001 — no pyogrio / no layer: fall back to splits.resolved only
        return {}
    out: dict[str, dict] = {}
    for rec in df.to_dict("records"):
        sid = str(rec.get("split_id") or "")
        if sid:
            out[sid] = {k: _clean_val(v) for k, v in rec.items()}
    return out


def split_meta(split_id: str) -> dict:
    """Merged curated-split metadata for one split id (splits.resolved.json + split_points attrs)."""
    m = dict(_splits_meta().get(split_id, {}))
    for k, v in _split_points_attrs().get(split_id, {}).items():
        if v is not None:
            m[k] = v
    return m


def invalidate_caches() -> None:
    """Drop cached graph-derived data so the next request reads freshly-rebuilt artifacts
    (registry.json, splits.resolved.json, graph.gpkg split_points). Call after a graph rebuild.
    item_geojson reads graph.gpkg fresh on every call, so it needs no clearing; the species list
    and row-image index come from static source, not the build, so they are left warm."""
    for fn in (_registry, _indices, _splits_meta, _split_points_attrs,
               _blk_to_item, _tributary_items, item_tributaries):
        fn.cache_clear()


def match_identity(name: str, region: str, mus: list[str]) -> MatchResult:
    """Resolve an entry's identity to a registry item **via the pipeline matcher** (same logic, same
    overrides as the build). `region`/`mus` are adapted to the row shape the matcher expects."""
    reg = _registry()
    name_index, id_index, ov_index = _indices()
    row = {"water": name, "region": region, "mu": list(mus or [])}
    return match_row(0, row, reg, name_index, id_index, ov_index)


# --------------------------------------------------------------------------- #
# Entry loading + reviewed overlay
# --------------------------------------------------------------------------- #

def _read_entryfile(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {e["entry_id"]: e for e in data.get("entries", [])}


def regions() -> list[str]:
    """Region ids that have a parser EntryFile (e.g. ['1','2',...])."""
    out = []
    for p in sorted(ENTRIES_DIR.glob("region-*.json")):
        out.append(p.stem.split("region-")[1])
    return out


def load_region(region: str) -> dict[str, dict]:
    """Parser entries for a region, overlaid by any reviewed copies (reviewed wins)."""
    base = _read_entryfile(ENTRIES_DIR / f"region-{region}.json")
    review = _read_entryfile(REVIEWED_DIR / f"region-{region}.json")
    base.update(review)
    return base


def _all_entries() -> list[tuple[str, dict]]:
    out = []
    for r in regions():
        for eid, e in load_region(r).items():
            out.append((r, e))
    return out


@lru_cache(maxsize=1)
def _row_image_index() -> dict:
    """raw_regs -> [(water_lower, image)]. Keyed on the verbatim regs (exact, near-unique); the parser
    title-cases the name so we can't key on it, but regs_verbatim is injected verbatim from the row."""
    idx: dict[str, list] = {}
    for r in load_synopsis_rows():
        if r.get("image") and r.get("raw_regs"):
            idx.setdefault(r["raw_regs"], []).append((str(r.get("water", "")).lower(), r["image"]))
    return idx


def entry_source_image(e: dict) -> str | None:
    """The source row-crop image for an entry, matched to its synopsis row by regs_verbatim (with the
    waterbody name, case-insensitive, as a tiebreaker if several rows share the same regs)."""
    cands = _row_image_index().get(e.get("regs_verbatim", ""), [])
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0][1]
    name = str(e.get("identity", {}).get("name", "")).lower()
    for w, img in cands:
        if w == name:
            return img
    return cands[0][1]


# --------------------------------------------------------------------------- #
# Derived views (status, boundaries, unused curated splits)
# --------------------------------------------------------------------------- #

def _rule_needs_review(e: dict) -> bool:
    return any(r.get("needs_review") or r.get("unresolved_locators") for r in e.get("rules", []))


def _item_for_entry(e: dict):
    """The registry item (RegistryItem) for an entry, resolved via the matcher over its identity."""
    ident = e.get("identity", {})
    mr = match_identity(ident.get("name", ""), ident.get("region", ""), ident.get("mus", []))
    if mr.item_id:
        return _registry().get(mr.item_id)
    return None


def _boundaries(item):
    return list(item.boundaries) if item else []


def _boundary_dict(b) -> dict:
    return {"id": b.id, "label": b.label, "kind": b.kind, "ref": b.ref, "wbk": b.wbk,
            "curated": str(b.ref or "").startswith("split:")}


def _curated_split_ids(item) -> set[str]:
    """Boundary ids that are CURATED splits (ref 'split:*') — not lake/outlet/headwaters auto-boundaries."""
    return {b.id for b in _boundaries(item) if str(b.ref or "").startswith("split:")}


def unused_curated_splits(e: dict, item: dict | None) -> list[dict]:
    """Curated splits on this item that NO rule's extents reference — surfaced so the curator can see a
    hand-authored cut that the parse never used (a likely missed reach). Reuses entry_models.unused_splits."""
    curated = _curated_split_ids(item)
    if not curated:
        return []
    try:
        entry = _to_entry(e)
    except Exception:
        return []
    meta = _splits_meta()
    out = []
    for sid in unused_splits(entry, curated):
        m = meta.get(sid, {})
        out.append({"id": sid, "label": m.get("label", sid), "anchor_type": m.get("anchor_type", "")})
    return out


def _item_split_source(item) -> list[dict]:
    """Live splits.json splits on the SAME waterbody as this registry item (applies_to matched to the
    item's ref_ids). May include splits not yet built into the graph (pending rebuild)."""
    if item is None:
        return []
    refs = set(item.ref_ids)
    out: list[dict] = []
    for wb in _load_splits().get("waterbodies", []):
        at = wb.get("applies_to") or {}
        keys: set[str] = set()
        if at.get("gnis_id") is not None:
            keys.add(f"gnis:{at['gnis_id']}")
        for g in (at.get("gnis_ids") or []):
            keys.add(f"gnis:{g}")
        if at.get("wsc"):
            keys.add(f"wsc:{at['wsc']}")
        if at.get("blk"):
            keys.add(f"blk:{at['blk']}")
        if keys & refs:
            out.extend(wb.get("splits", []))
    return out


def bindable(item) -> list[dict]:
    """The UNION a rule may bind: built graph boundaries + live splits.json splits for the item. Each
    entry is tagged `in_graph` / `in_splits` so the UI shows what's baked vs pending a rebuild, and a
    rule can bind a split that only exists in splits.json (it validates now, renders after rebuild)."""
    out: dict[str, dict] = {}
    for b in _boundaries(item):
        d = _boundary_dict(b)
        d["in_graph"], d["in_splits"] = True, False
        if str(b.ref or "").startswith("split:"):
            d["meta"] = split_meta(b.id)          # as-built resolved values
        out[b.id] = d
    for sp in _item_split_source(item):
        sid = sp.get("id")
        if not sid:
            continue
        live = {"label": sp.get("label"), "kind": sp.get("kind"),
                "anchor": sp.get("anchor"), "note": sp.get("note")}
        if sid in out:
            out[sid]["in_splits"] = True
            out[sid]["live"] = live
        else:                                     # in splits.json but NOT yet in the graph (pending)
            out[sid] = {"id": sid, "label": sp.get("label"), "kind": sp.get("kind"),
                        "ref": f"split:{sid}", "wbk": "", "curated": True,
                        "in_graph": False, "in_splits": True, "live": live}
    return list(out.values())


def _bindable_ids(item) -> set[str]:
    return {b["id"] for b in bindable(item)}


def item_bindable(item_id: str) -> list[dict]:
    """The bindable union for an arbitrary item id — used when a tributary exclude / cross-item extent
    scopes to a different item and needs that item's splits in the picker."""
    return bindable(_registry().get(item_id))


@lru_cache(maxsize=1)
def _blk_to_item() -> dict[str, str]:
    """blk -> owning registry item id (from each item's `blk:` ref_ids). Lets us map a graph node
    (`<blk>:<measure>`) back to the named stream it belongs to — for graph-based tributary lookup."""
    out: dict[str, str] = {}
    for it in _registry().values():
        for r in it.ref_ids:
            if r.startswith("blk:"):
                out[r.split(":", 1)[1]] = it.id
    return out


# Flow-edge kinds that make ``from_node`` a TRIBUTARY of ``to_node``: a stream confluence, or a stream
# emptying into a lake. (``continuation`` = same channel; ``lake_out`` = the lake's own outflow — neither
# is a tributary.)
_TRIB_EDGE_KINDS = {"confluence", "lake_in"}


@lru_cache(maxsize=1)
def _tributary_items() -> dict[str, set[str]]:
    """graph node ``to_node`` -> {owning item ids of the streams whose mouth flows into it}. Built ONCE
    from graph.gpkg's flow edges (the ``confluences`` layer) — this IS "each node's tributaries" the
    graph records. Everything downstream (stream OR lake tributaries) reads from here; no WSC guessing."""
    from collections import defaultdict
    out: dict[str, set[str]] = defaultdict(set)
    if not GRAPH_GPKG_PATH.exists():
        return out
    try:
        import pyogrio
        df = pyogrio.read_dataframe(GRAPH_GPKG_PATH, layer="confluences", read_geometry=False,
                                    columns=["from_node", "to_node", "kind"])
    except Exception:  # noqa: BLE001 — no pyogrio / no layer
        return out
    blk2item = _blk_to_item()
    for fn, tn, kind in zip(df["from_node"], df["to_node"], df["kind"]):
        if kind in _TRIB_EDGE_KINDS:
            iid = blk2item.get(str(fn).split(":", 1)[0])
            if iid:
                out[str(tn)].add(iid)
    return out


@lru_cache(maxsize=1024)
def item_tributaries(item_id: str) -> list[dict]:
    """Named tributaries of an item, for the carve-out picker — **graph only**. A tributary is a named
    stream whose mouth flows into one of the item's graph nodes (its section_ids, plus the lake node for
    a lake). Reads the graph's flow edges (confluence + lake_in), so it works identically for streams
    and lakes. Pick a tributary, then a reach on it (upstream_of a point excludes it + everything above,
    so deeper tributaries are reached through the first-order one they flow into)."""
    it = _registry().get(item_id)
    if it is None:
        return []
    nodes = set(it.section_ids)
    if it.id.startswith("wbk:"):
        nodes.add("lake:" + it.id.split(":", 1)[1])   # tributaries point at the lake node
    idx = _tributary_items()
    cand: set[str] = set()
    for n in nodes:
        cand |= idx.get(n, set())
    cand.discard(item_id)
    reg = _registry()
    out = [{"id": i, "name": reg[i].name}
           for i in cand if reg.get(i) and reg[i].kind == "stream" and reg[i].name]
    return sorted(out, key=lambda d: d["name"])


def _referenced_item_ids(entry_dict: dict) -> set[str]:
    """Every other-item id an extent scopes to (rule extents, entry scope, tributary excludes)."""
    ids: set[str] = set()

    def _scan(extents):
        for ex in extents or []:
            if ex.get("item"):
                ids.add(ex["item"])

    _scan(entry_dict.get("scope"))
    _scan((entry_dict.get("tributaries") or {}).get("excludes"))
    for r in entry_dict.get("rules") or []:
        _scan(r.get("extents"))
        _scan(r.get("tributary_excludes"))
    return ids


def entry_status(e: dict, item: dict | None) -> str:
    """One coarse status for the queue ordering/badge."""
    if e.get("locked"):
        return "confirmed"
    if e.get("registry_status") == "no_registry":
        return "no_registry"
    if _rule_needs_review(e):
        return "needs_review"
    if unused_curated_splits(e, item):
        return "unused_splits"
    return "unreviewed"


_STATUS_ORDER = {"no_registry": 0, "needs_review": 1, "unused_splits": 2, "unreviewed": 3, "confirmed": 4}


def queue(region: str | None = None, status: str | None = None) -> list[dict]:
    """Review queue rows, sorted so items needing attention float to the top."""
    rows = []
    src = [(region, e) for eid, e in load_region(region).items()] if region else _all_entries()
    for reg_id, e in src:
        item = _item_for_entry(e)
        st = entry_status(e, item)
        if status and st != status:
            continue
        rows.append({
            "entry_id": e["entry_id"],
            "region": reg_id if isinstance(reg_id, str) else e.get("identity", {}).get("region", ""),
            "name": e.get("identity", {}).get("name", ""),
            "mus": e.get("identity", {}).get("mus", []),
            "status": st,
            "locked": bool(e.get("locked")),
            "registry_status": e.get("registry_status", "matched"),
            "n_rules": len(e.get("rules", [])),
            "matched_item_id": item.id if item else None,
            "matched_item_name": item.name if item else None,
            "unused_curated_splits": len(unused_curated_splits(e, item)),
        })
    rows.sort(key=lambda r: (_STATUS_ORDER.get(r["status"], 9), r["name"]))
    return rows


def entry_detail(entry_id: str) -> dict | None:
    """Full entry + its resolved item's boundaries/variants + unused curated splits + match info."""
    for reg_id, e in _all_entries():
        if e["entry_id"] == entry_id:
            ident = e.get("identity", {})
            mr = match_identity(ident.get("name", ""), ident.get("region", ""), ident.get("mus", []))
            item = _registry().get(mr.item_id) if mr.item_id else None
            return {
                "entry": e,
                "region": reg_id,
                "match": {"item_id": mr.item_id, "status": mr.status, "reason": mr.reason,
                          "candidates": list(mr.candidates)},
                "item": None if not item else {
                    "id": item.id, "name": item.name, "kind": item.kind,
                    "variants": list(item.variants), "mus": list(item.mus),
                    "boundaries": bindable(item),   # built graph ∪ live splits.json (tagged)
                },
                "unused_curated_splits": unused_curated_splits(e, item),
                "source_image": entry_source_image(e),
            }
    return None


@lru_cache(maxsize=1)
def species_list() -> list[dict]:
    """All species/group codes with their common names — for the species picker. `is_group` marks a
    group code (e.g. AO = All Salmon)."""
    from pipeline.parsing.species import COMMON_NAME, GROUPS
    return sorted(
        ({"code": c, "name": n, "is_group": c in GROUPS} for c, n in COMMON_NAME.items()),
        key=lambda d: d["name"].lower(),
    )


def search_items(q: str, limit: int = 20) -> list[dict]:
    """Registry search by name/variant substring — for attaching an item to a no_registry entry."""
    ql = q.lower().strip()
    if not ql:
        return []
    out = []
    for it in _registry().values():
        if it.kind == "area":
            continue
        names = [it.name, *it.variants]
        if any(ql in (n or "").lower() for n in names):
            out.append({"id": it.id, "name": it.name, "kind": it.kind, "mus": list(it.mus)})
            if len(out) >= limit:
                break
    return out


# --------------------------------------------------------------------------- #
# Save (validated) to the reviewed overlay
# --------------------------------------------------------------------------- #

def _sql_in(col: str, vals) -> str:
    quoted = ",".join("'" + str(v).replace("'", "''") + "'" for v in vals)
    return f"{col} IN ({quoted})"


def _read_gpkg_features(layer: str, where: str, keep: list[str], kind: str) -> list[dict]:
    """Read a graph.gpkg layer with a driver-level WHERE (only matching features are scanned) and return
    GeoJSON features **reprojected to EPSG:4326 (lon/lat)** so they overlay the web-mercator PMTiles
    basemap in MapLibre. Chunked so a long IN clause stays safe."""
    import geopandas as gpd  # local import: heavy, only needed for the map endpoint

    feats: list[dict] = []
    if not GRAPH_GPKG_PATH.exists():
        return feats
    try:
        gdf = gpd.read_file(GRAPH_GPKG_PATH, layer=layer, where=where)
    except Exception:  # noqa: BLE001 — driver/where issue: return what we have (empty), don't 500
        return feats
    if gdf.empty:
        return feats
    gdf = gdf.to_crs(4326)                                # 3005 (BC Albers) -> lon/lat for MapLibre
    cols = [c for c in keep if c in gdf.columns]
    fc = json.loads(gdf[cols + ["geometry"]].to_json())
    for f in fc.get("features", []):
        props = f.get("properties", {})
        props["kind"] = kind
        feats.append({"type": "Feature", "geometry": f.get("geometry"), "properties": props})
    return feats


def item_geojson(item_id: str) -> dict:
    """Stream sections + curated split points for a registry item, as a GeoJSON FeatureCollection in
    EPSG:4326 (lon/lat) so it overlays the web-mercator PMTiles basemap. Streams selected by the item's
    own `section_ids`; splits by its curated `split:*` boundary ids. Features carry `properties.kind` =
    'stream' | 'split' for styling (colouring is done client-side)."""
    item = _registry().get(item_id)
    if item is None:
        return {"type": "FeatureCollection", "features": []}

    feats: list[dict] = []
    section_ids = list(item.section_ids)
    for i in range(0, len(section_ids), 400):                      # chunk long IN lists
        chunk = section_ids[i:i + 400]
        feats += _read_gpkg_features(
            "streams", _sql_in("node_id", chunk),
            ["node_id", "display_name", "location_identifier"], "stream")

    split_ids = list(_curated_split_ids(item))
    if split_ids:
        feats += _read_gpkg_features(
            "split_points", _sql_in("split_id", split_ids),
            ["split_id", "label", "anchor_type", "picked_up"], "split")

    return {"type": "FeatureCollection", "features": feats}


# --------------------------------------------------------------------------- #
# Editing splits.json (the hand-curated split source of truth)
# --------------------------------------------------------------------------- #
# splits.json is now hand-curated directly (waterbody-splits.json + build_splits are gone). Edits here
# write it in place; they take effect only after a graph rebuild (pipeline.build --full — CPU only, no
# credits), because splits are baked into the graph's section boundaries.

_SPLIT_EDITABLE = {"label", "note", "kind", "anchor"}


def _load_splits() -> dict:
    return json.loads(SPLITS_JSON_PATH.read_text(encoding="utf-8"))


def _write_splits(data: dict) -> None:
    # match the file's existing format (1-space indent, real UTF-8) so a 1-split edit is a 1-split diff,
    # not a whole-file reformat (ensure_ascii=True would escape every → / — / “ ” and churn the file)
    _atomic_write(SPLITS_JSON_PATH, json.dumps(data, indent=1, ensure_ascii=False))


def get_split(split_id: str) -> dict | None:
    """The raw editable split record from splits.json + which waterbody it's under."""
    for wb in _load_splits().get("waterbodies", []):
        for sp in wb.get("splits", []):
            if sp.get("id") == split_id:
                return {"split": sp, "waterbody": wb.get("name"), "applies_to": wb.get("applies_to")}
    return None


def save_split(split_id: str, patch: dict) -> dict:
    """Apply a patch (label / note / kind / anchor) to a split in splits.json and write it back.
    Returns {ok, errors, split}. A graph rebuild is required for the change to take effect."""
    if not isinstance(patch, dict):
        return {"ok": False, "errors": ["patch must be an object"]}
    bad = [k for k in patch if k not in _SPLIT_EDITABLE]
    if bad:
        return {"ok": False, "errors": [f"not editable: {bad}; allowed: {sorted(_SPLIT_EDITABLE)}"]}
    if "anchor" in patch and not (isinstance(patch["anchor"], dict) and patch["anchor"].get("type")):
        return {"ok": False, "errors": ["anchor must be an object with a 'type'"]}
    data = _load_splits()
    for wb in data.get("waterbodies", []):
        for sp in wb.get("splits", []):
            if sp.get("id") == split_id:
                sp.update(patch)
                _write_splits(data)
                return {"ok": True, "errors": [], "split": sp}
    return {"ok": False, "errors": [f"split {split_id} not found in splits.json"]}


def delete_split(split_id: str) -> dict:
    """Remove a split from splits.json (e.g. a duplicate/mis-anchored cut). Rebuild to apply."""
    data = _load_splits()
    for wb in data.get("waterbodies", []):
        splits = wb.get("splits", [])
        for i, sp in enumerate(splits):
            if sp.get("id") == split_id:
                del splits[i]
                _write_splits(data)
                return {"ok": True, "errors": []}
    return {"ok": False, "errors": [f"split {split_id} not found in splits.json"]}


def split_refs(split_id: str) -> list[dict]:
    """Every rule whose extents bind this split id (across parser entries + reviewed overlay). This is
    the IMPACT PREVIEW for a rename — the exact scope of rules that will be rewritten."""
    out: list[dict] = []
    for region in regions():
        for eid, e in load_region(region).items():
            for r in e.get("rules", []):
                if any(split_id in (ex.get("splits") or []) for ex in r.get("extents", [])):
                    out.append({"entry_id": eid, "region": region, "rule_id": r["rule_id"],
                                "details": r.get("details", ""), "entry_name": e.get("identity", {}).get("name", "")})
    return out


def rename_split(old_id: str, new_id: str) -> dict:
    """Rename a split id in splits.json AND rewrite every rule extent that binds it (to the reviewed
    overlay). Union validation means the new id is bindable immediately; a rebuild reconciles the graph."""
    import re as _re
    new_id = (new_id or "").strip()
    if not _re.fullmatch(r"[a-z0-9_]+", new_id):
        return {"ok": False, "errors": ["new id must be lowercase letters, digits, underscores"]}
    data = _load_splits()
    all_ids = {sp.get("id") for wb in data.get("waterbodies", []) for sp in wb.get("splits", [])}
    if new_id in all_ids:
        return {"ok": False, "errors": [f"id '{new_id}' already exists in splits.json"]}
    renamed = False
    for wb in data.get("waterbodies", []):
        for sp in wb.get("splits", []):
            if sp.get("id") == old_id:
                sp["id"] = new_id
                renamed = True
    if not renamed:
        return {"ok": False, "errors": [f"split '{old_id}' not found in splits.json"]}
    _write_splits(data)

    # cascade: rewrite rules that bind old_id -> new_id, save each affected entry to the overlay
    updated: list[dict] = []
    failed: list[dict] = []
    for ref in split_refs(old_id):
        region = ref["region"]
        e = load_region(region).get(ref["entry_id"])
        if not e:
            continue
        for r in e.get("rules", []):
            for ex in r.get("extents", []):
                if old_id in (ex.get("splits") or []):
                    ex["splits"] = [new_id if s == old_id else s for s in ex["splits"]]
        res = save_entry(region, e, lock=False)
        (updated if res["ok"] else failed).append({**ref, **({} if res["ok"] else {"error": res["errors"]})})
    return {"ok": True, "errors": [], "new_id": new_id, "updated_rules": updated, "failed_rules": failed}


def _to_entry(e: dict) -> Entry:
    return Entry(**e)


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def save_entry(region: str, entry_dict: dict, *, lock: bool = False, reviewed_by: str = "") -> dict:
    """Validate an edited entry (Entry model + split-id check against its matched item's boundaries) and
    write it to the reviewed overlay. Returns {ok, errors}. On lock, stamp reviewed_by/reviewed_at."""
    data = dict(entry_dict)
    if lock:
        data["locked"] = True
        data["reviewed_by"] = reviewed_by or data.get("reviewed_by", "") or "curator"
        data["reviewed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        entry = _to_entry(data)
    except Exception as ex:  # noqa: BLE001
        return {"ok": False, "errors": [f"schema: {ex}"]}
    item = _item_for_entry(data)
    allowed = _bindable_ids(item)               # built graph ∪ splits.json — bind pending splits too
    for iid in _referenced_item_ids(data):      # + splits of any cross-item extent / tributary exclude
        it2 = _registry().get(iid)
        if it2:
            allowed |= _bindable_ids(it2)
    errs = validate_entry_splits(entry, allowed)
    if errs:
        return {"ok": False, "errors": errs}

    path = REVIEWED_DIR / f"region-{region}.json"
    existing = _read_entryfile(path)
    existing[entry.entry_id] = json.loads(entry.model_dump_json())
    payload = {"region": region, "entries": sorted(existing.values(), key=lambda x: x["entry_id"])}
    _atomic_write(path, json.dumps(payload, indent=2, ensure_ascii=False))
    return {"ok": True, "errors": []}
