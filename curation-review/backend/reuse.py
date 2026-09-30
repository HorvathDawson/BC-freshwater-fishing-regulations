"""Data layer for the curation-review app — **reuses pipeline code, reimplements nothing.**

- Matching an entry -> registry item is done by `pipeline.regs.matching.matcher` (the exact matcher the
  pipeline uses), so the review tool resolves geometry/boundaries the same way the build does.
- Validation on save is `pipeline.regs.parsing.catalogue.CatalogueEntry` + a split-id check, and the
  write is `pipeline.regs.parsing.io.write_entryfile`, which validates the whole file as written.
- "Unused curated splits" counts curated (`ref="split:*"`) boundaries no extent binds, so
  lake/outlet/headwaters auto-boundaries don't count.

Write model: `data/curated/regulations/entries/catalogue/region-*.json` are the SINGLE SOURCE OF TRUTH.
Curator decisions are written straight back to them. There is no lock: a catalogue entry has no such
field. A re-parse (`run_parse.sh repass`) will not overwrite an entry edited here — ingest keeps any
entry whose file copy differs from the one it last wrote, and reports it.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from functools import lru_cache
from pathlib import Path

from pipeline.regs.matching.matcher import (
    MatchResult, build_id_index, build_name_index, build_override_index, load_overrides, match_row,
)
from pipeline.regs.parsing import io
from pipeline.regs.parsing.catalogue import CatalogueRule, label as rule_label
from pipeline.atlas.reach.covered import covered_ids as _pipeline_covered_ids
import model_api
from pipeline.regs.parsing.rows import load_synopsis_rows
from pipeline.atlas.registry import load_registry
from pipeline.atlas.reach.build import build_reach as _build_reach, resolve_carve_outs
from pipeline.atlas.reach.build import entry_scope as _entry_scope
from pipeline.atlas.reach.classify import wants_tributaries as _wants_tributaries
from pipeline.atlas.reach import extent as _resolve
from pipeline.common.utils.wsc import trim_wsc
from pipeline.common.curated import CURATED, GENERATED, SOURCE

_ROOT = Path(__file__).resolve().parents[2]
#: The catalogue region files the app reads AND WRITES. `CURATION_ENTRIES_DIR` points it at a copy
#: — the API tests and a headless click-through run against a temp copy, never the real files.
ENTRIES_DIR = (Path(os.environ["CURATION_ENTRIES_DIR"]) if os.environ.get("CURATION_ENTRIES_DIR")
               else CURATED.regulations.entries.catalogue)
# The build the app SERVES. `project_config.review_build_dir` is the one name for it, shared with
# rebuild.py (which writes this same directory) — see config.yaml `generated.atlas.default_build`. Hard-coding
# it here meant the app could serve a build months older than the pipeline and say nothing: every
# reach still renders, just against a stale graph, so a reviewer signs off on the wrong water.
_BUILD = GENERATED.build()
REGISTRY_PATH = _BUILD / "registry.json"
OVERRIDES_PATH = CURATED.regulations.overrides
SPLITS_RESOLVED_PATH = _BUILD / "splits.resolved.json"
GRAPH_PKL_PATH = _BUILD / "graph.pkl"
GEOMS_PKL_PATH = _BUILD / "geometries.pkl"
WBK_POLYS_PKL_PATH = _BUILD / "waterbody_polys.pkl"
BASEMAP_PMTILES = SOURCE / "bc.pmtiles"                  # the webapp's basemap (web-mercator)
SPLITS_JSON_PATH = CURATED.waters.splits                 # THE hand-curated split source (editable here)
ROW_IMAGES_DIR = GENERATED.regs.extraction / "row_images"  # source synopsis row crops


# --------------------------------------------------------------------------- #
# Registry + matcher indices (expensive — built once)
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=1)
def _registry() -> dict:
    return load_registry(REGISTRY_PATH)


@lru_cache(maxsize=1)
def _place_namer():
    """The bundle's own place-namer over the registry this app serves — so a label names a rule's
    place (a cut-point's curated label, an area's name) exactly as the bundle's label does."""
    from pipeline.common.curated import CURATED
    from pipeline.deliver.bundle.place_names import PlaceNamer, area_names, split_labels
    return PlaceNamer(_registry(), split_labels(json.loads(
        CURATED.waters.splits.read_text(encoding="utf-8"))), area_names(Path(REGISTRY_PATH).parent))


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
    """split_id -> the extra provenance beyond the anchor itself (concern, picked_up, offset_m) —
    this is where a mis-anchored split (wrong falls vs bridge) shows up.

    Read from `splits.resolved.json`, which carries all three: 42 splits with `concern`, 40 with
    `picked_up`, 11 with `offset_m`. It used to come from the `split_points` layer of graph.gpkg,
    which the build wrote FROM this same file — a 4.7 GB round trip for fields already on disk,
    and one that returned {} silently once the gpkg stopped being written.
    """
    return {sid: {k: _clean_val(v) for k, v in row.items()}
            for sid, row in _splits_meta().items()}


def split_meta(split_id: str) -> dict:
    """Merged curated-split metadata for one split id (splits.resolved.json + split_points attrs)."""
    m = dict(_splits_meta().get(split_id, {}))
    for k, v in _split_points_attrs().get(split_id, {}).items():
        if v is not None:
            m[k] = v
    return m


def invalidate_caches() -> None:
    """Drop cached graph-derived data so the next request reads freshly-rebuilt artifacts
    (registry.json, splits.resolved.json, graph.gpkg split_points, graph.pkl). Call after a graph
    rebuild. item_geojson reads graph.gpkg fresh on every call, so it needs no clearing; the species
    list and row-image index come from static source, not the build, so they are left warm.

    `_graph` MUST be in here: a rebuild renumbers the piece nodes (`{blk}:{measure}`), so a reach
    resolved against the pre-rebuild graph names sections the new registry no longer has — the reach
    silently comes back empty or wrong, with nothing on screen to say the graph is stale."""
    for fn in (_registry, _indices, _splits_meta, _split_points_attrs,
               _blk_to_item, _tributary_items, item_tributaries, _graph):
        fn.cache_clear()


def _ident(e: dict) -> dict:
    """{name, region, mus} for a catalogue entry.

    A catalogue entry is flat — `name` and `region` at the top, and the MUs the synopsis row was
    printed under encoded in `entry_id` after the `@` (that is what makes the id stable when
    matching moves). The app reads the catalogue region files only."""
    eid = str(e.get("entry_id", ""))
    mus = eid.split("@", 1)[1].split("+") if "@" in eid else []
    return {"name": e.get("name", ""), "region": str(e.get("region", "")), "mus": mus}


def match_identity(name: str, region: str, mus: list[str]) -> MatchResult:
    """Resolve an entry's identity to a registry item **via the pipeline matcher** (same logic, same
    overrides as the build). `region`/`mus` are adapted to the row shape the matcher expects."""
    reg = _registry()
    name_index, id_index, ov_index = _indices()
    row = {"water": name, "region": region, "mu": list(mus or [])}
    return match_row(0, row, reg, name_index, id_index, ov_index)


# --------------------------------------------------------------------------- #
# Entry loading
# --------------------------------------------------------------------------- #

_read_entryfile = io.read_entryfile                      # shared helper (io is the single home)


def regions() -> list[str]:
    """Region ids that have a catalogue region file (e.g. ['1','2',...])."""
    return io.region_ids(ENTRIES_DIR)


def load_region(region: str) -> dict[str, dict]:
    """Entries for a region. The region files are the SINGLE SOURCE OF TRUTH — curator edits are written back
    here."""
    return io.read_entryfile(ENTRIES_DIR / f"region-{region}.json")


def _all_entries() -> list[tuple[str, dict]]:
    out = []
    for r in regions():
        for eid, e in load_region(r).items():
            out.append((r, e))
    return out


def _shared_waters() -> frozenset[str]:
    """The waters more than one region prints a row for — read from the corpus as it is NOW, the
    same derivation the reach CLI makes (`outside.shared_waters`), so a row the curator reviews is
    held to the same regions as what ships."""
    from pipeline.atlas.reach.outside import shared_waters
    return shared_waters([e for _, e in _all_entries()])


def _rowed_waters() -> dict[str, tuple[str, ...]]:
    """The waters the tables print a row of their own for — read from the corpus as it is NOW, the
    same derivation the reach CLI makes (`outside.rowed_waters`), so a confluence cut's joining
    water goes with the cut, or stays out, exactly as it ships."""
    from pipeline.atlas.reach.outside import rowed_waters
    return rowed_waters([e for _, e in _all_entries()])


def _tidal() -> frozenset[str]:
    """The sections of the rows marked `tidal` (Nitinat Lake), read from the corpus as it is NOW —
    the same derivation the reach CLI makes (`outside.tidal_sections`), so the app takes the same
    tidal water out of every other row as what ships."""
    from pipeline.atlas.reach.outside import tidal_sections
    return tidal_sections([e for _, e in _all_entries()], _registry())


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
    """The source row-crop image for an entry, found by the row's printed text — or none.

    A catalogue entry records no image (`source.row_image` was the retired prose entry's, and
    reading it here found nothing on every entry), so the crop is looked up by `regs_verbatim`.
    When several rows print the same text ("No powered boats" alone is on dozens), the row whose
    name is this entry's is taken, and when none is, NO crop is shown: the first candidate would be
    a picture of a different water offered as evidence for this one.
    """
    cands = _row_image_index().get(e.get("regs_verbatim", ""), [])
    if len(cands) == 1:
        return cands[0][1]
    name = str(_ident(e)["name"]).lower()
    named = [img for w, img in cands if w == name]
    return named[0] if len(named) == 1 else None


# --------------------------------------------------------------------------- #
# Derived views (status, boundaries, unused curated splits)
# --------------------------------------------------------------------------- #

def _flagged(e: dict) -> bool:
    """A rule or licensing record carries a `review_reason` (or a rule an unresolved locator) —
    a reason present IS the flag; the model has no separate one."""
    return (any(r.get("review_reason") or r.get("unresolved_locators")
                for r in e.get("rules") or [])
            or any(x.get("review_reason") for x in e.get("licensing") or []))


def _covered_ids(e: dict) -> list[str]:
    """Every registry item this entry covers, primary first — `entry.matched`, filtered to the
    items this build has.

    `matched` IS AUTHORITATIVE AND EMPTY MEANS IT BINDS NOTHING. This used to fall back to a live
    re-match of the entry's cleaned-up name when `matched` was empty, which let the review app
    show — and resolve reaches against — a water the entry never recorded, while the bundle
    (whose own fallback is being removed in `pipeline.atlas.reach.covered`) binds nothing there.
    The two would disagree about the one thing a curator signs off. The shared implementation is
    called WITHOUT a matcher, so it reads `matched` and nothing else (AGENTS 16)."""
    return list(_pipeline_covered_ids(e, _registry()))


def _item_for_entry(e: dict):
    """The primary registry item of an entry: the first of its `matched` this build has, or None."""
    ids = _covered_ids(e)
    return _registry()[ids[0]] if ids else None


def _suggest_match(e: dict) -> MatchResult:
    """A LIVE re-match of the entry's name, SHOWN to a curator attaching an item to an entry whose
    `matched` is empty. A suggestion only: it never becomes the entry's water until the curator
    attaches it and saves."""
    ident = _ident(e)
    return match_identity(ident["name"], ident["region"], ident["mus"])


def _also_items(e: dict) -> list[dict]:
    """The OTHER registry items this entry covers — "CHILLIWACK / VEDDER RIVERS" is one synopsis row
    over the Chilliwack, the Vedder and the Vedder Canal, so the reviewer needs to see all three."""
    reg = _registry()
    return [{"id": i, "name": reg[i].name, "kind": reg[i].kind} for i in _covered_ids(e)[1:]]


def _boundaries(item):
    return list(item.boundaries) if item else []


def _boundary_dict(b) -> dict:
    # `aliases`: other curated split ids this same cut-point answers to (a split that landed inside a
    # lake run and was recorded on the lake's boundary rather than dropped). Surfaced so the picker can
    # say what a boundary represents instead of it silently standing for more than its own label.
    return {"id": b.id, "label": b.label, "kind": b.kind, "ref": b.ref, "wbk": b.wbk,
            "aliases": [str(a).split(":", 1)[-1] for a in (getattr(b, "aliases", ()) or ())],
            "curated": str(b.ref or "").startswith("split:")}


def _curated_split_ids(item) -> set[str]:
    """Boundary ids that are CURATED splits (ref 'split:*') — not lake/outlet/headwaters auto-boundaries."""
    return {b.id for b in _boundaries(item) if str(b.ref or "").startswith("split:")}


def unused_curated_splits(e: dict, item: dict | None) -> list[dict]:
    """Curated splits on this item that NO rule's extents reference — surfaced so the curator can see a
    hand-authored cut that the parse never used (a likely missed reach).

    `entry_models.unused_splits` walked the retired Entry's `.scope`; a CatalogueEntry has plain
    `extents` on the entry and on each rule, so the walk is local now."""
    curated = _curated_split_ids(item)
    if not curated:
        return []
    used: set[str] = set()
    for extents in [e.get("extents")] + [r.get("extents") for r in (e.get("rules") or [])]:
        for ex in (extents or ()):
            if isinstance(ex, dict):
                used.update(ex.get("splits") or ())
    meta = _splits_meta()
    out = []
    for sid in sorted(curated - used):
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
            keys.add(f"wsc:{trim_wsc(str(at['wsc']))}")   # registry ref_ids store the TRIMMED wsc
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


def _combined_bindable(item, e: dict) -> list[dict]:
    """`bindable` over every item this entry covers, each boundary tagged with the `item_id` it came
    from. The matched item goes first, so its ids win an id collision."""
    reg = _registry()
    items = [item] + [reg[i] for i in _covered_ids(e) if i != item.id]
    # The REGISTRY says which water a built cut-point is actually on; `bindable` also folds in
    # splits.json splits, which attach to every item their waterbody's `applies_to` names — so
    # Vedder Crossing Bridge would otherwise be labelled "on the Chilliwack" when the built cut
    # lives on the Vedder. Registry ownership wins; splits.json-only ids fall back to first seen.
    built = {b.id: it.id for it in items for b in it.boundaries}
    out: dict[str, dict] = {}
    for it in items:
        for b in bindable(it):
            out.setdefault(b["id"], {**b, "item_id": built.get(b["id"], it.id)})
    return list(out.values())


def _bindable_ids(item) -> set[str]:
    """Every id a rule may legally bind on this item — including each boundary's ALIASES.

    An alias is a curated split that could not be cut because its measure landed inside a lake run
    (a dam at the outlet), recorded as another name for the boundary standing at that place. It is a
    real, resolvable cut-point, so binding it must validate; without this the id resolves on the map
    but is rejected on save."""
    out: set[str] = set()
    for b in bindable(item):
        out.add(b["id"])
        out.update(b.get("aliases") or ())
    return out


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
    from the graph's own flow edges — this IS "each node's tributaries" the graph records. Everything
    downstream (stream OR lake tributaries) reads from here; no WSC guessing.

    These edges were read out of graph.gpkg's `confluences` layer, which was a projection of
    `graph.edges` — the same tuples, one artifact later. Read from the graph directly, so the
    tributary picker no longer goes empty when the gpkg is not written.
    """
    from collections import defaultdict
    out: dict[str, set[str]] = defaultdict(set)
    blk2item = _blk_to_item()
    for e in _graph().edges:
        if e.kind in _TRIB_EDGE_KINDS:
            iid = blk2item.get(str(e.from_node).split(":", 1)[0])
            if iid:
                out[str(e.to_node)].add(iid)
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


# --------------------------------------------------------------------------- #
# Extent -> sections (the REACH a rule actually selects)
# --------------------------------------------------------------------------- #
# Nothing in the pipeline resolves an Extent to sections yet; the ops are recorded intent. The review
# tool needs the answer NOW, because a curator cannot confirm a reach they cannot see — so this is the
# reference implementation of the contract documented on `pipeline.regs.parsing.entry_models.Op`:
#
#   whole            every section of every covered item (or of `item_id`, if scoped)
#   upstream_of X    follow the WATER up from X, crossing between covered items
#   downstream_of X  follow the WATER down from X, crossing between covered items
#   between A,B      the sections below A and above B (order-insensitive)
#   within(area)     handled by the area catalog, not here -> returns None (unknown)
#
# `item_id` narrows the candidate set to that one item, which is how a reach that stops at a
# junction is expressed (the Chilliwack ENDS at Vedder Crossing).


@lru_cache(maxsize=1)
def _graph():
    """The built StreamGraph — node bounds + flow adjacency. Big (~0.7 GB) but loaded once per
    process and only when a reach is actually requested."""
    from pipeline.common.io.serialize import read_artifact
    from pipeline.atlas.registry import regions
    g = read_artifact(str(GRAPH_PKL_PATH))
    # The region each straddling section lies in, exactly as the reach CLI attaches it — so a zone
    # rule the curator reviews binds what ships (`registry.regions`).
    regions.attach(g, regions.homes(_BUILD, _registry()))
    from pipeline.atlas.reach import position       # the same placement the reach CLI makes
    position.attach(g, _BUILD)
    return g


def resolve_extent(covered_ids: list[str], ex: dict) -> dict | None:
    """The app's binding of the shared resolver: same code the builder runs, this app's cached pair.

    The implementation lives in `pipeline.atlas.reach.extent` so the review app and the artifact builder
    can never drift. Callers here keep the two-argument form they always had."""
    return _resolve.resolve_extent(_registry(), _graph(), covered_ids, ex)


def _waters(section_ids):
    return _resolve._waters(_graph(), section_ids)












def item_tributary_geojson(item_id: str, limit: int = 6000) -> dict:
    """One level of tributaries: the streams that flow DIRECTLY into this item, as map geometry.

    A rule can extend to tributaries (`includes_tributaries`), and a curator cannot confirm that
    without seeing them. "One level" = the streams whose mouth joins one of this item's sections —
    each drawn in full, not just the joining piece — so the shape you see is the shape the flag means.
    Their own tributaries are NOT followed; that is the next level down and would swamp a big river.

    Features carry `properties.kind = 'tributary'` and the tributary's name, so the map can draw them
    distinctly from the item itself.

    Capped at `limit` SECTIONS, and the cap is reported rather than hidden. At the old cap of 400 the
    Fraser drew 125 of its 389 tributaries while still answering "389", so two thirds of them were
    missing from the map with nothing to say so — indistinguishable from a tributary that had lost its
    connection. Whole tributaries only: a half-drawn stream is worse than an absent one."""
    reg = _registry()
    it = reg.get(item_id)
    if it is None:
        return {"type": "FeatureCollection", "features": []}
    g = _graph()
    own = set(it.section_ids)
    mouths = set()
    for nid in own:                                        # incoming edges = what flows into us
        for i in g.up_adj.get(nid, []):
            frm = g.edges[i].from_node
            if frm not in own:
                mouths.add(frm)
    # expand each joining piece to the whole named stream it belongs to
    by_node: dict[str, str] = {}
    for iid, item in reg.items():
        if item.kind != "stream" or iid == item_id:
            continue
        for sid in item.section_ids:
            if sid in mouths:
                by_node[sid] = iid
    trib_ids = {by_node[m] for m in mouths if m in by_node}
    sections: list[str] = []
    drawn = 0
    for tid in sorted(trib_ids, key=lambda t: (len(reg[t].section_ids), t)):   # small ones first
        ids = reg[tid].section_ids
        if sections and len(sections) + len(ids) > limit:
            break                                          # never draw half a stream
        sections.extend(ids)
        drawn += 1
    feats = []
    for i in range(0, len(sections), 400):
        feats += _features("streams", sections[i:i + 400],
                                     ["node_id", "display_name"], "tributary")
    return {"type": "FeatureCollection", "features": feats, "n_tributaries": len(trib_ids),
            "n_drawn": drawn, "truncated": drawn < len(trib_ids)}


def _scope_sections(e: dict, covered: list[str]) -> set[str] | None:
    """The stretch the ENTRY is about, as node ids — or None when it is about the whole water.

    The synopsis qualifies a row in its NAME: "FRASER RIVER (upstream of the CPR Bridge at Mission)",
    "ADAMS RIVER (downstream of Adams Lake)". The rules inside such a row are written relative to that
    stretch and almost never restate it, so a rule reading "whole" means the whole of THIS row, not the
    whole river. Left unapplied, the two Adams rows — one above the lake, one below — resolved to the
    same river, and so did the Fraser's four regional rows.

    Multiple scope extents are unioned: the row's own idea of where it applies.

    Returns ``(sections, failed)``. A scope that CANNOT be resolved is reported, not swallowed: the
    old code returned None both for "this row has no scope" and for "this row's scope is broken", and
    the caller treated None as "do not clip". A Fraser regional row whose boundary stopped resolving
    would then silently widen from its region to the entire river — fail-open, in the direction that
    tells someone a rule applies where it does not. All ten scoped entries resolve today, so this is
    a latent path, which is exactly when it is cheap to close."""
    # THE BUILDER'S OWN FUNCTION, not a copy of it. This body used to re-implement the scope
    # (the entry's `extents`, unioned), and when the builder began holding a regional row to its
    # own region(s) — the Fraser's four regional rows each bound all 251 Fraser sections — a copy
    # would have left the app clipping one way and the bundle another (AGENTS 16).
    return _entry_scope(e, covered, _registry(), _graph())


def _json_safe(obj):
    """Strip non-finite floats before they reach `json.dumps`.

    `resolve_extent` reports the measure window an extent resolved to, and `upstream_of` has
    no upper bound — internally that is `INF`. JSON has no infinity, so serialising the reach
    payload raised `ValueError: Out of range float values are not JSON compliant` and the
    endpoint 500'd. `null` says the same thing ("unbounded on that side") and survives the
    trip. The artifact writer never carries `window`, so this is an app-boundary concern only.
    """
    if isinstance(obj, float):
        return None if (math.isinf(obj) or math.isnan(obj)) else obj
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


def entry_reaches(entry_id: str) -> dict:
    """What each rule selects, for the review UI — AND what the bundle will actually ship.

    The per-extent geometry comes from `pipeline.atlas.reach.extent`; the OUTCOME of each
    rule comes from `pipeline.atlas.reach`, the same builder that writes the artifact. That
    matters: the builder makes decisions the raw resolver does not — a straddling piece is
    included for a closure and excluded otherwise, an empty reach after the row's scope is
    a distinct failure from an unresolvable one. If the curator confirmed against the raw
    resolver instead, they would be signing off on something subtly different from what
    ships.

    So each rule also carries `outcome`, `reason` and `diagnostics` (doc 10 ㊴).
    """
    for _, e in _all_entries():
        if e["entry_id"] != entry_id:
            continue
        covered = _covered_ids(e)
        clip, scope_failed = _scope_sections(e, covered)
        shared = _shared_waters()
        tidal = _tidal()
        owned = _rowed_waters()

        out: dict = {}
        per_rule: dict[str, list] = {}
        for r in e.get("rules") or []:
            per = [_clip(resolve_extent(covered, ex), clip) for ex in (r.get("extents") or [])]
            out[r["rule_id"]] = per
            per_rule[r["rule_id"]] = per

        # ONE call into the builder — the same function the artifact build uses, so the
        # app cannot resolve, clip, classify or expand differently from what ships.
        verdict: dict = {}
        for r in e.get("rules") or []:
            binding, diags = _build_reach(e, r, _registry(), _graph(), covered=covered, clip=clip,
                                          shared=shared, tidal=tidal, owned=owned)
            verdict[r["rule_id"]] = {
                "outcome": binding.outcome.value,
                "reason": binding.reason.value if binding.reason else None,
                "detail": binding.detail,
                "n_sections": len(binding.sections),
                "sections": list(binding.sections),
                "tributaries_pending": binding.tributaries_pending,
                "diagnostics": [{"kind": d.kind, **d.payload} for d in diags],
            }

        return _json_safe({"covered": covered, "rules": out, "scope_sections": sorted(clip or ()),
                           "scope_unresolved": scope_failed, "verdict": verdict})
    return {}


def _clip(got: dict | None, clip: set[str] | None) -> dict | None:
    """Cut a resolved reach down to the entry's scope, recomputing the waters it lands on.

    An empty result is kept as an empty reach rather than turned into None: "this rule selects nothing
    inside this row's stretch" is a real answer and a curation signal, whereas None means the extent
    could not be resolved at all."""
    if got is None or clip is None:
        return got
    sections = [n for n in got["sections"] if n in clip]
    return {**got, "sections": sections,
            "unclassified": [n for n in got["unclassified"] if n in clip],
            "waters": _waters(sections)}


def related_entries(entry_id: str) -> list[dict]:
    """Other entries covering ANY of the same registry items — the ones to review alongside this one.

    The synopsis splits one regulation across several rows: "CHILLIWACK / VEDDER RIVERS …" carries the
    rules, and a separate "VEDDER RIVER" row just says *See Chilliwack River*. They are different rows
    (so different entries, faithful to the synopsis), but they regulate the same water, and reviewing
    one without the other is how a half-linked entry gets confirmed. Overlap is computed on `matched`,
    so a combined override's extra items count.
    """
    reg = _registry()
    mine: set[str] = set()
    for _, e in _all_entries():
        if e["entry_id"] == entry_id:
            mine = set(_covered_ids(e))
            break
    if not mine:
        return []
    out: list[dict] = []
    for reg_id, e in _all_entries():
        if e["entry_id"] == entry_id:
            continue
        shared = mine & set(_covered_ids(e))
        if not shared:
            continue
        out.append({
            "entry_id": e["entry_id"],
            "region": reg_id if isinstance(reg_id, str) else _ident(e)["region"],
            "name": _ident(e)["name"],
            "n_rules": len(e.get("rules", [])),
            # a pointer row ("See Chilliwack River") is the common case worth calling out
            "pointer": e.get("regs_verbatim", "").strip().lower().startswith("see "),
            "shared_items": [{"id": i, "name": reg[i].name} for i in sorted(shared) if i in reg],
        })
    return sorted(out, key=lambda r: r["name"])


def entry_kind(e: dict) -> str:
    """`zone` or `water` — WHICH PART OF THE BOOK this entry came from.

    A `z`-prefixed entry is a REGIONAL or PROVINCIAL rule, transcribed from a region chapter
    or the provincial pages: "single barbless hook in all streams of Region 1", the Cutthroat
    Trout Reward Tagging Program on page 12. It names no water on purpose, and its reach is
    an AREA carried on the entry (`within area:region:1`).

    Everything else is a row of a water table and names one water.

    They were shown as one list, so a regional notice appeared as a synopsis row that had
    failed to match an item — correctness rendered as a defect, and the two kinds of
    regulation mixed in a queue where they need different questions asked of them.
    """
    return "zone" if str(e.get("entry_id", "")).startswith("z") else "water"


def entry_status(e: dict, item: dict | None) -> str:
    """One coarse status for the queue ordering/badge."""
    # A zone entry has no water to match, so `no_registry` is not a finding about it — it is
    # the definition of it. Saying otherwise put 117 correct entries at the top of the queue.
    if entry_kind(e) == "zone":
        return "flagged" if _flagged(e) else "zone"
    if item is None:              # `matched` names no item this build has: it binds nothing
        return "no_registry"
    if _flagged(e):
        return "flagged"
    if unused_curated_splits(e, item):
        return "unused_splits"
    return "unreviewed"


_STATUS_ORDER = {"no_registry": 0, "flagged": 1, "unused_splits": 2, "unreviewed": 3,
                 "zone": 4}


#: Zone first. A regional rule binds to EVERY stream in its region — one of them is 160,000
#: section-bindings where a water row is a few dozen — and none of the 117 has ever been
#: through the parser or the agent review pass: they were transcribed by hand from the region
#: chapters. Highest leverage, least verified, so they lead. `kind=water` filters them out
#: when the job is the water tables.
_KIND_ORDER = {"zone": 0, "water": 1}


def queue(region: str | None = None, status: str | None = None,
          kind: str | None = None) -> list[dict]:
    """Review queue rows, sorted so items needing attention float to the top.

    Regional/provincial entries form their own block ahead of the water rows — they answer a
    different question and are checked against a different source (the region chapter, not a
    table row), so interleaving them by name made the queue two jobs shuffled together.
    """
    rows = []
    src = [(region, e) for eid, e in load_region(region).items()] if region else _all_entries()
    for reg_id, e in src:
        item = _item_for_entry(e)
        st = entry_status(e, item)
        if status and st != status:
            continue
        if kind and entry_kind(e) != kind:
            continue
        rows.append({
            "entry_id": e["entry_id"],
            "region": reg_id if isinstance(reg_id, str) else _ident(e)["region"],
            "name": _ident(e)["name"],
            "mus": _ident(e)["mus"],
            "status": st,
            "kind": entry_kind(e),        # zone (regional/provincial) | water (a table row)
            "n_rules": len(e.get("rules", [])),
            "matched_item_id": item.id if item else None,
            "matched_item_name": item.name if item else None,
            "also_item_ids": [a["id"] for a in _also_items(e)],      # combined-override items
            "unused_curated_splits": len(unused_curated_splits(e, item)),
        })
    rows.sort(key=lambda r: (_KIND_ORDER.get(r["kind"], 9),
                             _STATUS_ORDER.get(r["status"], 9), r["name"]))
    return rows


def entry_detail(entry_id: str) -> dict | None:
    """Full entry + its item's boundaries/variants + unused curated splits.

    The entry is served with each rule's and each licensing record's GENERATED `label` stamped on
    it — `catalogue.label` / `catalogue.licensing_label`, the functions the bundle uses, so the
    curator reads exactly what the reader will. `save_entry` strips them again.

    `match` is a SUGGESTION, filled only when `matched` is empty: the live matcher's answer for the
    row's name, for the attach flow. It is never the entry's water until attached and saved."""
    for reg_id, e in _all_entries():
        if e["entry_id"] != entry_id:
            continue
        item = _item_for_entry(e)
        lab = model_api.labels(e, ENTRIES_DIR, _place_namer())
        e = dict(e,
                 rules=[dict(r, label=lab["rules"][i] or r.get("verbatim", ""))
                        for i, r in enumerate(e.get("rules") or [])],
                 licensing=[dict(x, label=lab["licensing"][i] or x.get("verbatim", ""))
                            for i, x in enumerate(e.get("licensing") or [])])
        match = None
        if item is None and entry_kind(e) == "water":
            mr = _suggest_match(e)
            match = {"item_id": mr.item_id, "status": mr.status, "reason": mr.reason,
                     "candidates": list(mr.candidates)}
        return {
            "entry": e,
            "kind": entry_kind(e),
            "region": reg_id,
            "match": match,
            "item": None if not item else {
                "id": item.id, "name": item.name, "kind": item.kind,
                "variants": list(item.variants), "mus": list(item.mus),
                # built graph ∪ live splits.json, over the primary item AND a combined
                # override's other items — the same closed set the parser was given, so a
                # Vedder rule on the Chilliwack/Vedder entry has a Vedder cut-point to bind.
                "boundaries": _combined_bindable(item, e),
            },
            "also_items": _also_items(e),      # a combined override's other items (Vedder, …)
            "related_entries": related_entries(entry_id),   # other rows over the same water
            "unused_curated_splits": unused_curated_splits(e, item),
            "source_image": entry_source_image(e),
        }
    return None


def species_list() -> list[dict]:
    """Every species code a rule may name — `catalogue.KNOWN_SPECIES`, with the model's own words
    for it and, for a group, its members. It was read from `species.COMMON_NAME`, the official CSV
    list, which is a different vocabulary: it offered codes the model refuses."""
    return sorted(model_api.vocab([])["species"], key=lambda d: d["name"].lower())


def vocab() -> dict:
    """Every option list the editors offer, read off the model (see `model_api.vocab`)."""
    return model_api.vocab([e for _, e in _all_entries()])


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
# Save (validated) to the catalogue region files
# --------------------------------------------------------------------------- #

def _rep_point(geom: dict | None) -> list[float] | None:
    """A representative lon/lat point for a GeoJSON geometry (its first coordinate) — used to drop a
    clickable marker at a lake boundary's location."""
    if not geom:
        return None
    c = geom.get("coordinates")
    while isinstance(c, list) and c and isinstance(c[0], list):
        c = c[0]
    return c if isinstance(c, list) and len(c) >= 2 and isinstance(c[0], (int, float)) else None



def rule_resolved_reach(entry_id: str, rule_id: str, limit: int = 6000) -> dict:
    """**What this rule actually covers, and what it excepts** — drawn from the reach builder.

    One question, asked once: *what is IN this rule and what is OUT?* The answer is
    `pipeline.atlas.reach.build.build_reach`, the same call the artifact build makes, so what a
    curator confirms here is exactly what ships.

    ## Why this cannot come from the item layer

    The map draws the entry's own item geometry and highlights sections within it. That works
    only while a rule stays inside its own water, and two common shapes do not:

    * **tributaries** — "between A and B, including tributaries" resolves to streams that are
      other registry items entirely;
    * **`within(area)`** — "Pitt River within Garibaldi Park" binds **466** sections of which
      only **18** belong to the Pitt. The other 448 — 96% of the rule — had no geometry loaded
      and simply could not appear on the map.

    So this returns geometry for every bound section the item layer does not already carry,
    plus the carve-outs, and lets the map draw both.

    ## What comes back

    * ``direct``   sections the extents resolve to before any expansion
    * ``added``    what the builder ADDED — the tributary walk, or an area's other waters
    * ``excluded`` what the EXCEPT carve-outs removed. Drawn separately and in a different
      colour: "except Hunlen Creek above the falls" should be *visible as an exclusion*, not
      inferable from a smaller total.

    `sections` is `direct | added` — the exact set the build ships. It is deliberately NOT
    trimmed to one level of tributaries: showing less than ships is precisely the failure the
    `build_reach` docstring records, and costs nothing to avoid since the builder has already
    walked it.

    Capped at ``limit`` sections with the cap REPORTED, never hidden.
    """
    entry = next((e for _, e in _all_entries() if e["entry_id"] == entry_id), None)
    if entry is None:
        return {"error": f"no entry {entry_id}"}
    rule = next((r for r in entry.get("rules") or [] if r.get("rule_id") == rule_id), None)
    if rule is None:
        return {"error": f"no rule {rule_id} on {entry_id}"}

    reg, graph = _registry(), _graph()
    covered = _covered_ids(entry)
    clip, _scope_failed = _scope_sections(entry, covered)

    # DIRECT = resolve + clip, no expansion. Same path entry_reaches uses for the raw view, so
    # the two panels cannot disagree about where the extent itself ends.
    direct: set[str] = set()
    for ex in rule.get("extents") or []:
        got = _clip(_resolve.resolve_extent(reg, graph, covered, ex), clip)
        if got:
            direct |= set(got.get("sections") or ())

    binding, _diags = _build_reach(entry, rule, reg, graph, covered=covered, clip=clip,
                                   shared=_shared_waters(), tidal=_tidal(),
                                   owned=_rowed_waters())
    total = set(binding.sections)
    if any((ex or {}).get("op") == "rest" for ex in rule.get("extents") or []):
        # "OTHER PARTS" (op `rest`) resolves only against its siblings, so no extent alone says
        # its direct reach: it is what the builder bound, less what its own walk added.
        direct = total - set(binding.via_tributary)
    added = sorted(total - direct)

    # Carve-outs only bite where the rule actually expands to tributaries — `classify` calls the
    # expander only then, so on a mainstem-only rule the builder never applies them. Reporting
    # them anyway said "out 1,466" on rules the EXCEPT does not touch (Atnarko r4/r7, both
    # "Bella Coola River mainstem only"), which reads as water removed from a reach that never
    # contained it. `carve_outs_apply` keeps the distinction visible instead of just hiding them.
    wants = bool(_wants_tributaries(rule, entry))
    carve_rows, blocked = ([], set())
    if wants:
        carve_rows, blocked = resolve_carve_outs(entry, rule, reg, graph, covered)
        for row in carve_rows:                  # the UI wants counts, not 3,000 ids
            row["n_sections"] = len(row.pop("sections"))
    n_authored = len(rule.get("tributary_excludes") or [])

    # Geometry for everything the item layer does NOT already carry, plus the exclusions.
    own: set[str] = set()
    for iid in covered:
        it = reg.get(iid)
        if it:
            own |= set(it.section_ids)
    draw_in = [n for n in sorted(total) if n not in own]
    draw_out = sorted(blocked)
    truncated = len(draw_in) + len(draw_out) > limit
    room = max(limit - len(draw_out), 0)

    feats = _sections_geojson(draw_out[:limit], "reach_excluded")
    feats += _sections_geojson(draw_in[:room], "reach_extra")

    return _json_safe({
        "entry_id": entry_id, "rule_id": rule_id,
        "outcome": binding.outcome.value,
        "wants_tributaries": wants,
        # the entry/rule AUTHORS this many carve-outs; they apply only to a tributary rule
        "n_carve_outs_authored": n_authored,
        "carve_outs_apply": wants and n_authored > 0,
        "tributaries_only": bool(rule.get("tributaries_only")),
        "within_area": any((ex or {}).get("op") == "within" for ex in rule.get("extents") or []),
        "n_direct": len(direct), "n_added": len(added), "n_total": len(total),
        "n_excluded": len(blocked),
        "n_offitem": len(draw_in),          # bound sections the item layer never had
        "carve_outs": carve_rows,
        "sections": sorted(total),
        "truncated": truncated, "limit": limit,
        "geojson": {"type": "FeatureCollection", "features": feats},
    })


def _sections_geojson(section_ids: list[str], kind: str) -> list[dict]:
    """Geometry for an arbitrary set of section ids, streams and waterbodies alike.

    Sections are `{blk}:{measure}` (the `streams` layer) or `lake:{wbk}` (the `lakes` layer);
    a rule's reach routinely contains both, and a lake dropped silently would leave a hole in
    the middle of a drawn reach."""
    feats: list[dict] = []
    streams = [n for n in section_ids if not str(n).startswith("lake:")]
    for i in range(0, len(streams), 400):                       # chunk long IN lists
        feats += _features("streams", streams[i:i + 400], kind)
    wbks = [str(n).split(":", 1)[1] for n in section_ids if str(n).startswith("lake:")]
    for i in range(0, len(wbks), 400):
        for lf in _features("lakes", wbks[i:i + 400], kind):
            wbk = str(lf["properties"].get("wbk") or "")
            if wbk:
                lf["properties"]["node_id"] = f"lake:{wbk}"     # highlight matches on node_id
            feats.append(lf)
    return feats



# --------------------------------------------------------------------------- #
# MAP GEOMETRY, from the build's own artifacts.
#
# This read `graph.gpkg` until that artifact was turned off. It was 742 of the build's 1,810
# seconds — 41%, more than the splits and the graph construction together — and the commit that
# made it `--write-gpkg` (off by default) recorded that "nothing downstream read it". The DFO
# dossier, its other consumer, had already moved to `item_points.json`. This file had not, so the
# map went blank for every item: streams, lakes, splits, the lot. Nothing raised, because
# `_read_gpkg_features` returned [] for a missing file rather than 500 — the endpoints kept
# answering 200 with an empty FeatureCollection, which on a map is indistinguishable from a water
# that has no geometry.
#
# The geometry was never actually gone. `geometries.pkl` holds all 2,056,634 sections, streams and
# `lake:{wbk}` nodes alike, and `waterbody_polys.pkl` holds the polygon for an isolated waterbody
# no stream runs through — which is exactly the pair `export_graph_gpkg` itself consumed to write
# the gpkg in the first place. So this reads the source the gpkg was derived FROM, one step earlier
# in the same chain, and the 4.7 GB intermediate stays off.
#
# Loaded once per process and then dict lookups, so it is faster than the driver-level WHERE it
# replaces; the cost is ~2.9 GB resident, which is the right trade for a local single-curator tool.
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=1)
def _geoms() -> dict:
    """node_id -> geometry (EPSG:3005), for `{blk}:{measure}` sections AND `lake:{wbk}` nodes."""
    if not GEOMS_PKL_PATH.exists():
        return {}
    from pipeline.common.io.serialize import read_artifact
    return read_artifact(str(GEOMS_PKL_PATH))


@lru_cache(maxsize=1)
def _wbk_polys() -> dict:
    """wbk -> its own FWA polygon (EPSG:3005). The fallback for a MINTED waterbody: a lake no
    stream passes through has no section geometry, so its own polygon stands in — the same
    substitution `export_graph_gpkg` made."""
    if not WBK_POLYS_PKL_PATH.exists():
        return {}
    from pipeline.common.io.serialize import read_artifact
    return read_artifact(str(WBK_POLYS_PKL_PATH))


@lru_cache(maxsize=1)
def _to_lonlat():
    """BC Albers -> lon/lat, so features overlay the web-mercator PMTiles basemap in MapLibre."""
    from pyproj import Transformer
    return Transformer.from_crs(3005, 4326, always_xy=True).transform


@lru_cache(maxsize=1)
def _blk_index() -> dict:
    """blk -> [(down_m, up_m, node_id)], sorted. Used to place a split, which `splits.resolved.json`
    records as a blue-line key and a route measure and NOT as a coordinate — the gpkg carried the
    interpolated point, so it has to be recomputed here."""
    idx: dict[str, list] = {}
    for n in _graph().nodes.values():
        if getattr(n, "blk", None) is None:
            continue
        idx.setdefault(str(n.blk), []).append(
            (getattr(n, "down_m", 0.0) or 0.0, getattr(n, "up_m", 0.0) or 0.0, n.node_id))
    for v in idx.values():
        v.sort()
    return idx



@lru_cache(maxsize=4096)
def _fwa_lake_poly(wbk: str):
    """A lake's REAL outline (EPSG:3005) from the FWA source, by WATERBODY_KEY.

    THE LINE UNDER A LAKE IS NOT THE LAKE. A lake is a graph node because stream fids run THROUGH
    it, so its `geometries.pkl` entry is that through-line — Kootenay Lake came back as a
    MultiLineString, which on a map is a river with a name the curator does not recognise, drawn
    across water whose shape they are trying to verify. The gpkg had the same defect, because
    `export_graph_gpkg` read the same sidecar.

    And some waterbodies have no sidecar geometry at all: BLUEY 1 is a registry item, is named, is
    inside a **No Fishing** closure, and drew NOTHING — the layer it needed was never written for
    it. Its polygon is in the source data the whole time, one lookup away on the key the item is
    already named by.

    Read straight from the fisheries geopackage on the same WATERBODY_KEY the `wbk:` item id
    carries, so this is a lookup rather than a guess; cached per key. Returns None when the key is
    not a lake, and the caller falls back to the through-line.
    """
    import sqlite3
    src = SOURCE / "bc_fisheries_data.gpkg"
    if not src.exists():
        return None
    try:
        from shapely import wkb as _wkb
        from shapely.ops import unary_union
        db = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
        rows = db.execute("SELECT geom FROM lakes WHERE WATERBODY_KEY = ?", (int(wbk),)).fetchall()
        db.close()
        parts = []
        for (blob,) in rows:
            if not blob:
                continue
            # GeoPackage binary: "GP", version, flags, srs_id, an envelope whose size the flags
            # encode, then the WKB proper.
            env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}.get((blob[3] >> 1) & 0x07, 0)
            g = _wkb.loads(bytes(blob[8 + env:]))
            if g is not None and not g.is_empty:
                parts.append(g)
        if not parts:
            return None
        return parts[0] if len(parts) == 1 else unary_union(parts)
    except Exception:  # noqa: BLE001 — a missing layer/key must not 500 the map
        return None


def _geo(geom, kind: str, props: dict) -> dict | None:
    """One reprojected GeoJSON feature, or None when the geometry is missing/empty."""
    if geom is None or getattr(geom, "is_empty", True):
        return None
    from shapely.geometry import mapping
    from shapely.ops import transform
    props = {k: _clean_val(v) for k, v in props.items()}
    props["kind"] = kind
    return {"type": "Feature", "geometry": mapping(transform(_to_lonlat(), geom)),
            "properties": props}


def _split_point(split_id: str):
    """A split's coordinate, interpolated along the section that contains its route measure."""
    from shapely.geometry import Point
    meta = _splits_meta().get(split_id)
    if not meta or not meta.get("blk"):
        return None
    m = float(meta.get("route_measure") or 0.0)
    for down, up, nid in _blk_index().get(str(meta["blk"]), []):
        if down <= m <= up:
            g = _geoms().get(nid)
            if g is None or g.is_empty:
                return None
            # `route_measure` is chainage along the blue line; the section starts at `down`.
            d = min(max(m - down, 0.0), g.length)
            return Point(g.interpolate(d))
    return None


def _features(layer: str, ids, kind: str) -> list[dict]:
    """The `graph.gpkg` layers, served from the artifacts they were built from.

    Same three layer names and the same feature shape the client already styles on, so this is a
    swap of the source, not of the contract."""
    feats: list[dict] = []
    nodes = _graph().nodes
    if layer == "streams":
        for nid in ids:
            n = nodes.get(nid)
            f = _geo(_geoms().get(nid), kind,
                     {"node_id": nid,
                      "display_name": getattr(n, "display_name", None) if n else None,
                      "location_identifier": getattr(n, "location_identifier", None) if n else None})
            if f: feats.append(f)
    elif layer == "lakes":
        for wbk in ids:
            wbk = str(wbk)
            nid = f"lake:{wbk}"
            # The real outline first — a lake drawn as the river through it is not a lake.
            g = _fwa_lake_poly(wbk)
            if g is None or getattr(g, "is_empty", True):
                g = _geoms().get(nid)             # fall back to the through-line
            if g is None or getattr(g, "is_empty", True):
                g = _wbk_polys().get(wbk)         # minted/isolated: its own polygon stands in
            n = nodes.get(nid)
            f = _geo(g, kind, {"wbk": wbk,
                               "display_name": getattr(n, "display_name", None) if n else None})
            if f: feats.append(f)
    elif layer == "split_points":
        for sid in ids:
            meta = _splits_meta().get(sid) or {}
            f = _geo(_split_point(sid), kind,
                     {"split_id": sid, "label": meta.get("label"),
                      "anchor_type": meta.get("anchor_type"), "picked_up": meta.get("picked_up")})
            if f: feats.append(f)
    return feats


def item_side_channels(item_id: str) -> list[str]:
    """Registry items that are ANABRANCHES of this one — a channel that leaves the river and rejoins it.

    A named side channel is split into its own item on purpose: folded in, its name became the river's
    (the Fraser displayed as "Seabird Island North Side Channel") and a rule about the slough resolved
    to the whole mainstem. But splitting it out then dropped it off the river's map, so the Fraser drew
    its dozens of ANONYMOUS side channels — those are still its own sections — while Seabird Island
    North Side Channel, Herrling Island Side Channel and Maria Slough silently vanished.

    Told apart from a tributary by direction, not by name: a side channel both TAKES water from this
    item and RETURNS it, while a tributary only ever flows in."""
    reg = _registry()
    it = reg.get(item_id)
    if it is None:
        return []
    g = _graph()
    own = set(it.section_ids)
    takes_from, gives_to = set(), set()
    for nid in own:
        for i in g.down_adj.get(nid, []):
            if (t := g.edges[i].to_node) not in own:
                takes_from.add(t)                          # water leaves us and enters them
        for i in g.up_adj.get(nid, []):
            if (f := g.edges[i].from_node) not in own:
                gives_to.add(f)                            # water comes back from them
    owner: dict[str, str] = {}
    for iid, item in reg.items():
        if iid == item_id:
            continue
        for sid in item.section_ids:
            owner[sid] = iid
    a = {owner[n] for n in takes_from if n in owner}
    b = {owner[n] for n in gives_to if n in owner}

    # An anabranch is the MINOR channel, and "leaves and rejoins" is symmetric: Maria Slough takes
    # from the Fraser and gives back to it, so without this the Fraser is Maria Slough's side channel
    # too. Every covered side-channel item then dragged the whole river in behind it and the Fraser
    # was drawn thirteen times over — 1,996 features for one river. Length breaks the tie: the small
    # one is the channel.
    def _len(iid: str) -> float:
        item = reg.get(iid)
        if item is None:
            return 0.0
        return sum((n.length_m or 0.0) for sid in item.section_ids
                   if (n := g.nodes.get(sid)) is not None)

    mine = _len(item_id)
    return sorted(i for i in (a & b) if _len(i) < mine)


def item_geojson(item_id: str) -> dict:
    """Stream sections + curated split points for a registry item, as a GeoJSON FeatureCollection in
    EPSG:4326 (lon/lat) so it overlays the web-mercator PMTiles basemap. Streams selected by the item's
    own `section_ids`; splits by its curated `split:*` boundary ids. Features carry `properties.kind` =
    'stream' | 'split' for styling (colouring is done client-side)."""
    item = _registry().get(item_id)
    if item is None:
        return {"type": "FeatureCollection", "features": []}

    feats: list[dict] = []
    section_ids = [n for n in item.section_ids if not str(n).startswith("lake:")]
    for i in range(0, len(section_ids), 400):                      # chunk long IN lists
        chunk = section_ids[i:i + 400]
        feats += _features("streams", chunk, "stream")

    # Named side channels draw WITH the river. They are separate items so their names and their rules
    # stay their own, but on the map a river missing its named channels while showing every anonymous
    # one is just wrong. Tagged `side_channel` so the client can style them apart from the mainstem.
    reg = _registry()
    side_ids = [n for sid in item_side_channels(item_id)
                for n in reg[sid].section_ids if not str(n).startswith("lake:")]
    for i in range(0, len(side_ids), 400):
        feats += _features("streams", side_ids[i:i + 400], "side_channel")

    # A LAKE/wetland item's sections are `lake:{wbk}` nodes, which live in the `lakes` layer, not
    # `streams` — without this a lake item draws nothing at all (the Vedder Canal on the combined
    # Chilliwack/Vedder entry). Its own wbk is included so an isolated lake with no graph node
    # (minted by add_waterbody_items) still gets its polygon.
    lake_wbks = {str(n).split(":", 1)[1] for n in item.section_ids if str(n).startswith("lake:")}
    if item.id.startswith("wbk:"):
        lake_wbks.add(item.id.split(":", 1)[1])
    if lake_wbks:
        lake_feats = _features("lakes", sorted(lake_wbks), "waterbody")
        # Stamp the graph node id the same way a stream feature carries it. The reach highlight matches
        # features on `node_id`, so without this a lake or canal in a rule's reach draws nothing — the
        # Vedder Canal is `lake:329707189` and is the WHOLE extent of three Chilliwack/Vedder rules.
        for lf in lake_feats:
            wbk = str(lf["properties"].get("wbk") or "")
            if wbk:
                lf["properties"]["node_id"] = f"lake:{wbk}"
        feats += lake_feats

    split_ids = list(_curated_split_ids(item))
    if split_ids:
        feats += _features("split_points", split_ids, "split")

    # Auto (non-curated) LAKE boundaries have no split_point; surface each as a clickable point at the
    # lake's location so a curator can "see where it is" — same select/fly machinery as curated splits.
    lake_by_wbk = {b.wbk: b for b in _boundaries(item)
                   if b.wbk and not str(b.ref or "").startswith("split:")}
    if lake_by_wbk:
        for lf in _features("lakes", list(lake_by_wbk), "split"):
            b = lake_by_wbk.get(str(lf["properties"].get("wbk")))
            pt = _rep_point(lf.get("geometry"))
            if b and pt:
                feats.append({"type": "Feature",
                              "geometry": {"type": "Point", "coordinates": pt},
                              "properties": {"kind": "split", "split_id": b.id, "label": b.label,
                                             "anchor_type": "lake", "auto": True}})

    return {"type": "FeatureCollection", "features": feats}


# --------------------------------------------------------------------------- #
# Editing splits.json (the hand-curated split source of truth)
# --------------------------------------------------------------------------- #
# splits.json is now hand-curated directly (waterbody-splits.json + build_splits are gone). Edits here
# write it in place; they take effect only after a graph rebuild (pipeline.atlas.build --full — CPU only, no
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


def _extent_lists(e: dict):
    """(part id, label, extents list) for every place an entry stores extents: its scope, each
    rule's extents and tributary excludes, each licensing record's."""
    yield "(entry scope)", "the entry's own reach", e.get("extents") or []
    for r in e.get("rules") or []:
        for key in ("extents", "tributary_excludes"):
            yield r["rule_id"], _label(r), r.get(key) or []
    for x in e.get("licensing") or []:
        for key in ("extents", "tributary_excludes"):
            yield x.get("id", ""), f"{x.get('kind')}: {x.get('verbatim', '')}", x.get(key) or []


def split_refs(split_id: str) -> list[dict]:
    """Every rule, licensing record or entry scope whose extents bind this split id (across the
    catalogue region files). The IMPACT PREVIEW for a rename — the exact scope it rewrites."""
    out: list[dict] = []
    for region in regions():
        for eid, e in load_region(region).items():
            seen: set = set()
            for part, label, exts in _extent_lists(e):
                if part not in seen and any(split_id in (ex.get("splits") or []) for ex in exts):
                    seen.add(part)
                    out.append({"entry_id": eid, "region": region, "rule_id": part,
                                "label": label, "entry_name": e.get("name", "")})
    return out


def rename_split(old_id: str, new_id: str) -> dict:
    """Rename a split id in splits.json AND rewrite every rule extent that binds it (in the catalogue
    region files). Union validation means the new id is bindable immediately; a rebuild reconciles
    the graph."""
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

    # cascade: rewrite rules that bind old_id -> new_id, and save each affected entry
    updated: list[dict] = []
    failed: list[dict] = []
    refs = split_refs(old_id)
    for region, eid in dict.fromkeys((r["region"], r["entry_id"]) for r in refs):
        e = load_region(region).get(eid)
        if not e:
            continue
        for _part, _label_, exts in _extent_lists(e):
            for ex in exts:
                if old_id in (ex.get("splits") or []):
                    ex["splits"] = [new_id if s == old_id else s for s in ex["splits"]]
        res = save_entry(region, e)
        for ref in (r for r in refs if r["entry_id"] == eid):
            (updated if res["ok"] else failed).append(
                {**ref, **({} if res["ok"] else {"error": res["errors"]})})
    return {"ok": True, "errors": [], "new_id": new_id, "updated_rules": updated, "failed_rules": failed}


def _label(rule: dict) -> str:
    """The generated line for one rule, for the split-rename impact preview. A rule too malformed
    to render shows its verbatim, so a curator looking at it still sees something."""
    try:
        return rule_label(CatalogueRule.model_validate(rule)) or (rule.get("verbatim") or "")
    except Exception:                                    # noqa: BLE001
        return (rule.get("verbatim") or "")


def _check_splits(entry_dict: dict, allowed: set[str]) -> list[dict]:
    """Every split id an extent binds must be a cut-point the curator can actually bind: the entry
    scope, every rule's extents, and every licensing record's extents. A cut-point answers to its
    own id AND to any alias of it, because extent.py resolves either."""
    errs: list[dict] = []

    def visit(extents, path: list) -> None:
        for i, ex in enumerate(extents or ()):
            if not isinstance(ex, dict):
                continue
            for sid in (ex.get("splits") or ()):
                if sid not in allowed:
                    errs.append({"path": ".".join(map(str, path + [i, "splits"])),
                                 "msg": f"unknown split id {sid!r} — not a cut-point of any water "
                                        f"this entry or extent names"})

    visit(entry_dict.get("extents"), ["extents"])
    for j, r in enumerate(entry_dict.get("rules") or ()):
        if isinstance(r, dict):
            visit(r.get("extents"), ["rules", j, "extents"])
    for j, x in enumerate(entry_dict.get("licensing") or ()):
        if isinstance(x, dict):
            visit(x.get("extents"), ["licensing", j, "extents"])
    return errs


_atomic_write = io.atomic_write                          # shared helper (io is the single home)


def _referenced_item_ids(entry_dict: dict) -> set[str]:
    """Every other-item id an extent scopes to (entry scope, rule extents and tributary excludes,
    licensing extents and tributary excludes)."""
    ids: set[str] = set()

    def _scan(extents):
        for ex in extents or []:
            if isinstance(ex, dict):
                if ex.get("item_id"):
                    ids.add(ex["item_id"])
                ids.update(ex.get("item_ids") or ())

    _scan(entry_dict.get("extents"))
    for r in list(entry_dict.get("rules") or []) + list(entry_dict.get("licensing") or []):
        if isinstance(r, dict):
            _scan(r.get("extents"))
            _scan(r.get("tributary_excludes"))
    return ids


def _find(entry_id: str) -> tuple[str, dict] | None:
    for region, e in _all_entries():
        if e["entry_id"] == entry_id:
            return region, e
    return None


def check_entry(entry_dict: dict, region: str | None = None) -> dict:
    """EVERYTHING a save would refuse, without writing: the model, the pass-through fields, the
    split ids. Plus the generated label of every rule and record, so the editor shows what the app
    will say while the curator types. `{ok, errors, warnings, labels}`; each error is
    `{path, msg}` with `path` in the entry's own JSON keys (`rules.2.gear.0.max`)."""
    data = model_api.strip_served(entry_dict)
    labels = model_api.labels(data, ENTRIES_DIR, _place_namer())
    entry, errs = model_api.check(data)
    found = _find(str(data.get("entry_id", "")))
    if found is None:
        errs.append({"path": "entry_id", "msg": f"{data.get('entry_id')!r} is not an entry in "
                     f"the catalogue — the app edits entries, it does not create or move them"})
    else:
        on_region, on_disk = found
        if region is not None and region != on_region:
            errs.append({"path": "region", "msg": f"the entry is in region-{on_region}.json, not "
                                                 f"region-{region}.json"})
        errs += model_api.pass_through_changes(on_disk, data)
    if entry is not None:
        item = _item_for_entry(data)
        allowed = _bindable_ids(item)           # built graph ∪ splits.json — bind pending splits too
        for iid in _referenced_item_ids(data):  # + splits of any cross-item extent / exclude
            it2 = _registry().get(iid)
            if it2:
                allowed |= _bindable_ids(it2)
        errs += _check_splits(data, allowed)
    return {"ok": not errs, "errors": errs, "warnings": model_api.warnings_of(data),
            "labels": labels}


def save_entry(region: str, entry_dict: dict) -> dict:
    """Validate an edited entry (`check_entry`) and write it back to its region file, the single
    source of truth, through `io.write_entryfile` — every other entry in the file is written back
    exactly as it was read, and the whole file is validated as written. Returns `{ok, errors}`.

    There is no confirm/lock — a catalogue entry has no such field; a re-parse leaves an entry
    edited here alone (see the module doc)."""
    res = check_entry(entry_dict, region)
    if not res["ok"]:
        return {"ok": False, "errors": res["errors"]}
    entry, _ = model_api.check(model_api.strip_served(entry_dict))
    path = ENTRIES_DIR / f"region-{region}.json"
    existing: dict[str, object] = dict(io.read_entryfile(path))
    existing[entry.entry_id] = entry
    try:
        io.write_entryfile(path, region, existing.values())
    except Exception as ex:  # noqa: BLE001
        return {"ok": False, "errors": [{"path": "", "msg": f"file: {ex}"}]}
    return {"ok": True, "errors": []}
