"""Data layer for the curation-review app — **reuses pipeline code, reimplements nothing.**

- Matching an entry -> registry item is done by `pipeline.matching.matcher` (the exact matcher the
  pipeline uses), so the review tool resolves geometry/boundaries the same way the build does.
- Validation on save is `pipeline.parsing.entry_models.Entry` + `validate_entry_splits`.
- "Unused curated splits" reuses `entry_models.unused_splits`, restricted to curated (`ref="split:*"`)
  boundaries so lake/outlet/headwaters auto-boundaries don't count.

Write model: `pipeline/parsing/entries/region-*.json` are the SINGLE SOURCE OF TRUTH. Curator decisions
are written straight back to them (the old separate reviewed/ overlay has been merged in and retired).
A parser re-run preserves `locked` entries and, with --skip-existing, skips entries already present —
so curator edits are safe as long as re-parses stay targeted.
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
from pipeline.parsing import io
from pipeline.parsing.entry_models import Entry, unused_splits, validate_entry_splits
from pipeline.parsing.rows import load_synopsis_rows
from project_config import get_config
from pipeline.registry import load_registry
from pipeline.reach.build import build_reach as _build_reach
from pipeline.reach import extent as _resolve
from pipeline.utils.wsc import trim_wsc

_ROOT = Path(__file__).resolve().parents[2]
ENTRIES_DIR = _ROOT / "pipeline" / "parsing" / "entries"
# The build the app SERVES. `project_config.review_build_dir` is the one name for it, shared with
# rebuild.py (which writes this same directory) — see config.yaml `output.review_build`. Hard-coding
# it here meant the app could serve a build months older than the pipeline and say nothing: every
# reach still renders, just against a stale graph, so a reviewer signs off on the wrong water.
_BUILD = get_config().review_build_dir
REGISTRY_PATH = _BUILD / "registry.json"
OVERRIDES_PATH = _ROOT / "pipeline" / "matching" / "overrides.json"
SPLITS_RESOLVED_PATH = _BUILD / "splits.resolved.json"
GRAPH_GPKG_PATH = _BUILD / "graph.gpkg"
GRAPH_PKL_PATH = _BUILD / "graph.pkl"
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
    (registry.json, splits.resolved.json, graph.gpkg split_points, graph.pkl). Call after a graph
    rebuild. item_geojson reads graph.gpkg fresh on every call, so it needs no clearing; the species
    list and row-image index come from static source, not the build, so they are left warm.

    `_graph` MUST be in here: a rebuild renumbers the piece nodes (`{blk}:{measure}`), so a reach
    resolved against the pre-rebuild graph names sections the new registry no longer has — the reach
    silently comes back empty or wrong, with nothing on screen to say the graph is stale."""
    for fn in (_registry, _indices, _splits_meta, _split_points_attrs,
               _blk_to_item, _tributary_items, item_tributaries, _graph):
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

_read_entryfile = io.read_entryfile                      # shared helper (io is the single home)


def regions() -> list[str]:
    """Region ids that have a parser EntryFile (e.g. ['1','2',...])."""
    return io.region_ids(ENTRIES_DIR)


def load_region(region: str) -> dict[str, dict]:
    """Entries for a region. EntryFiles are the SINGLE SOURCE OF TRUTH — curator edits are written back
    here (the old reviewed/ overlay was merged in), so no overlay to apply."""
    return io.read_entryfile(ENTRIES_DIR / f"region-{region}.json")


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


def _match_and_item(e: dict):
    """(MatchResult, RegistryItem|None) for an entry. The MatchResult is the live re-match — the same
    logic and overrides the build used, kept so the UI can show WHY a match is ambiguous. The ITEM
    prefers `entry.matched`, which is authoritative for the same reason `_covered_ids` trusts it.

    This matters most where the live match cannot decide: four separate lakes are all gazetted
    "Nation Lakes", so re-matching that name returns `ambiguous` with four candidates and no item —
    but the entry already records WHICH one it is. Reading the item off the live match instead left
    20 such entries with no item at all: a blank name in the queue and an empty boundary picker, even
    though the lake was known all along."""
    ident = e.get("identity", {})
    mr = match_identity(ident.get("name", ""), ident.get("region", ""), ident.get("mus", []))
    reg = _registry()
    for iid in (*(e.get("matched") or []), mr.item_id):
        if iid and iid in reg:
            return mr, reg[iid]
    return mr, None


def _item_for_entry(e: dict):
    """The registry item (RegistryItem) for an entry, resolved via the matcher over its identity."""
    return _match_and_item(e)[1]


def _covered_ids(e: dict, mr) -> list[str]:
    """Every registry item this entry covers, primary first.

    `entry.matched` is authoritative — the matcher wrote it against the synopsis row's VERBATIM name,
    which is what a combined override is keyed on ("CHILLIWACK / VEDDER RIVERS (does not include Sumas
    River) …"). Re-matching the entry here cannot recover that: the entry only stores the item's name
    ("Chilliwack River"), which finds the Chilliwack and never learns about the Vedder. So the live
    match is only a fallback for entries stamped before `matched` was filled."""
    reg = _registry()
    stored = [i for i in (e.get("matched") or []) if i in reg]
    if stored:
        return stored
    return [i for i in (mr.item_id, *mr.also) if i and i in reg]


def _also_items(e: dict, mr) -> list[dict]:
    """The OTHER registry items this entry covers — "CHILLIWACK / VEDDER RIVERS" is one synopsis row
    over the Chilliwack, the Vedder and the Vedder Canal, so the reviewer needs to see all three.

    "Other" means other than the item actually shown as primary, which `_match_and_item` may take from
    `entry.matched` when the live match is ambiguous. Keying this on `mr.item_id` instead listed the
    primary a second time whenever those two differed."""
    reg = _registry()
    _, item = _match_and_item(e)
    primary = item.id if item is not None else mr.item_id
    return [{"id": i, "name": reg[i].name, "kind": reg[i].kind}
            for i in _covered_ids(e, mr) if i != primary]


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


def _combined_bindable(item, e: dict, mr) -> list[dict]:
    """`bindable` over every item this entry covers, each boundary tagged with the `item_id` it came
    from. The matched item goes first, so its ids win an id collision."""
    reg = _registry()
    items = [item] + [reg[i] for i in _covered_ids(e, mr) if i != item.id]
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


# --------------------------------------------------------------------------- #
# Extent -> sections (the REACH a rule actually selects)
# --------------------------------------------------------------------------- #
# Nothing in the pipeline resolves an Extent to sections yet; the ops are recorded intent. The review
# tool needs the answer NOW, because a curator cannot confirm a reach they cannot see — so this is the
# reference implementation of the contract documented on `pipeline.parsing.entry_models.Op`:
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
    from pipeline.io.serialize import read_artifact
    return read_artifact(str(GRAPH_PKL_PATH))


def resolve_extent(covered_ids: list[str], ex: dict) -> dict | None:
    """The app's binding of the shared resolver: same code the builder runs, this app's cached pair.

    The implementation lives in `pipeline.reach.extent` so the review app and the artifact builder
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
        feats += _read_gpkg_features("streams", _sql_in("node_id", sections[i:i + 400]),
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
    out: set[str] = set()
    failed: list[dict] = []
    for sc in e.get("scope") or []:
        got = resolve_extent(covered, sc)
        if got is None:
            failed.append(sc)
            continue
        out |= set(got["sections"])
    return (out or None), failed


def entry_reaches(entry_id: str) -> dict:
    """What each rule selects, for the review UI — AND what the bundle will actually ship.

    The per-extent geometry comes from `pipeline.reach.extent`; the OUTCOME of each
    rule comes from `pipeline.reach`, the same builder that writes the artifact. That
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
        mr, item = _match_and_item(e)
        covered = _covered_ids(e, mr)
        clip, scope_failed = _scope_sections(e, covered)

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
            binding, diags = _build_reach(e, r, _registry(), _graph(), covered=covered, clip=clip)
            verdict[r["rule_id"]] = {
                "outcome": binding.outcome.value,
                "reason": binding.reason.value if binding.reason else None,
                "detail": binding.detail,
                "n_sections": len(binding.sections),
                "sections": list(binding.sections),
                "tributaries_pending": binding.tributaries_pending,
                "diagnostics": [{"kind": d.kind, **d.payload} for d in diags],
            }

        return {"covered": covered, "rules": out, "scope_sections": sorted(clip or ()),
                "scope_unresolved": scope_failed, "verdict": verdict}
    return {}


def _scope_clipped(per: list, clip: set[str] | None) -> bool:
    """Did the entry's scope actually remove sections this rule had resolved?"""
    return bool(clip) and any(g is not None and not g["sections"] for g in per)


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


def _referenced_item_ids(entry_dict: dict) -> set[str]:
    """Every other-item id an extent scopes to (rule extents, entry scope, tributary excludes)."""
    ids: set[str] = set()

    def _scan(extents):
        for ex in extents or []:
            if ex.get("item_id"):
                ids.add(ex["item_id"])
            ids.update(ex.get("item_ids") or ())

    _scan(entry_dict.get("scope"))
    _scan((entry_dict.get("tributaries") or {}).get("excludes"))
    for r in entry_dict.get("rules") or []:
        _scan(r.get("extents"))
        _scan(r.get("tributary_excludes"))
    return ids


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
            mr, item = _match_and_item(e)
            mine = set(_covered_ids(e, mr))
            break
    if not mine:
        return []
    out: list[dict] = []
    for reg_id, e in _all_entries():
        if e["entry_id"] == entry_id:
            continue
        mr, _ = _match_and_item(e)
        shared = mine & set(_covered_ids(e, mr))
        if not shared:
            continue
        out.append({
            "entry_id": e["entry_id"],
            "region": reg_id if isinstance(reg_id, str) else e.get("identity", {}).get("region", ""),
            "name": e.get("identity", {}).get("name", ""),
            "locked": bool(e.get("locked")),
            "n_rules": len(e.get("rules", [])),
            # a pointer row ("See Chilliwack River") is the common case worth calling out
            "pointer": e.get("regs_verbatim", "").strip().lower().startswith("see "),
            "shared_items": [{"id": i, "name": reg[i].name} for i in sorted(shared) if i in reg],
        })
    return sorted(out, key=lambda r: r["name"])


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
        mr, item = _match_and_item(e)
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
            "revisit": bool(e.get("revisit")),
            "reference_only": bool(e.get("reference_only")),   # "See X" pointer row, no regs of its own
            "registry_status": e.get("registry_status", "matched"),
            "n_rules": len(e.get("rules", [])),
            "matched_item_id": item.id if item else None,
            "matched_item_name": item.name if item else None,
            "also_item_ids": [a["id"] for a in _also_items(e, mr)],      # combined-override items
            "unused_curated_splits": len(unused_curated_splits(e, item)),
        })
    rows.sort(key=lambda r: (_STATUS_ORDER.get(r["status"], 9), r["name"]))
    return rows


def entry_detail(entry_id: str) -> dict | None:
    """Full entry + its resolved item's boundaries/variants + unused curated splits + match info."""
    for reg_id, e in _all_entries():
        if e["entry_id"] == entry_id:
            mr, item = _match_and_item(e)
            return {
                "entry": e,
                "region": reg_id,
                "match": {"item_id": mr.item_id, "status": mr.status, "reason": mr.reason,
                          "candidates": list(mr.candidates),
                          "also": [a["id"] for a in _also_items(e, mr)]},
                "item": None if not item else {
                    "id": item.id, "name": item.name, "kind": item.kind,
                    "variants": list(item.variants), "mus": list(item.mus),
                    # built graph ∪ live splits.json, over the primary item AND a combined
                    # override's other items — the same closed set the parser was given, so a
                    # Vedder rule on the Chilliwack/Vedder entry has a Vedder cut-point to bind.
                    "boundaries": _combined_bindable(item, e, mr),
                },
                "also_items": _also_items(e, mr),      # a combined override's other items (Vedder, …)
                "related_entries": related_entries(entry_id),   # other rows over the same water
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


def _rep_point(geom: dict | None) -> list[float] | None:
    """A representative lon/lat point for a GeoJSON geometry (its first coordinate) — used to drop a
    clickable marker at a lake boundary's location."""
    if not geom:
        return None
    c = geom.get("coordinates")
    while isinstance(c, list) and c and isinstance(c[0], list):
        c = c[0]
    return c if isinstance(c, list) and len(c) >= 2 and isinstance(c[0], (int, float)) else None


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
        feats += _read_gpkg_features(
            "streams", _sql_in("node_id", chunk),
            ["node_id", "display_name", "location_identifier"], "stream")

    # Named side channels draw WITH the river. They are separate items so their names and their rules
    # stay their own, but on the map a river missing its named channels while showing every anonymous
    # one is just wrong. Tagged `side_channel` so the client can style them apart from the mainstem.
    reg = _registry()
    side_ids = [n for sid in item_side_channels(item_id)
                for n in reg[sid].section_ids if not str(n).startswith("lake:")]
    for i in range(0, len(side_ids), 400):
        feats += _read_gpkg_features(
            "streams", _sql_in("node_id", side_ids[i:i + 400]),
            ["node_id", "display_name", "location_identifier"], "side_channel")

    # A LAKE/wetland item's sections are `lake:{wbk}` nodes, which live in the `lakes` layer, not
    # `streams` — without this a lake item draws nothing at all (the Vedder Canal on the combined
    # Chilliwack/Vedder entry). Its own wbk is included so an isolated lake with no graph node
    # (minted by add_waterbody_items) still gets its polygon.
    lake_wbks = {str(n).split(":", 1)[1] for n in item.section_ids if str(n).startswith("lake:")}
    if item.id.startswith("wbk:"):
        lake_wbks.add(item.id.split(":", 1)[1])
    if lake_wbks:
        lake_feats = _read_gpkg_features(
            "lakes", _sql_in("wbk", sorted(lake_wbks)), ["wbk", "display_name"], "waterbody")
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
        feats += _read_gpkg_features(
            "split_points", _sql_in("split_id", split_ids),
            ["split_id", "label", "anchor_type", "picked_up"], "split")

    # Auto (non-curated) LAKE boundaries have no split_point; surface each as a clickable point at the
    # lake's location so a curator can "see where it is" — same select/fly machinery as curated splits.
    lake_by_wbk = {b.wbk: b for b in _boundaries(item)
                   if b.wbk and not str(b.ref or "").startswith("split:")}
    if lake_by_wbk:
        for lf in _read_gpkg_features("lakes", _sql_in("wbk", list(lake_by_wbk)),
                                      ["wbk", "display_name"], "split"):
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


_atomic_write = io.atomic_write                          # shared helper (io is the single home)


def save_entry(region: str, entry_dict: dict, *, lock: bool = False, reviewed_by: str = "") -> dict:
    """Validate an edited entry (Entry model + split-id check against its matched item's boundaries) and
    write it back to the region EntryFile (the single source of truth). Returns {ok, errors}. On lock,
    stamp reviewed_by/reviewed_at. A future parser re-run preserves locked entries; keep non-locked
    curator edits safe by only re-parsing with --skip-existing (which skips entries already present)."""
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

    path = ENTRIES_DIR / f"region-{region}.json"
    existing = io.read_entryfile(path)
    existing[entry.entry_id] = json.loads(entry.model_dump_json())
    io.write_entryfile(path, region, existing.values())   # atomic, via the model (single home)
    return {"ok": True, "errors": []}
