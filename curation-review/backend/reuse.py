"""Data layer for the curation-review app — **reuses pipeline code, reimplements nothing.**

- An entry's water is its `matched` (`pipeline.atlas.reach.covered`, the builder's own reading), so
  the review tool resolves geometry/boundaries the same way the build does.
- The gate a mark is refused on is `pipeline.regs.parsing.validate_catalogue.gate` (`check_entry`).
- "Unused curated splits" counts curated (`ref="split:*"`) boundaries no extent binds, so
  lake/outlet/headwaters auto-boundaries don't count.

READ ONLY but for one write, a reviewer's mark (`post_mark`, appended to the marks store through
`writes.commit`). The catalogue region files and splits.json are read, never written: a fix is a
reviewed change to the curated files (T185), not something this app does.
"""

from __future__ import annotations

import copy
import json
import math
import os
import re
import threading
from functools import lru_cache
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.regs.parsing.catalogue_parts.walk import iter_extents
from pipeline.atlas.reach.covered import covered_ids as _pipeline_covered_ids
import answer
import model_api
import writes
from pipeline.regs.parsing.rows import load_synopsis_rows
from pipeline.atlas.registry import load_registry
from pipeline.atlas.reach.build import build_reach as _build_reach, resolve_carve_outs
from pipeline.atlas.reach.build import entry_scope as _entry_scope
from pipeline.atlas.reach.classify import wants_tributaries as _wants_tributaries
from pipeline.atlas.reach import extent as _resolve
from pipeline.common.curated import CURATED, GENERATED, SOURCE
from pipeline.atlas.splits.sectionizer import minted_id as _auto_split
from pipeline.common.models.registry import bare_split_id
from pipeline.atlas.splits.splits import splits_on
from pipeline.common.models.splits import confidence_of, confidence_words

_ROOT = Path(__file__).resolve().parents[2]
#: The catalogue region files the app reads (never writes). `CURATION_ENTRIES_DIR` points it at a
#: copy — the API tests and a headless click-through run against a temp copy.
ENTRIES_DIR = (Path(os.environ["CURATION_ENTRIES_DIR"]) if os.environ.get("CURATION_ENTRIES_DIR")
               else CURATED.regulations.entries.catalogue)
# The build the app SERVES. `project_config.review_build_dir` is the one name for it — see
# config.yaml `generated.atlas.default_build`. Hard-coding it here meant the app could serve a build months older than the pipeline and say nothing: every
# reach still renders, just against a stale graph, so a reviewer signs off on the wrong water.
# `CURATION_BUILD` points it at another atlas — a scratch rehearsal of the review (T136), whose printed
# rebuild then writes that copy (`vintage.Target`), never the live one.
_BUILD = (Path(os.environ["CURATION_BUILD"]) if os.environ.get("CURATION_BUILD")
          else GENERATED.build())
REGISTRY_PATH = _BUILD / "registry.json"
OVERRIDES_PATH = CURATED.regulations.overrides
SPLITS_RESOLVED_PATH = _BUILD / "splits.resolved.json"
GRAPH_PKL_PATH = _BUILD / "graph.pkl"
GEOMS_PKL_PATH = _BUILD / "geometries.pkl"
WBK_POLYS_PKL_PATH = _BUILD / "waterbody_polys.pkl"
BASEMAP_PMTILES = SOURCE / "bc.pmtiles"                  # the webapp's basemap (web-mercator)
#: THE hand-curated split source (read here, never written). `CURATION_SPLITS_JSON` points it at a
#: copy (tests).
SPLITS_JSON_PATH = (Path(os.environ["CURATION_SPLITS_JSON"]) if os.environ.get("CURATION_SPLITS_JSON")
                    else CURATED.waters.splits)
ROW_IMAGES_DIR = GENERATED.regs.extraction / "row_images"  # source synopsis row crops
#: THE REVIEWERS' MARKS (T97): the append-only store `pipeline.regs.review.marks` reads — never a
#: field of an entry. `CURATION_MARKS` points it at a copy (tests).
MARKS_PATH = (Path(os.environ["CURATION_MARKS"]) if os.environ.get("CURATION_MARKS")
              else CURATED.regulations.review.marks)
#: THE BUILT VINTAGE the tool holds each entry against (T133): the reach run of the served build
#: and the release root (the bundle is `pipeline.deliver.bundle.read.BUNDLE`). `CURATION_REACH_DIR`
#: / `CURATION_RELEASE_ROOT` point them at copies (tests).
REACH_DIR = (Path(os.environ["CURATION_REACH_DIR"]) if os.environ.get("CURATION_REACH_DIR")
             else GENERATED.reaches / _BUILD.name)
RELEASE_ROOT = (Path(os.environ["CURATION_RELEASE_ROOT"]) if os.environ.get("CURATION_RELEASE_ROOT")
                else GENERATED.release)
#: The tile set a rebuild of THIS target writes (T136): only the printed commands name it — the tool
#: reads no tile. `CURATION_TILES` points it at a copy.
TILES_DIR = (Path(os.environ["CURATION_TILES"]) if os.environ.get("CURATION_TILES")
             else GENERATED.tiles)
if os.environ.get("CURATION_BUNDLE"):
    # THE BUNDLE the tool reads (answers, marks' bundle half, the vintage): `pipeline.deliver.bundle.read`
    # holds the one path every reader of it asks; a scratch review points it at its copy (T136).
    from pipeline.deliver.bundle import read as _bundle_read
    _bundle_read.BUNDLE = os.environ["CURATION_BUNDLE"]


# --------------------------------------------------------------------------- #
# Registry (expensive — built once)
# --------------------------------------------------------------------------- #

#: ONE loader at a time for the big artifacts. FastAPI runs these sync endpoints on a thread pool
#: and `lru_cache` does not hold a miss: when a page fires its five or six first requests at a cold
#: backend (startup, or `--reload` after an edit), each thread loaded its own graph (~0.7 GB) and
#: geometry (~2.9 GB) side by side. Measured: a reach that answers in 1.3 s warm took 7 min 50 s
#: under that stampede.
_LOAD_LOCK = threading.RLock()


def _single_load(fn):
    """`lru_cache(maxsize=1)` whose first load is serialised; keeps `cache_clear`."""
    cached = lru_cache(maxsize=1)(fn)

    def wrapper():
        with _LOAD_LOCK:
            return cached()
    wrapper.cache_clear = cached.cache_clear
    wrapper.__doc__, wrapper.__name__ = fn.__doc__, fn.__name__
    return wrapper


@_single_load
def _registry() -> dict:
    return load_registry(REGISTRY_PATH)


def _place_namer():
    """The bundle's own place-namer over the registry this app serves — so a label names a rule's
    place (a cut-point's curated label, an area's name) exactly as the bundle's label does.

    KEYED ON splits.json's stamp: it carries every split's label, and a split edited by hand must
    name itself by its new label on the next request, not after a restart."""
    return _place_namer_for(_stamp((SPLITS_JSON_PATH,)))


@lru_cache(maxsize=1)
def _place_namer_for(_key: tuple):
    from pipeline.deliver.bundle.place_names import PlaceNamer, area_names, split_labels
    return PlaceNamer(_registry(), split_labels(json.loads(
        SPLITS_JSON_PATH.read_text(encoding="utf-8"))), area_names(Path(REGISTRY_PATH).parent))


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
    """Drop EVERY cache derived from the served build (and the bundle), so the next request reads
    the artifacts on disk now. Called by `ensure_fresh` whenever the build's files change under the
    app — a promote or a rebuild run from a terminal (the app starts neither).
    The species list, row-image index, projection and FWA lake outlines come from static source, not
    the build, so they are left warm.

    `_graph` MUST be in here: a rebuild renumbers the piece nodes (`{blk}:{measure}`), so a reach
    resolved against the pre-rebuild graph names sections the new registry no longer has — the reach
    silently comes back empty or wrong, with nothing on screen to say the graph is stale. So must
    `_geoms` and `_blk_index` (keyed by those same node ids) — they were missing, and a promoted
    build drew its splits from the old geometry."""
    for fn in _BUILD_CACHES:
        fn.cache_clear()
    answer.bundle_meta.cache_clear()


#: The served build's files every build-derived cache is read from. Their (mtime, size) is the
#: build's stamp: a promote renames a new directory into place, so every one of them changes.
_BUILD_FILES = (REGISTRY_PATH, SPLITS_RESOLVED_PATH, GRAPH_PKL_PATH, GEOMS_PKL_PATH,
                WBK_POLYS_PKL_PATH, _BUILD / "region_home.json")


def _stamp(paths) -> tuple:
    out = []
    for p in paths:
        try:
            st = Path(p).stat()
            out.append((str(p), st.st_mtime_ns, st.st_size))
        except FileNotFoundError:
            out.append((str(p), None, None))
    return tuple(out)


_SEEN: dict[str, tuple] = {}


def ensure_fresh() -> bool:
    """Drop the build-derived caches if the served build (or the bundle) changed on disk since
    they were last checked. Cheap — a handful of `stat`s — and run before every API request
    (app.py middleware), so a cache can never outlive the files it was read from, whoever rebuilt
    them. Returns whether anything was dropped."""
    from pipeline.deliver.bundle import read as _R
    now = {"build": _stamp(_BUILD_FILES), "bundle": _stamp((_R.BUNDLE,))}
    with _LOAD_LOCK:
        changed = [k for k, v in now.items() if k in _SEEN and _SEEN[k] != v]
        _SEEN.update(now)
        if "build" in changed:
            invalidate_caches()
        elif "bundle" in changed:
            answer.bundle_meta.cache_clear()
    return bool(changed)


def _ident(e: dict) -> dict:
    """{name, region, mus} for a catalogue entry.

    A catalogue entry is flat — `name` and `region` at the top, and the MUs the synopsis row was
    printed under encoded in `entry_id` after the `@` (that is what makes the id stable when
    matching moves). The app reads the catalogue region files only."""
    eid = str(e.get("entry_id", ""))
    mus = eid.split("@", 1)[1].split("+") if "@" in eid else []
    return {"name": e.get("name", ""), "region": str(e.get("region", "")), "mus": mus}


# --------------------------------------------------------------------------- #
# Entry loading
# --------------------------------------------------------------------------- #

def regions() -> list[str]:
    """Region ids that have a catalogue region file (e.g. ['1','2',...])."""
    return io.region_ids(ENTRIES_DIR)


def load_region(region: str) -> dict[str, dict]:
    """Entries for a region. The region files are the SINGLE SOURCE OF TRUTH. Read through the
    corpus' one read point, so an id this registry folded into another
    (`registry.flowing`) names its item here as it does in the reach run and the bundle."""
    return io.read_entryfile(ENTRIES_DIR / f"region-{region}.json", registry=_registry())


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
    `tiers.flagged`, the one predicate (the review tiers read it too)."""
    from pipeline.regs.review.tiers import flagged
    return flagged(e)


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
    # Each alias as a rule names it (`registry.bare_split_id`: only the graph's `split:` strip — the old
    # split-at-the-first-colon turned `split:area:5` right but `lake:329016195` into a bare number).
    # `names`: every other NAME at the place (`SectionBoundary.names`), shown "label (also: …)".
    return {"id": b.id, "label": b.label, "kind": b.kind, "ref": b.ref, "wbk": b.wbk,
            "aliases": [bare_split_id(str(a)) for a in (getattr(b, "aliases", ()) or ())],
            "names": list(getattr(b, "names", ()) or ()),
            "curated": str(b.ref or "").startswith("split:"),
            # T207: high | medium | low — "approximate location (low confidence)" beside its label
            "confidence": split_confidence(b.id, None, b) if str(b.ref or "").startswith("split:") else "high",
            # a position the pipeline minted (a gauge, a length or area cut), not a split anyone
            # authored in splits.json — so neither "orphan" when splits.json lacks it, nor an
            # "unused curated split" when no rule binds it
            "minted": _auto_split(b.id)}


def cut_label(b, meta: dict | None = None, water: str = "") -> str:
    """What a reviewer should read on a cut the pipeline MINTED (not one anyone authored): its kind and
    specifics, never the water's name it falls back to ("Iskut River" on seven dots told nothing).
    The kind is in the id (`sectionizer.minted_id`): `gauge__08HB008`, `length:<blk>:<m>`,
    `area:<N>` / `area:<park>`, `border:<blk>:<n>`; a lake edge is a `lake` boundary."""
    sid = str(b.id)
    base = sid[len("split:"):] if sid.startswith("split:") else sid
    label = str((meta or {}).get("label") or getattr(b, "label", "") or "").strip()
    if getattr(b, "kind", "") == "lake" and not _auto_split(sid):
        lake = label if label and not label.startswith("lake ") else ""
        return f"lake edge · {lake}" if lake else "lake edge"
    if base.startswith("gauge__"):
        stn = base[len("gauge__"):]
        name = label.split("·", 1)[1].strip() if "·" in label else (label if label != stn else "")
        return f"gauge {stn} · {name}" if name else f"gauge {stn}"
    fam, _, rest = base.partition(":")
    if fam == "length":
        try:
            km = f"{float(rest.rsplit(':', 1)[1]) / 1000:.1f} km"
        except (IndexError, ValueError):
            km = ""
        at = label if label and label != water and label != base else ""
        return " · ".join(x for x in ("length cut", km, at) if x)
    if fam == "area":
        if re.fullmatch(r"\d+[A-Za-z]?", rest):
            return f"Region {rest} line"
        name = re.sub(r"\s*\[\d+\]$", "", rest).strip()
        return f"{name} boundary"
    if fam == "border":
        return label if label and label != base else "BC border"
    return label or base


def _curated_split_ids(item) -> set[str]:
    """Boundary ids that are CURATED splits (ref 'split:*') — not lake/outlet/headwaters auto-boundaries."""
    return {b.id for b in _boundaries(item) if str(b.ref or "").startswith("split:")}


def _used_splits() -> frozenset[str]:
    """Every curated split id something in the corpus uses (`split_use.used_split_ids`) — keyed on
    the files' stamps, so a curated edit is seen on the next request."""
    dfo = sorted(Path(CURATED.regulations.entries.dfo_salmon).glob("region-*.json"))
    regions_ = sorted(Path(ENTRIES_DIR).glob("region-*.json"))
    return _used_splits_for(_stamp((SPLITS_JSON_PATH, REGISTRY_PATH, *regions_, *dfo)))


@lru_cache(maxsize=1)
def _used_splits_for(key: tuple) -> frozenset[str]:
    from pipeline.regs.review.split_use import used_split_ids
    dfo = [json.loads(Path(p).read_text(encoding="utf-8")) for p, *_ in key
           if Path(p).parent == Path(CURATED.regulations.entries.dfo_salmon)]
    return frozenset(used_split_ids([e for _r, e in _all_entries()],
                                    json.loads(SPLITS_JSON_PATH.read_text(encoding="utf-8")),
                                    dfo=dfo, registry=_registry()))


def unused_curated_splits(e: dict, item: dict | None) -> list[dict]:
    """Curated splits on this item that NO extent of the entry references — surfaced so the curator can see a
    hand-authored cut that the parse never used (a likely missed reach).

    `entry_models.unused_splits` walked the retired Entry's `.scope`; a CatalogueEntry has plain
    `extents` on the entry and on each rule, so the walk is local now."""
    if not e.get("rules"):
        # a pointer row ("See Atnarko/Bella Coola Rivers") binds nothing by design; every cut on
        # its water read as "unused" and put it in the unused_splits queue as a finding
        return []
    curated = _curated_split_ids(item)
    if not curated:
        return []
    # EVERY extent the entry holds (`walk.iter_extents`, T112) — a cut bound only by a rule's or a
    # designation's carve-out, or by a licensing record, is used. This walked the scope and the
    # rules' extents alone, so the two Skeena cuts only its classified-water record binds (the
    # Kitsumkalum confluence, the Ecological Reserve) were listed "unused" (T119 review).
    used = {sid for _path, ex, _role in iter_extents(e) for sid in (ex.get("splits") or ())}
    # T207 (owner 2026-10-26): USED ANYWHERE counts — by id, by the AREA a rule names (Pitt River's
    # "within Garibaldi Park" binds the park boundary), by a merged place, or by a DFO entry
    # (`split_use.used_split_ids`, the one answer the unused-splits audit reads too).
    used |= _used_splits()
    # A rule naming ANY id a cut-point answers to uses it — a merged place's alias binds it too.
    for b in _boundaries(item):
        if used & set(b.answers_to()):
            used.add(b.id)
    meta = _splits_meta()
    out = []
    for sid in sorted(curated - used):
        if _auto_split(sid):
            continue
        m = meta.get(sid, {})
        out.append({"id": sid, "label": m.get("label", sid), "anchor_type": m.get("anchor_type", "")})
    return out


def _item_split_source(item) -> list[dict]:
    """Live splits.json splits on the SAME waterbody as this registry item (applies_to matched to the
    item's ref_ids). May include splits not yet built into the graph (pending rebuild).

    `pipeline.atlas.splits.splits.splits_on` is the one answer (the review fingerprint reads it
    too); a split's own `applies_to` counts beside its waterbody's — reading only the waterbody's,
    the Atnarko/Bella Coola cut-points showed "orphan" while splits.json held them unchanged."""
    if item is None:
        return []
    return splits_on(_load_splits(), set(item.ref_ids))


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
    # a curated split that MERGED into another place is that place under another id, not "pending"
    alias_of = {a: b.id for b in _boundaries(item) for a in b.answers_to()[1:]}
    for sp in _item_split_source(item):
        sid = sp.get("id")
        if not sid:
            continue
        live = {"label": sp.get("label"), "kind": sp.get("kind"),
                "anchor": sp.get("anchor"), "note": sp.get("note"), "review": split_review(sp)}
        if sid not in out and alias_of.get(sid) in out:
            out[alias_of[sid]].setdefault("alias_live", {})[sid] = live
            continue
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


@_single_load
def _graph():
    """The built StreamGraph — node bounds + flow adjacency. Big (~0.7 GB) but loaded once per
    process and only when a reach is actually requested."""
    from pipeline.common.io.serialize import read_artifact
    from pipeline.atlas.registry import regions
    g = read_artifact(str(GRAPH_PKL_PATH))
    # The region each straddling section lies in, read from the atlas's own `region_home.json`
    # exactly as the reach CLI reads it — so a zone rule the curator reviews binds what ships
    # (`registry.regions`), and nothing measures it a second time.
    regions.attach_from(_BUILD, g, _registry())
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
    from pipeline.atlas.graph.tributaries import joining
    mouths = set(joining(_graph(), it.section_ids))      # what the walk's first step enters (T117)
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
        feats += _features("streams", sections[i:i + 400], "tributary")
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
    Trout Reward Tagging Program on page 13. It names no water on purpose, and its reach is
    an AREA carried on the entry (`within area:region:1`).

    Everything else is a row of a water table and names one water.

    They were shown as one list, so a regional notice appeared as a synopsis row that had
    failed to match an item — correctness rendered as a defect, and the two kinds of
    regulation mixed in a queue where they need different questions asked of them.
    """
    return "zone" if str(e.get("entry_id", "")).startswith("z") else "water"


def _area_matched(e: dict) -> list[str]:
    from pipeline.regs.review.tiers import area_matched
    return area_matched(e) if entry_kind(e) == "water" else []


def matched_areas(e: dict) -> list[dict]:
    """MATCHED BY AREA (owner 2026-10-26): [{id, name}] of the areas a water row with no registry
    item binds through (every rule has an area extent, `tiers.area_matched`); [] otherwise. The
    screen says "matched by area: <name>" instead of NO REGISTRY."""
    ids = _area_matched(e)
    if not ids:
        return []
    names = _area_name_map()
    return [{"id": a, "name": names.get(a) or a} for a in ids]


def _area_name_map() -> dict[str, str]:
    """{area id: name} of the served atlas, re-read when its area catalogue changes (a rebuild)."""
    return _area_name_map_for(_stamp((Path(REGISTRY_PATH).parent / "area_catalog.gpkg",)))


@lru_cache(maxsize=1)
def _area_name_map_for(_key: tuple) -> dict[str, str]:
    from pipeline.deliver.bundle.place_names import area_names
    try:
        return area_names(Path(REGISTRY_PATH).parent)
    except FileNotFoundError:
        return {}


def search_areas(q: str, limit: int = 30) -> list[dict]:
    """The atlas's areas (`area_catalog.gpkg`) whose id or name contains `q` (case-folded):
    [{id, name}] — the "within an area" op's pick list. An empty `q` lists the first `limit`."""
    want = q.strip().casefold()
    out = [{"id": a, "name": n} for a, n in _area_name_map().items()
           if not want or want in f"{a} {n}".casefold()]
    out.sort(key=lambda x: (not x["name"].casefold().startswith(want), len(x["name"]), x["name"]))
    return out[:limit]


def entry_status(e: dict, item: dict | None) -> str:
    """One coarse status for the queue's badge and filter — what the PARSE and the build left, not
    review progress (the reviewer's marks are `verify`). T135 (PLAN §4 phase 4): `parser-uncertain`
    was "flagged", and `plain` was "unreviewed" — a word that read as review progress when it only
    meant "nothing the parser or the build flagged"."""
    # A zone entry has no water to match, so `no_registry` is not a finding about it — it is
    # the definition of it. Saying otherwise put all 132 at the top of the queue.
    if entry_kind(e) == "zone":
        return "parser-uncertain" if _flagged(e) else "zone"
    if item is None and not _area_matched(e):   # no item and no area on every rule: binds nothing
        return "no_registry"
    if _flagged(e):
        return "parser-uncertain"
    if unused_curated_splits(e, item):
        return "unused_splits"
    return "plain"


#: every status `entry_status` gives
STATUSES = ("no_registry", "parser-uncertain", "unused_splits", "plain", "zone")


#: Zone first. A regional rule binds to EVERY stream in its region — one of them is 160,000
#: section-bindings where a water row is a few dozen — and none of the 132 has ever been
#: through the parser or the agent review pass: they were transcribed by hand from the region
#: chapters. Highest leverage, least verified, so they lead. `kind=water` filters them out
#: when the job is the water tables.
_KIND_ORDER = {"zone": 0, "water": 1}


def queue(region: str | None = None, status: str | None = None,
          kind: str | None = None, verify: str | None = None,
          order: str = "risk", tier: str | None = None,
          severity: str | None = None) -> list[dict]:
    """Review queue rows — ONE QUEUE ACROSS ALL REGIONS by default (`region` None).

    `order="risk"` (the default) is the review order: by SEVERITY first (`severity.entry_severity`,
    owner 2026-10-26: high, medium, low, none), then (T135 / PLAN §3, §4 phase 4) by tier (P1 Z P2
    P3 P4 sweep; entries only, T184), within a tier the most urgent risk factor first, then the most severity-A pre-checks, then the book's order
    (`tiers.queue_rank`, the one ordering function).

    `order="book"` is the order to go through EVERY entry in: by region, then the page it is
    printed on, the region chapter's entries before that page's table rows, then the file's
    order (alphabetical within a table, as the book prints it).

    `verify` filters on the reviewer's mark (`MARK_STATUSES`): unmarked | verified | wrong |
    unsure | deferred | stale (marked, then changed — `mark_reasons` says why); `todo` is
    everything not currently verified (human-reviewed); `recheck` the entries human-reviewed once
    whose fingerprint has moved since (T184: back for a re-check). `tier` filters on the review tier (`tiers.entry_tier`, PLAN §3:
    P1 Z P2 P3 P4 sweep); `severity` on the entry's severity (`severity.FILTERS`: a level, or
    `issues` — high and medium)."""
    from pipeline.regs.review import severity as S
    from pipeline.regs.review import tiers as T
    from pipeline.regs.review.precheck import severe as _severe
    if order not in QUEUE_ORDERS:
        raise ValueError(f"order is one of {', '.join(QUEUE_ORDERS)}, not {order!r}")
    states = _mark_states()
    on = built()                      # the vintage on disk, read once for every row (T133)
    run = run_facts()                 # the reach run's facts, read once (T135)
    to_verify = splits_to_verify()
    rows = []
    src =[(region, e) for eid, e in load_region(region).items()] if region else _all_entries()
    for pos, (reg_id, e) in enumerate(src):
        item = _item_for_entry(e)
        st = entry_status(e, item)
        if status and st != status:
            continue
        if kind and entry_kind(e) != kind:
            continue
        tr = T.entry_tier(e, binds=item is not None, run=run)
        if tier and tr != tier:
            continue
        mst = _entry_mark_state(e, states)
        vst = mst["status"]
        rev = mst["reviewed"]
        if verify and not (vst == verify or (verify == "todo" and vst != "verified")
                           or (verify == "recheck" and rev is not None and not rev["holds"])):
            continue
        pages = [p for p in (e.get("source_pages") or []) if isinstance(p, int)]
        vin = entry_vintage(e, on)
        found = entry_prechecks(e, run=run, to_verify=to_verify)
        n_a = _severe(found)
        rk = T.risks(e, binds=item is not None, run=run)
        n_unused = len(unused_curated_splits(e, item))
        sev = S.entry_severity(found, risks=rk, unused_splits=n_unused,
                               lake=bool(item is not None and item.kind == "lake"))
        if not S.matches(sev["level"], severity):
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
            "matched_areas": [] if item else matched_areas(e),   # matched by area (no item)
            "also_item_ids": [a["id"] for a in _also_items(e)],      # combined-override items
            "unused_curated_splits": n_unused,
            "page": min(pages) if pages else None,
            "pointer": bool(e.get("see")) and not e.get("rules"),
            "verify": vst,
            "verify_note": mst["note"],
            # T184: human-reviewed — by whom, when, and whether it still holds
            "reviewed": mst["reviewed"],
            "mark_reasons": mst["reasons"],
            "vintage": vin["state"],             # current | awaiting build (T133)
            "vintage_reasons": vin["reasons"],
            "tier": tr,                           # PLAN §3 (T134: progress and ETA by tier)
            "risks": rk,                          # T135, most urgent first
            # owner 2026-10-26: ONE chip — is there a real issue to review — and why
            "severity": sev["level"],
            "severity_reasons": sev["reasons"],
            # an `I` finding (a note, T207: "on parts") is neither a fact nor a hint
            "prechecks": {"A": n_a, "B": sum(1 for f in found if f["severity"] == "B")},
            "_rank": (S.rank(sev["level"]),
                      *T.queue_rank(e, binds=item is not None, run=run, severe=n_a)),
            "_pos": pos,
        })

    def book(r):
        return (_region_key(r["region"]), r["page"] if r["page"] is not None else 10_000,
                _KIND_ORDER.get(r["kind"], 9), r["_pos"])
    if order == "book":
        rows.sort(key=book)
    else:
        rows.sort(key=lambda r: (r["_rank"], book(r)))
    for r in rows:
        del r["_pos"], r["_rank"]
    return rows


#: the queue's orders: the review order (tiers, risk first; the default) and the book's
QUEUE_ORDERS = ("risk", "book")


def _region_key(region: str) -> tuple:
    """'provincial' first (the book's front pages), then 1, 2, … 7, 7a, 7b, 8."""
    if region == "provincial":
        return (0, 0, "")
    num = "".join(ch for ch in region if ch.isdigit())
    return (1, int(num) if num else 99, region)


# --------------------------------------------------------------------------- #
# Review marks (T97): `pipeline.regs.review` decides; this only serves and stores
# --------------------------------------------------------------------------- #

#: what the queue reports for an entry's marks (`pipeline.regs.review.marks.status`)
MARK_STATUSES = ("unmarked", "verified", "wrong", "unsure", "deferred", "stale")


def _canonical(e: dict) -> dict:
    """An entry read RAW, with absorbed ids folded as `load_region` folds them."""
    from pipeline.atlas.registry.flowing import canonical_ids
    return canonical_ids(e, _registry())


def _review_context():
    """The fingerprint context of the vintage on disk NOW: splits.json, the overrides, the served
    registry and the bundle's facts. Keyed on their stamps, so an edited split or a rebuilt bundle
    is read on the next request."""
    from pipeline.deliver.bundle import read as _R
    return _review_context_for(_stamp((SPLITS_JSON_PATH, OVERRIDES_PATH, Path(_R.BUNDLE),
                                       REGISTRY_PATH)))


@lru_cache(maxsize=1)
def _review_context_for(_key: tuple):
    from pipeline.deliver.bundle import read as _R
    from pipeline.regs.review import fingerprint as F
    # No bundle yet is a state, not an error: a mark then holds no bundle half (`reach`/`answer`
    # None) and "awaiting build" cannot arise from it. `Context.on_disk` is the CLIs' context too.
    ctx = F.Context.on_disk(SPLITS_JSON_PATH, OVERRIDES_PATH, _registry(), _R.BUNDLE)
    ctx.prints_memo = {}                       # entry content -> prints, for this vintage only
    return ctx


def _prints(e: dict):
    """The entry's prints now (every record), memoised per vintage on the entry's content."""
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review.marks import Prints
    ctx = _review_context()
    key = json.dumps(e, sort_keys=True, ensure_ascii=False)
    got = ctx.prints_memo.get(key)
    if got is None:
        got = ctx.prints_memo[key] = Prints.model_validate(F.entry_fingerprints(e, ctx))
    return got


def _mark_states() -> dict[str, list]:
    """target -> the marks in force on it (`marks.latest`), read from the store now."""
    from pipeline.regs.review import marks as M
    out: dict[str, list] = {}
    for m in M.latest(M.load(MARKS_PATH)).values():
        out.setdefault(m.target, []).append(m)
    return out


def _mark_view(m, now) -> dict:
    from pipeline.regs.review import marks as M
    return {**json.loads(m.model_dump_json()), **M.status(m, now)}


def _entry_mark_state(e: dict, states: dict[str, list]) -> dict:
    """The queue's one word for an entry's marks: its entry mark's status; for an entry marked
    per rule (zone entries), stale / wrong / unsure / deferred if any rule mark is, verified when
    every rule holds a verified mark, else unmarked. `reasons` are the stale reasons.

    `reviewed` is THE HUMAN-REVIEWED STATE (T184): the reviewers and the (latest) date of the
    verified mark(s) — every check answered y, `marks.verdict_from_answers` — that make the entry
    reviewed, with `holds`: true while they hold; false once its fingerprint moved (text, rules or
    extent — T97's `marks.stale_reasons`), when it is back in the queue for a re-check. None when no
    verified mark ever covered it (a zone entry: one on every rule)."""
    from pipeline.regs.review import marks as M
    ms = states.get(e["entry_id"]) or []
    if not ms:
        return {"status": "unmarked", "note": "", "reasons": [], "reviewed": None}
    now = _prints(e)

    def reviewed(verified: list, holds: bool) -> dict:
        return {"by": sorted({m.reviewer for m in verified}),
                "on": max(m.date for m in verified), "holds": holds}

    entry_mark = next((m for m in ms if m.scope == "entry"), None)
    if entry_mark is not None:
        st = M.status(entry_mark, now)
        return {"status": st["status"], "note": entry_mark.note, "reasons": st["reasons"],
                "reviewed": reviewed([entry_mark], st["status"] == "verified")
                if entry_mark.verdict == "verified" else None}
    sts = [(m, M.status(m, now)) for m in ms if m.scope == "rule"]
    every = {r.get("rule_id") for r in e.get("rules") or []}
    # a zone entry WAS reviewed when every rule's mark in force was a verified one
    was = ([m for m, _ in sts] if every and all(m.verdict == "verified" for m, _ in sts)
           and every <= {m.rule_id for m, _ in sts} | {M.held_rule(m, now) for m, _ in sts}
           else [])
    for word in ("stale", "wrong", "unsure", "deferred"):
        hit = [(m, st) for m, st in sts if st["status"] == word]
        if hit:
            return {"status": word, "note": hit[0][0].note,
                    "reasons": sorted({r for _, st in hit for r in st["reasons"]},
                                      key=[x.value for x in M.STALE_REASONS].index),
                    "reviewed": reviewed(was, False) if was else None}
    # the rule each verified mark is on NOW — a renumbered rule keeps its mark (`marks.held_rule`)
    marked = {M.held_rule(m, now) for m, st in sts if st["status"] == "verified"}
    done = bool(every) and every <= marked
    return {"status": "verified" if done else "unmarked", "note": "", "reasons": [],
            "reviewed": reviewed([m for m, _ in sts], True) if done else None}


def entry_marks(entry_id: str) -> list[dict]:
    """The marks in force on this entry (its entry mark and its rule marks), each with its
    status and stale reasons NOW."""
    found = _find(entry_id)
    if found is None:
        return []
    now = _prints(found[1])
    return [_mark_view(m, now) for m in _mark_states().get(entry_id, [])
            if m.scope in ("entry", "rule")]


def marks_view(target: str | None = None, scope: str | None = None) -> list[dict]:
    """The marks in force (optionally on one target / of one scope), each with its status and
    stale reasons against its target as it is NOW."""
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review.marks import Prints
    ctx = _review_context()
    entries = {e["entry_id"]: e for _, e in _all_entries()}
    out = []
    for tgt, ms in sorted(_mark_states().items()):
        if target is not None and tgt != target:
            continue
        for m in ms:
            if scope is not None and m.scope != scope:
                continue
            if m.scope == "split":
                try:
                    now = Prints.model_validate(F.split_fingerprints(tgt, ctx))
                except KeyError:
                    now = None
            elif m.scope == "dfo":                     # T164: a DFO location
                import dfo
                got = dfo._locations().get(tgt)
                now = dfo._now(got[1], got[2]) if got else None
            elif m.scope == "tidal":                   # T212: a proposed tidal boundary
                from pipeline.regs.review import tidal as T
                got = T.prints_of(tgt)
                now = Prints.model_validate(got) if got is not None else None
            else:
                now = _prints(entries[tgt]) if tgt in entries else None
            out.append(_mark_view(m, now))
    return out


def split_mark_states() -> dict[str, dict]:
    """splits.json split id -> its mark status now (`marks.split_states`), and whether the served
    atlas holds it as authored (`vintage`: a split added by hand reads "awaiting build" until
    the atlas is rebuilt and promoted)."""
    from pipeline.regs.review import marks as M
    ctx = _review_context()
    states = M.split_states(M.load(MARKS_PATH), ctx)
    return {sid: dict(st, vintage=split_vintage(sid)) for sid, st in states.items()}


# --------------------------------------------------------------------------- #
# One vintage on screen (T133): `pipeline.regs.review.vintage` decides; this reads the disk
# --------------------------------------------------------------------------- #

def _vintage_paths():
    from pipeline.deliver.bundle import read as _R
    return _BUILD, REACH_DIR, Path(_R.BUNDLE), RELEASE_ROOT


def rebuild_target():
    """WHAT A REBUILD PRINTED HERE REBUILDS (`vintage.Target`, T136): the corpus and splits.json
    this tool edits, and the atlas, reach run, bundle, release and tiles it serves — read at call
    time, so a tool pointed at copies prints commands that rebuild the copies."""
    from pipeline.regs.review import vintage as V
    from pipeline.deliver.bundle import read as _R
    return V.Target(entries=ENTRIES_DIR, splits=SPLITS_JSON_PATH, build=_BUILD, reach=REACH_DIR,
                    bundle=Path(_R.BUNDLE), release=RELEASE_ROOT, tiles=TILES_DIR)


def built():
    """The digests of the vintage on disk NOW (`vintage.Built`): the served atlas, its reach run,
    the bundle and the newest release. KEYED ON THEIR FILES' STAMPS, so a reach run, a deliver or a
    promote finished in a terminal is read on the next request — "awaiting build" clears with no
    restart. The atlas half (its registry digest reads 100 MB) is keyed on the atlas's own files."""
    from pipeline.regs.review import vintage as V
    return _built_for(_stamp(V.stamp_paths(*_vintage_paths())))


@lru_cache(maxsize=1)
def _built_atlas(_key: tuple):
    from pipeline.regs.review import vintage as V
    return V.read_atlas(_BUILD)


@lru_cache(maxsize=1)
def _built_for(key: tuple):
    from pipeline.regs.review import vintage as V
    # the first four stamps are the atlas's own files (`vintage.stamp_paths`)
    return V.read_built(*_vintage_paths(), atlas=_built_atlas(key[:4]))


def _anchors_now(e: dict) -> dict[str, str]:
    """split id -> its anchor digest in splits.json NOW, for every split the entry's binding reads
    (`fingerprint.entry_split_ids`). A named id splits.json does not have is the gate's to refuse."""
    from pipeline.atlas.splits.splits import anchor_digest
    from pipeline.regs.review import fingerprint as F
    ctx = _splits_context()
    idx = ctx.split_index
    return {sid: anchor_digest(idx[sid]) for sid in sorted(F.entry_split_ids(e, ctx)) if sid in idx}


def _splits_context():
    """A fingerprint context of splits.json and the registry only — what `entry_split_ids` reads.
    The queue asks it for every entry, so it never reads the bundle's facts (`_review_context`
    does, once, for marks). Keyed on the files' stamps like the rest."""
    return _splits_context_for(_stamp((SPLITS_JSON_PATH, REGISTRY_PATH)))


@lru_cache(maxsize=1)
def _splits_context_for(_key: tuple):
    from pipeline.regs.review import fingerprint as F
    return F.Context(splits=json.loads(SPLITS_JSON_PATH.read_text(encoding="utf-8")),
                     registry=_registry())


def entry_vintage(e: dict, on=None) -> dict:
    """Is this entry built (`vintage.entry_state`)? `e` as `load_region` reads it — the form the
    reach run and the bundle digest. `on`: a `built()` already read for this request (the queue
    asks for every entry)."""
    from pipeline.regs.review import vintage as V
    return V.entry_state(e["entry_id"], io.entry_digest(e), _anchors_now(e),
                         on if on is not None else built(), rebuild_target())


def split_vintage(split_id: str, on=None) -> dict:
    """Is this split built as authored now (`vintage.split_state`)?"""
    from pipeline.atlas.splits.splits import anchor_digest
    from pipeline.regs.review import vintage as V
    idx = _splits_context().split_index
    if split_id not in idx:
        raise KeyError(split_id)
    return V.split_state(split_id, anchor_digest(idx[split_id]),
                         on if on is not None else built(), rebuild_target())


def awaiting_build(scope: str, target: str) -> bool:
    """Whether a mark's target awaits its build NOW (`entry_vintage` / `split_vintage`), so only T
    and R can be answered — what a refused mark's words depend on (`marks.unanswered_words`). An
    unknown target is not awaiting (it is refused as unknown)."""
    from pipeline.regs.review import vintage as V
    try:
        if scope == "split":
            st = split_vintage(target)
        else:
            found = _find(target)
            if found is None:
                return False
            st = entry_vintage(found[1])
    except KeyError:
        return False
    return st.get("state") != V.CURRENT


# --------------------------------------------------------------------------- #
# The one queue (T135, PLAN §4 phase 4): `pipeline.regs.review.tiers` orders it, `precheck`
# finds, `runfacts` reads the run — this only reads the disk for them
# --------------------------------------------------------------------------- #

def run_facts():
    """The served reach run's facts (`runfacts.RunFacts`: ambiguous cuts, unresolved rules,
    unplaced pieces), keyed on its files' stamps — a reach run finished in a terminal is read on
    the next request. None while there is no COMPLETE run (`report.json` is written last): the
    run-based risk factors and pre-checks are then not claimed, and `queue_facts` says so."""
    from pipeline.regs.review.runfacts import DIAGNOSTIC, UNRESOLVED
    return _run_facts_for(_stamp((REACH_DIR / "report.json", REACH_DIR / f"{DIAGNOSTIC}.jsonl",
                                  REACH_DIR / f"{UNRESOLVED}.jsonl")))


@lru_cache(maxsize=1)
def _run_facts_for(_key: tuple):
    from pipeline.regs.review.runfacts import RunFacts
    try:
        return RunFacts.read(REACH_DIR)
    except FileNotFoundError:
        return None


def splits_to_verify() -> frozenset[str]:
    """The split points still to verify (`tiers.split_needs_verify` with each split's old mark's
    status now) — what a rule binding through one is pre-checked for (`split_to_verify`)."""
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import tiers as T
    states = M.split_states(M.load(MARKS_PATH), _review_context())
    return T.splits_to_verify(_load_splits(), {k: v.get("status") for k, v in states.items()})


def entry_prechecks(e: dict, *, run=None, to_verify=None, gate: bool = False) -> list[dict]:
    """`precheck.prechecks` for one entry against the served build and run. `gate`: also run the
    gate (`check_entry`; the open entry only — the queue does not run it for every row)."""
    from pipeline.regs.review.precheck import prechecks
    errs = check_entry(e)["errors"] if gate else None
    return prechecks(e, binds=_item_for_entry(e) is not None,
                     run=run if run is not None else run_facts(),
                     to_verify=to_verify if to_verify is not None else splits_to_verify(),
                     gate_errors=errs)


def queue_facts() -> dict:
    """What the queue's order was decided on: the reach run read (its digest), or why none was."""
    from pipeline.regs.review import vintage as V
    run = run_facts()
    return {"run": run.digest if run is not None else None,
            "why": None if run is not None else
            f"no complete reach run in {REACH_DIR} — the ambiguous-cut and unresolved factors are "
            f"not known (run: " + "; then ".join(V.reach_run_commands(rebuild_target())) + ")"}


def parse_review_findings() -> dict[str, list[str]]:
    """entry_id -> the second-pass agent's findings on it (`io.read_review_findings`, every
    severity), from the parse work dir's `reviews/`. None exist yet: {}."""
    work = io.default_work_dir()
    if not (work / "reviews").is_dir():
        return {}
    return io.read_review_findings(work, severities=("high", "medium", "low"))


def pending_build() -> dict:
    """THE PENDING BUILD PANEL (PLAN §5): every entry and split awaiting its build, why, and what
    to run — empty when the vintage on disk is the one on screen. Read by the queue's panel."""
    from pipeline.regs.review import vintage as V
    entries, reasons = [], set()
    on = built()
    for region, e in _all_entries():
        st = entry_vintage(e, on)
        if st["state"] != V.CURRENT:
            entries.append({"entry_id": e["entry_id"], "region": region, "name": _ident(e)["name"],
                            "reasons": st["reasons"], "splits": st["splits"]})
            reasons.update(st["reasons"])
    splits = []
    for sid in sorted(_splits_context().split_index):
        st = split_vintage(sid, on)
        if st["state"] != V.CURRENT:
            splits.append({"split_id": sid, "reasons": st["reasons"]})
            reasons.update(st["reasons"])
    # the ONE vintage every row above was decided on (a second `built()` could read a rebuild
    # that landed mid-request and describe another vintage than the rows)
    return {"built": on.summary(), "entries": entries, "splits": splits,
            "splits_checked": "anchors" if on.anchors is not None else "ids",
            "commands": V.rebuild_commands(reasons, rebuild_target())}


def _live_counts(entry_id: str) -> dict[str, int | None]:
    """rule_id -> the sections the reach builder binds it to NOW on the served atlas (the app's
    `entry_reaches`, the builder's own function), None where one entry alone cannot say."""
    got = entry_reaches(entry_id).get("verdict") or {}
    return {rid: (None if v.get("reason") == "needs_corpus" else v.get("n_sections"))
            for rid, v in got.items()}


_PDF_SHA: dict[tuple, str] = {}


def _pdf_sha256() -> str:
    """The sha256 of the book the reviewer reads (the repo copy, whose bytes are not gov.bc.ca's)."""
    import hashlib
    import synopsis_pages
    key = _stamp((synopsis_pages.PDF_PATH,))
    if key not in _PDF_SHA:
        _PDF_SHA.clear()
        _PDF_SHA[key] = hashlib.sha256(Path(synopsis_pages.PDF_PATH).read_bytes()).hexdigest()
    return _PDF_SHA[key]


def post_mark(body: dict) -> dict:
    """Record a reviewer's mark: fingerprints taken from the target as it is on disk NOW, the
    date and the book's sha256 stamped here, appended to the store through the backed-up commit.
    REFUSED (ValueError) on an entry that fails the gate, and on a target AWAITING BUILD for any
    answer but T and R (`vintage.refuse_unbuilt`, T133: a verified mark needs the entry's digest
    the same in the reach run, the bundle and the release); KeyError for an unknown target.
    ENTRIES ONLY (T184): an entry or one of its rules; a split is checked by proxy, on the map of
    an entry whose rules name it, and is refused here (ValueError)."""
    import datetime as _dt
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review import marks as M
    from pipeline.regs.review import vintage as V
    scope, target = body.get("scope"), str(body.get("target") or "")
    if scope == "proposal":
        # T185 (owner 2026-10-27): the marks ARE the decisions — there is no accept step
        raise ValueError("a proposal is not marked: the marks behind it are the decision "
                         "(Claude applies them, `apply_proposals`)")
    if scope not in ("entry", "rule", "dfo", "tidal"):
        raise ValueError(f"a mark is on an entry or one of its rules, not a {scope}")
    with writes.LOCK, M.store_lock(MARKS_PATH):          # threads, then other processes
        if scope == "dfo":
            # T164: a DFO salmon location — its prints from `review.dfo`, each NEW split's anchor
            # checked and its resolved point stamped (`dfo.mark_for`); the same one commit below
            import dfo
            mark, now, reused = dfo.mark_for(body, date=_dt.date.today().isoformat(),
                                             pdf_sha256=_pdf_sha256())
        elif scope == "tidal":
            # T212: a proposed tidal boundary — its prints from `review.tidal`, each new split
            # snapped to the stream and stamped (`tidal.mark_for`); the same one commit below
            import tidal
            mark, now, reused = tidal.mark_for(body, date=_dt.date.today().isoformat(),
                                               pdf_sha256=_pdf_sha256())
        else:
            reused = []
            ctx = _review_context()
            found = _find(target)
            if found is None:
                raise KeyError(target)
            e = found[1]
            M.refuse_on_gate(target, check_entry(e)["errors"])
            V.refuse_unbuilt(scope, body.get("answers") or {}, entry_vintage(e))
            prints = F.entry_fingerprints(e, ctx, rule_id=body.get("rule_id")
                                          if scope == "rule" else None)
            pages = body.get("pages") or [p for p in e.get("source_pages") or []
                                          if isinstance(p, int)]
            mark = M.Mark.model_validate({**body, "date": _dt.date.today().isoformat(),
                                          "pdf_sha256": _pdf_sha256(), "pages": pages,
                                          "prints": prints})
            now = None
        writes.commit([(MARKS_PATH, M.appended(MARKS_PATH, [mark]))], "mark")
    view = _mark_view(mark, now if scope in ("dfo", "tidal") else _prints(e))
    if reused:                       # T210: "already proposed — reused", said back to the screen
        view = {**view, "reused": reused}
    return view


# --------------------------------------------------------------------------- T185: proposals
# READS ONLY: the proposals `python -m pipeline.regs.review.proposals build` wrote. The owner's
# marks ARE the decisions (2026-10-27): there is no accept step, the tab is a read-only "waiting
# to apply" list, and Claude's `apply_proposals` lands each one (binding check, side rebuild where
# a cut changes, one commit each).

#: where the proposals were written; `CURATION_PROPOSALS_DIR` points it at a copy (tests)
PROPOSALS_DIR = (Path(os.environ["CURATION_PROPOSALS_DIR"])
                 if os.environ.get("CURATION_PROPOSALS_DIR") else None)


def _proposals_dir() -> Path:
    from pipeline.regs.review import proposals as P
    return PROPOSALS_DIR or P.OUT_DIR


def proposals_view() -> dict:
    """THE PROPOSALS WAITING TO APPLY: every proposal (index.json) as last built, read only — each
    with the marks behind it, its edits, its check and (a new split) its confidence."""
    from pipeline.regs.review import proposals as P
    idx = P.read_index(_proposals_dir())
    return {"made": idx.get("made"), "proposals": list(idx.get("proposals") or []),
            "commands": {"build": "python -m pipeline.regs.review.proposals build --check",
                         "apply": "python -m pipeline.regs.review.apply_proposals --dry-run"}}


def entry_answer_context(entry_id: str) -> dict | None:
    """The bundle's copy of this entry beside the file's: per-rule labels and section counts, and
    where each licensing record binds (`answer.entry_in_bundle`)."""
    found = _find(entry_id)
    if found is None:
        return None
    _, e = found
    from pipeline.regs.review import vintage as V
    vin = entry_vintage(e)
    if vin["state"] != V.CURRENT:
        # THE BUNDLE'S COPY IS NOT SHOWN for an entry awaiting its build (T133): it is another
        # vintage's, and a reviewer must not sign off on it as this one's
        return {"vintage": vin, "awaiting": True, "bundle_matches": False}
    lab = model_api.labels(e, ENTRIES_DIR, _place_namer())
    now = {r["rule_id"]: lab["rules"][i] for i, r in enumerate(e.get("rules") or [])}
    return {**answer.entry_in_bundle(entry_id, now, _live_counts(entry_id)), "vintage": vin,
            "awaiting": False}


def entry_detail(entry_id: str) -> dict | None:
    """Full entry + its item's boundaries/variants + unused curated splits.

    The entry is served with each rule's and each licensing record's GENERATED `label` stamped on
    it — `catalogue.label` / `catalogue.licensing_label`, the functions the bundle uses, so the
    curator reads exactly what the reader will."""
    for reg_id, e in _all_entries():
        if e["entry_id"] != entry_id:
            continue
        item = _item_for_entry(e)
        vintage = entry_vintage(e)          # of the entry AS READ, before labels are stamped on
        reviewed = _entry_mark_state(e, _mark_states())["reviewed"]       # likewise
        lab = model_api.labels(e, ENTRIES_DIR, _place_namer())
        e = dict(e,
                 rules=[dict(r, label=lab["rules"][i] or r.get("verbatim", ""))
                        for i, r in enumerate(e.get("rules") or [])],
                 licensing=[dict(x, label=lab["licensing"][i] or x.get("verbatim", ""))
                            for i, x in enumerate(e.get("licensing") or [])])
        return {
            "entry": e,
            "kind": entry_kind(e),
            "region": reg_id,
            "item": None if not item else {
                "id": item.id, "name": item.name, "kind": item.kind,
                "variants": list(item.variants), "mus": list(item.mus),
                # built graph ∪ live splits.json, over the primary item AND a combined
                # override's other items — the same closed set the parser was given, so a
                # Vedder rule on the Chilliwack/Vedder entry has a Vedder cut-point to bind.
                "boundaries": _combined_bindable(item, e),
            },
            "also_items": _also_items(e),      # a combined override's other items (Vedder, …)
            "matched_areas": [] if item else matched_areas(e),   # matched by area (no item)
            "related_entries": related_entries(entry_id),   # other rows over the same water
            "unused_curated_splits": unused_curated_splits(e, item),
            "source_image": entry_source_image(e),
            "marks": entry_marks(entry_id),
            # T184: HUMAN-REVIEWED (by whom, when; `holds` false = changed since, re-check)
            "reviewed": reviewed,
            # ONE VINTAGE ON SCREEN (T133): "current", or "awaiting build" with why and what to
            # run — the binding and the answer are this entry's only when current
            "vintage": vintage,
        }
    return None


# --------------------------------------------------------------------------- #
# Save (validated) to the catalogue region files
# --------------------------------------------------------------------------- #

def _rep_point(geom: dict | None) -> list[float] | None:
    """A representative lon/lat point INSIDE a GeoJSON geometry (shapely's `representative_point`)
    — used to drop a clickable, labelled marker at a lake boundary's location. A geometry made by
    `shapely.geometry.mapping` holds TUPLES: reading it for lists only found nothing, so every lake
    boundary (and every lake a rule names) had no marker (T184)."""
    if not geom or not geom.get("coordinates"):
        return None
    from shapely.geometry import shape
    try:
        p = shape(geom).representative_point()
    except Exception:  # noqa: BLE001 — a malformed geometry has no point; the caller lists it
        return None
    return None if p.is_empty else [p.x, p.y]



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

@_single_load
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
        # a pipeline-minted cut (gauge / length / area / border) is labelled by its kind, and flagged so
        # the client draws it small and muted with a hover label (only rule-named splits are labelled)
        by_id = {b.id: b for b in _boundaries(item)}
        for f in feats:
            pr = f["properties"]
            b = by_id.get(pr.get("split_id")) if pr.get("kind") == "split" else None
            if b is not None and _auto_split(b.id):
                pr["label"] = cut_label(b, _splits_meta().get(b.id), item.name)
                pr["minted"] = True

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
                              "properties": {"kind": "split", "split_id": b.id,
                                             "label": cut_label(b), "anchor_type": "lake",
                                             "auto": True, "minted": True}})

    return {"type": "FeatureCollection", "features": feats}


@lru_cache(maxsize=1)
def _boundary_index() -> dict[str, tuple[str, object]]:
    """Every cut-point id of the served registry — each boundary's own id AND every alias it answers
    to (`answers_to`) — -> (the item it is on, the boundary). First item (sorted) wins a collision."""
    out: dict[str, tuple[str, object]] = {}
    reg = _registry()
    for iid in sorted(reg):
        for b in reg[iid].boundaries:
            for sid in b.answers_to():
                out.setdefault(sid, (iid, b))
    return out


def entry_scope_words(e: dict) -> dict:
    """THE ENTRY'S EXTENT AND EVERY RULE'S, IN PLAIN WORDS (T189, `scope_words`): named by the
    bundle's own place-namer, a cut it cannot name by its built or splits.json label."""
    from pipeline.regs.review import scope_words as S
    labels = {sid: sp.get("label") for sid, (_, sp) in _split_records().items() if sp.get("label")}
    labels.update({sid: m["label"] for sid, m in _splits_meta().items() if m.get("label")})
    names = S.namer_names(_place_namer(), labels)
    return {"entry": S.entry_scope(e, names),
            "rules": {r.get("rule_id"): S.rule_scope(r, e, names)
                      for r in e.get("rules") or [] if isinstance(r, dict)}}


def _split_records() -> dict[str, tuple[str, dict]]:
    """splits.json split id -> (its waterbody's name, the split record)."""
    return {sp["id"]: (str(wb.get("name") or ""), sp)
            for wb in _load_splits().get("waterbodies", []) for sp in wb.get("splits", [])
            if sp.get("id")}


def split_confidence(sid: str, sp: dict | None = None, b=None) -> str:
    """(T207) A cut's confidence: splits.json's authored value now (`SplitDef.confidence`), else the
    built boundary's, else high."""
    if sp is None:
        authored = _confidences_for(_stamp((SPLITS_JSON_PATH,))).get(sid)
    else:
        authored = sp.get("confidence")
    if authored:
        return confidence_of(authored)
    return str(getattr(b, "confidence", "") or "high")


@lru_cache(maxsize=1)
def _confidences_for(_key: tuple) -> dict[str, str]:
    """splits.json split id -> its authored `confidence` (only the splits that carry one)."""
    return {sid: sp["confidence"] for sid, (_w, sp) in _split_records().items() if sp.get("confidence")}


def entry_split_points(entry_id: str) -> dict | None:
    """EVERY SPLIT THE ENTRY'S RULES NAME, as map points labelled with their names (T184): the
    proxy check of a split. No split is a review target of its own; the reviewer sees each cut an
    entry's rules (its scope, its licensing records) bind, where the served atlas put it, under its
    own name — a wrong one is that entry's "Place: wrong".

    A FeatureCollection of Points, one per split id, each with `split_id`, `label` (the built label,
    else splits.json's, else the boundary's), `water` (its splits.json waterbody; a lake boundary,
    the item it is on), `name` ("WATER: label"), `rules` (the rule ids that name it, in rule order) and `parts` (any other part that
    does: "entry scope", a licensing record's id), `to_verify` (`"verify": true` or a stale mark; a
    note never counts — `tiers.split_needs_verify`) and `grade`. Where the built atlas has no point
    for an id (not built yet, or a cut with no location of its own) it is listed in `unplaced`
    with why, never dropped. None: no such entry."""
    from pipeline.regs.review.tiers import split_grade, split_needs_verify
    found = _find(entry_id)
    if found is None:
        return None
    e = found[1]
    named: dict[str, dict] = {}
    rules = e.get("rules") or []
    for path, ex, _role in iter_extents(e):
        if path[0] == "rules":
            who, key = rules[path[1]].get("rule_id"), "rules"
        elif path[0] == "licensing":
            who, key = (e.get("licensing") or [])[path[1]].get("id"), "parts"
        else:
            who, key = "entry scope", "parts"
        for sid in ex.get("splits") or ():
            got = named.setdefault(sid, {"rules": [], "parts": []})
            if who and who not in got[key]:
                got[key].append(who)
    meta, records, bounds = _splits_meta(), _split_records(), _boundary_index()
    feats, unplaced = [], []
    for sid, by in named.items():
        water, sp = records.get(sid, ("", {}))
        on_item, b = bounds.get(sid, (None, None))
        if not water and on_item in _registry():          # a lake boundary: the water it is on
            water = _registry()[on_item].name
        label = ((meta.get(sid) or {}).get("label") or sp.get("label")
                 or (getattr(b, "label", None) if b is not None else None) or sid)
        # T207: a cut placed approximately says so on its map label and wherever it is named (splits.json's
        # own value, so it shows before the rebuild; else the built boundary's)
        conf = split_confidence(sid, sp, b)
        cw = confidence_words(conf)
        label = f"{label}, {cw}" if cw else label
        props = {"split_id": sid, "label": label, "water": water, "confidence": conf,
                 "name": f"{water}: {label}" if water else label, **by,
                 "to_verify": split_needs_verify(sp) if sp else False,
                 "grade": split_grade(sp) if sp else None, "item_id": on_item}
        geom = None
        if sid in meta:                               # a cut on a line: its point on the section
            f = _geo(_split_point(sid), "named_split", {})
            geom = f["geometry"] if f else None
        # a lake boundary: the lake's own point — also a curated cut whose point stood on the LAKE side of the
        # edge it took (T179: Aberfeldie Dam; its measure is in no section, the lake rides as a `lake:` alias)
        wbk = None if b is None else (getattr(b, "wbk", None) or next(
            (a[len("lake:"):] for a in (getattr(b, "aliases", None) or ()) if a.startswith("lake:")), None))
        if geom is None and wbk:
            lake = _features("lakes", [str(wbk)], "split")
            pt = _rep_point(lake[0].get("geometry")) if lake else None
            geom = {"type": "Point", "coordinates": pt} if pt else None
        if geom is None:
            unplaced.append({**props, "why": "not in the served atlas yet — awaiting its build"
                             if sp and sid not in meta else "the served atlas has no point for it"})
            continue
        feats.append({"type": "Feature", "geometry": geom, "properties": props})
    return {"type": "FeatureCollection", "features": feats, "unplaced": unplaced}


# --------------------------------------------------------------------------- #
# splits.json (the hand-curated split source of truth) — read only
# --------------------------------------------------------------------------- #
# A split edited by hand takes effect only after an atlas rebuild the owner runs from a terminal
# (`python -m pipeline build --atlas`, through scripts/heavy.sh — the app starts no process), because
# splits are baked into the graph's section boundaries; `GET /api/build` lists it until then.

def _load_splits() -> dict:
    return json.loads(SPLITS_JSON_PATH.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- T191: extent fix
# READS ONLY. The reviewer's proposed extent lives in the mark (`marks.ExtentFix`); these find the
# points it names and resolve it for a preview on an IN-MEMORY copy of the entry.

def search_splits(q: str, limit: int = 30) -> list[dict]:
    """Curated splits whose id, label or water contains `q` (case-folded): {id, label, water, kind}."""
    want = q.strip().casefold()
    if len(want) < 2:
        return []
    out = []
    for sid, (water, sp) in _split_records().items():
        hay = f"{sid} {sp.get('label') or ''} {water}".casefold()
        if want in hay:
            out.append({"id": sid, "label": sp.get("label") or sid, "water": water,
                        "kind": (sp.get("anchor") or {}).get("type")})
            if len(out) >= limit:
                break
    return out


def search_items(q: str, kind: str | None = None, limit: int = 30) -> list[dict]:
    """Registry items whose name (or a variant) contains `q`: {id, name, kind, mus}. `kind`
    narrows to stream / lake / wetland."""
    want = q.strip().casefold()
    if len(want) < 2:
        return []
    out = []
    for iid, it in _registry().items():
        if kind and it.kind != kind:
            continue
        if want in it.name.casefold() or any(want in v.casefold() for v in it.variants or ()):
            out.append({"id": iid, "name": it.name, "kind": it.kind, "mus": list(it.mus)[:4]})
    out.sort(key=lambda x: (not x["name"].casefold().startswith(want), len(x["name"]), x["name"]))
    return out[:limit]


def extent_preview(entry_id: str, fix, limit: int = 6000) -> dict | None:
    """WHAT A PROPOSED EXTENT (`marks.ExtentFix`) WOULD BIND, by the builder's own `build_reach`
    on a COPY of the entry with that extent in place (rule scope: the rule's `extents` and
    `includes_tributaries`; entry scope: the entry's `extents` and `includes_tributaries`, every
    rule then bound under it). Nothing is written. A NEW point (confluence, lake inlet/outlet, a
    map point) is not a split yet, so the extent cannot be resolved: `resolvable: false` and why —
    T185 builds the split, and the review continues on that build. None: no such entry."""
    import copy
    found = _find(entry_id)
    if found is None:
        return None
    e = copy.deepcopy(found[1])
    new = [p for p in fix.points if p.kind != "split"]
    if new:
        return {"resolvable": False, "why": "a NEW point (" + ", ".join(p.kind for p in new)
                + ") is not a split yet: it is resolved once T185 builds it — the fix is kept "
                "in the mark as proposed", "n_sections": None}
    if fix.op in ("rest", "steelhead_waters"):
        return {"resolvable": False, "why": f"op {fix.op} resolves against its siblings / the "
                "whole corpus, not on its own", "n_sections": None}
    ex: dict = {"op": fix.op, "splits": [p.split_id for p in fix.points]}
    if len(fix.items) == 1:
        ex["item_id"] = fix.items[0]
    elif fix.items:
        ex["item_ids"] = list(fix.items)
    if fix.op == "within":
        area = str(fix.area)
        ex["area_id"] = area if area.startswith("area:") else None
        if ex["area_id"] is None:
            return {"resolvable": False, "why": "within needs an area id (area:…) to preview",
                    "n_sections": None}
    rules = e.get("rules") or []
    if fix.scope == "entry":
        e["extents"] = [ex]
        if fix.tributaries is not None:
            e["includes_tributaries"] = fix.tributaries
        targets = rules
    else:
        rule = next((r for r in rules if r.get("rule_id") == fix.scope), None)
        if rule is None:
            return {"resolvable": False, "why": f"no rule {fix.scope} on {entry_id}",
                    "n_sections": None}
        rule["extents"] = [ex]
        if fix.tributaries is not None:
            rule["includes_tributaries"] = fix.tributaries
        targets = [rule]
    reg, graph = _registry(), _graph()
    covered = _covered_ids(e)
    reasons: list = []
    got = _resolve.resolve_extent(reg, graph, covered, ex, reasons=reasons)
    if got is None:
        return {"resolvable": False, "why": "the extent does not resolve: " + "; ".join(
            f"{c}{': ' + d if d else ''}" for c, d in reasons) if reasons
            else "the extent does not resolve", "n_sections": 0}
    clip, _failed = _scope_sections(e, covered)
    # the ENTRY's scope is drawn as itself (the stretch every rule is clipped to); a RULE's as the
    # builder binds it (clipped, its tributaries walked)
    total: set[str] = set(got.get("sections") or ()) if fix.scope == "entry" else set()
    outcomes = {}
    for r in targets:
        binding, _d = _build_reach(e, r, reg, graph, covered=covered, clip=clip,
                                   shared=_shared_waters(), tidal=_tidal(), owned=_rowed_waters())
        outcomes[r.get("rule_id")] = binding.outcome.value
        if fix.scope != "entry":
            total |= set(binding.sections)
    ids = sorted(total)
    return _json_safe({"resolvable": True, "why": None, "n_sections": len(ids),
                       "outcomes": outcomes, "truncated": len(ids) > limit,
                       "geojson": {"type": "FeatureCollection",
                                   "features": _sections_geojson(ids[:limit], "preview")}})


# --------------------------------------------------------------------------- T185: binding change
# READS ONLY. A proposed edit (`pipeline.regs.review.proposals`) bound in memory before and after,
# by the same `build_reach` path the extent preview uses — nothing is written.

_ABSENT = "__absent__"


def _set_fields(target: dict, field: str, value) -> None:
    if value == _ABSENT:
        target.pop(field, None)
    else:
        target[field] = copy.deepcopy(value)


def _binding_rows(before: dict[str, tuple], after: dict[str, tuple]) -> dict:
    graph = _graph()

    def km(ids) -> float:
        return round(sum(getattr(graph.nodes.get(s), "length_m", 0.0) or 0.0 for s in ids) / 1000, 2)
    rows = {}
    for rid in sorted(set(before) | set(after)):
        b, ob = before.get(rid, (set(), None))
        a, oa = after.get(rid, (set(), None))
        rows[rid] = {"n_before": len(b), "n_after": len(a), "gained": len(a - b),
                     "lost": len(b - a), "km_before": km(b), "km_after": km(a),
                     "outcome_before": ob, "outcome_after": oa,
                     "sample_gained": sorted(a - b)[:5], "sample_lost": sorted(b - a)[:5]}
    return {"cheap": True, "why": None, "rules": rows,
            "changed": sorted(r for r, x in rows.items()
                              if x["gained"] or x["lost"] or x["outcome_before"] != x["outcome_after"])}


def binding_change(entry_id: str, changes: list[dict]) -> dict | None:
    """WHAT A PROPOSED EDIT DOES TO AN ENTRY'S BINDINGS (T185): every rule bound by the builder's
    own `build_reach` on the entry as it is and on a COPY with `changes` made ({scope: "entry" |
    rule_id, field, after}), as the extent preview binds (T191). Per rule: sections before / after,
    gained / lost, km, outcome. None: no such entry. Writes nothing."""
    found = _find(entry_id)
    if found is None:
        return None
    before = found[1]
    after = copy.deepcopy(before)
    for c in changes:
        t = after if c["scope"] == "entry" else next(
            (r for r in after.get("rules") or [] if r.get("rule_id") == c["scope"]), None)
        if t is None:
            return {"cheap": False, "why": f"no rule {c['scope']} on {entry_id}"}
        _set_fields(t, c["field"], c["after"])
    reg, graph = _registry(), _graph()

    def bind(e: dict) -> dict[str, tuple]:
        covered = _covered_ids(e)
        clip, _failed = _scope_sections(e, covered)
        out = {}
        for r in e.get("rules") or []:
            b, _d = _build_reach(e, r, reg, graph, covered=covered, clip=clip,
                                 shared=_shared_waters(), tidal=_tidal(), owned=_rowed_waters())
            out[r.get("rule_id")] = (set(b.sections), b.outcome.value)
        return out
    return _json_safe(_binding_rows(bind(before), bind(after)))


def dfo_binding_change(location_id: str, changes: list[dict]) -> dict | None:
    """`binding_change` for a DFO location (T164): its binding before and after `changes`
    ({field: extents | tributaries, after}), bound as the DFO mode's reach binds it. Writes nothing."""
    import dfo
    got = dfo._locations().get(location_id)
    if got is None:
        return None
    _slug, loc, water = got
    after = copy.deepcopy(loc)
    for c in changes:
        _set_fields(after.setdefault("binding", {}), c["field"], c["after"])
    reg, graph = _registry(), _graph()

    def bind(lc: dict) -> dict[str, tuple]:
        entry, rule = dfo._reach_input(lc, water)
        if not rule["extents"]:
            return {rule["rule_id"]: (set(), "no extent")}
        b, _d = _build_reach(entry, rule, reg, graph)
        return {rule["rule_id"]: (set(b.sections), b.outcome.value)}
    return _json_safe(_binding_rows(bind(loc), bind(after)))


def split_review(sp: dict) -> dict:
    """WHAT THE CURATOR LEFT ON A CUT-POINT: its `_note` (the history of how it was placed; history,
    never a request to verify) and the auto-proposal GRADE that note opens with — `[auto-proposal L]`
    is a low-confidence machine placement nobody has confirmed. The tool used to send neither, so a
    reviewer saw a confident cut where the file said "Verify: …" (T96). `grade` is None for a cut
    that is not an open auto-proposal; only a LEADING tag counts (a later one is history) —
    `tiers.split_grade`, the one reading of it (`tiers.split_needs_verify` asks it too)."""
    from pipeline.regs.review.tiers import split_grade
    return {"note": str(sp.get("_note") or ""), "grade": split_grade(sp)}


def _cut_points(iid: str):
    """`cut_points` for `validate_catalogue.cut_point_menu`: what a rule may bind on one item in the
    app — the built boundaries AND the splits.json splits pending a rebuild (`bindable`), each with
    its aliases. None for an item this build does not have.

    THE ALIASES BIND. An alias is a curated split that could not be cut because its measure landed
    inside a lake run (a dam at the outlet), recorded as another name for the boundary standing at
    that place. It is a real, resolvable cut-point, so binding it must validate (the gate stores
    the boundary's own id in its place); without it the id resolves on the map but is refused.

    SPELLED AS THE PARSE MENU SPELLS THEM (`parse_context.cut_points`: `split:` stripped, `lake:`
    kept — the form `extent.py` resolves). This read the picker's display spelling, which strips
    every prefix, so the app accepted a bare `329069968` that ingest refuses and refused the
    `lake:329069968` ingest accepts (T119 review). A pending split has no aliases."""
    from pipeline.regs.parsing.parse_context import cut_points
    it = _registry().get(iid)
    if it is None:
        return None
    built = dict(cut_points(it))
    return [(b["id"], built.get(b["id"], ())) for b in bindable(it)]


def _find(entry_id: str) -> tuple[str, dict] | None:
    for region, e in _all_entries():
        if e["entry_id"] == entry_id:
            return region, e
    return None


def _gate(data: dict):
    """THE GATE (`validate_catalogue.gate`, through `model_api.check`) with the build this app
    serves: its registry, and the cut-points of every item the entry matches or an extent is scoped
    to (`cut_point_menu` over `_cut_points`). Returns (model or None, addressed errors)."""
    from pipeline.regs.parsing.validate_catalogue import cut_point_menu
    return model_api.check(data, menu=cut_point_menu(data, _cut_points), registry=_registry())


def check_entry(entry_dict: dict) -> dict:
    """THE GATE AN ENTRY IS MARKED AGAINST (PLAN §4 phase 1): `validate_catalogue.gate` — the
    model, the checks after it, absorbed ids, split parents, every split id against the cut-points
    it may bind — with the build this app serves. A mark on an entry that fails it is refused
    (`marks.refuse_on_gate`), and the open entry's pre-checks show it. `{ok, errors}`; each error is
    `{path, msg}` with `path` in the entry's own JSON keys (`rules.2.gear.0.max`). Writes nothing."""
    _, errs = _gate(model_api.strip_served(entry_dict))
    return {"ok": not errs, "errors": errs}


#: Every cache read from the served build — what `invalidate_caches` drops.
_BUILD_CACHES = (_registry, _place_namer_for, _splits_meta, _split_points_attrs,
                 _blk_to_item, _graph, _geoms, _wbk_polys,
                 _blk_index, _review_context_for, _built_atlas, _built_for, _splits_context_for,
                 _run_facts_for, _boundary_index,
                 # T207: corpus-wide split use (reads the registry) and the authored confidences
                 _used_splits_for, _confidences_for,
                 # owner 2026-10-26: the atlas's area names (matched by area, the within-op list)
                 _area_name_map_for)


# --------------------------------------------------------------------------- #
# Warm before the reviewer sits down (T136, PLAN §4 phase 5)
# --------------------------------------------------------------------------- #

#: What a cold backend loads before its first entry answers quickly, in the order a first entry
#: needs it: the registry (the queue), the bundle's facts (marks), the vintage and the reach run's
#: facts (awaiting build, risk factors), the graph (reaches) and the section geometry (the map). The
#: queue itself is computed last, once, so every one of its readers is warm.
WARM_STEPS: tuple[tuple[str, str], ...] = (
    ("registry", "_registry"), ("bundle facts", "_review_context"),
    ("vintage", "built"), ("reach run facts", "run_facts"), ("graph", "_graph"),
    ("geometry", "_geoms"), ("rule section counts", "_warm_rule_counts"), ("queue", "queue"),
)
def _warm_rule_counts() -> None:
    """Every rule's bound-section count, counted once per bundle (`fingerprint.rule_section_counts`):
    ~50 s on a zone-heavy bundle, so a reviewer never waits for it on a zone entry."""
    import answer
    from pipeline.regs.review.fingerprint import rule_section_counts
    db = answer._db()
    try:
        rule_section_counts(db, "")
    finally:
        db.close()


_READY: dict = {"ready": False, "warming": False, "done": [], "seconds": None, "error": None}


def warm(steps=WARM_STEPS) -> dict:
    """Load every cache a first entry reads (`WARM_STEPS`), recording each step as it finishes —
    what `readiness` reports and `GET /api/ready` serves. Run once at startup in a thread when
    `CURATION_WARM=1` (`run.sh` sets it), so a reviewer is never the one who waits."""
    import time as _t
    t0 = _t.time()
    _READY.update(ready=False, warming=True, done=[], seconds=None, error=None)
    try:
        for name, fn in steps:
            globals()[fn]()
            _READY["done"].append({"step": name, "at_s": round(_t.time() - t0, 1)})
        _READY["ready"] = True
    except Exception as ex:  # noqa: BLE001 — reported by /api/ready, never swallowed silently
        _READY["error"] = f"{type(ex).__name__}: {ex}"
    finally:
        _READY.update(warming=False, seconds=round(_t.time() - t0, 1))
    return readiness()


def readiness() -> dict:
    """THE READY SIGNAL: {ready, warming, done: [{step, at_s}], todo: [step], seconds, error}."""
    done = [d["step"] for d in _READY["done"]]
    return {**{k: _READY[k] for k in ("ready", "warming", "seconds", "error")},
            "done": list(_READY["done"]), "todo": [n for n, _ in WARM_STEPS if n not in done]}
