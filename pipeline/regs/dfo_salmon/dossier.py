"""Everything known about ONE unresolved DFO water, in the order a curator needs it.

The 25 open waters are 4 ambiguous (an exact name hit several registry items) and 21 unmatched (no
exact hit at all). They need different evidence, so this prints different things for each:

* **Ambiguous** — the candidates are the question, so each is shown with its kind, MUs, section
  count and the cut-points already curated on it. Picking between "Yakoun River gnis:3485" and
  "Yakoun Lake wbk:329163190" is a decision about which blue line DFO means.
* **Unmatched** — there are no candidates, so there is nothing registry-side to show. What IS known
  is what DFO itself wrote: the name verbatim, the region, and every locator phrase attached to it.

Either way the locators are the point: they are the reaches DFO regulates, and each one that names
a place is a split that has to exist before the rule can bind. They are printed VERBATIM from
`source_text` — a locator paraphrased is a locator that cannot be checked against the page.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.dossier list
    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.dossier "Yakoun River"
"""

from __future__ import annotations

import argparse
import functools
import json
from pathlib import Path
from pipeline.common.curated import CURATED, GENERATED, SOURCE

ENTRIES = CURATED.regulations.entries.dfo_salmon
SPLITS = CURATED.waters.splits
OVERRIDES = CURATED.regulations.overrides


def _regions() -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(ENTRIES.glob("region-*.json"))]


def unresolved() -> list[tuple[dict, dict]]:
    """[(water, region_doc)] for every water the matcher could not bind on its own."""
    out = []
    for doc in _regions():
        for w in doc.get("waters", []):
            if (w.get("match") or {}).get("status") in ("ambiguous", "unmatched"):
                out.append((w, doc))
    return out


def _splits_for(item_id: str) -> list[dict]:
    """Curated cut-points already on this registry item."""
    body = json.loads(SPLITS.read_text(encoding="utf-8")).get("waterbodies", [])
    kind, _, val = item_id.partition(":")
    key = {"gnis": "gnis_id", "wbk": "waterbody_key"}.get(kind, kind)
    out = []
    for wb in body:
        ap = wb.get("applies_to") or {}
        if str(ap.get(key, "")) == val:
            out.append(wb)
    return out


def _overrides_for(name: str) -> list[dict]:
    if not OVERRIDES.exists():
        return []
    data = json.loads(OVERRIDES.read_text(encoding="utf-8"))
    rows = data if isinstance(data, list) else data.get("overrides", data.get("entries", []))
    n = name.strip().casefold()
    return [r for r in rows if isinstance(r, dict)
            and n in json.dumps(r).casefold()]


def _locations_for(water: dict, doc: dict) -> list[dict]:
    wid = water.get("water_id")
    name = (water.get("name") or "").casefold()
    out = []
    for loc in doc.get("locations", {}).values() if isinstance(doc.get("locations"), dict) \
            else doc.get("locations", []):
        if loc.get("water_id") == wid or (loc.get("water") or "").casefold() == name:
            out.append(loc)
    return out


DEFAULT_REGISTRY = GENERATED.build() / "registry.json"
ITEM_POINTS = GENERATED.build() / "item_points.json"


def osm_link(lat: float, lon: float, zoom: int = 14) -> str:
    """A pin a curator can open. Registry ids mean nothing on a map; a coordinate does."""
    return f"https://www.openstreetmap.org/?mlat={lat:.5f}&mlon={lon:.5f}#map={zoom}/{lat:.5f}/{lon:.5f}"


@functools.lru_cache(maxsize=1)
def _item_points() -> dict[str, list[float]]:
    """`{item_id: [lon, lat]}` — the build's own pin precompute, about a megabyte."""
    p = ITEM_POINTS
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def item_pin(item_id: str) -> tuple[float, float] | None:
    """A representative (lat, lon) for a registry item.

    READS `item_points.json`, a ~21k-row precompute the build writes from geometry it
    already has in memory. This used to open `graph.gpkg` with a WHERE clause — which meant
    a curator wanting one coordinate kept a 4.7 GB GeoPackage alive, and that file is 40% of
    a build. Nothing else needed it.

    A representative point, never a centroid: the centroid of a bent river lands on dry
    ground, and a curator following that pin ends up looking at a hillside.
    """
    pt = _item_points().get(item_id)
    if pt is None:
        return None
    lon, lat = pt
    return (lat, lon)


def _registry(path: str | Path | None = None):
    from pipeline.atlas.registry import default_registry_path, load_registry
    p = Path(path) if path else (DEFAULT_REGISTRY if DEFAULT_REGISTRY.exists()
                                 else default_registry_path())
    return load_registry(p)


def render(water: dict, doc: dict, registry=None) -> str:
    m = water.get("match") or {}
    L: list[str] = []
    add = L.append
    add("=" * 90)
    add(f"{water.get('name')!r}    [{m.get('status','?').upper()}]")
    add("=" * 90)
    add(f"  water_id   : {water.get('water_id')}")
    add(f"  region     : {doc.get('region_number')}  {doc.get('region_name','')}")
    add(f"  DFO scopes : {', '.join(water.get('sections') or []) or '(none)'}")
    if water.get("aliases"):
        add(f"  aliases    : {water['aliases']}")
    add(f"  tributaries: {water.get('tributaries')}")
    add(f"  why open   : {m.get('reason') or '(none given)'}")

    if m.get("candidates"):
        add("\n  CANDIDATES — pick one (or several; item_ids is a list)")
        for c in m["candidates"]:
            iid = c.get("item_id") or c.get("id") or ""
            item = (registry or {}).get(iid)
            add(f"    - {c.get('name')}   {iid}")
            pin = item_pin(iid)
            if pin:
                add(f"        map: {osm_link(*pin)}")
            if item is not None:
                add(f"        kind={item.kind}  MUs={list(item.mus)}  sections={len(item.section_ids)}")
                if item.boundaries:
                    add(f"        registry cut-points ({len(item.boundaries)}):")
                    for b in item.boundaries:
                        add(f"          · {b.id}  — {b.label} [{b.kind}]")
                else:
                    add("        registry cut-points: NONE")
            for wb in _splits_for(iid):
                add(f"        curated splits under {wb.get('name')!r}:")
                for sp in wb.get("splits", []):
                    add(f"          · {sp.get('id')}  — {sp.get('label')} [{sp.get('kind')}]")
                    co = sp.get("_coord") or ((sp.get("anchor") or {}).get("coord"))
                    if co:                                # stored lon,lat
                        add(f"              {osm_link(co[1], co[0], 15)}")
    if m.get("suggestions"):
        add("\n  MATCHER SUGGESTIONS (never applied — a guess, shown for context)")
        for s in m["suggestions"]:
            add(f"    - {s}")

    ov = _overrides_for(water.get("name") or "")
    add(f"\n  EXISTING OVERRIDES mentioning this name: {len(ov)}")
    for r in ov[:3]:
        add(f"    {json.dumps(r)[:220]}")

    locs = _locations_for(water, doc)
    add(f"\n  DFO LOCATORS — {len(locs)} (verbatim; each one naming a place needs a split)")
    for loc in locs:
        st = loc.get("source_text") or {}
        add(f"    [{loc.get('section')}] {loc.get('location_id')}   op={st.get('op')}"
            f"  anchors={st.get('anchor_types')}")
        add(f"        waters       : {st.get('waters')!r}")
        add(f"        specific_area: {st.get('specific_area')!r}")
        if st.get("excludes"):
            add(f"        excludes     : {st['excludes']}")
        if st.get("areas"):
            add(f"        areas        : {st['areas']}")
    return "\n".join(L)


def main() -> None:
    ap = argparse.ArgumentParser(description="Dossier for an unresolved DFO salmon water.")
    ap.add_argument("name", nargs="?", help="water name (substring, case-insensitive)")
    ap.add_argument("--registry", help=f"registry.json (default: {DEFAULT_REGISTRY})")
    ap.add_argument("--no-registry", action="store_true", help="skip loading the registry (faster)")
    args = ap.parse_args()

    rows = unresolved()
    if not args.name or args.name == "list":
        print(f"{len(rows)} unresolved DFO waters\n")
        for w, doc in sorted(rows, key=lambda r: ((r[0].get('match') or {}).get('status', ''),
                                                  r[0].get('name', ''))):
            m = w.get("match") or {}
            print(f"  [{m.get('status','?'):<9}] r{doc.get('region_number')}  {w.get('name')}")
        return

    registry = None if args.no_registry else _registry(args.registry)
    hits = [(w, d) for w, d in rows if args.name.casefold() in (w.get("name") or "").casefold()]
    if not hits:
        print(f"no unresolved water matching {args.name!r}")
        return
    for w, d in hits:
        print(render(w, d, registry))


if __name__ == "__main__":
    main()
