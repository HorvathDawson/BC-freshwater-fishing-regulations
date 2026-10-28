"""Packages a build into the client bundle.

    python -m pipeline.deliver.bundle --build data/generated/atlas/full --out data/generated/bundle/bundle.sqlite

WHAT THIS IS
    The last step of the pipeline. Everything upstream produces artifacts for US — a graph,
    a registry, resolved reaches. This produces the one artifact a CLIENT reads, in the
    format `app/design/data-contract.html` settles: SQLite, so a phone can hold it resident
    and a browser can range-read the same bytes off R2.

WHY IT IS NOT THE TILE BUILDER
    They split on what changes. Geometry changes when the province republishes the FWA —
    rarely, and it invalidates 875 MB. Regulations change every edition and invalidate about
    10 MB. Feeds change every half hour. Three clocks, three artifacts; a client can update
    the cheap one without refetching the expensive one.

HONESTY ABOUT COVERAGE
    Not every table can be filled from the artifacts that exist today. `report()` says which
    are populated, which are empty and why — an empty table that nobody mentions is how a
    client ends up rendering "no regulations here" for a water that has them.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from pipeline.gauges.consume import bundle_tables as _gauge_tables
from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT, SOURCE
from pipeline.deliver.bundle import rules as _rules
from pipeline.common.registry_kinds import is_water
from pipeline.common.search_key import stored_key

HERE = Path(__file__).parent
_ROOT = REPO_ROOT
SCHEMA = HERE / "schema.sql"
INDEXES = HERE / "indexes.sql"

#: THE BUNDLE'S FORMAT (P07): `meta.format`. Bump it when a reader must refuse an older bundle.
BUNDLE_FORMAT = "bundle/1"


def schema_sha256() -> str:
    """`meta.schema_sha256`: the digest of the DDL the bundle was written with (schema.sql + indexes.sql). The old
    `meta.schema` was schema.sql's first comment line, identical in every revision."""
    import hashlib
    return hashlib.sha256(SCHEMA.read_bytes() + b"\0" + INDEXES.read_bytes()).hexdigest()[:16]

@dataclass
class Coverage:
    """What went in, and what did not. Printed at the end of every run."""

    rows: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, str] = field(default_factory=dict)

    def filled(self, table: str, n: int) -> None:
        self.rows[table] = n

    def skip(self, table: str, why: str) -> None:
        self.skipped[table] = why

    def report(self) -> None:
        for t, n in self.rows.items():
            print(f"  {t:<15} {n:>9,} rows")
        for t, why in self.skipped.items():
            print(f"  {t:<15} {'—':>9}  not wired: {why}")


def _connect(out: Path) -> sqlite3.Connection:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)
    db = sqlite3.connect(out)
    # Deterministic bytes, so a rebuild that changed nothing produces an identical file and
    # a diff means something.
    db.execute("PRAGMA page_size = 4096")
    db.execute("PRAGMA journal_mode = OFF")
    # THE NEW TABLES' FOREIGN KEYS ARE ENFORCED (DATAFLOW P2: rule_key, section_ruleset, part)
    db.execute("PRAGMA foreign_keys = ON")
    db.executescript(SCHEMA.read_text())
    return db


def _reach_run(build_dir: Path) -> Path | None:
    """The reach run built from THIS atlas, found by asking each run which atlas it read.

    A bundle built from one atlas against another atlas's reaches binds rules to section ids
    that no longer exist — and it fails SILENTLY, as regulations that are simply absent, on
    exactly the screens where absent means "you may fish here". So the pairing is not a
    naming convention to be kept by hand: every run writes the build it read into
    `report.json`, and this matches on that.

    Newest wins when several runs name the same build, because re-running the builder is how
    its output is corrected and the older one is the stale copy. The choice is printed
    rather than assumed.
    """
    from pipeline.common.section_handles import digest_for
    # NO SILENT FALLBACK (plan 2.13): an atlas whose handle table cannot be read used to pair by folder NAME alone —
    # the pairing this function exists to replace. It stops, naming the file (the bundle needs it anyway).
    want = digest_for(build_dir)

    runs, by_name = [], []
    for report in sorted((GENERATED.reaches).glob("*/report.json")):
        try:
            got = json.loads(report.read_text())
        except (OSError, ValueError):
            continue
        if want and got.get("handles") == want:
            runs.append((report.stat().st_mtime, report.parent))
        elif not got.get("handles") and got.get("build") == build_dir.name:
            # A run written before the digest existed. Matched by folder name, which is what
            # this always did — kept so an older run still pairs rather than vanishing.
            by_name.append((report.stat().st_mtime, report.parent))
    # CONTENT BEATS NAME, ALWAYS. Promotion renames directories, so a run built against a
    # scratch path (`full_next`) stops matching the moment its atlas becomes `full` — while
    # the stale run that was renamed to `full.prev` still carries `build: "full"` and wins.
    # That happened: the bundle was handed the previous run's rule->section rows, and only
    # the handle-table check in `rules.write` stopped it shipping rules bound to sections
    # that no longer exist. A digest of `section_handles.txt` cannot be renamed.
    chosen = runs or by_name
    if not chosen:
        return None
    return max(chosen)[1]


def _lake_parts(items: list[dict]) -> dict[str, str]:
    """{part item_id: parent item_id} — every curated lake PART in this build, and its lake.

    THE RELATIONSHIP TRAVELS WITH THE ATLAS. `added_lakes.geojson` carries `part_of` on each part;
    the atlas build writes it onto the registry (`registry.add_lake_parts`, refusing a part or a
    parent that is not an item there), and this reads the registry's answer — so no reader of the
    bundle, and not the bundle itself, opens the curated file to learn that Kootenay Lake's Main
    Body is part of Kootenay Lake. REFUSED when a part names a parent that is not an item here:
    a reader grouping by `part_of` would show a lake with a rung missing.
    """
    have = {i["id"] for i in items}
    out: dict[str, str] = {}
    bad: list[str] = []
    for i in items:
        parent = i.get("part_of") or ""
        if not parent:
            continue
        if parent not in have:
            bad.append(f"{i['id']} ({i.get('name')}) is part_of {parent}, which is not a water here")
        else:
            out[i["id"]] = parent
    if bad:
        raise SystemExit("item.part_of: the registry names a lake that is not a water in this "
                         "build:\n  " + "\n  ".join(bad))
    return out


def _items(db: sqlite3.Connection, build_dir: Path, registry_items: list[dict],
           cov: Coverage) -> None:
    """item / alias / item_section, from the registry.

    The registry is the province-wide answer to "what is this water called and which
    sections is it". It is also the whole search index: 20,609 distinct strings, which is
    ~90 KB gzipped — small enough that no server-side search is needed anywhere.
    """
    items = [i for i in registry_items if is_water(i)]
    # A LAKE WITH PARTS OWNS NO SECTION (user ruling 2026-10-03; `registry.add_lake_parts`): its
    # parts are the water. REFUSED, not trimmed, when the registry still gives it one — the ghost
    # section would answer with the zone base beside parts that carry the lake's own rows.
    parents = {i["part_of"] for i in items if i.get("part_of")}
    ghosts = [i["id"] for i in items if i["id"] in parents and i.get("section_ids")]
    if ghosts:
        raise SystemExit(f"item: a lake cut into parts still owns a section of its own: {ghosts} "
                         f"— the registry predates the ruling; rewrite it with "
                         f"`python -m pipeline.atlas.sidecars`")
    # `ord` is assigned here, by insertion order, and is the ONLY place it is assigned.
    # setdefault rather than enumerate: the old write was INSERT OR REPLACE, so a repeated
    # item_id collapsed silently — with ord as the key it would instead insert twice and
    # trip the UNIQUE on item_id, turning a tolerated duplicate into a failed build.
    ords: dict[str, int] = {}
    for i in items:
        ords.setdefault(i["id"], len(ords))
    part_of = _lake_parts(items)
    # COLUMNS NAMED. `item` grew a column and the positional form would have silently
    # shifted name into item_id — the same fault that shipped an empty `entry` table.
    # `name_key` (T114): what the app's search filters on — `search_key`, the app's `searchKey` rule, stored only
    # where `lower(name)` is not already it (`stored_key`; the reader COALESCEs).
    db.executemany("INSERT OR REPLACE INTO item (ord, item_id, name, kind, part_of, name_key) "
                   "VALUES (?,?,?,?,?,?)",
                   ((ords[i["id"]], i["id"], i["name"], i.get("kind"), part_of.get(i["id"]),
                     stored_key(i["name"]))
                    for i in items))
    cov.filled("item", len(ords))

    # A variant is a NEW alias only when its name key (`names.key`, through `tiles.names.normalise`)
    # is new. The registry carries both cases of many strings ("EAST WHITE RIVER" and "East White
    # River"); the key folds them, and this used to compare raw,
    # so the tile dropped ~3,000 shouting duplicates and the bundle kept them. Two search
    # paths that disagree about what a distinct name is will return different results for
    # the same query, which is the one thing a search index may not do.
    from pipeline.deliver.tiles.names import normalise

    aliases = []
    for i in items:
        seen = {normalise(i["name"])}
        for v in i.get("variants", []):
            key = normalise(v)
            if not key or key in seen:
                continue
            seen.add(key)
            aliases.append((i["id"], v, stored_key(v)))
    db.executemany("INSERT INTO alias (item_id, alias, alias_key) VALUES (?,?,?)", aliases)
    cov.filled("alias", len(aliases))
    # The ids the registry absorbed (`registry.flowing`), each naming its water now.
    absorbed = sorted((a, i["id"]) for i in items for a in i.get("aliases", []))
    db.executemany("INSERT INTO item_alias (alias, item_id) VALUES (?,?)", absorbed)
    cov.filled("item_alias", len(absorbed))

    # Clustered by item so one water's sections land together: `regsForItem` should be one
    # or two range reads, and that is a property of the write order, not of the format.
    # HANDLES ON BOTH SIDES — item.ord and the section handle. A section the handle table
    # does not know cannot be written: it would mean this registry and that atlas are not the
    # same build, and a wrong handle is worse than a missing row.
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    _unknown = [s for i in items for s in i.get("section_ids", []) if s not in sid]
    if _unknown:
        raise SystemExit(f"item_section: {len(_unknown):,} sections are not in the handle "
                         f"table (e.g. {_unknown[:3]}) — the registry and section_handles.txt "
                         f"are from different builds")
    pairs = [(ords[i["id"]], sid[s]) for i in items for s in i.get("section_ids", [])]
    db.executemany("INSERT INTO item_section (ord, sid) VALUES (?,?)", pairs)
    cov.filled("item_section", len(pairs))


def _kind_registry(registry_items: list[dict]) -> dict:
    """`{item id: (kind, section_ids)}` — the registry as `reach.water_kind.kind_of` and
    `registry.regions` read it, from the bundle's parsed `registry_items`."""
    from types import SimpleNamespace
    return {i["id"]: SimpleNamespace(kind=i.get("kind"), section_ids=i.get("section_ids") or [])
            for i in registry_items}


def _water_places(build_dir: Path, graph, registry_items: list[dict]) -> dict:
    """`{sid: (kind, regions, mus)}` for every section of the atlas (S1, S1b: `region_place`,
    `mu_set`, `unnamed_part.kind`):

      kind     `reach.water_kind.kind_of` — THE water kind of a section (the owner item's, else the
               graph node's), the one every `feature_types` filter placed the rules by; for a
               section no water owns, its node's: stream, lake or wetland (read for those only)
      regions  the `area:region:*` items holding it, by THE ONE region membership
               (`registry.regions.section_regions`, T115): a straddling section — stream piece
               or lake — is in both regions (T183) — the regions whose zone tables it takes
      mus      the MUs the atlas graph says it touches (`StreamNode.mus`)

    Read once, here, from the atlas the bundle is built from; `derived.write` groups and proves
    them (a section in B.C. with no place, or a place in no region, stops the build). A NAMED
    section the graph lacks gets no place here and is refused there if it lies in B.C."""
    from pipeline.atlas.reach.water_kind import kind_of
    from pipeline.atlas.registry import regions as _regions
    from pipeline.common.section_handles import read as _read_handles

    ids, sid = _read_handles(build_dir)
    registry = _kind_registry(registry_items)
    named = {s for i in registry_items if is_water(i) for s in i.get("section_ids") or []}
    _regions.attach_from(build_dir, graph, registry)
    # THE ONE REGION MEMBERSHIP (T115): the regions whose zone tables a section takes are asked of
    # `registry.regions`, never re-derived here.
    regions_of = _regions.section_regions(registry, graph)
    interned: dict[tuple, tuple] = {}
    out: dict[int, tuple] = {}
    for h in ids:
        node = graph.nodes.get(h)
        if node is None and h in named:
            continue
        if node is None:
            raise SystemExit(f"water_place: section {h} is in the handle table but not the graph "
                             f"— the atlas files are from different builds")
        p = (kind_of(graph, registry, h), tuple(sorted(regions_of.get(h, ()))),
             tuple(sorted(set(node.mus))))
        out[sid[h]] = interned.setdefault(p, p)
    print(f"     places: {len(out):,} sections ({len(out) - len(named & set(ids)):,} unnamed), "
          f"{len(interned):,} distinct (kind, regions, MUs)")
    return out


def _lake_areas(db: sqlite3.Connection, build_dir: Path, registry_items: list[dict],
                cov: Coverage) -> None:
    """`item.area_ha`: how big each LAKE is, for ranking — the lake's size signal, as stream
    magnitude is a stream's.

    Search and "water near a town" rank by how much water a candidate is, before a tile is ever
    loaded, and a lake has no magnitude: the app stood a constant in for every lake (Williston
    ranked like a pond). The area is the SAME polygon the tile draws (`waterbody_polys.pkl`, the
    tile's `area_m2`), read from the same build, so the two cannot be of different vintages.
    Whole hectares: ranking is log-scaled, and a pond under half a hectare reads 0, which is what
    it is next to a lake. One value per lake item (a lake part's is its own polygon's); NULL for
    every other kind. Measured: 7,742 lakes, Williston 172,669 ha, median 17 ha."""
    poly_path = build_dir / "waterbody_polys.pkl"
    if not poly_path.exists():                  # P2: no missing-file fallbacks
        raise SystemExit(f"item.area_ha: no {poly_path.name} in {build_dir} — an atlas build "
                         "writes it; without it every lake ranks as a pond")
    import pickle
    with poly_path.open("rb") as fh:
        polys = pickle.load(fh)
    rows = []
    for i in registry_items:
        if i.get("kind") != "lake" or not is_water(i):
            continue
        got = [polys[s] for s in i.get("section_ids", []) if s in polys]
        if got:
            rows.append((int(round(sum(g.area for g in got) / 1e4)), i["id"]))
    db.executemany("UPDATE item SET area_ha = ? WHERE item_id = ?", rows)
    cov.filled("item.area_ha", len(rows))


def touching_pairs(edges, sid: dict[str, int],
                   waters_of: dict[int, set[int]]) -> list[tuple[int, int]]:
    """Every pair of sections of ONE water that a flow edge joins, as sorted handle pairs.

    `edges` are the graph's (from_node, to_node) pairs; `waters_of` maps a section handle to the
    items it belongs to. An edge counts only when both ends belong to a common water, and a
    section never touches itself. See `section_touch` in schema.sql for what that includes.
    """
    out: set[tuple[int, int]] = set()
    for f, t in edges:
        a, b = sid.get(f), sid.get(t)
        if a is None or b is None or a == b:
            continue
        if waters_of.get(a, set()) & waters_of.get(b, set()):
            out.add((a, b) if a < b else (b, a))
    return sorted(out)


def _section_touch(db: sqlite3.Connection, build_dir: Path, graph, cov: Coverage) -> None:
    """`section_touch`, from the atlas graph and the `item_section` already written.

    REFUSED, not skipped, without a graph: an empty table reads as "no part of any water borders
    another", and a reader merging neighbours would then show every stretch apart.
    """
    if graph is None:
        raise SystemExit(f"section_touch: no {build_dir / 'graph.pkl'} — the bundle cannot say "
                         f"which parts of a water border each other without the atlas graph")
    from pipeline.common.section_handles import read as _read_handles

    _, sid = _read_handles(build_dir)
    waters_of: dict[int, set[int]] = {}
    for ord_, s in db.execute("SELECT ord, sid FROM item_section"):
        waters_of.setdefault(s, set()).add(ord_)
    pairs = touching_pairs(((e.from_node, e.to_node) for e in graph.edges), sid, waters_of)
    db.executemany("INSERT INTO section_touch (a, b) VALUES (?,?)", pairs)
    cov.filled("section_touch", len(pairs))


def _place_id(p: dict) -> str:
    """A stable id for a place.

    NOT the name. 373 names repeat in the gazetteer — Hope, Richmond, Salmon Valley — and
    keying on the name made `INSERT OR REPLACE` last-write-wins, silently destroying 532
    places. Worse, the fetch sorts least-important-last, so the survivor was the wrong one:
    "Hope" became a hamlet in Idaho rather than the BC town, and `watersNear("Hope")` would
    have answered with northern Idaho. Coordinates disambiguate and are stable across
    fetches for the same OSM element.
    """
    return f"{p['name']}@{p['lat']:.4f},{p['lon']:.4f}"


def _in_bc(boundary: Path):
    """A predicate: is this lon/lat inside British Columbia?

    The Overpass query is a bounding box whose corners are Alberta, Alaska, Washington and
    the Pacific — 1,872 of the 4,677 places it returns are not in BC. Calgary is not a
    place from which to ask about BC fishing regulations, and an out-of-province label on
    the map is a claim we cannot back.
    """
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.prepared import prep

    gdf = gpd.read_file(boundary).to_crs(4326)
    poly = prep(gdf.geometry.union_all())
    return lambda lon, lat: poly.contains(Point(lon, lat))


def _places(db: sqlite3.Connection, places_json: Path, boundary: Path,
            cov: Coverage) -> list[dict]:
    """The gazetteer: what a person types when they mean "near here"."""
    if not places_json.exists():                # `build` already refuses this; P2: never a skip
        raise SystemExit(f"place: {places_json} not fetched — python data/fetch_data.py")
    # NOT re-clipped here. `data/fetch_data.py` clips to the province when it writes the
    # gazetteer, so the file on disk is already only BC. Filtering in both places means two
    # answers to "is this in British Columbia" and one of them eventually goes stale.
    kept = json.loads(places_json.read_text())
    rows = kept
    db.executemany("INSERT INTO place VALUES (?,?,?,?,?,?,?)",
                   ((n, _place_id(p), p["name"], p.get("place"), p.get("pop"),
                     p["lat"], p["lon"]) for n, p in enumerate(kept, 1)))
    cov.filled("place", len(kept))
    if len(kept) < len(rows):
        print(f"     ({len(rows) - len(kept):,} places outside BC dropped)")
    return kept


def _place_water(db: sqlite3.Connection, build_dir: Path, places: list[dict],
                 registry_items: list[dict], geoms, cov: Coverage,
                 radius_km: float = 25.0) -> None:
    """What water is near each town.

    THE PRECOMPUTE, done here so the phone never has to hold geometry. Measured to every
    named WATERBODY, not to every section — that is what makes the full matrix cheap enough
    to compute exactly rather than approximate: only 85,464 sections belong to a named item
    at all, so the tree is small and every distance is a real distance to real linework
    rather than to a centroid.

    Works in BC Albers, where a metre is a metre. Doing this in degrees is the classic way
    to get a radius that is 40% wrong at the top of the province.
    """
    if not places or geoms is None:             # P2: no missing-file fallbacks
        raise SystemExit("place_water: needs geometries.pkl and a non-empty gazetteer")

    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    items = registry_items
    # section -> (item_id, name), for WATERS that have a name a person could search.
    #
    # `_is_water` for the same reason as in the gauge tables (`gauges.consume.bundle_tables`): an `area:` item's name is a slug
    # (`area:indigenous_land:becher_bay_1`), nobody types it, and because `area:` sorts first
    # `setdefault` gave it the section before the river could claim it. That put 35,133 park
    # polygons into "water near this town" AND cost the real water those sections as distance
    # candidates, so the km it reports was measured to whatever was left over.
    owner: dict[str, tuple[str, str]] = {}
    for i in items:
        if not i.get("name") or not is_water(i):
            continue
        for sec in i.get("section_ids", []):
            owner.setdefault(sec, (i["id"], i["name"]))

    keys = [s for s in owner if s in geoms]
    if not keys:
        raise SystemExit("place_water: no named section has geometry in this build")
    tree = STRtree([geoms[s] for s in keys])
    # Read the handles back rather than passing them across half the module: `item` is
    # already written by the time this runs, and one source for `ord` cannot drift.
    ords: dict[str, int] = dict(db.execute("SELECT item_id, ord FROM item"))

    pts = gpd.GeoSeries([Point(p["lon"], p["lat"]) for p in places], crs=4326).to_crs(3005)

    radius_m = radius_km * 1000.0
    rows: list[tuple[int, int, int]] = []
    for n, (place, pt) in enumerate(zip(places, pts), 1):
        best: dict[str, tuple[str, float]] = {}
        for ix in tree.query(pt.buffer(radius_m)):
            d = geoms[keys[ix]].distance(pt)
            if d > radius_m:
                continue                      # the query is the bbox; this is the circle
            item_id, name = owner[keys[ix]]
            if item_id not in best or d < best[item_id][1]:
                best[item_id] = (name, d)
        for item_id, (_name, d) in best.items():
            # The SAME number today's REAL column holds, as an integer: round to the two
            # decimals of a km first, then scale. Rounding the metres directly would differ
            # from the old value at a .5 boundary, and this is meant to be lossless.
            rows.append((n, int(round(round(d / 1000.0, 2) * 100)), ords[item_id]))

    db.executemany("INSERT INTO place_water (place_id, ckm, ord) VALUES (?,?,?)", rows)
    cov.filled("place_water", len(rows))


def edition_valid_until(data_dir: Path) -> str:
    """The last day the edition's regulations are in force (ISO day): the end of the period the synopsis in
    `data_dir` states on its cover (`synopsis_pages.synopsis_period`). The ONE place the release's `valid_until` comes
    from — no date is typed anywhere. A source tree without the book is a hard error: an empty stamp would ship a
    release the app cannot tell has lapsed."""
    from pipeline.common.synopsis_pages import PDF_NAME, synopsis_period
    return synopsis_period(Path(data_dir) / PDF_NAME).valid_until


def build(build_dir: Path, out: Path, *, data_dir: Path | None = None,
          reaches: Path | None = None, entries: Path | None = None, tiles: Path | None = None) -> Path:
    """Write the bundle. Returns the path written.

    ``data_dir`` is where FETCHED source lives, and it comes from config — not from anything
    derived from the build directory, and not from a literal.

    THIS HAS NOW BEEN WRONG TWICE, in opposite directions, and both times it was silent:

      · it defaulted to `build_dir.parents[1] / "data"`, which for `output/v2/full` resolved
        to `output/data` — a directory that never existed;
      · then to a literal `<repo>/data`, which was right until the fetched files moved into
        `data/source/` and stopped being found.

    Both produced a bundle missing `gauge`, `section_gauge`, `section_down`, `place` and
    `place_water` — and a cheerful success message. That is the `--splits` incident twice
    over, so the path comes from `SOURCE` and a missing file is now a hard error rather than
    a skipped table.
    """
    data_dir = Path(data_dir) if data_dir else Path(SOURCE)
    if not data_dir.is_dir():
        raise FileNotFoundError(
            f"source data not found: {data_dir} — check `data_tree.source` in config.yaml")
    _need = ["bc_hydrometric_stations.json", "bc_places.json"]
    _missing = [n for n in _need if not (data_dir / n).exists()]
    if _missing:
        raise FileNotFoundError(
            f"{data_dir} is missing {', '.join(_missing)}.\n"
            "  A bundle without these silently loses gauge, section_gauge, section_down, "
            "place and place_water\n"
            "  and still reports success. Fetch them: python data/fetch_data.py")
    cov = Coverage()
    db = _connect(out)

    registry = build_dir / "registry.json"
    if not registry.exists():
        raise FileNotFoundError(f"{registry} not found — run the build first")
    # THE ATLAS'S BIG INPUTS, EACH LOADED ONCE (DATAFLOW P1b, M10.2): `registry.json` was parsed
    # five times and `graph.pkl` / `geometries.pkl` twice each. Loaded here, handed down, dropped
    # after their last reader.
    import pickle
    registry_items = json.loads(registry.read_text())["items"]
    _items(db, build_dir, registry_items, cov)
    _lake_areas(db, build_dir, registry_items, cov)
    # THE BOOK'S RIVER PAIRINGS, READ FROM THE CATALOGUE NOW (T76): the donor panels below treat a continuation's gauges
    # as a section's own, and they are built while the graph is loaded — before `entry` exists. Read from the catalogue
    # the rules will be written from (the same sources, the same registry, `rules.read_entries`), against `item` alone,
    # and BEFORE the pickles: the read parses registry.json again, and the build's peak is graph + geometries.
    # `_continuation.write` settles again from `entry` and stops the build if the two differ.
    from pipeline.atlas.registry import load_registry as _load_registry
    from pipeline.deliver.bundle import continuation as _continuation
    _book = _continuation.pairs_of(db, _continuation.catalogue_heads(
        Path(entries) if entries else CURATED.regulations.entries.catalogue.parent,
        _load_registry(str(Path(build_dir) / "registry.json"))))
    graph_path, geom_path = build_dir / "graph.pkl", build_dir / "geometries.pkl"
    for _p in (graph_path, geom_path):          # P2: the atlas's own files are required
        if not _p.exists():
            raise FileNotFoundError(f"{_p} not found — an atlas build writes it")
    with graph_path.open("rb") as fh:
        graph = pickle.load(fh)
    _section_touch(db, build_dir, graph, cov)
    # WHICH WATER EACH WATER DRAINS INTO (ROUND14 1, `nearer.item_down`): read from the graph while it
    # is loaded; `trib_nearer` is derived from it once the rule sets are written
    from pipeline.deliver.bundle import nearer as _nearer
    _nearer.write_item_down(db, graph, registry_items, cov)
    # WHICH STREAM GOES ON AS ANOTHER (`water_continue`, the Chilliwack as the Vedder): linked here,
    # from item_down and the graph while it is loaded; written as parts once `part_section` exists
    # (audit F07: the release used to unpickle graph.pkl again just for this)
    from pipeline.common.section_handles import read as _read_handles
    _handle_ids = list(_read_handles(build_dir)[0])
    _continues = _continuation.links(db, _handle_ids, graph)
    # SETTLED NOW, BEFORE THE GAUGES (T76), with the pairings read above (`_book`)
    _settled = _continuation.settle(_continues, _book)
    del _book
    # The same graph says where each section lies along its water and what ends it — the cuts a part's runs go between
    # (`section_span`, `split`). A natural lower end's words (`continues:` / `confluence:` / `mouth`) are the links just
    # made (T108: `continuation.goes_on` decides it once; the span end and the ▲ button cannot disagree). Before the
    # geometries load: `spans.write` parses registry.json again.
    from pipeline.deliver.bundle import spans as _spans
    _spans.write(db, graph, build_dir, cov, ends=_continuation.lower_ends(
        _continues, _settled, dict(db.execute("SELECT ord, item_id FROM item")),
        _continuation.ranks(graph, _handle_ids, {h for r in _continues for h in (r[1], r[3])})))
    places =_places(db, data_dir / "bc_places.json",
                     data_dir / "bc_boundary.geojson", cov)
    with geom_path.open("rb") as fh:
        geoms = pickle.load(fh)
    _place_water(db, build_dir, places, registry_items, geoms, cov)
    _gauge_tables.write(db, build_dir, data_dir, registry_items, graph, geoms, cov,
                        kinds=_kind_registry(registry_items),
                        continues=_continuation.item_pairs(db, _settled))
    # WHERE EACH WATER'S MOUTH FLOWS (T59: `water_exit` named Annacis Channel for the Fraser): read here, while the
    # graph is loaded; `topology.write` keeps only the exits the mouth reaches
    from pipeline.deliver.bundle import topology as _topology
    _secs: dict = {}
    for _o, _s in db.execute("SELECT ord, sid FROM item_section"):
        _secs.setdefault(_o, set()).add(_s)
    _mouths = _topology.mouth_downs(graph, _handle_ids, _secs)
    del _secs, _handle_ids
    # WHERE EVERY UNNAMED SECTION IS AND WHAT WATER IT IS (S1): read from the atlas here, while it
    # is loaded, and handed to the derived tables, which group the unnamed sections into parts
    places = _water_places(build_dir, graph, registry_items)
    # THE HOME MAP `_water_places` attached, kept past the graph (T140): the rules ask the one region
    # membership for the regions a row's own water lies in (`rules.regions_of_waters`)
    from pipeline.atlas.registry import regions as _regions
    _home = _regions.home_view(graph)
    # P09: the named sections' shapes, kept until the parts exist (part_geo / part_mark / water_exit)
    named_geo = _topology.keep_named(db, build_dir, geoms)
    del graph, geoms, registry_items
    # The regulations. The reach builder resolved which water every rule covers; this only
    # interns and writes. See pipeline/deliver/bundle/rules/ for why it must not re-derive.
    if reaches is not None:
        # A NAMED RUN, for building against a reach run that is not (yet) under
        # GENERATED.reaches — a side build. Held to the same pairing `_reach_run` enforces: a
        # run resolved against a different atlas binds rules to sections that are not these.
        from pipeline.common.section_handles import digest_for
        _rep = json.loads((Path(reaches) / "report.json").read_text())
        if _rep.get("handles") != digest_for(build_dir):
            raise SystemExit(f"--reaches {reaches} was resolved against atlas handles "
                             f"{_rep.get('handles')!r}, not {build_dir}'s")
        _reaches = Path(reaches)
    else:
        _reaches = _reach_run(build_dir)
    if _reaches is None:
        # NOT `cov.skip`. A skipped table prints "not wired" and exits 0, and for the
        # regulations that is a province-wide lie: `statusFor` is written so that a section
        # with no rule row means "open under the general rules", which is correct against a
        # table that HAS rows. Against an empty one, every water in British Columbia renders
        # OPEN · GENERAL RULES and the build reports success.
        #
        # `skip` is right for a table whose producer genuinely does not exist yet — the
        # bathymetry charts, the stocking roster. It is wrong for one whose absence changes
        # what the app asserts about closed water.
        raise SystemExit(
            f"no reach run reports build {build_dir.name!r} under {GENERATED.reaches}.\n"
            f"  Bundling without one produces a bundle in which every water in the province\n"
            f"  renders as open. Run:\n"
            f"    python -m pipeline.atlas.reach.cli --build {build_dir} --out "
            f"{GENERATED.reaches / build_dir.name}")
    else:
        # THE SAME REGISTRY THE REACH RUN READ (P2): handles pair section numbers; the registry
        # digest pairs the items' names and kinds, which `pipeline.atlas.sidecars` can rewrite
        # without touching a handle.
        from pipeline.common.section_handles import registry_digest_for
        _registry_digest = registry_digest_for(build_dir)
        _ran_reg = json.loads((Path(_reaches) / "report.json").read_text()).get("registry_digest")
        if _ran_reg != _registry_digest:
            raise SystemExit(
                f"the reach run at {_reaches} was made against registry "
                f"{_ran_reg or '(none stated: the run predates the registry digest)'}, not "
                f"{build_dir}'s ({_registry_digest}) — re-run the reach builder:\n"
                f"    python -m pipeline.atlas.reach.cli --build {build_dir} --out {_reaches}")
        print(f"     rules: reading {_reaches}")
        # `entries` names a side copy of the entry sources, for a side build against a reach run
        # made from that copy (`reach.cli --entries`); the two must be the same corpus, and
        # `rules.write` refuses a run that binds rules the entries do not have.
        _rules.write(db, _reaches,
                     Path(entries) if entries else CURATED.regulations.entries.catalogue.parent,
                     cov, build_dir=build_dir, home=_home)
        # THE DERIVED TABLES (DATAFLOW P2): rule_ix, rule.closure_grade, part, part_section — from
        # the rows just written, proved against them.
        from pipeline.deliver.bundle import derived as _derived
        _derived.write(db, cov, places=places)
        _continuation.write(db, _continues, cov, built_with=_settled)
        _topology.write(db, named_geo, cov, mouths=_mouths)
        del named_geo
        # THE NEARER ✱ ROW (ROUND14 1, RULINGS T14): from item_down, entry.matched and the rule sets
        _nearer.write_trib_nearer(db, cov)
        _nearer.prove(db)

    # Everything below needs a producer that does not exist yet, or exists but has not been
    # pointed at this. Named individually rather than left silently empty.
    cov.skip("chart", "needs the bathymetry fetch (fetch_data: bathymetry_contours)")
    cov.skip("stock_water", "needs the FIDQ fetch — no waterbody roster on disk yet")
    cov.skip("stock_code", "needs the FIDQ fetch: the species/stage dictionaries come with it")
    cov.skip("release", "RETIRED — keyed on a name; stock_water + the stocking feed replace it")

    # ACCESS (T54): the no-access, permit, reserve and private land each section runs through, and how much of it —
    # measured against the TILES' OWN polygons and section geometry (`tiles`, default GENERATED.tiles), refused when the
    # tiles were cut from another atlas. Not regulation: no status reads it; the app colours and words it.
    from pipeline.common.section_handles import digest_for as _handles_of
    from pipeline.deliver.bundle import access as _access
    _access.write(db, Path(tiles) if tiles else GENERATED.tiles, handles=_handles_of(build_dir), cov=cov)
    # WHAT THE TILES LEFT OUT (T64: unnamed wetlands in no item) must be answered by land or area alone
    if _reaches is not None:
        from pipeline.deliver.bundle import undrawn as _undrawn
        _undrawn.check(db, Path(tiles) if tiles else GENERATED.tiles, cov=cov)
    # THE BOOK'S WORDS THE GLOSSARY QUOTES (plan 2.14): copied in, so the answers read them from here
    from pipeline.deliver.bundle import glossary_source as _glossary_source
    _glossary_source.write(db, cov)
    # THE DFO SALMON (T211): the federal salmon rules per chain of locations and day, and each placed section's chain —
    # read once here from the reach run's placements, the curated locations and the typed feed (`pipeline.deliver.salmon`)
    if _reaches is not None:
        from pipeline.deliver import salmon as _salmon
        _files, _feeds, _placed = _salmon.load_inputs(
            _reaches, _load_registry(str(Path(build_dir) / "registry.json")))
        # each water's sections (an absorbed id read as the water it names now): a DFO place not drawn yet marks its
        # whole water "check DFO" (partial coverage, owner 2026-10-28)
        _item_secs: dict = {}
        for _it, _s in db.execute("SELECT i.item_id, s.sid FROM item_section s JOIN item i ON i.ord = s.ord"):
            _item_secs.setdefault(_it, []).append(_s)
        for _al, _it in db.execute("SELECT alias, item_id FROM item_alias").fetchall():
            if _it in _item_secs:
                _item_secs.setdefault(_al, _item_secs[_it])
        # THE REGION HEADER's waters: a watershed is its river and every item upstream of it (`item_down`, the registry's
        # flow between waters), read here once (owner 2026-10-28: "Squamish watershed = Squamish + tributaries")
        _ord_of = dict(db.execute("SELECT item_id, ord FROM item"))
        _id_of = {o: i for i, o in _ord_of.items()}
        _up: dict = {}
        for _a, _d in db.execute("SELECT ord, down_ord FROM item_down"):
            _up.setdefault(_d, []).append(_a)

        def _upstream(items):
            seen = {_ord_of[i] for i in items if i in _ord_of}
            todo = list(seen)
            while todo:
                for u in _up.get(todo.pop(), ()):
                    if u not in seen:
                        seen.add(u)
                        todo.append(u)
            return sorted(_id_of[o] for o in seen)

        _payload, _salmon_secs = _salmon.build(_files, _feeds, _placed, _read_handles(build_dir)[1], _item_secs,
                                               headers=_salmon.load_headers(_feeds),
                                               scope_headers=_salmon.load_current_headers(_feeds), upstream=_upstream)
        _salmon.write_bundle(db, _payload, _salmon_secs)
        cov.filled("salmon_section", len(_salmon_secs))
        del _files, _feeds, _placed, _payload, _salmon_secs

    db.executescript(INDEXES.read_text())
    # THE BUNDLE CARRIES THE POLICY IT WAS BUILT UNDER.
    #
    # `section_gauge.trust` says `fair`; this says what `fair` meant when that row was
    # written. Without it a client explaining a band ("a major branch — at least 1% of the
    # gauge's watershed") is quoting a number from ITS OWN build, and a phone holding last
    # season's bundle would explain last season's bands with this season's floors. Stamping
    # them makes every bundle self-describing, so the explanation cannot outrun the data.
    #
    # It is also the only copy that can never drift, because it ships inside the artifact
    # rather than beside it. The generated TypeScript is a compile-time convenience — a
    # browser cannot import shed.py, and a union type has to exist before the bundle is
    # opened — and `pipeline.tools.emit.gauge_policy --check` is what keeps THAT honest.
    from pipeline.gauges.consume.shed import TRUST_BANDS

    # The edition this bundle is: the atlas build it came from, and the last day its
    # regulations are in force — the synopsis period's end, READ OFF THE BOOK'S COVER
    # (`edition_valid_until`; owner 2026-10-22 late). The release copies it into
    # `manifest.regulations.valid_until`, so the app can say when the regulations lapse.
    from pipeline.common.section_handles import digest_for as _digest_for

    _handles_digest = _digest_for(build_dir)
    _version = f"{build_dir.name}+{GENERATED.reaches.name}"
    _valid_until = edition_valid_until(data_dir)
    _reach_digest, _reach_run_name = "", ""
    if _reaches is not None:
        # NO FALLBACK: a run whose report cannot be read, or states no digest, stops the build —
        # an empty `reach_digest` is refused by every reader downstream anyway.
        _rep = json.loads((_reaches / "report.json").read_text())
        _reach_digest = str(_rep["digest"])
        if not _reach_digest:
            raise SystemExit(f"bundle: {_reaches}/report.json states no `digest`")
        _reach_run_name = _reaches.name

    db.executemany("INSERT INTO meta VALUES (?,?)", [
        ("schema", SCHEMA.read_text().split("\n")[0]),
        ("format", BUNDLE_FORMAT),
        ("schema_sha256", schema_sha256()),
        ("build", str(build_dir)),
        ("generated_by", "python -m pipeline.deliver.bundle"),
        ("trust_bands", json.dumps({b: f for b, f in TRUST_BANDS})),
        # THE VINTAGE THE TILE MUST MATCH. Every `sid` in this bundle is an index into the
        # atlas's section_handles.txt; the tile carries handles from the same table and
        # publishes this digest in its sidecar. If the two differ, a lookup does not miss —
        # it HITS THE WRONG SECTION, so the app must refuse to colour rather than proceed.
        ("section_handles", _handles_digest),
        # THE REGISTRY THE TILES MUST ALSO CARRY (P2): names and kinds, not only handles.
        ("registry_digest", _registry_digest),
        # THE TWO KEYS THE CLIENT ACTUALLY READS, and neither was written.
        #
        # `source.ts` asks for `version` and `valid_until`; this table held `schema`,
        # `build`, `generated_by` and `trust_bands`. So `info()` returned
        # `{version: "unknown", validUntil: null}` on every real bundle — silently, through
        # `??` — and `validUntil` is the STALENESS GATE. The app's own contract says a stale
        # answer must never render as a live one, and it could not know it was stale.
        #
        # It survived because the dev fixture writes a THIRD key set that happens to include
        # `version`, so the app's tests were green against a file the pipeline does not
        # produce. Same shape as the date-window bug: two artifacts agreeing with each other
        # and neither agreeing with the client.
        ("version", _version),
        ("valid_until", _valid_until or ""),
        # WHICH ATLAS THESE RULES WERE RESOLVED AGAINST.
        #
        # `section_ruleset` binds rules to section ids, and section ids are minted by the
        # atlas build. Pair a bundle with tiles from a DIFFERENT build and the ids do not
        # line up — and it fails silently, as regulations that are simply absent, on exactly
        # the screens where absent reads as "you may fish here". The tiles and the bundle
        # were already required to be a matched pair for colour; this makes the pairing
        # checkable, and load-bearing for correctness.
        ("reach_digest", _reach_digest),
        ("reach_run", _reach_run_name),
    ])
    db.commit()
    db.execute("VACUUM")
    db.close()

    size = out.stat().st_size
    print(f"  ✅ {out}  {size / 1e6:.1f} MB")
    cov.report()
    return out
