"""Packages a build into the client bundle.

    python -m pipeline.bundle --build output/v2/full --out output/bundle/bundle.sqlite

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

HERE = Path(__file__).parent
SCHEMA = HERE / "schema.sql"
INDEXES = HERE / "indexes.sql"


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
    db.executescript(SCHEMA.read_text())
    return db


def _is_water(item: dict) -> bool:
    """Is this a water a person could search for, or administrative geography?

    The registry holds both. An `area:` item is a park, a reserve or a closure polygon —
    real, and already on the TILE as the `areas` attribute of every feature inside it.
    Carrying its membership in the bundle as well put 778,411 of 838,228 item_section rows
    there (93%) and took the artifact from 10 MB to 140 MB, against a budget of about ten.
    Its name is a slug (`area:indigenous_land:lukesstsissum_9`), so it is not something
    anyone types either.

    One fact, one place: geography on the tile, regulations in the bundle.
    """
    return not item["id"].startswith("area:")


def _items(db: sqlite3.Connection, registry: Path, cov: Coverage) -> None:
    """item / alias / item_section, from the registry.

    The registry is the province-wide answer to "what is this water called and which
    sections is it". It is also the whole search index: 20,609 distinct strings, which is
    ~90 KB gzipped — small enough that no server-side search is needed anywhere.
    """
    items = [i for i in json.loads(registry.read_text())["items"] if _is_water(i)]
    db.executemany("INSERT OR REPLACE INTO item VALUES (?,?,?)",
                   ((i["id"], i["name"], i.get("kind")) for i in items))
    cov.filled("item", len(items))

    # Normalised the way the TILE normalises its search haystack — same function, imported.
    # The registry carries both cases of many strings ("EAST WHITE RIVER" and "East White
    # River"); `pipeline/tiles/names.normalise` folds them, and this used to compare raw,
    # so the tile dropped ~3,000 shouting duplicates and the bundle kept them. Two search
    # paths that disagree about what a distinct name is will return different results for
    # the same query, which is the one thing a search index may not do.
    from pipeline.tiles.names import normalise

    aliases = []
    for i in items:
        seen = {normalise(i["name"])}
        for v in i.get("variants", []):
            key = normalise(v)
            if not key or key in seen:
                continue
            seen.add(key)
            aliases.append((i["id"], v))
    db.executemany("INSERT INTO alias VALUES (?,?)", aliases)
    cov.filled("alias", len(aliases))

    # Clustered by item so one water's sections land together: `regsForItem` should be one
    # or two range reads, and that is a property of the write order, not of the format.
    pairs = [(i["id"], s) for i in items for s in i.get("section_ids", [])]
    db.executemany("INSERT INTO item_section VALUES (?,?)", pairs)
    cov.filled("item_section", len(pairs))


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
    if not places_json.exists():
        cov.skip("place", f"{places_json.name} not fetched")
        return []
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
                 cov: Coverage, radius_km: float = 25.0) -> None:
    """What water is near each town.

    THE PRECOMPUTE, done here so the phone never has to hold geometry. Measured to every
    named WATERBODY, not to every section — that is what makes the full matrix cheap enough
    to compute exactly rather than approximate: only 85,464 sections belong to a named item
    at all, so the tree is small and every distance is a real distance to real linework
    rather than to a centroid.

    Works in BC Albers, where a metre is a metre. Doing this in degrees is the classic way
    to get a radius that is 40% wrong at the top of the province.
    """
    geom_path = build_dir / "geometries.pkl"
    registry = build_dir / "registry.json"
    if not places or not geom_path.exists():
        cov.skip("place_water", f"needs {geom_path.name} and a gazetteer")
        return

    import pickle
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.strtree import STRtree

    items = json.loads(registry.read_text())["items"]
    # section -> (item_id, name), for items that have a name a person could search
    owner: dict[str, tuple[str, str]] = {}
    for i in items:
        if not i.get("name"):
            continue
        for sec in i.get("section_ids", []):
            owner.setdefault(sec, (i["id"], i["name"]))

    with geom_path.open("rb") as fh:
        geoms = pickle.load(fh)

    keys = [s for s in owner if s in geoms]
    if not keys:
        cov.skip("place_water", "no named section has geometry in this build")
        return
    tree = STRtree([geoms[s] for s in keys])

    pts = gpd.GeoSeries([Point(p["lon"], p["lat"]) for p in places], crs=4326).to_crs(3005)

    radius_m = radius_km * 1000.0
    rows: list[tuple[int, str, float]] = []
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
            rows.append((n, item_id, round(d / 1000.0, 2)))

    db.executemany("INSERT INTO place_water VALUES (?,?,?)", rows)
    cov.filled("place_water", len(rows))


def _gauges(db: sqlite3.Connection, build_dir: Path, data_dir: Path, cov: Coverage) -> None:
    """Which water has a gauge, and how well that gauge speaks for it.

    THE ANSWER A USER IS ACTUALLY ASKING FOR. Not "where are the gauges" — a map of 440
    dots tells a person nothing about the creek under their feet. The question is "is
    anything measuring THIS water", and for the great majority of BC the honest answer is
    no. Writing that answer down is the point: a section with no row here is a section the
    app must say nothing about, rather than reaching for the nearest dot.

    Three artifacts, three costs:
      · the station roster        — a fetch, cheap, travels with the repo
      · station -> node           — geometry, expensive, cached beside the build
      · node -> shed              — topology only, re-run freely

    `section_down` is written from the same pass because its contract ties it to this one:
    only shed members are stored, since every hop on a trace to a gauge is inside that
    gauge's shed by definition.
    """
    stations_path = data_dir / "bc_hydrometric_stations.json"
    graph_path = build_dir / "graph.pkl"
    geom_path = build_dir / "geometries.pkl"
    if not stations_path.exists():
        cov.skip("section_gauge", f"no {stations_path.name} "
                                  "(data/fetch_data.py --layers hydrometric_stations)")
        cov.skip("section_down", "needs section_gauge")
        return
    if not graph_path.exists():
        cov.skip("section_gauge", f"no {graph_path.name} in this build")
        cov.skip("section_down", "needs section_gauge")
        return

    import pickle

    from pipeline.hydro import build_gauge_sheds, lake_gauge_links
    from pipeline.hydro.match import nodes_for, read_match, summarise
    from pipeline.hydro.shed import downstream_map, load_stations

    stations = load_stations(stations_path)
    with graph_path.open("rb") as fh:
        graph = pickle.load(fh)

    # THE FROZEN MATCH — `pipeline/gauge_match.json`, the same file the build read to cut
    # rivers at their gauges. Nothing is matched here: the file says where each station is
    # and which water it is on, and `nodes_for` projects that coordinate onto THIS graph,
    # taking the section that begins at the gauge and runs upstream. Node ids move when the
    # sectionizer cuts differently; a published coordinate does not.
    matches = read_match()
    if not matches:
        cov.skip("section_gauge", "no pipeline/gauge_match.json — run "
                                  "`python -m pipeline.hydro.match --build <build>`")
        cov.skip("section_down", "needs section_gauge")
        return

    with geom_path.open("rb") as fh:
        geoms = pickle.load(fh)
    matched = nodes_for(matches, graph, geoms)
    del geoms
    prov = {m.station: m for m in matches}

    # section -> the item that owns it, so a client can name the water, not just the gauge.
    owner: dict[str, str] = {}
    for i in json.loads((build_dir / "registry.json").read_text())["items"]:
        for sec in i.get("section_ids", []):
            owner.setdefault(sec, i["id"])

    # No liveness column of any kind -- see schema.sql. The bundle says a gauge exists and
    # where it is; the feed index says which are talking, by containing them.
    db.executemany("INSERT INTO gauge VALUES (?,?,?,?,?,?,?,?,?,?)", [
        (s["station"], s["name"],
         owner.get(matched.get(s["station"], "")), matched.get(s["station"]),
         s["lon"], s["lat"], s.get("area_km2"),
         (graph.nodes[matched[s["station"]]].stream_magnitude
          if s["station"] in matched else None),
         prov[s["station"]].resolved_by if s["station"] in prov else None,
         prov[s["station"]].distance_m if s["station"] in prov else None)
        for s in stations])
    cov.filled("gauge", len(stations))

    # ECCC's own ACTIVE status, not today's traffic. 27 BC stations are active but silent
    # right now — seasonal gauges shut for the winter — and filtering on traffic would
    # delete their sheds every autumn and rebuild them every spring. Retired stations keep
    # their `gauge` row and climatology; what they lose is the claim to speak for reaches
    # they can no longer say anything about.
    active = {s["station"] for s in stations if s.get("active")}
    links = build_gauge_sheds(graph, stations, matched, prefer=active)
    # `links` arrives grouped by section, best ratio first; `seq` freezes that order so a
    # client takes row 0 and never has to re-derive the judgement.
    db.executemany("INSERT INTO section_gauge VALUES (?,?,?,?)",
                   [(l.section_id, l.station, l.trust,
                     graph.nodes[l.section_id].stream_magnitude) for l in links])
    cov.filled("section_gauge", len(links))

    # Lake stations, linked to the lake's registry item rather than to a reach.
    lakes = [(owner[node], st) for node, st in lake_gauge_links(graph, stations, matched)
             if node in owner]
    db.executemany("INSERT OR IGNORE INTO lake_gauge VALUES (?,?)", sorted(set(lakes)))
    cov.filled("lake_gauge", len(set(lakes)))

    # THE ENVELOPE, from the same file the feed publisher reads.
    #
    # ONE PRODUCER, TWO CONSUMERS. `pipeline.hydro.climatology` reads HYDAT and writes
    # clim.json; the 30-minute feed job turns today's discharge into a percentile with it,
    # and the bundle snapshots it here so a client can draw a seasonal band and date an old
    # spot offline. Building it twice would be two envelopes that can disagree about the
    # same river.
    clim_path = data_dir.parent / "output/feeds/gauge/clim.json"
    if clim_path.exists():
        clim = json.loads(clim_path.read_text(encoding="utf-8"))
        rows = [(st, param, int(pent), *bands)
                for st, by_param in sorted(clim.get("stations", {}).items())
                for param, by_pent in sorted(by_param.items())
                for pent, bands in sorted(by_pent.items(), key=lambda kv: int(kv[0]))]
        db.executemany("INSERT INTO gauge_clim VALUES (?,?,?,?,?,?,?,?)", rows)
        cov.filled("gauge_clim", len(rows))

        srows = [(st, param, s_.get("from_year"), s_.get("to_year"), s_.get("years"),
                  s_.get("days"))
                 for st, by_param in sorted(clim.get("stats", {}).items())
                 for param, s_ in sorted(by_param.items())]
        db.executemany("INSERT INTO gauge_stats VALUES (?,?,?,?,?,?)", srows)
        cov.filled("gauge_stats", len(srows))
        print(f"     envelope: HYDAT {clim.get('release')}")
    else:
        cov.skip("gauge_clim", f"no {clim_path.name} — "
                               "run pipeline.hydro.climatology (needs HYDAT)")
        cov.skip("gauge_stats", "same file as gauge_clim")

    down = downstream_map(graph, (l.section_id for l in links))
    db.executemany("INSERT INTO section_down VALUES (?,?)",
                   [(s, down[s]) for s in sorted(down)])
    cov.filled("section_down", len(down))

    bands: dict[str, int] = {}
    for l in links:
        bands[l.trust] = bands.get(l.trust, 0) + 1
    print(f"     match: {summarise(matches)}")
    print(f"     bands: " + ", ".join(f"{bands.get(b, 0):,} {b}" for b, _ in
                                      __import__("pipeline.hydro",
                                                 fromlist=["x"]).TRUST_BANDS))




def build(build_dir: Path, out: Path, *, data_dir: Path | None = None) -> Path:
    """Write the bundle. Returns the path written."""
    data_dir = data_dir or build_dir.parents[1] / "data"
    cov = Coverage()
    db = _connect(out)

    registry = build_dir / "registry.json"
    if not registry.exists():
        raise FileNotFoundError(f"{registry} not found — run the build first")
    _items(db, registry, cov)
    places = _places(db, data_dir / "bc_places.json",
                     data_dir / "bc_boundary.geojson", cov)
    _place_water(db, build_dir, places, cov)
    _gauges(db, build_dir, data_dir, cov)

    # Everything below needs a producer that does not exist yet, or exists but has not been
    # pointed at this. Named individually rather than left silently empty.
    cov.skip("entry", "curation entries are per-region JSON; needs pipeline.parsing.io")
    cov.skip("rule", "same as entry")
    cov.skip("rule_section", "needs pipeline.reach.covered over the full corpus")
    cov.skip("chart", "needs the bathymetry fetch (fetch_data: bathymetry_contours)")
    cov.skip("stock_water", "needs the FIDQ fetch — no waterbody roster on disk yet")
    cov.skip("stock_code", "needs the FIDQ fetch: the species/stage dictionaries come with it")
    cov.skip("release", "RETIRED — keyed on a name; stock_water + the stocking feed replace it")

    db.executescript(INDEXES.read_text())
    db.executemany("INSERT INTO meta VALUES (?,?)", [
        ("schema", SCHEMA.read_text().split("\n")[0]),
        ("build", str(build_dir)),
        ("generated_by", "python -m pipeline.bundle"),
    ])
    db.commit()
    db.execute("VACUUM")
    db.close()

    size = out.stat().st_size
    print(f"  ✅ {out}  {size / 1e6:.1f} MB")
    cov.report()
    return out
