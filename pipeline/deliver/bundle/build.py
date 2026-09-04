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

from pipeline.atlas.gauges.build_panels import build as build_panels
from pipeline.atlas.gauges.panel import MIN_RECORD_YEARS
from pipeline.atlas.graph.drainage import AreaModel, fit_area_model
from pipeline.common.curated import GENERATED, REPO_ROOT, SOURCE
from pipeline.common.registry_kinds import is_water

HERE = Path(__file__).parent
_ROOT = REPO_ROOT
SCHEMA = HERE / "schema.sql"
INDEXES = HERE / "indexes.sql"

#: The envelope, written by `pipeline.gauges.feed.climatology` and read by BOTH the feed publisher
#: and this. Anchored on the repo root rather than derived from `data_dir`, which is how it
#: used to resolve to `output/output/feeds/...` whenever `data_dir` was defaulted.
CLIM_PATH = GENERATED.gauges.feeds / "clim.json"


def _area_model(gpkg: Path) -> AreaModel:
    """Magnitude -> catchment area, fitted per basin from the named watersheds.

    THE SAME 11,580 POLYGONS THE MAP ALREADY DRAWS, and the reason to fit against them
    rather than against the gauges is that they measure area directly instead of inferring
    it through a station's own catchment: 5.7 times the sample, and splittable by basin, so
    a wet coastal drainage and a dry plateau stop sharing one constant.
    """
    with sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True) as gp:
        pts = [(a * 0.01, m, w) for a, m, w in gp.execute(
            "SELECT AREA_HA, STREAM_MAGNITUDE, FWA_WATERSHED_CODE FROM watersheds "
            "WHERE AREA_HA > 0 AND STREAM_MAGNITUDE > 0 AND FWA_WATERSHED_CODE IS NOT NULL")]
    return fit_area_model(pts)


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


def _report(what: str, lost: list[str], live: set[str]) -> None:
    """Print every station that fell out of a stage, transmitting ones first.

    NOT a debug aid. A gauge that disappears between stages is a river the app tells people
    is unmeasured, and until this existed the only way to notice was to diff two bundles. A
    silent zero is the one thing this pipeline has repeatedly got wrong: `lake_gauge` went
    from 220 rows to 0 and the build reported success.
    """
    if not lost:
        return
    hot = [x for x in lost if x.split()[0] in live]
    print(f"     {len(lost)} stations {what}"
          + (f" — {len(hot)} of them transmitting:" if hot else ":"))
    for line in hot[:12]:
        print(f"       LIVE  {line}")
    rest = len(lost) - len(hot[:12])
    if rest > 0:
        print(f"       … and {rest:,} more (all in the run log)")




def _items(db: sqlite3.Connection, registry: Path, cov: Coverage) -> None:
    """item / alias / item_section, from the registry.

    The registry is the province-wide answer to "what is this water called and which
    sections is it". It is also the whole search index: 20,609 distinct strings, which is
    ~90 KB gzipped — small enough that no server-side search is needed anywhere.
    """
    items = [i for i in json.loads(registry.read_text())["items"] if is_water(i)]
    db.executemany("INSERT OR REPLACE INTO item VALUES (?,?,?)",
                   ((i["id"], i["name"], i.get("kind")) for i in items))
    cov.filled("item", len(items))

    # Normalised the way the TILE normalises its search haystack — same function, imported.
    # The registry carries both cases of many strings ("EAST WHITE RIVER" and "East White
    # River"); `pipeline/deliver/tiles/names.normalise` folds them, and this used to compare raw,
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
    # section -> (item_id, name), for WATERS that have a name a person could search.
    #
    # `_is_water` for the same reason as in `_gauges`: an `area:` item's name is a slug
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

    from pipeline.gauges import build_gauge_sheds, lake_gauge_links
    from pipeline.gauges.generate.match import nodes_for, read_match, summarise
    from pipeline.gauges.consume.shed import downstream_map, load_stations

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
                                  "`python -m pipeline.gauges.generate.match --build <build>`")
        cov.skip("section_down", "needs section_gauge")
        return

    with geom_path.open("rb") as fh:
        geoms = pickle.load(fh)
    # EVERY LOSS IS COUNTED. A station that matched and then failed to place, or placed and
    # then won no section, is a river the app will call ungauged — and both used to happen
    # in silence: 79 stations (7 of them active) fell out of `nodes_for` against a cap the
    # matcher did not share, and six active stations, the Fraser at Whonock and the
    # Chilliwack above Slesse among them, lost every section to a tiebreak on station id.
    # Neither showed in any output. See HANDOFF: "the build hides its own regressions".
    place_lost: list[str] = []
    shed_lost: list[str] = []
    matched = nodes_for(matches, graph, geoms, report=place_lost)
    del geoms
    prov = {m.station: m for m in matches}

    # section -> the WATER that owns it, so a client can name the river, not just the gauge.
    #
    # `_is_water` IS LOAD-BEARING HERE, not tidiness. The registry holds 1,473 `area:` items
    # carrying 783,492 section ids against 19,698 waters carrying 61,879 — and `area:` sorts
    # first, so `setdefault` handed every reach inside a park, reserve or indigenous land to
    # the polygon. A gauge measures water; it does not care whose land it stands on. Measured
    # before the filter: 542 of 2,018 `gauge.item_id` and 123 of 220 `lake_gauge` rows named
    # an `area:` id that `_items` never inserted, so the app looked them up and found nothing.
    owner: dict[str, str] = {}
    for i in json.loads((build_dir / "registry.json").read_text())["items"]:
        if not is_water(i):
            continue
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
    links = build_gauge_sheds(graph, stations, matched, prefer=active, report=shed_lost)
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
    # ONE PRODUCER, TWO CONSUMERS. `pipeline.gauges.feed.climatology` reads HYDAT and writes
    # clim.json; the 30-minute feed job turns today's discharge into a percentile with it,
    # and the bundle snapshots it here so a client can draw a seasonal band and date an old
    # spot offline. Building it twice would be two envelopes that can disagree about the
    # same river.
    clim_path = CLIM_PATH
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
                               "run pipeline.gauges.feed.climatology (needs HYDAT)")
        cov.skip("gauge_stats", "same file as gauge_clim")

    # THE DONOR PANELS, AFTER `gauge_stats` BECAUSE THEY READ IT. Built earlier, the record
    # lengths came back empty and every panel was silently skipped — the only symptom was a
    # "not wired" line that looked like a missing input rather than an ordering bug.
    #
    # `section_gauge` above is one station per reach; this is the SET that
    # can each say something, which is the same question asked of a province whose gauge
    # network is far sparser than its stream network.
    #
    # THE GATES NEED FACTS FROM TWO OTHER FILES, and a panel built without them is worse
    # than no panel: a regulated donor reports a dam's release schedule as if it were
    # rainfall, and a three-year record cannot carry a percentile at all. So this is skipped
    # rather than approximated when either is missing.
    hydat = Path(SOURCE) / "hydat.sqlite3"
    years_by_station = {r[0]: int(r[1] or 0) for r in db.execute(
        "SELECT station, max(years) FROM gauge_stats GROUP BY station")}
    regulated: set[str] = set()
    if hydat.exists():
        with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as hy:
            # ANY regulated period bars the station. `STN_REGULATION` carries year ranges,
            # so a gauge that was natural until a dam was built could in principle speak for
            # its own early record — but the percentile it publishes today is computed over
            # the whole record, so today it cannot.
            regulated = {r[0] for r in hy.execute(
                "SELECT DISTINCT STATION_NUMBER FROM STN_REGULATION WHERE REGULATED=1")}
    else:
        print("     panels: no HYDAT — cannot screen regulated donors, skipping")

    if not years_by_station or not hydat.exists():
        cov.skip("section_panel", "no gauge_stats — needs clim.json for record lengths")
        cov.skip("panel_member", "needs section_panel")
    else:
        model = _area_model(Path(SOURCE) / "bc_fisheries_data.gpkg")
        # REGULATED STATIONS ARE PASSED THROUGH, NOT FILTERED OUT HERE.
        #
        # The regulation flag travels with the donor and `panel.eligible` decides what it
        # means: barred from being carried onto other water, admitted for water that is all
        # but its own. Dropping them here made that decision unreachable — 137 of the 431
        # stations transmitting today are flagged regulated, the Skagit at the International
        # Boundary among them, and the rule that was supposed to let it speak for the Skagit
        # never ran because the station was gone two steps earlier.
        donors = [(st, matched[st], years_by_station.get(st, 0), st in regulated)
                  for st in matched
                  if years_by_station.get(st, 0) >= MIN_RECORD_YEARS]
        panels = build_panels(graph, model, donors)
        db.executemany("INSERT INTO section_panel VALUES (?,?,?)", panels.section_rows())
        db.executemany("INSERT INTO panel_member VALUES (?,?,?,?,?,?,?)",
                       panels.member_rows())
        cov.filled("section_panel", len(panels.by_section))
        cov.filled("panel_member", sum(len(v) for v in panels.members.values()))
        print(f"     panels: {len(panels.by_section):,} sections share "
              f"{len(panels.members):,} donor sets "
              f"({len(panels.by_section)/max(len(panels.members),1):.0f}x), "
              f"from {len(donors):,} eligible stations")


    down = downstream_map(graph, (l.section_id for l in links))
    db.executemany("INSERT INTO section_down VALUES (?,?)",
                   [(s, down[s]) for s in sorted(down)])
    cov.filled("section_down", len(down))

    bands: dict[str, int] = {}
    for l in links:
        bands[l.trust] = bands.get(l.trust, 0) + 1
    live = {s["station"] for s in stations if s.get("realtime")}
    _report("could not be placed on this graph", place_lost, live)
    _report("placed but spoke for no section", shed_lost, live)
    print(f"     match: {summarise(matches)}")
    print(f"     bands: " + ", ".join(f"{bands.get(b, 0):,} {b}" for b, _ in
                                      __import__("pipeline.gauges",
                                                 fromlist=["x"]).TRUST_BANDS))




def build(build_dir: Path, out: Path, *, data_dir: Path | None = None) -> Path:
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
    _items(db, registry, cov)
    places = _places(db, data_dir / "bc_places.json",
                     data_dir / "bc_boundary.geojson", cov)
    _place_water(db, build_dir, places, cov)
    _gauges(db, build_dir, data_dir, cov)

    # Everything below needs a producer that does not exist yet, or exists but has not been
    # pointed at this. Named individually rather than left silently empty.
    cov.skip("entry", "curation entries are per-region JSON; needs pipeline.regs.parsing.io")
    cov.skip("rule", "same as entry")
    cov.skip("rule_section", "needs pipeline.atlas.reach.covered over the full corpus")
    cov.skip("chart", "needs the bathymetry fetch (fetch_data: bathymetry_contours)")
    cov.skip("stock_water", "needs the FIDQ fetch — no waterbody roster on disk yet")
    cov.skip("stock_code", "needs the FIDQ fetch: the species/stage dictionaries come with it")
    cov.skip("release", "RETIRED — keyed on a name; stock_water + the stocking feed replace it")

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
    # opened — and `pipeline.tools.emit_gauge_policy --check` is what keeps THAT honest.
    from pipeline.gauges.consume.shed import TRUST_BANDS

    db.executemany("INSERT INTO meta VALUES (?,?)", [
        ("schema", SCHEMA.read_text().split("\n")[0]),
        ("build", str(build_dir)),
        ("generated_by", "python -m pipeline.deliver.bundle"),
        ("trust_bands", json.dumps({b: f for b, f in TRUST_BANDS})),
    ])
    db.commit()
    db.execute("VACUUM")
    db.close()

    size = out.stat().st_size
    print(f"  ✅ {out}  {size / 1e6:.1f} MB")
    cov.report()
    return out
