"""The bundle's GAUGE TABLES, written in one place (audit F04, plan 2.7).

    gauge · section_gauge · lake_gauge · gauge_clim · gauge_stats · section_panel · panel_member ·
    basin_member · section_down

`pipeline.deliver.bundle.build` calls `write()` while the atlas graph and geometries are loaded; nothing
here matches a station (the frozen `matches.json` is read through `pipeline.gauges.matches` and placed
by `pipeline.gauges.consume.place`) and nothing here opens the bundle file itself.
"""

from __future__ import annotations

import collections
import json
import sqlite3
from pathlib import Path

from pipeline.atlas.graph.drainage import AreaModel, fit_area_model
from pipeline.common.curated import GENERATED, SOURCE
from pipeline.common.registry_kinds import is_water
from pipeline.gauges.consume.panels import build as build_panels
from pipeline.gauges.feed.record import MIN_RECORD_YEARS, QUANTITIES, record_years

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


def write(db: sqlite3.Connection, build_dir: Path, data_dir: Path, registry_items: list[dict],
          graph, geoms, cov, *, kinds: dict, continues=()) -> None:
    """Which water has a gauge, and how well that gauge speaks for it.

    `kinds`: the registry as `reach.water_kind.kind_of` reads it (`bundle.build._kind_registry`).
    `continues`: the settled `water_continue` links as item ids (`continuation.item_pairs`) — a continuation's gauges
    are a section's own in the donor panels (T76).

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
    if not stations_path.exists():              # P2: no missing-file fallbacks
        raise SystemExit(f"section_gauge: no {stations_path.name} "
                         "(data/fetch_data.py --layers hydrometric_stations)")
    if graph is None:
        raise SystemExit("section_gauge: no graph.pkl in this build")

    from pipeline.gauges import build_gauge_sheds, lake_gauge_links
    from pipeline.gauges.consume.place import nodes_for, summarise
    from pipeline.gauges.matches import check_record, read_match
    from pipeline.gauges.consume.shed import downstream_map, load_stations
    from pipeline.gauges.liveness import may_speak, speaks_now

    from pipeline.common.section_handles import read as _read_handles

    # Every section this function writes is named by its handle. Loaded once (the loader
    # memoises) and shared by gauge, section_gauge, section_panel and section_down, so those
    # four cannot disagree about what a section is called.
    _, sid = _read_handles(build_dir)

    stations = load_stations(stations_path)

    # THE FROZEN MATCH — `pipeline/gauge_match.json`, the same file the build read to cut
    # rivers at their gauges. Nothing is matched here: the file says where each station is
    # and which water it is on, and `nodes_for` projects that coordinate onto THIS graph,
    # taking the section that begins at the gauge and runs upstream. Node ids move when the
    # sectionizer cuts differently; a published coordinate does not.
    # THE SAME VERSION THE ATLAS CUT AT (N8): refused when the file changed since the atlas build
    check_record(build_dir)
    matches = read_match()                  # a missing file raises (P2: no missing-file fallbacks)
    if not matches:
        from pipeline.gauges.matches import REGENERATE
        raise SystemExit("section_gauge: the gauge match file holds no stations — restore it "
                         f"(git checkout) or regenerate it: `{REGENERATE}`")

    if geoms is None:
        raise SystemExit("section_gauge: no geometries.pkl in this build — stations cannot be "
                         "placed on the graph")
    # EVERY LOSS IS COUNTED. A station that matched and then failed to place, or placed and
    # then won no section, is a river the app will call ungauged — and both used to happen
    # in silence: 79 stations (7 of them active) fell out of `nodes_for` against a cap the
    # matcher did not share, and six active stations, the Fraser at Whonock and the
    # Chilliwack above Slesse among them, lost every section to a tiebreak on station id.
    # Neither showed in any output. See HANDOFF: "the build hides its own regressions".
    place_lost: list[str] = []
    shed_lost: list[str] = []
    matched = nodes_for(matches, graph, geoms, report=place_lost)
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
    for i in registry_items:
        if not is_water(i):
            continue
        for sec in i.get("section_ids", []):
            owner.setdefault(sec, i["id"])

    # TIDE AND BACKWATER (T78, owner 2026-10-22: "check and disclose tidal or backwater
    # influence where the record shows it"). HYDAT's tidal remarks + the authored list; a
    # missing HYDAT or list raises (P2) — a flag silently all-0 would say the Fraser at
    # Mission is an ordinary river gauge.
    from pipeline.gauges.consume.tidal import backwater_stations, tidal_stations
    tidal = tidal_stations(Path(SOURCE) / "hydat.sqlite3")   # the authored list: TIDAL_FILE
    # A LAKE'S STANDING BACKWATER (T160): the authored rows, each checked against the station's own HYDAT remark.
    backwater = backwater_stations(Path(SOURCE) / "hydat.sqlite3")   # the authored list: BACKWATER_FILE

    # No liveness column of any kind -- see schema.sql. The bundle says a gauge exists and
    # where it is; the feed index says which are talking, by containing them.
    db.executemany("INSERT INTO gauge (station, name, item_id, sid, lon, lat, area_km2, mag, "
                   "matched_by, match_m, tidal, backwater) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        (s["station"], s["name"],
         owner.get(matched.get(s["station"], "")),
         sid.get(matched[s["station"]]) if s["station"] in matched else None,
         s["lon"], s["lat"], s.get("area_km2"),
         (graph.nodes[matched[s["station"]]].stream_magnitude
          if s["station"] in matched else None),
         prov[s["station"]].resolved_by if s["station"] in prov else None,
         prov[s["station"]].distance_m if s["station"] in prov else None,
         1 if s["station"] in tidal else 0,
         1 if s["station"] in backwater else 0)
        for s in stations])
    cov.filled("gauge", len(stations))

    # ECCC's own ACTIVE status, not today's traffic (`liveness.may_speak`, T122). 27 BC stations are active but silent
    # right now — seasonal gauges shut for the winter — and filtering on traffic would
    # delete their sheds every autumn and rebuild them every spring. Retired stations keep
    # their `gauge` row and climatology; what they lose is the claim to speak for reaches
    # they can no longer say anything about.
    active = {s["station"] for s in stations if may_speak(s)}
    # THE WATER KIND (T121): a station is a lake's by the registry's kind of its water, never by the
    # node's shape (`shed.is_lake_station` -> `reach.water_kind.kind_of`)
    links = build_gauge_sheds(graph, stations, matched, prefer=active, report=shed_lost,
                              registry=kinds)
    # `links` arrives grouped by section, best ratio first; `seq` freezes that order so a
    # client takes row 0 and never has to re-derive the judgement.
    db.executemany("INSERT INTO section_gauge (sid, station, trust, mag) VALUES (?,?,?,?)",
                   [(sid[l.section_id], l.station, l.trust,
                     graph.nodes[l.section_id].stream_magnitude) for l in links])
    cov.filled("section_gauge", len(links))

    # Lake stations, linked to the lake's registry item rather than to a reach.
    lakes = [(owner[node], st) for node, st in lake_gauge_links(graph, stations, matched, kinds)
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
        # ONE BLOB PER (station, parameter) — the packing is described in schema.sql. Built
        # here rather than in a helper because this is the only writer; the only READER is
        # the app, and `pipeline/tests/test_bundle_clim.py` pins the two to the same layout.
        import struct

        rows, n_pentads = [], 0
        for st, by_param in sorted(clim.get("stations", {}).items()):
            for param, by_pent in sorted(by_param.items()):
                buf = bytearray()
                for pent, bands in sorted(by_pent.items(), key=lambda kv: int(kv[0])):
                    pe = int(pent)
                    if not 0 <= pe < 73:      # the axis is 73 pentads; anything else is a bug
                        raise SystemExit(f"gauge_clim: pentad {pe} out of range for {st}/{param}")
                    buf.append(pe)
                    buf += struct.pack("<5f", *(float(b) for b in bands))
                    n_pentads += 1
                if buf:
                    rows.append((st, param, bytes(buf)))
        db.executemany("INSERT INTO gauge_clim (station, parameter, bands) VALUES (?,?,?)",
                       rows)
        cov.filled("gauge_clim", len(rows))
        print(f"     clim: {n_pentads:,} pentads packed into {len(rows):,} envelopes")

        srows = [(st, param, s_.get("from_year"), s_.get("to_year"), s_.get("years"),
                  s_.get("days"))
                 for st, by_param in sorted(clim.get("stats", {}).items())
                 for param, s_ in sorted(by_param.items())]
        db.executemany("INSERT INTO gauge_stats VALUES (?,?,?,?,?,?)", srows)
        cov.filled("gauge_stats", len(srows))
        print(f"     envelope: HYDAT {clim.get('release')}")
    else:                                       # P2: no missing-file fallbacks
        raise SystemExit(f"gauge_clim: no {clim_path} — "
                         "run pipeline.gauges.feed.climatology (needs HYDAT)")

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
    # THE RECORD PER QUANTITY (T110): `record_years(stats, station, quantity)` over clim.json's `stats` — the same rows
    # `gauge_stats` was just written from. It was `max(years)` across quantities here, so 07ED001 (59 years of
    # discharge, 4 of level) weighed its level reading as a 59-year record.
    rec_stats = clim.get("stats", {})
    years_by_station = {st: {q: record_years(rec_stats, st, q) for q in QUANTITIES} for st in sorted(rec_stats)}
    regulated: set[str] = set()
    if hydat.exists():
        with sqlite3.connect(f"file:{hydat}?mode=ro", uri=True) as hy:
            # ANY regulated period bars the station. `STN_REGULATION` carries year ranges,
            # so a gauge that was natural until a dam was built could in principle speak for
            # its own early record — but the percentile it publishes today is computed over
            # the whole record, so today it cannot.
            regulated = {r[0] for r in hy.execute(
                "SELECT DISTINCT STATION_NUMBER FROM STN_REGULATION WHERE REGULATED=1")}
    else:                                       # P2: no missing-file fallbacks
        raise SystemExit(f"section_panel: no {hydat} — cannot screen regulated donors "
                         "(python data/fetch_data.py --layers hydat)")

    if not years_by_station:
        raise SystemExit("section_panel: gauge_stats is empty — clim.json carries no record "
                         "lengths")
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
        donors = [(st, matched[st], years_by_station[st], st in regulated)
                  for st in matched
                  if any(y >= MIN_RECORD_YEARS for y in years_by_station.get(st, {}).values())]
        # `owner` (section -> water) is what makes a donor a section's OWN gauge, which then
        # leads its answer — owner 2026-10-22, "a water's own gauge comes first" (panel.py).
        # NO `realtime` FLAG (owner 2026-10-22, "chosen live"): every donor with a record is a
        # member or lead candidate, ranked as ever; which of them answers is the feed's business,
        # at read time. The roster's flag is a snapshot that drifts — see build_panels.build.
        measures: dict[str, set[str]] = collections.defaultdict(set)
        for st, param in db.execute("SELECT station, parameter FROM gauge_stats"):
            measures[st].add(param)
        # `kind_of`: a STREAM water's polygons (sloughs, canals — owner 2026-10-22 late) are walked like its lines,
        # and one left unanswered takes its line's answer; the water's kind decides, never the polygon's geometry. See
        # build_panels.flowing and build_panels.stream_polygons.
        # `continues`: across a `water_continue` link the gauges are the section's own (T76: the Vedder is led by the
        # Chilliwack's 08MH001) — one system, the same gates. See build_panels.own_distances.
        panels = build_panels(graph, model, donors, water_of=owner, continues=continues,
                              kind_of={i["id"]: i.get("kind") for i in registry_items if is_water(i)},
                              measures=measures,
                              published={s["station"]: s["area_km2"] for s in stations
                                         if s.get("area_km2")})
        db.executemany("INSERT INTO section_panel (sid, panel_id, area_km2) VALUES (?,?,?)",
                       [(sid[_s], _p, _a) for _s, _p, _a in panels.section_rows()])
        db.executemany("INSERT INTO panel_member VALUES (?,?,?,?,?,?,?,?,?,?)",
                       panels.member_rows())
        cov.filled("section_panel", len(panels.by_section))
        cov.filled("panel_member", sum(len(v) for v in panels.members.values()))
        print(f"     panels: {len(panels.by_section):,} sections share "
              f"{len(panels.members):,} donor sets "
              f"({len(panels.by_section)/max(len(panels.members),1):.0f}x), "
              f"from {len(donors):,} eligible stations")


    # ---- the zoomed-out field: which station speaks for each catchment ---------------
    #
    # PRECOMPUTED HERE and not on the client, for the same reason the panels are: it needs
    # geometry the client does not ship, it changes with the gauge network rather than with
    # the weather, and doing it on every pan would be a point-in-polygon sweep of 11,000
    # shapes. The tile carries a bare `basin_id`; this is what turns it into a reading.
    try:
        from pipeline.gauges.consume.basins import gauged_groups, roster
        from pipeline.deliver.tiles.basins import groups as _groups

        _frame = _groups(str(Path(SOURCE) / "bc_fisheries_data.gpkg"))
        # EVERY station with a coordinate, live or not. Which of them is transmitting is the
        # feed's business and changes every half hour; this file must not encode it.
        #
        # A BASELINE IS NOT LIVENESS, though, and this conflated them. The field colours a
        # group by where its reading sits against that station's own history, so a station
        # with no climatology cannot produce a percentile TODAY OR EVER. Unlike liveness,
        # that is static, known here, and disqualifying — so it is filtered here, while
        # which gauges are transmitting stays the feed's business.
        _pts = [(r[0], r[1], r[2], r[3]) for r in db.execute(
            "SELECT g.station, g.lon, g.lat, g.area_km2 FROM gauge g "
            "WHERE g.lon IS NOT NULL AND g.lat IS NOT NULL "
            "  AND EXISTS (SELECT 1 FROM gauge_clim c WHERE c.station = g.station)")]
        _basins = roster(gauged_groups(_pts, _frame), rec_stats)
        if _basins:
            db.executemany("INSERT INTO basin_member VALUES (?,?,?,?,?)",
                           ((b, *row) for b, rows in sorted(_basins.items()) for row in rows))
            cov.filled("basin_member",
                       sum(len(rows) for rows in _basins.values()))
            print(f"     basins: {len(_basins):,} of {len(_frame):,} watershed groups have "
                  f"a gauge in them "
                  f"({100*len(_basins)/max(len(_frame),1):.0f}% of the province)")
        else:
            raise SystemExit("basin_member: no watershed groups resolved to a station")
    except Exception as _exc:                                   # noqa: BLE001
        # P2 (no missing-file fallbacks): a missing groups layer used to be a skip, and the
        # bundle built "fine" with no basins. It stops the build now, naming the cause.
        raise SystemExit(f"basin_member: not built: {_exc}") from _exc

    down = downstream_map(graph, (l.section_id for l in links))
    db.executemany("INSERT INTO section_down (sid, down_sid) VALUES (?,?)",
                   [(sid[s], sid[down[s]]) for s in sorted(down)])
    cov.filled("section_down", len(down))

    bands: dict[str, int] = {}
    for l in links:
        bands[l.trust] = bands.get(l.trust, 0) + 1
    live = {s["station"] for s in stations if speaks_now(s)}
    _report("could not be placed on this graph", place_lost, live)
    _report("placed but spoke for no section", shed_lost, live)
    print(f"     match: {summarise(matches)}")
    print(f"     bands: " + ", ".join(f"{bands.get(b, 0):,} {b}" for b, _ in
                                      __import__("pipeline.gauges",
                                                 fromlist=["x"]).TRUST_BANDS))
