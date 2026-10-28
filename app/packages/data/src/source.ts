/**
 * THE ONE INTERFACE every platform's source implements (the release reader, the fixture). Every method exists because
 * a screen asks for it, and every one is in the conformance suite, which runs against every implementation.
 * Named for what it reads — waters, gauges, places — and not regulations, which the release loader reads (T62 3.7).
 */
import type { Bbox, WaterFix } from "@app/core";
import type {
  Aged, BasinMember, BundleCounts, BundleInfo, GaugeLink, GaugeWaters, ItemId, LakeInfo, NameHit, NearHit, Panel, Parameter, PanelRoute, PlaceHit, PlaceId, PointerHit, Reading, Release, SectionId, Series, StationId, Water,
} from "./types";

export interface WaterSource {
  info(): Promise<BundleInfo>;
  /** How much this bundle holds. Read, never asserted by a screen. */
  counts(): Promise<BundleCounts>;

  // ---- identity -------------------------------------------------------
  itemForSection(id: SectionId): Promise<ItemId | null>;
  /**
   * What water a reach is part of — its id, its name, and what kind of water it is.
   *
   * `itemForSection` above gives the id and leaves the caller to fetch the name. A screen
   * that wants a TITLE for a reach asks this — one indexed read — rather than `water()`,
   * which also lists every section of the item.
   */
  waterFor(id: SectionId): Promise<{ item: ItemId; name: string; kind: string } | null>;

  /** One water by its durable id, with its sections. Null when the bundle has no such item. */
  water(id: ItemId): Promise<Water | null>;

  // ---- search ---------------------------------------------------------
  searchNames(q: string, limit: number): Promise<readonly NameHit[]>;
  searchPlaces(q: string, limit: number): Promise<readonly PlaceHit[]>;
  /** The group names that only point (`PointerHit`) whose name is, or starts with, `q` — ordered by `rankPointers`. */
  searchPointers(q: string, limit: number): Promise<readonly PointerHit[]>;
  /**
   * Precomputed: named water within 25 km, NEAREST FIRST, each with its size signals. The
   * list a reader sees is re-ordered by `rankNear` (@app/core); the source stays nearest-
   * first so every implementation returns one checkable order.
   */
  watersNear(place: PlaceId): Promise<readonly NearHit[]>;
  /**
   * Where a water is, as far as the bundle can say: points ON it (a gauge on one of its
   * reaches, a stocking site) and the towns within 25 km with their distance to it.
   *
   * The bundle holds no geometry, so this is evidence rather than an outline —
   * `fixOf` in @app/core turns it into a camera box and the map refines that against the
   * tiles. Both lists empty is a real answer: remote water with no station and no town.
   */
  locate(item: ItemId): Promise<WaterFix>;
  /**
   * WHERE A WATER IS, FROM ITS OWN GEOMETRY: the union of its parts' boxes (`part_geo`), in degrees [w, s, e, n] —
   * `boxOfParts` in @app/core. A water split into closure areas (Kootenay, Williston and Shannon lakes own no section
   * of their own) is placed by its areas' parts (`item.part_of`). Null when the source holds no geometry for it (a
   * release that ships no water file); then `locate`'s evidence is all there is (`extentOf`).
   */
  waterBox(item: ItemId): Promise<Bbox | null>;

  // ---- conditions -----------------------------------------------------
  gaugeForSection(id: SectionId): Promise<GaugeLink | null>;
  /**
   * The donor panel for each of these sections.
   *
   * A section with no panel is ABSENT from the map rather than present with an empty
   * array: the two mean different things — "nothing qualified here" versus "this section
   * was outside the query" — and a caller that cannot tell them apart will render silence
   * as a loading state or the reverse.
   */
  panelsFor(sections: readonly SectionId[]): Promise<ReadonlyMap<SectionId, Panel>>;
  /**
   * Lakes among these sections that have a station IN them.
   *
   * SEPARATE FROM `panelsFor` BECAUSE A LAKE IS NOT A PANEL. A panel carries a reading from
   * one catchment to another; a lake's stage is set by its outlet and its own storage, so
   * nothing transfers to it and `build_panels` refuses lake nodes outright. A gauged lake
   * is coloured by its own reading and an ungauged one stays grey — there is no middle.
   */
  lakeStationsFor(
    sections: readonly SectionId[],
  ): Promise<ReadonlyMap<SectionId, StationId>>;
  /**
   * WHICH WATER EACH OF THESE STATIONS STANDS ON, and which waters this section is (T168): the facts core's
   * `gaugeSide` splits a panel into the gauges ON THIS WATER and the donors estimating from other waters.
   *
   * `waters` is the section's own water(s) (`item_section`) and every water joined to one across `water_continue`,
   * both ways and transitively — ONE SYSTEM, as `build_panels.own_distances` reads a water's own gauges (the Vedder and
   * the Chilliwack). `gauges` is each station's `gauge.item_id` and `gauge.sid` (null where the release has none:
   * 296 stations stand on no named water). A station the release does not hold is absent.
   */
  gaugeWaters(section: SectionId, stations: readonly StationId[]): Promise<GaugeWaters>;
  /**
   * Which station speaks for each catchment, and how far the reading travelled.
   *
   * The whole table, because it is 9,642 rows and the zoomed-out map needs most of them at
   * once — a viewport at z5 is half the province. Precomputed at build time: it needs
   * geometry the client does not ship, and it changes with the gauge network rather than
   * with the weather.
   *
   * `levelsUp` is 0 where a gauge stands in the catchment itself. Only 10% of them do, so
   * the number is not a footnote — it is what stops the field claiming more than it knows.
   */
  basinMembers(): Promise<ReadonlyMap<string, readonly BasinMember[]>>;
  /**
   * THE CONDITIONS MAP'S STATIC HALF, for the whole release: every section's standing CLASS (a donor panel at one
   * target area, or a lake's own station) and the basins' stations — what the map reads a section's value through as
   * its tile arrives, so a feed refresh or a horizon change asks one value per class, never one panel read per section
   * (`standing.ts`).
   * Read once per source; the same panels, lakes and basins `panelsFor`, `lakeStationsFor` and `basinMembers` answer.
   */
  standingClasses(): Promise<import("./standing").StandingClasses>;
  /**
   * THE LAKE MODES' VALUES, for the whole release (`lakes`, @app/core `decodeLakeFacts`): per lake section, whether it
   * was surveyed and when it was last stocked — what the map's Depth and Stocked modes colour by, through the colouring
   * engine. Null where the release ships none (an older release, a bundle, the fixture): the lakes then stay plain.
   */
  lakeFacts(): Promise<import("@app/core").LakeFacts | null>;
  /**
   * (T211) THE DFO SALMON, for the whole release (`salmon`, @app/core `decodeSalmon`): each section's chain of DFO
   * locations and each chain's answers per day — what the Salmon card and the Salmon colouring read. Null where the
   * release ships none (an older release, the fixture): the Salmon toggle is then not offered.
   */
  salmonFacts(): Promise<import("@app/core").SalmonFacts | null>;
  /**
   * (T213) BAIT BAN AND FLY FISHING ONLY per section and day (`method`, @app/core `decodeMethod`) — what the map's
   * "Bait & fly" colouring reads. Optional, and null where the release ships none (an older release, the fixture): the
   * choice is then not offered.
   */
  methodFacts?(): Promise<import("@app/core").MethodFacts | null>;
  /** Every station's position, for drawing the gauges themselves. A few hundred rows. */
  gaugePoints(): Promise<readonly { station: StationId; name: string;
                                    lon: number; lat: number;
                                    /** FWA stream magnitude at the station's own node,
                                     *  null when the node never got one. Drives the zoom
                                     *  the dot appears at. */
                                    mag: number | null }[]>;
  gaugeNow(station: StationId): Promise<Aged<Reading> | null>;
  /**
   * `parameter` omitted means "whatever this station actually measures" — which the client
   * cannot know and must not guess. Passing one explicitly is the toggle in the sheet.
   */
  /** `model` names one BC River Forecast Centre run; omitted, the freshest is chosen. */
  gaugeSeries(station: StationId, span: "72h" | "year",
              parameter?: Parameter, model?: string): Promise<Aged<Series> | null>;
  /** Which quantities this station has an envelope for, so a toggle can offer only those. */
  gaugeParameters(station: StationId): Promise<readonly Parameter[]>;
  /** Downstream from here to the station that measures it, via the build's pointers. */
  traceToGauge(from: SectionId): Promise<readonly SectionId[]>;
  /**
   * Every donor in this section's panel, placed on the map with the water between.
   *
   * `traceToGauge` above answers for the ONE matched station and only downstream. This
   * answers for the whole panel in both directions, which is what the estimate is actually
   * built from — a reader shown four contributing gauges and a map of one has been shown
   * the wrong model.
   *
   * Ordered as the panel stores it; the caller sorts by the weight it computes, so the map
   * and the table beneath it stay in the same order.
   */
  panelRoutes(section: SectionId): Promise<readonly PanelRoute[]>;

  // ---- lakes ----------------------------------------------------------
  // PREPARED, NOT FED (owner 2026-10-22, audit wave 1): no screen calls these yet and no release ships the data
  // (the release reader answers null / []). They are kept on purpose for the stocking and bathymetry feeds — do not
  // delete them as dead code (decisions.md "the audit plan"; T70).
  lakeInfo(id: ItemId): Promise<LakeInfo | null>;
  stockingHistory(id: ItemId): Promise<readonly Release[]>;

  // ---- regulations ----------------------------------------------------
  /**
   * The answer keys of a water's parts — the regulations read a water's page starts from (P11; T01/T03 decode the
   * frames). On the release this opens the files that hold them and nothing else: core, water, and the region file of
   * each key (`loader.regionDb`), so a water in another region fetches that region's file. Empty when the source holds
   * no regulations (the fixture does not; a release with no water file does not).
   */
  regulationKeys(id: ItemId): Promise<readonly number[]>;
}
