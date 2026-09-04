/**
 * @app/data — ONE interface, one implementation per platform.
 *
 * The shapes below are no longer placeholders. Every method exists because a screen asks
 * for it (the query census in "One Interface, Two Storages" §1), and every one is in the
 * conformance suite, which runs against EVERY implementation. That suite is what stops
 * the phone and the browser answering the same question two ways — the v1 failure where
 * `waterbodyDataService.ts` was 1,157 lines on web and 116 on mobile.
 *
 * Two shapes carry the design:
 *
 *   regsForItem  returns a whole sheet in ONE call. Six queries is six page faults on a
 *                phone and six round trips on the web.
 *   statusFor    takes an ARRAY, because the map colours a viewport, not a feature.
 */
import type {
  Band, GaugeTrust, PlainDate, Rule, SpeciesGroup, Standing, Status,
} from "@app/core";

/** Durable across rebuilds — 99.88% stable. The only id that crosses an artifact boundary. */
export type ItemId = string & { readonly __brand: "ItemId" };
/** A section of water. Valid only against the tile set it shipped with. */
export type SectionId = string & { readonly __brand: "SectionId" };
export type StationId = string & { readonly __brand: "StationId" };
export type PlaceId = string & { readonly __brand: "PlaceId" };

export interface BundleInfo {
  /** Content-addressed. Tiles and bundle are pinned together; a mixed pair is refused. */
  version: string;
  /** Synopsis edition expiry. Past this the client degrades loudly, never silently. */
  validUntil: string | null;
}

/**
 * How much is actually in this bundle.
 *
 * EXISTS BECAUSE THE APP MADE THESE UP. `App.tsx` passed `waters={255} reaches={6967}
 * surveyed={35} stations={5}` — figures transcribed from `design/riffle.html`, whose
 * fixture is one valley. The shipped province bundle holds 19,699 named waters and 2,324
 * stations, and the Layers sheet reported five of them under a heading that reads
 * "EVERY VALUE HAS AN AGE".
 *
 * A count a screen states about the data must be READ FROM the data. Anything a source
 * cannot answer is `null`, which renders as an omission — never as a zero, and never as a
 * number borrowed from somewhere else.
 */
export interface BundleCounts {
  /** Named waters — what search is searching. */
  waters: number;
  /** Reaches: rows in `item_section`. What the map draws. */
  reaches: number;
  /** Lakes with a bathymetry survey sheet. `null` until the sheet matcher lands. */
  surveyed: number | null;
  /** Hydrometric stations the bundle knows a position for. */
  stations: number;
}

/** Every live value carries its age. There is no way to read one without it. */
export interface Aged<T> {
  value: T;
  fetchedAt: number | null;
}

export interface Reading {
  discharge: number | null;
  level: number | null;
  at: string;
  /** Where this sits in the station's own record for this day of year. */
  percentile: number | null;
  standing: Standing;
}

export interface GaugeLink {
  station: StationId;
  name: string;
  /** How much of the gauge's watershed this reach is — below `none` we claim nothing. */
  trust: GaugeTrust;
  /** FWA magnitudes the trust was computed from, so the sheet can show its working. */
  reachMagnitude: number;
  gaugeMagnitude: number;
  /**
   * Whether the station is transmitting — from the FEED, never from the bundle.
   *
   * Three states, and the third is the one that matters. `true` is talking, `false` is a
   * station that exists but has stopped, and **`null` means we could not check** — offline,
   * or no feed wired. A river gauged since 1913 whose station closed in 2004 still HAS a
   * gauge; it anchors a climatology. But `null` must never render as `false`, or an
   * offline reader is told every gauge in the province has shut down.
   */
  live: boolean | null;
  /** Gross drainage area at the station, km2. ECCC's own figure; null when unpublished. */
  areaKm2: number | null;
  /** Where the station is, for the map. Null when ECCC published no coordinate. */
  lon: number | null;
  lat: number | null;
  /** The reach the answer is FOR — not always the one asked about, for `gaugeForItem`. */
  section: SectionId | null;
}

/**
 * One donor's own facts, exactly as `panel_member` stores them.
 *
 * NOTHING DERIVED IS STORED — not the weight, not the ratio, not a trust band. Each of
 * those is a calibration, and freezing a calibration into the bundle means re-measuring it
 * needs a rebuild rather than a release. The client has both catchments and derives all
 * three, which is also what keeps the number on screen and the number the pipeline gated
 * on from ever being two different numbers.
 *
 * It is also what makes the dictionary small: a weight depends on the TARGET as well as
 * the donor, so baking weights in gave adjacent reaches on one river different panels —
 * 16,127 of them where the donor sets collapse to 1,898.
 */
export interface PanelMember {
  station: StationId;
  /** Where the donor sits relative to the tapped point. */
  role: "up" | "down";
  /** The DONOR's catchment, km². Half of every ratio; the other half is on the section. */
  areaKm2: number;
  /** Its record length, in years. A percentile from ten is not one from ninety. */
  years: number;
  /**
   * A dam governs this station's water.
   *
   * NOT A WEIGHT — a change of MEANING. The reading is a percentile of somebody's dispatch
   * decision rather than of the weather, and such a donor is admitted only for water that
   * is all but its own, where that schedule is what this water is doing. The screen has to
   * say so: presenting a release schedule as a description of rainfall is the one thing
   * this whole panel exists to avoid.
   */
  regulated: boolean;
}

/**
 * The panel for one section: who may speak for it, and the section's own catchment.
 *
 * `areaKm2` IS HERE AND NOT ON THE MEMBERS because it belongs to the target, and it is what
 * lets a shared panel still produce exact per-section weights. It is the value at the
 * section's OUTLET; a tap partway up is refined by the drainage staircase.
 *
 * MEMBERS ARE IN NO PARTICULAR ORDER. They cannot be: the order depends on weights, and
 * weights depend on the target, so two sections sharing a panel can legitimately rank the
 * same donors differently. Sort by the weight you compute — which is also the order to
 * display, so the number and the table beneath it come from one calculation.
 */
export interface Panel {
  /** The target's own catchment, km². Null where the graph had no magnitude for it. */
  areaKm2: number | null;
  members: readonly PanelMember[];
}

/**
 * One donor, placed: where it stands and how the water gets from here to it.
 *
 * SEPARATE FROM `PanelMember` ON PURPOSE. A member is interned in the panel dictionary and
 * shared by every section with the same donor set, so it can hold nothing that varies with
 * the target — and a route is nothing but that. This is resolved per section, on demand,
 * only for the one reach a reader has opened.
 */
export interface PanelRoute {
  station: StationId;
  name: string | null;
  /** ECCC's own coordinate. Null where they published none — pin nothing rather than guess. */
  lon: number | null;
  lat: number | null;
  role: "up" | "down";
  /**
   * The chain of reaches between the spot and this gauge, SPOT FIRST, gauge last.
   *
   * Empty when the chain cannot be walked — the pointers only exist inside a gauge's
   * watershed, and a donor reached through country outside every shed has a real
   * relationship this table cannot draw. An empty path means "not drawable", never "not
   * related": the donor is still in the panel and still carries its weight.
   */
  path: readonly SectionId[];
}

/** Which quantity a chart is about. Never mixed — see `Series.parameter`. */
export type Parameter = "discharge" | "level";

/**
 * What the river did, over one span, in ONE quantity.
 *
 * `parameter` IS PART OF THE ANSWER, not a formatting hint. 237 BC stations measure stage
 * and never discharge, and a percentile computed from a level against a discharge envelope
 * is arithmetic across two different units — a number that looks entirely reasonable and
 * means nothing. So a series carries which quantity it is, and the envelope that came with
 * it was built from that same quantity.
 */
export interface Series {
  step: "1h" | "1d" | "5d";
  from: string;
  parameter: Parameter;
  /** ISO timestamp per sample, so an axis can be labelled with real dates. */
  at: readonly string[];
  /** Observations. A null is a gap in the record, never a zero. */
  values: readonly (number | null)[];
  /** Percentile envelope aligned to `values`, from the bundle's climatology. */
  band: readonly (Band | null)[];
  /**
   * Where the live reading sits on this axis.
   *
   * Explicit rather than "the last value", because the seasonal chart has NO observations
   * of its own — it is a year of envelope with today's reading marked on it — and taking
   * the last element there would put the dot on New Year's Eve.
   */
  now: { index: number; value: number } | null;
  /** The chosen model run continuing past today, or null outside every model's season. */
  forecast: Forecast | null;
  /** Every model running for this station, so a reader can pick. Empty out of season. */
  forecasts: readonly Forecast[];
  /**
   * The last complete years of the daily record, on this series' own day index.
   *
   * A band shows what is NORMAL and has no shape in time — it cannot show that last summer
   * was dry too, which is the question a person actually asks standing on a low river.
   */
  priorYears: readonly { year: number; values: readonly (number | null)[] }[];
}

/**
 * A BC River Forecast Centre run. A PREDICTION, and never rendered as an observation.
 *
 * `extreme` says which end of the range the headline number is, and it is not cosmetic: a
 * freshet model is asked how HIGH and a low-flow model how LOW, so showing an average would
 * smooth away the question each was run to answer.
 */
export interface Forecast {
  model: string;
  issuedAt: string | null;
  horizonDays: number;
  /** The headline number, and which end of the range it is. */
  value: number;
  extreme: "min" | "ave" | "max";
  min: number | null;
  ave: number | null;
  max: number | null;
  unit: string;
  /**
   * EVERY STEP THE MODEL PUBLISHED, not one number.
   *
   * The summary layer carries a single forecast value per station; drawn on a chart that
   * is one point, and one point joined to today's reading is a triangle — which is exactly
   * what it looked like, because that is all it was. This is the model's own output:
   * hourly for ten days (CLEVER) or daily for thirty (ELF), each step with bounds.
   *
   * Null when the per-station file has not been fetched for this run yet. The headline
   * number above still works; there is simply no ribbon to draw.
   */
  series: ForecastSeries | null;
  /** The Centre's own words out of the CSV header. Shown verbatim beside the chart. */
  disclaimer: string | null;
}

export interface ForecastSeries {
  step: "1h" | "1d";
  at: readonly string[];
  /** The forecast trace and its published bounds, in the series' own quantity. */
  mid: readonly (number | null)[];
  lo: readonly (number | null)[];
  hi: readonly (number | null)[];
}

/** One water's whole sheet. Assembled by the source, never by the client. */
export interface ItemRegs {
  item: ItemId;
  name: string;
  /** Ordered mouth -> source, so the sheet can draw a river's stretches in order. */
  reaches: readonly {
    section: SectionId;
    seq: number;
    /** The landmarks that bound this stretch, e.g. "Vedder Crossing Bridge". */
    lowerLabel: string | null;
    upperLabel: string | null;
    status: Status;
  }[];
  /** Tier 1: written for this water. */
  rules: readonly Rule[];
  /** Tiers 2 and 3: the zone and province-wide rules that also apply here. */
  area: readonly AreaRule[];
  /** The synopsis paragraph, and where inside it the first rule was parsed from. */
  verbatim: { text: string; clauseStart: number; clauseLength: number } | null;
  /** Named in a rule we could not place. Shown, never applied. */
  unplaceable: readonly { rule: Rule; detail: string }[];
}

export interface AreaRule {
  rule: Rule;
  /** What the reader is told this applies to: "Everywhere in MU 2-2". */
  scopeLabel: string;
}

export interface NameHit {
  item: ItemId;
  name: string;
  /** Set when the query matched an alias rather than the display name. */
  matchedAs: string | null;
  pieces: number;
}

export interface PlaceHit {
  place: PlaceId;
  name: string;
  kind: string;
}

export interface NearHit {
  item: ItemId;
  name: string;
  km: number;
}

export interface LakeInfo {
  item: ItemId;
  /** Depth charts. `digitised` draws offline; `scan` is a PDF you choose to download. */
  charts: readonly {
    id: string;
    title: string;
    kind: "digitised" | "scan";
    drafted: string | null;
    scale: number | null;
    bytes: number | null;
  }[];
  lastStocked: { date: string; species: string; count: number | null } | null;
}

export interface Release {
  date: string;
  species: string;
  count: number | null;
  stage: string | null;
}

export interface RegsSource {
  info(): Promise<BundleInfo>;
  /** How much this bundle holds. Read, never asserted by a screen. */
  counts(): Promise<BundleCounts>;

  // ---- identity -------------------------------------------------------
  itemExists(id: ItemId): Promise<boolean>;
  itemForSection(id: SectionId): Promise<ItemId | null>;
  /**
   * What water a reach is part of — its id, its name, and what kind of water it is.
   *
   * `itemForSection` above gives the id and leaves the caller to fetch the name, and the
   * only thing that fetched a name was `regsForItem`, which also reads every section and
   * every rule for the item. A screen that wants a TITLE was therefore either loading the
   * whole regulation sheet or going without — and the Conditions screen went without, so a
   * reader could open a chart with nothing on screen saying which river it was.
   */
  waterFor(id: SectionId): Promise<{ item: ItemId; name: string; kind: string } | null>;

  // ---- regulations ----------------------------------------------------
  regsForItem(id: ItemId, on: PlainDate, group: SpeciesGroup): Promise<ItemRegs | null>;
  /** Map colouring. `group` is required: there is no blended "overall" answer. */
  statusFor(
    ids: readonly SectionId[], on: PlainDate, group: SpeciesGroup,
  ): Promise<ReadonlyMap<SectionId, Status>>;

  // ---- search ---------------------------------------------------------
  searchNames(q: string, limit: number): Promise<readonly NameHit[]>;
  searchPlaces(q: string, limit: number): Promise<readonly PlaceHit[]>;
  /** Precomputed: named water within 25 km, nearest first. */
  watersNear(place: PlaceId): Promise<readonly NearHit[]>;

  // ---- conditions -----------------------------------------------------
  gaugeForSection(id: SectionId): Promise<GaugeLink | null>;
  /**
   * The best gauge anywhere on this water, or null if none speaks for any of it.
   *
   * Answers "does this river have a gauge?" — which `gaugeForSection` cannot, because a
   * water's reaches disagree. Null here is a real answer and must be shown as one.
   */
  gaugeForItem(id: ItemId): Promise<GaugeLink | null>;
  /**
   * Which station speaks for each of these reaches.
   *
   * Bulk and viewport-scoped, for colouring the map. The whole table is 558,746 rows —
   * holding it client-side to answer a question about the ~300 reaches on screen would be
   * most of the bundle in memory.
   */
  stationsFor(sections: readonly SectionId[]): Promise<ReadonlyMap<SectionId, StationId>>;
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
  basinStations(): Promise<ReadonlyMap<string, { station: StationId; levelsUp: number }>>;
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
  lakeInfo(id: ItemId): Promise<LakeInfo | null>;
  stockingHistory(id: ItemId): Promise<readonly Release[]>;
}

/** User pins — a separate store with a separate lifecycle. See pins.ts. */
export type { Pin, PinPhoto, PinStore } from "./pins";

export { httpFeed, type GaugeFeed } from "./feed/http";
