/**
 * The shapes `RegsSource` answers in: the ids (an item is durable, a section is a handle), what a bundle holds, the
 * gauges' links, panels and routes, series and forecasts, and the search hits. Data only — the interface is source.ts.
 */
import type {
  Band, GaugeTrust, NearPlace, RecordYears, RunChange, SourceChange, Standing, WaterSignals,
} from "@app/core";

/** Durable across rebuilds — 99.88% stable. The only id that crosses an artifact boundary. */
export type ItemId = string & { readonly __brand: "ItemId" };
/**
 * A section of water — an INTEGER HANDLE, not a name.
 *
 * It is an index into the atlas's `section_handles.txt`, carried identically by the tile
 * (as the feature id state is set on) and by every section-keyed table in the bundle. It is
 * meaningless without that table and it changes whenever the atlas is rebuilt, so it must
 * never be persisted, put in a URL, or sent to a feed — which is AGENTS rule 5, now
 * structural rather than a rule to remember.
 *
 * The bundle's `meta.section_handles` and the tile's `atlas.meta.json` carry the table's
 * digest. A mismatch is not a lookup that misses; it is a lookup that HITS THE WRONG
 * SECTION, so the pair is refused rather than rendered.
 */
export type SectionId = number & { readonly __brand: "SectionId" };
export type StationId = string & { readonly __brand: "StationId" };
export type PlaceId = string & { readonly __brand: "PlaceId" };

export interface BundleInfo {
  /** Content-addressed. Tiles and bundle are pinned together; a mixed pair is refused. */
  version: string;
  /** Synopsis edition expiry. Past this the client degrades loudly, never silently. */
  validUntil: string | null;
  /**
   * The digest of the atlas's section handle table this bundle was built against.
   *
   * THE SENTENCE ABOVE USED TO BE AN ASPIRATION. "A mixed pair is refused" was written when
   * nothing compared anything, and a mixed pair shipped: the tiles were rebuilt with new
   * handles while `bundle.sqlite` was left behind, so every section the map reported was
   * looked up in a table that had renumbered. Nothing errored — the Conditions map simply
   * painted every river as unmeasured, which is a state the app draws on purpose.
   *
   * A section is an integer index into `section_handles.txt` (see SectionId), so a bundle
   * and a tile set from different atlases do not MISS each other — they agree on a number
   * that means two different rivers. `null` when the bundle predates this field, which is
   * itself a mismatch worth refusing.
   */
  sectionHandles: string | null;
  /** The bundle's `meta.reach_digest`: which rule bindings it carries; the status index must match. */
  reachDigest: string | null;
}

/**
 * How much is actually in this bundle.
 *
 * EXISTS BECAUSE THE APP MADE THESE UP. `App.tsx` passed `waters={255} reaches={6967}
 * surveyed={35} stations={5}` — figures transcribed from a design mock-up, whose
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
  /**
   * Which quantity the station shows and `percentile` is about, as the feed publisher declared it; null on a feed
   * file written before it did. Read through core's `shownParameter`, never inferred from which value is present.
   */
  parameter: Parameter | null;
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
  /** The reach the answer is for (the section `gaugeForSection` was asked about). */
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
  /**
   * Its record length, in years, PER QUANTITY (`discharge_years` / `level_years`, T110). A percentile from ten is not
   * one from ninety — and a level percentile is ranked against the level record, so read it with `recordYears` for the
   * quantity shown, never as one number.
   */
  years: RecordYears;
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
  /**
   * On the SAME BLUE LINE as the reach it speaks for.
   *
   * NOT DERIVABLE FROM THE AREAS, which is why it is stored. Two donors of identical
   * catchment size can be two entirely different relationships — the water flows past both
   * points, or they share a rain shadow — and it changes the weight fourfold. The Skeena
   * is the case: two gauges on the Skeena at the 77th and 78th percentile, weighted equally
   * with the Babine at the 24th, disagreeing past what the interval can express.
   */
  sameRiver: boolean;
  /**
   * The section's OWN gauge (matched to its water) and so may LEAD the answer: 1 the nearest, 2 the next; absent or
   * 0 when it may not — far along the water, much smaller than the section, or another water's gauge. Owner
   * 2026-10-22, "a water's own gauge comes first": decided by the pipeline (panel.py `may_lead`), applied by core's
   * `estimate` (`leaderOf`, `leadShares`). Optional so a panel written by hand without it means "nothing leads".
   */
  lead?: number;
}


/** `RegsSource.gaugeWaters`: a section's waters and each station's own water and reach. */
export interface GaugeWaters {
  waters: readonly ItemId[];
  gauges: ReadonlyMap<StationId, { item: ItemId | null; section: SectionId | null }>;
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
   * `true` only: the tide or a tidal river's backwater moves this station's level — the release's `gauge.tidal`
   * (T78: HYDAT's tidal remarks + data/curated/gauges/tidal.json). Absent otherwise, and on a release built before
   * the flag. Core's `tidalNote` says so in the Level view.
   */
  tidal?: true;
  /**
   * `true` only: a LAKE below this station backs water up onto it, standing — the release's `gauge.backwater` (T160:
   * data/curated/gauges/backwater.json, each row checked against the station's HYDAT remark; 08HB074, Comox Lake).
   * Absent otherwise, and on a release built before the flag. Core's `backwaterNote` says so in the Level view.
   */
  backwater?: true;
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
   * The station's expected change (T144, the feed's station-file `change`): CLEVER and COFFEE combined, one alone, or
   * ELF marked low-water — decided by the publisher. Absent on a feed from before T144 or when no fresh run can say.
   */
  change?: SourceChange | null;
  /**
   * The last complete years of the daily record, on this series' own day index — each the WHOLE year, not cut to
   * `values` (a chart cuts it to its frame; a spot's lead-up reads last December out of it in January, S1). Last
   * year's line also carries the feed's own December while it is January, where HYDAT has nothing yet.
   *
   * A band shows what is NORMAL and has no shape in time — it cannot show that last summer
   * was dry too, which is the question a person actually asks standing on a low river.
   */
  priorYears: readonly { year: number; values: readonly (number | null)[] }[];
  /**
   * A "year" series only: the envelope for EVERY day of the year (366), where `band` stops with `values`. The normal
   * range is the same each year, so a prior year's day is placed against it (a spot's lead-up into last December).
   */
  yearBand?: readonly (Band | null)[];
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
  /**
   * What this run expects over the next ten days, read day by day (T52, the feed's `forecasts[model].change`): its peak
   * and trough, each dated, spike or step. Absent on a run that cannot say (stale, no series, a zero today) and on a
   * feed from before T52. Flow only.
   */
  change?: RunChange | null;
}

export interface ForecastSeries {
  step: "1h" | "1d";
  at: readonly string[];
  /** The forecast trace and its published bounds, in the series' own quantity. */
  mid: readonly (number | null)[];
  lo: readonly (number | null)[];
  hi: readonly (number | null)[];
}

/**
 * One named water: what its sheet is titled with, and the reaches it covers.
 *
 * `sections` is every section of the water in the water's own order — blue line, then
 * distance up it — which is what a highlight draws and where a "conditions on this water"
 * door starts (the first one, nearest the mouth). It is NOT a list of stretches that
 * differ: that grouping was a regulation concept and left with the regulations.
 */
export interface Water {
  item: ItemId;
  name: string;
  kind: string;
  sections: readonly SectionId[];
}

export interface NameHit {
  item: ItemId;
  name: string;
  /** stream | lake | wetland — a lake and a river of one name are different answers. */
  kind: string;
  /** Set when the query matched an alias rather than the display name. */
  matchedAs: string | null;
  pieces: number;
  /**
   * How notable it is: the FWA stream magnitude of its biggest reach — how many headwaters
   * drain through it. Null when the bundle carries no figure (every lake).
   */
  size: number | null;
  /** How big a lake is, in hectares — its size in place of a magnitude. Null for a stream. */
  areaHa: number | null;
  /**
   * The town it is best described by — "near Fernie · 3 km". What tells two waters of one
   * name apart. Null when no named place is within 25 km of it.
   */
  near: NearPlace | null;
}

/**
 * A GROUP NAME THAT ONLY POINTS (owner 2026-10-22; the bundle's `see_pointer`): a book row whose only words are
 * "See …" ("TEE PEE LAKES  See Friday, Saturday, and Sunday Lakes"). No regulation, no water of its own to draw: the
 * waters it names, in the book's order, each under its own name with the water opening it opens (null: the named row
 * matched no water). Said by `seeLine` (@app/core).
 */
export interface PointerHit {
  /** The pointer row (`entry_id`): one hit per row. */
  entry: string;
  name: string;
  see: readonly { name: string; item: ItemId | null }[];
}

export interface PlaceHit {
  place: PlaceId;
  name: string;
  kind: string;
  lat: number;
  lon: number;
  /** Population, when the gazetteer has one. Null is "not recorded", not zero. */
  pop: number | null;
}

export interface NearHit {
  item: ItemId;
  name: string;
  kind: string;
  km: number;
  /**
   * How much water it is, as the bundle records it — what `rankNear` in @app/core weighs
   * against `km`. The source reports; the order is core's decision.
   */
  signals: WaterSignals;
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

/**
 * One gauge standing inside a watershed group, with what is needed to weight it.
 *
 * The field used to name ONE station per group — the biggest catchment — and that made the
 * group's colour hostage to it: 13 groups elected a station with no climatology (so no
 * percentile was ever possible), and any group whose one station happened to be quiet went
 * grey despite its other gauges reporting. Chilliwack has six.
 */
export interface BasinMember {
  station: StationId;
  /** Its catchment: how much of the group this gauge actually observes. */
  areaKm2: number | null;
  /** Years of record per quantity — how well its own baseline is known in Flow and in Level (`recordYears`, T110). */
  years: RecordYears;
}
