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

export interface Series {
  step: "1h" | "1d";
  from: string;
  discharge: readonly (number | null)[];
  /** Percentile envelope for the same span, every 5 days. */
  band: readonly (Band | null)[];
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

  // ---- identity -------------------------------------------------------
  itemExists(id: ItemId): Promise<boolean>;
  itemForSection(id: SectionId): Promise<ItemId | null>;

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
  /** Every station's position, for drawing the gauges themselves. A few hundred rows. */
  gaugePoints(): Promise<readonly { station: StationId; name: string;
                                    lon: number; lat: number }[]>;
  gaugeNow(station: StationId): Promise<Aged<Reading> | null>;
  gaugeSeries(station: StationId, span: "72h" | "year"): Promise<Aged<Series> | null>;
  /** Downstream from here to the station that measures it, via the build's pointers. */
  traceToGauge(from: SectionId): Promise<readonly SectionId[]>;

  // ---- lakes ----------------------------------------------------------
  lakeInfo(id: ItemId): Promise<LakeInfo | null>;
  stockingHistory(id: ItemId): Promise<readonly Release[]>;
}

/** User pins — a separate store with a separate lifecycle. See pins.ts. */
export type { Pin, PinPhoto, PinStore } from "./pins";

export { httpFeed, type GaugeFeed } from "./feed/http";
