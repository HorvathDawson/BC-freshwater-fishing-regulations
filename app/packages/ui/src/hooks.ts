/**
 * The app's questions, as hooks. Data out, never elements — which is what lets the phone
 * and the desktop render the same answers with completely different components.
 */
import { useMemo } from "react";
import type { GaugeTrace, PlainDate, SpeciesGroup, Status } from "@app/core";
import { statusWord } from "@app/core";
import type {
  GaugeLink, ItemId, ItemRegs, LakeInfo, NameHit, Parameter, PlaceHit, PlaceId, RegsSource,
  SectionId, StationId,
} from "@app/data";
import { useAsync, useDebounced, type Async } from "./async";

/** A date as a stable string, for query identity. */
const day = (d: PlainDate): string => `${d.year}-${d.month}-${d.day}`;
import { buildHydrograph, type Hydrograph } from "./hydrograph";
import { gaugeGeoJSON } from "./gaugePoints";

/** Map colouring for a viewport's worth of features. */
export function useStatuses(
  source: RegsSource,
  sections: readonly SectionId[],
  on: PlainDate,
  group: SpeciesGroup,
): Async<ReadonlyMap<SectionId, Status>> {
  // The identity of the array changes every render; its CONTENT is what the query depends on.
  return useAsync(
    () => source.statusFor(sections, on, group),
    `status:${sections.join(",")}:${day(on)}:${group}`,
  );
}

/** One water's whole sheet, in one call. */
export function useWaterSheet(
  source: RegsSource,
  item: ItemId | null,
  on: PlainDate,
  group: SpeciesGroup,
): Async<ItemRegs | null> {
  return useAsync(
    () => (item ? source.regsForItem(item, on, group) : Promise.resolve(null)),
    `sheet:${item}:${day(on)}:${group}`,
    item !== null,
  );
}

export interface SearchResults {
  waters: readonly NameHit[];
  places: readonly PlaceHit[];
}

/** Names and aliases, plus the towns you can ask "what is near here" about. */
export function useSearch(
  source: RegsSource,
  query: string,
  limit = 40,
  debounceMs = 160,
): Async<SearchResults> {
  const q = useDebounced(query.trim(), debounceMs);
  return useAsync(
    async () => {
      if (q.length < 2) return { waters: [], places: [] };
      const [waters, places] = await Promise.all([
        source.searchNames(q, limit),
        source.searchPlaces(q, 6),
      ]);
      return { waters, places };
    },
    `search:${q}:${limit}`,
  );
}

/** Everything near a town, nearest first. Precomputed at 25 km. */
export function useWatersNear(source: RegsSource, place: PlaceId | null) {
  return useAsync(
    () => (place ? source.watersNear(place) : Promise.resolve([])),
    `near:${place}`,
    place !== null,
  );
}

export interface Conditions {
  /** null when no station is entitled to speak for this water. */
  station: StationId | null;
  stationName: string | null;
  discharge: number | null;
  level: number | null;
  percentile: number | null;
  standing: string | null;
  /** How much of the gauge's watershed this reach is. */
  trust: string | null;
  /** Age of the reading. A stale value must never render as a live one. */
  fetchedAt: number | null;
  /** The reaches between here and the station that measures it. */
  trace: readonly SectionId[];
}

export function useConditions(source: RegsSource, section: SectionId | null): Async<Conditions> {
  return useAsync(
    async (): Promise<Conditions> => {
      const empty: Conditions = {
        station: null, stationName: null, discharge: null, level: null, percentile: null,
        standing: null, trust: null, fetchedAt: null, trace: [],
      };
      if (!section) return empty;
      const link = await source.gaugeForSection(section);
      // "none" means the station drains far too much to describe this water. Returning its
      // number anyway is the failure this whole path exists to prevent.
      if (!link || link.trust === "none") return empty;
      const [now, trace] = await Promise.all([
        source.gaugeNow(link.station),
        source.traceToGauge(section),
      ]);
      return {
        station: link.station, stationName: link.name, trust: link.trust, trace,
        discharge: now?.value.discharge ?? null,
        level: now?.value.level ?? null,
        percentile: now?.value.percentile ?? null,
        standing: now?.value.standing ?? null,
        fetchedAt: now?.fetchedAt ?? null,
      };
    },
    `conditions:${section}`,
    section !== null,
  );
}

/**
 * How this reach reaches its gauge, live.
 *
 * The other source of a `GaugeTrace` is a saved spot, which carries one frozen at capture
 * time. Both feed the SAME component — a gauge's representativeness is the most
 * misreadable number in the app, so the sentence explaining it may not have two versions.
 *
 * `metres` is null here and that is not an oversight: along-channel distance to the station
 * is not computed by anything yet. The component renders "not known", which is the truth.
 * A zero would read as "the gauge is right here".
 */
export function useGaugeTrace(
  source: RegsSource, section: SectionId | null,
): Async<GaugeTrace> {
  return useAsync(
    async (): Promise<GaugeTrace> => {
      const none: GaugeTrace = {
        station: null, stationName: null, trust: null, path: [],
        metres: null, reachMagnitude: null, gaugeMagnitude: null,
        lon: null, lat: null,
      };
      if (!section) return none;
      const link = await source.gaugeForSection(section);
      // "none" means the station drains far too much to describe this water. Returning it
      // as a trace anyway would put a real station id on a panel that must claim nothing.
      if (!link || link.trust === "none") return none;
      return {
        station: link.station,
        stationName: link.name,
        trust: link.trust,
        path: await source.traceToGauge(section),
        metres: null,
        reachMagnitude: link.reachMagnitude || null,
        gaugeMagnitude: link.gaugeMagnitude || null,
        // ECCC's own coordinate, straight through. Null when they published none — which
        // the panel draws as no map rather than as a map of the wrong place.
        lon: link.lon,
        lat: link.lat,
      };
    },
    `trace:${section}`,
    section !== null,
  );
}

/**
 * Does this WATER have a gauge — the question asked before any number is wanted.
 *
 * Not `useGaugeTrace` with a different argument. That one asks about the reach a person is
 * looking at, and answers "nothing speaks for this stretch" for most of a river that is
 * perfectly well gauged eight kilometres down. This asks whether the water has a station
 * anywhere on it, which is what a person means by "is this river gauged".
 *
 * Returns null for genuinely ungauged water — a real answer, and the app must show it as
 * one rather than as an empty panel.
 */
export function useWaterGauge(
  source: RegsSource, item: ItemId | null,
): Async<GaugeLink | null> {
  return useAsync(
    async (): Promise<GaugeLink | null> => {
      if (!item) return null;
      const link = await source.gaugeForItem(item);
      // "none" is stored nowhere, but a source that computes rather than reads could
      // return it; treat it as what it says — nobody speaks for this water.
      return !link || link.trust === "none" ? null : link;
    },
    `watergauge:${item}`,
    item !== null,
  );
}

/**
 * The percentile that colours each reach on screen, for the Conditions view.
 *
 * TWO JOINS, NEITHER OF WHICH THE MAP CAN DO ITSELF. A tile feature knows its section id
 * and nothing else; the bundle knows which station speaks for that section; the feed knows
 * what that station is reading today. This is where the three meet.
 *
 * Scoped to the sections passed in — whatever the map currently has rendered — because the
 * full table is 558,746 rows and the question is about a few hundred features.
 *
 * THREE OUTCOMES, and the middle one is easy to lose:
 *
 *   absent   no gauge speaks for this reach -> the map paints it as unmeasured water
 *   -1       a gauge reports here, but there is no record to compare today against
 *   0..1     a real percentile
 *
 * A reach with no gauge is ABSENT, never zero: zero is the bottom of the scale and would
 * paint every ungauged creek as a river in drought. And a reach whose gauge works but has
 * no history is NOT the same as an ungauged one — saying nothing about it implies nobody
 * is measuring it, when somebody is.
 */
export function useStandings(
  source: RegsSource,
  feed: { index(): Promise<{ stations: Record<string, { percentile: number | null }> } | null> }
        | undefined,
  sections: readonly SectionId[],
): ReadonlyMap<SectionId, number> {
  const key = sections.length ? `${sections.length}:${sections[0]}:${sections[sections.length - 1]}` : "";
  const got = useAsync(
    async (): Promise<ReadonlyMap<SectionId, number>> => {
      const out = new Map<SectionId, number>();
      if (!feed || !sections.length) return out;
      const [stations, idx] = await Promise.all([
        source.stationsFor(sections),
        feed.index(),
      ]);
      if (!idx) return out;              // offline: colour nothing rather than colour wrong
      for (const [section, station] of stations) {
        const row = idx.stations[station];
        if (!row) continue;                       // not transmitting: say nothing
        out.set(section, typeof row.percentile === "number" ? row.percentile : -0.01);
      }
      return out;
    },
    `standings:${key}`,
    sections.length > 0,
  );
  const empty = useMemo(() => new Map<SectionId, number>(), []);
  return got.state === "ready" ? got.value : empty;
}

/**
 * The gauges as GeoJSON, ready for the map — dots with their readings.
 *
 * Fetched once per feed tick rather than per pan: the station list is a few hundred rows
 * and does not depend on the viewport, so re-querying it on every movement would be work
 * for nothing.
 */
export function useGaugeGeoJSON(
  source: RegsSource,
  feed: { index(): Promise<Parameters<typeof gaugeGeoJSON>[1]> } | undefined,
  enabled: boolean,
): string | null {
  const got = useAsync(
    async () => {
      if (!feed) return null;
      const [points, idx] = await Promise.all([source.gaugePoints(), feed.index()]);
      return gaugeGeoJSON(points, idx);
    },
    "gaugepoints",
    enabled,
  );
  return got.state === "ready" ? got.value : null;
}

/** The chart, as numbers. The component only draws the shapes it is handed (rule 25). */
/**
 * One chart, in one quantity, over one span.
 *
 * FOUR THINGS THE READER CAN CHANGE, and each of them changes what is being claimed rather
 * than how it looks:
 *
 *   parameter   discharge (the whole river) or level (one cross-section). Not a unit
 *               swap — a stage percentile moves when the channel does and a discharge
 *               percentile does not, so they are different statements about the water.
 *   span        "72h" is what the river is doing; "year" is where today sits in the season.
 *   envelope    always drawn when the bundle has one; its absence is the answer for the
 *               eight stations whose record is too thin to build one.
 *   forecast    drawn when a BC River Forecast Centre model is running, which is seasonal.
 *
 * Passing `parameter` as undefined means "whatever this station measures", which the client
 * genuinely cannot know: 237 BC stations never measure discharge at all.
 */
export function useHydrograph(
  source: RegsSource,
  station: StationId | null,
  span: "72h" | "year",
  parameter?: Parameter,
): Async<Hydrograph | null> {
  const series = useAsync(
    () => (station ? source.gaugeSeries(station, span, parameter) : Promise.resolve(null)),
    `series:${station}:${span}:${parameter ?? "auto"}`,
    station !== null,
  );
  return useMemo(() => {
    if (series.state !== "ready") return series as Async<Hydrograph | null>;
    const s = series.value;
    if (!s) return { state: "ready", value: null, error: null };
    const v = s.value;
    const n = v.values.length;
    // One stored band covers a short span; a year has one per pentad. A band array that is
    // neither is a build defect, so it is repeated rather than truncated — a chart with a
    // short envelope would show the normal range ending mid-frame.
    const bands = v.band.length === n
      ? v.band
      : Array.from({ length: n }, () => v.band[0] ?? null);
    const spanDays = span === "72h" ? 3 : 365;
    return {
      state: "ready",
      value: buildHydrograph({
        values: v.values,
        bands,
        xLabels: span === "72h" ? ["3d ago", "2d", "1d", "now"]
                                : ["Jan", "Apr", "Jul", "Oct"],
        nowIndex: v.now?.index ?? -1,
        nowValue: v.now?.value ?? null,
        forecast: v.forecast
          ? { lo: v.forecast.min, mid: v.forecast.value, hi: v.forecast.max,
              days: v.forecast.horizonDays, spanDays }
          : null,
        // A YEAR OF FLOW SPANS TWO ORDERS OF MAGNITUDE and a linear axis spends nine tenths
        // of its height on the freshet, flattening the summer — which is the half of the
        // year anybody is fishing. Level does not: stage is metres above a datum and its
        // range is narrow, so a log axis there would exaggerate centimetres into a story.
        log: span === "year" && v.parameter === "discharge",
      }),
      error: null,
    };
  }, [series, span]);
}

/** Which quantities this station can be charted in — an empty list means no envelope. */
export function useGaugeParameters(
  source: RegsSource, station: StationId | null,
): Async<readonly Parameter[]> {
  return useAsync(
    () => (station ? source.gaugeParameters(station) : Promise.resolve([])),
    `params:${station}`,
    station !== null,
  );
}

export function useLake(source: RegsSource, item: ItemId | null): Async<LakeInfo | null> {
  return useAsync(
    () => (item ? source.lakeInfo(item) : Promise.resolve(null)),
    `lake:${item}`,
    item !== null,
  );
}

/** The word every surface shows. Never re-worded locally (AGENTS.md rule 23). */
export { statusWord };
