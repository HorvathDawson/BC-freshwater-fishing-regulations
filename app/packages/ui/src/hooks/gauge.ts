/**
 * THE GAUGES: one station's reading, a reach's trace to its gauge, the map's dots, a station's series and the
 * quantities it can be charted in.
 */
import { useMemo } from "react";
import type { GaugeTrace } from "@app/core";
import { FEED_TTL_MS } from "@app/data";
import type { Horizon, Parameter, RegsSource, SectionId, Series, StationId } from "@app/data";
import { useAsync, usePoll, type Async } from "../async";
import { askOnce, peekAnswer, seriesKey } from "../answers";
import { gaugeGeoJSON, type GaugeQuantity } from "../gaugePoints";

export interface Conditions {
  /** null when no station is entitled to speak for this water. */
  station: StationId | null;
  stationName: string | null;
  discharge: number | null;
  level: number | null;
  /** Which quantity the station shows and `percentile` is about, as the feed declared it (T106; `shownParameter`). */
  parameter: Parameter | null;
  percentile: number | null;
  standing: string | null;
  /** How much of the gauge's watershed this reach is. */
  trust: string | null;
  /** Age of the reading. A stale value must never render as a live one. */
  fetchedAt: number | null;
  /** The reading's own observation time (ISO), or null when the feed does not say (P02). */
  observedAt: string | null;
  /** The reaches between here and the station that measures it. */
  trace: readonly SectionId[];
}

// NO `useConditions` (T105): it read the station `section_gauge` matched to a reach for the sheet's reading, a second
// speaker beside the map's (`speakerFor` in @app/data). The sheet reads the speaker's lead through `useStationReading`.
/**
 * One station's own latest reading — whichever station you ask about.
 *
 * The deleted `useConditions` answered "what does the ONE station matched to this reach say", and
 * the station was not a parameter. That was the whole answer when a reach had one gauge; a
 * donor panel has up to four, and a reader looking at a table of four with a chart of a
 * fifth (the matched one, which need not be in the panel at all) is being shown two models
 * and told they are one. This is the same reading, for a station the caller chooses.
 */
/** How often an open view re-asks the feed for a station's reading: the feed's cache TTL (5 min). */
export const READING_POLL_MS = FEED_TTL_MS;

export function useStationReading(
  source: RegsSource, station: StationId | null,
): Async<Conditions> {
  // P02: re-asked every READING_POLL_MS while a view holds it (the feed's own cache lives as long), keeping the last
  // reading on screen meanwhile; each tick also re-renders, so its age line moves on.
  const tick = usePoll(READING_POLL_MS, station !== null);
  // ASKED THROUGH THE ANSWER STORE (answers.ts), under the key the Conditions prefetch uses, so a gauge picked in the
  // switch reads out in the same render. Reused for half a poll: a tick is always older than that, so it asks again.
  const key = `reading:${station ?? ""}`;
  return useAsync(
    () => askOnce(source, key, () => stationReading(source, station), READING_REUSE_MS),
    `${key}#${tick}`,
    station !== null,
    key,
    () => peekAnswer<Conditions>(source, key, READING_REUSE_MS),
  );
}

/** How long a station's reading is reused across components (a prefetch, a switch): half a poll. */
export const READING_REUSE_MS = READING_POLL_MS / 2;

/** One station's reading as the conditions view holds it — what `useStationReading` and the prefetch ask. */
export async function stationReading(source: RegsSource, station: StationId | null): Promise<Conditions> {
  const empty: Conditions = {
    station: null, stationName: null, discharge: null, level: null, parameter: null, percentile: null,
    standing: null, trust: null, fetchedAt: null, observedAt: null, trace: [],
  };
  if (!station) return empty;
  // The reading only (P16): the chart reads its own series.
  const now = await source.gaugeNow(station);
  if (!now) return { ...empty, station };
  return {
    // `trust` and `trace` belong to a reach-to-station RELATIONSHIP, and there is none
    // here: this is a station being asked what it reads. Null rather than borrowed from
    // the matched link, which would attach one reach's trust band to another's gauge.
    //
    // `stationName` is null for the same reason it is not on a `Reading`: a reading has
    // no name, and inventing one here would mean a second place that decides what a
    // station is called. The caller already holds the name — it came with the donor.
    station, stationName: null, trust: null, trace: [],
    discharge: now.value.discharge, level: now.value.level, parameter: now.value.parameter,
    percentile: now.value.percentile, standing: now.value.standing,
    fetchedAt: now.fetchedAt, observedAt: now.value.at || null,
  };
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
      // No link is the refusal — see above. Returning a trace anyway would put a real
      // station id on a panel that must claim nothing.
      if (!link) return none;
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
  quantity: GaugeQuantity = "flow",
  horizon: Horizon = 0,
): string | null {
  const got = useAsync(
    async () => {
      if (!feed) return null;
      const [points, idx] = await Promise.all([source.gaugePoints(), feed.index()]);
      return gaugeGeoJSON(points, idx, quantity, horizon);
    },
    // THE QUANTITY IS PART OF THE CACHE KEY. These are different sets of features, not
    // three colourings of one — 361 stations publish a discharge percentile, 419 a level
    // and 274 a temperature, and they are not nested. A shared cache would show one
    // roster under another's heading. SO IS THE HORIZON (T126): the dots follow it, and a
    // day no run reaches drops the dots a forecast cannot speak for.
    `gaugepoints:${quantity}:${horizon}`,
    enabled,
  );
  return got.state === "ready" ? got.value : null;
}

/**
 * The whole series, not just its shapes — the model runs, the record, the disclaimer.
 *
 * The panel lays the chart out from it (`chart.ts`, `buildHydrograph`) and reads the caption off it. Asking twice is
 * free: every caller asks through the answer store (`askOnce`, answers.ts) under one `seriesKey`, so the second is the
 * first's promise — `useAsync` alone shares nothing between callers.
 */
export function useSeries(
  source: RegsSource, station: StationId | null, span: "72h" | "year",
  parameter?: Parameter, model?: string, enabled = true,
): Async<Series | null> {
  const key = seriesKey(station, span, parameter, model);
  const got = useAsync(
    () => (station ? askOnce(source, key, () => source.gaugeSeries(station, span, parameter, model))
                   : Promise.resolve(null)),
    key,
    station !== null && enabled,
    undefined,
    () => peekAnswer(source, key),
  );
  return useMemo(() => (got.state === "ready"
    ? { state: "ready" as const, value: got.value?.value ?? null, error: null }
    : (got as Async<Series | null>)), [got]);
}

/** Which quantities this station can be charted in — an empty list means no envelope. */
export function useGaugeParameters(
  source: RegsSource, station: StationId | null,
): Async<readonly Parameter[]> {
  // Through the answer store, so a prefetched donor's quantities are known the instant it is picked — otherwise the
  // chart would ask for the station's "auto" series first and its picked quantity a frame later (two loads).
  const key = `params:${station}`;
  return useAsync(
    () => (station ? askOnce(source, key, () => source.gaugeParameters(station)) : Promise.resolve([])),
    key,
    station !== null,
    undefined,
    () => peekAnswer<readonly Parameter[]>(source, key),
  );
}
