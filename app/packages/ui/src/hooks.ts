/**
 * The app's questions, as hooks. Data out, never elements — which is what lets the phone
 * and the desktop render the same answers with completely different components.
 */
import { useMemo, useState } from "react";
import type { GaugeTrace, SectionKey, StatusIndex } from "@app/core";
import { mapStatusOn } from "@app/core";
import { FEED_TTL_MS } from "@app/data";
import type {
  BundleCounts, GaugeFeed, Horizon, ItemId, LakeInfo, NameHit,
  Parameter, PlaceHit, PlaceId, PointerHit, RegsSource, SectionId, Series, StationId, Water,
} from "@app/data";
import { useAsync, useDebounced, usePoll, type Async } from "./async";
import { askOnce, peekAnswer, seriesKey } from "./answers";
import { gaugeGeoJSON, type GaugeQuantity } from "./gaugePoints";

/**
 * One water — its name and its sections. What the water sheet titles itself with.
 *
 * No regulations: they are not integrated (see `regulations.ts` in @app/core). The sheet
 * renders a placeholder where they will go.
 */
export function useWater(source: RegsSource, item: ItemId | null): Async<Water | null> {
  return useAsync(
    () => (item ? source.water(item) : Promise.resolve(null)),
    `water-item:${item}`,
    item !== null,
  );
}

/**
 * (T175) THE SECTIONS A WATER'S CONDITIONS MAP HOLDS AT EVERY ZOOM: the named water's own (`water.sections`), or — an
 * unnamed reach — the tapped section alone. What `MapProps.waterLine` keeps from the tiles; never a geometry.
 */
export function waterLineSections(water: Water | null, item: ItemId | null,
                                  tapSid: SectionId | null): readonly SectionId[] {
  if (item !== null && water?.item === item) return water.sections;
  return tapSid !== null ? [tapSid] : [];
}

/** `waterLineSections` for a water read from the source (the named water's sections once read; the tap meanwhile). */
export function useWaterLine(source: RegsSource, item: ItemId | null, tapSid: SectionId | null): readonly SectionId[] {
  const w = useWater(source, item);
  const water = w.state === "ready" ? w.value ?? null : null;
  return useMemo(() => waterLineSections(water, item, tapSid), [water, item, tapSid]);
}

/** (T175) A water's box from the release (`RegsSource.waterBox`): where its Conditions map frames it without a tap. */
export function useWaterBox(source: RegsSource, item: ItemId | null):
    readonly [number, number, number, number] | null {
  const b = useAsync(() => (item ? source.waterBox(item) : Promise.resolve(null)), `water-box:${item}`, item !== null);
  return b.state === "ready" ? b.value ?? null : null;
}

export interface SearchResults {
  waters: readonly NameHit[];
  places: readonly PlaceHit[];
  /** Group names that only point ("Tee Pee Lakes: see Friday Lake, …"), each leading to the waters it names. */
  pointers: readonly PointerHit[];
}

/** How many group names that only point a search shows: a few, above the waters (`resultGroups`). */
export const POINTER_LIMIT = 3;

/**
 * HOW LONG THE SEARCH BOX WAITS after the last keystroke before it asks (owner decision 2026-10-20).
 *
 * Search matches a substring anywhere in a name, a full scan of ~20k names + aliases: ~5 ms p95 on the laptop, so
 * ~27–54 ms on a phone (A5 in tools/release-perf.test.ts). That was accepted on the condition that the box does not
 * query on every keystroke. 200 ms is past the gap between letters of a fast typist (~100–180 ms on a phone keyboard),
 * so a word typed in one go is ONE query; and under the ~250 ms where a reader starts to wait for results. Pinned with
 * fake timers in search.debounce.render.test.tsx (a burst of keystrokes -> exactly one search).
 */
export const SEARCH_DEBOUNCE_MS = 200;

/** Names and aliases, plus the towns you can ask "what is near here" about. Debounced: see `SEARCH_DEBOUNCE_MS`. */
export function useSearch(
  source: RegsSource,
  query: string,
  limit = 40,
  debounceMs = SEARCH_DEBOUNCE_MS,
): Async<SearchResults> {
  const q = useDebounced(query.trim(), debounceMs);
  return useAsync(
    async () => {
      if (q.length < 2) return { waters: [], places: [], pointers: [] };
      const [waters, places, pointers] = await Promise.all([
        source.searchNames(q, limit),
        source.searchPlaces(q, 6),
        source.searchPointers(q, POINTER_LIMIT),
      ]);
      return { waters, places, pointers };
    },
    `search:${q}:${limit}`,
  );
}

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
 * Which water a reach belongs to, by name.
 *
 * One indexed read, so a screen can put a TITLE on itself without listing the whole water.
 * The Conditions screen once had no title at all because the only thing that knew a water's
 * name also read everything else about it.
 */
export function useWaterName(
  source: RegsSource, section: SectionId | null,
): Async<{ item: ItemId; name: string; kind: string } | null> {
  return useAsync(
    () => (section ? source.waterFor(section) : Promise.resolve(null)),
    `water:${section ?? ""}`,
    section !== null,
  );
}

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


/**
 * What the Layers sheet says about the data it is sitting on.
 *
 * THE POINT OF THIS HOOK IS THAT THE NUMBERS ARE READ. `App.tsx` used to pass
 * `waters={255} reaches={6967} surveyed={35} stations={5}` and a `fetchedAt` string, all
 *  five transcribed by hand from a design mock-up — whose fixture is one valley. Against
 * the shipped province bundle the sheet therefore reported 5 stations where there are
 * 2,324, and an age three days older than the feed it was showing, under a heading that
 * reads "EVERY VALUE HAS AN AGE".
 *
 * The bundle's counts come from the bundle; the live count and the age come from the feed's
 * own index. Anything unavailable stays `null` and the sheet omits it — a fabricated figure
 * is worse than a missing one, because a missing one is visibly missing.
 */
export function useDataFacts(source: RegsSource, feed?: GaugeFeed, wanted = true): Async<{
  counts: BundleCounts | null;
  /** Stations the LIVE FEED is publishing — not the same as the bundle's roster. */
  liveStations: number | null;
  /** When the feed was last built, ISO. Null when it could not be reached. */
  fetchedAt: string | null;
}> {
  // READ ON DEMAND (P11): on the release `counts()` opens core, which a cold start does not fetch before the first map.
  // So nothing is asked until a screen that shows the figures is up (`wanted`), and once asked they are kept.
  const [asked, setAsked] = useState(wanted);
  if (wanted && !asked) setAsked(true);
  return useAsync(async () => {
    // The bundle is local and always answers; the feed is a network call that may not. Each
    // is caught on its own, so a feed that is down cannot blank the bundle's counts.
    const [counts, index] = await Promise.all([
      source.counts().catch(() => null),
      feed ? feed.index().catch(() => null) : Promise.resolve(null),
    ]);
    return {
      counts,
      liveStations: index ? Object.keys(index.stations).length : null,
      fetchedAt: index?.fetchedAt ?? null,
    };
  }, `datafacts:${feed ? "feed" : "nofeed"}`, asked);
}

/**
 * The map's per-feature values for the `status` colouring: every section on screen, on `on`.
 *
 * NO LONGER THE WEB MAP'S PATH (colour-latency AP): the Shell hands the map the index itself (`MapProps.status`) and the
 * map colours each tile as it arrives through the `profile` mode. Kept for a renderer with no per-tile event — how the
 * native map colours is open until N2 — and it reads `mapStatusOn` (the status, or T54's no access), the answer the
 * per-tile labels give, so the two cannot disagree.
 *
 * EVERY visible section gets a value, base and none included — feature-state is sticky, so a
 * section that was closed yesterday and is base today must be told so, not left red. A
 * section with no status (tidal, outside B.C.) is set to null: the mode's `missing`.
 * The same values on every per-section layer, because feature-state is per source-layer
 * (a stream's polygon — `lake_flowing` / `wetland_flowing` — reads its own source-layer's): `sections` is @app/map's
 * `SECTION_DATA`, which the map fans out to `perSectionLayers()` (T124).
 */
export function statusData(index: StatusIndex | null, visible: readonly SectionKey[],
                           on: Date): Record<string, Record<string, Record<string, unknown>>> {
  const values: Record<string, Record<string, unknown>> = {};
  for (const s of visible) values[String(s)] = { status: mapStatusOn(index, s, on) };
  return { sections: values };
}

/**
 * THE REGULATIONS READ OF THE WATER THAT IS OPEN (P11): its answer keys, through the source — on the release that opens
 * core, water and the region file holding them (`RegsSource.regulationKeys`), so opening a water in another region
 * fetches that region's file. Nothing is asked while no water is open. T01/T03 draw the page from what this opened.
 */
export function useRegulationKeys(source: RegsSource, item: ItemId | null): Async<readonly number[]> {
  return useAsync(async () => (item ? source.regulationKeys(item) : []), `regulation-keys:${item ?? ""}`, item !== null);
}
