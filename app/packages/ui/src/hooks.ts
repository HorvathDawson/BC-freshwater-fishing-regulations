/**
 * The app's questions, as hooks. Data out, never elements — which is what lets the phone
 * and the desktop render the same answers with completely different components.
 */
import { useMemo, useState } from "react";
import type { GaugeTrace, SectionKey, StatusIndex } from "@app/core";
import { decodeStatusIndex, monthAbbr, statusOn } from "@app/core";
import type {
  BundleCounts, GaugeFeed, GaugeLink, ItemId, LakeInfo, NameHit,
  Parameter, PlaceHit, PlaceId, RegsSource, SectionId, Series, StationId, Water,
} from "@app/data";
import { useAsync, useDebounced, usePoll, type Async } from "./async";
import { askOnce, peekAnswer, seriesKey } from "./answers";
import { buildHydrograph, type Hydrograph } from "./hydrograph";
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

export interface SearchResults {
  waters: readonly NameHit[];
  places: readonly PlaceHit[];
}

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
  /** The reading's own observation time (ISO), or null when the feed does not say (P02). */
  observedAt: string | null;
  /** The reaches between here and the station that measures it. */
  trace: readonly SectionId[];
}

export function useConditions(source: RegsSource, section: SectionId | null): Async<Conditions> {
  return useAsync(
    async (): Promise<Conditions> => {
      const empty: Conditions = {
        station: null, stationName: null, discharge: null, level: null, percentile: null,
        standing: null, trust: null, fetchedAt: null, observedAt: null, trace: [],
      };
      if (!section) return empty;
      const link = await source.gaugeForSection(section);
      // NO LINK IS THE REFUSAL. A station that drains far too much to describe this water
      // gets no `section_gauge` row at all, so `gaugeForSection` returns null — there is no
      // "none" band to test for, and testing for one implied the row existed and was
      // labelled. Returning its number anyway is the failure this path exists to prevent.
      if (!link) return empty;
      // NO SERIES HERE (P16). This fetched the station's 72-hour series for the model run
      // behind it, and nothing read the answer: the chart asks for its own (`useSeries`).
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
        observedAt: now?.value.at || null,
      };
    },
    `conditions:${section}`,
    section !== null,
  );
}

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
 * `useConditions` above answers "what does the ONE station matched to this reach say", and
 * the station is not a parameter. That was the whole answer when a reach had one gauge; a
 * donor panel has up to four, and a reader looking at a table of four with a chart of a
 * fifth (the matched one, which need not be in the panel at all) is being shown two models
 * and told they are one. This is the same reading, for a station the caller chooses.
 */
/** How often an open view re-asks the feed for a station's reading: the feed's cache TTL (5 min). */
export const READING_POLL_MS = 5 * 60_000;

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
    station: null, stationName: null, discharge: null, level: null, percentile: null,
    standing: null, trust: null, fetchedAt: null, observedAt: null, trace: [],
  };
  if (!station) return empty;
  // The reading only — see `useConditions`: the chart reads its own series.
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
    discharge: now.value.discharge, level: now.value.level,
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
      return link ?? null;
    },
    `watergauge:${item}`,
    item !== null,
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
): string | null {
  const got = useAsync(
    async () => {
      if (!feed) return null;
      const [points, idx] = await Promise.all([source.gaugePoints(), feed.index()]);
      return gaugeGeoJSON(points, idx, quantity);
    },
    // THE QUANTITY IS PART OF THE CACHE KEY. These are different sets of features, not
    // three colourings of one — 361 stations publish a discharge percentile, 419 a level
    // and 274 a temperature, and they are not nested. A shared cache would show one
    // roster under another's heading.
    `gaugepoints:${quantity}`,
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
  model?: string,
): Async<Hydrograph | null> {
  const series = useAsync(
    () => (station ? source.gaugeSeries(station, span, parameter, model)
                   : Promise.resolve(null)),
    `series:${station}:${span}:${parameter ?? "auto"}:${model ?? "auto"}`,
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
    // REAL DATES on the axis. The seasonal chart used to say Jan/Apr/Jul/Oct whatever year
    // it was showing, and the 72-hour one said "3d ago" — neither of which tells a reader
    // where the forecast they are looking at ends.
    const labels = axisLabels(v.at, v.forecast?.series?.at ?? [], span);
    return {
      state: "ready",
      value: buildHydrograph({
        values: v.values,
        bands,
        at: v.at,
        xLabels: labels,
        nowIndex: v.now?.index ?? -1,
        nowValue: v.now?.value ?? null,
        priorYears: v.priorYears,
        forecast: v.forecast?.series
          ? { at: v.forecast.series.at, mid: v.forecast.series.mid,
              lo: v.forecast.series.lo, hi: v.forecast.series.hi }
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

/**
 * The whole series, not just its shapes — the model runs, the record, the disclaimer.
 *
 * `useHydrograph` returns geometry, which is what a chart draws and nothing a caption can
 * read. The panel needs both, and asking twice is free: both ask through the answer store
 * (`askOnce`, answers.ts) under one `seriesKey`, so the second is the first's promise — it was
 * not before, whatever this comment said: `useAsync` alone shares nothing between callers.
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
 * Five labels across whatever the chart actually spans, as real dates.
 *
 * "3d ago / 2d / 1d / now" was fine while the frame ended at now. It stopped being fine the
 * moment a ten-day forecast extended it: the reader could see a ribbon reaching to the
 * right-hand edge and had no way to tell whether that edge was Thursday or a month away.
 *
 * The seasonal axis gets months, because a year labelled by date is unreadable; everything
 * shorter gets day-and-month, with the hour when the whole span is under three days.
 */
function axisLabels(at: readonly string[], ahead: readonly string[],
                    span: "72h" | "year"): string[] {
  const all = [...at, ...ahead].map((t) => Date.parse(t)).filter(Number.isFinite);
  if (all.length < 2) return [];
  const t0 = Math.min(...all);
  const t1 = Math.max(...all);
  if (span === "year")
    return ["Jan", "Mar", "May", "Jul", "Sep", "Nov"];
  const hours = (t1 - t0) / 3_600_000;
  const fmt = (t: number): string => {
    const d = new Date(t);
    // THE READER'S CLOCK (P16), as every other time on the Conditions screen: a tick at
    // "06h" in UTC is 11 p.m. the evening before in BC.
    const day = `${d.getDate()} ${monthAbbr(d.getMonth())}`;
    return hours <= 72 ? `${day} ${String(d.getHours()).padStart(2, "0")}h` : day;
  };
  return [0, 0.25, 0.5, 0.75, 1].map((f) => fmt(t0 + (t1 - t0) * f));
}


/**
 * What the Layers sheet says about the data it is sitting on.
 *
 * THE POINT OF THIS HOOK IS THAT THE NUMBERS ARE READ. `App.tsx` used to pass
 * `waters={255} reaches={6967} surveyed={35} stations={5}` and a `fetchedAt` string, all
 * five transcribed by hand from `design/riffle.html` — whose fixture is one valley. Against
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
 * THE DEV PATH ONLY (`EXPO_PUBLIC_DATA_SOURCE=bundle`, until P20): the status index beside the
 * tiles, `status_index.bin`. The app reads the status from the release's paint (`./release.ts`).
 */
export function statusIndexUrl(atlasUrl: string): string {
  return atlasUrl.replace(/[^/]*$/, "status_index.bin");
}

/** One fetch and one decode per URL and digest, whichever screens ask. */
const STATUS_INDEXES = new Map<string, Promise<StatusIndex | null>>();

/**
 * Fetch and decode the status index, once. `handles` is the bundle's `section_handles`: an
 * index built against any other atlas is REFUSED — its integers name different rivers — and so
 * is any index when the bundle has no digest to hold it to. `reach` is the bundle's
 * `reach_digest`: an index cut from other rule bindings is refused too (null: not held to it). Any failure (no file, a 404, a
 * damaged or refused file) is null: "not asked", never "every water is base" — and it is NOT
 * kept (P11): the next ask fetches again, so a server that was down at start does not leave the
 * map uncoloured for the rest of the session.
 */
export function loadStatusIndex(url: string, handles: string | null,
                                fetcher: typeof fetch = fetch,
                                reach: string | null = null): Promise<StatusIndex | null> {
  if (!handles) return Promise.resolve(null);
  const key = `${url}#${handles}#${reach ?? ""}`;
  let got = STATUS_INDEXES.get(key);
  if (!got) {
    got = fetcher(url)
      .then(async (res) => (res.ok
        ? decodeStatusIndex(new Uint8Array(await res.arrayBuffer()), handles, reach) : null))
      .catch((e: unknown) => {
        console.warn("status index:", e instanceof Error ? e.message : e);
        return null;
      })
      .then((ix) => { if (ix === null) STATUS_INDEXES.delete(key); return ix; });
    STATUS_INDEXES.set(key, got);
  }
  return got;
}

/**
 * The map's per-feature values for the `status` colouring: every section on screen, on `on`.
 *
 * NO LONGER THE WEB MAP'S PATH (colour-latency AP): the Shell hands the map the index itself (`MapProps.status`) and the
 * map colours each tile as it arrives through the `profile` mode. Kept for a renderer with no per-tile event — how the
 * native map colours is open until N2 — and it reads the same `statusOn`, so the two cannot disagree.
 *
 * EVERY visible section gets a value, base and none included — feature-state is sticky, so a
 * section that was closed yesterday and is base today must be told so, not left red. A
 * section with no status (tidal, outside B.C.) is set to null: the mode's `missing`.
 * The same values for streams, lakes and wetlands, because feature-state is per source-layer
 * (a stream's polygon — `lake_flowing` / `wetland_flowing` — reads its own source-layer's).
 */
export function statusData(index: StatusIndex | null, visible: readonly SectionKey[],
                           on: Date): Record<string, Record<string, Record<string, unknown>>> {
  const values: Record<string, Record<string, unknown>> = {};
  for (const s of visible) values[String(s)] = { status: statusOn(index, s, on) };
  // and on the wetland source-layer, for a slough drawn as a wetland (`wetland_flowing`)
  return { stream: values, lake: values, wetland: values };
}

/**
 * THE REGULATIONS READ OF THE WATER THAT IS OPEN (P11): its answer keys, through the source — on the release that opens
 * core, water and the region file holding them (`RegsSource.regulationKeys`), so opening a water in another region
 * fetches that region's file. Nothing is asked while no water is open. T01/T03 draw the page from what this opened.
 */
export function useRegulationKeys(source: RegsSource, item: ItemId | null): Async<readonly number[]> {
  return useAsync(async () => (item ? source.regulationKeys(item) : []), `regulation-keys:${item ?? ""}`, item !== null);
}
