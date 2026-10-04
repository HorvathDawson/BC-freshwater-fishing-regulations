/**
 * The app's questions, as hooks. Data out, never elements — which is what lets the phone
 * and the desktop render the same answers with completely different components.
 */
import { useMemo } from "react";
import type { GaugeTrace, SectionKey, StatusIndex } from "@app/core";
import { decodeStatusIndex, monthAbbr, statusOn } from "@app/core";
import type {
  BundleCounts, Forecast, GaugeFeed, GaugeLink, ItemId, LakeInfo, NameHit,
  Parameter, PlaceHit, PlaceId, RegsSource, SectionId, Series, StationId, Water,
} from "@app/data";
import { useAsync, useDebounced, type Async } from "./async";
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
  /**
   * The model run behind the chart's ribbon — for the issue time, the model's name and its
   * provider's disclaimer, none of which belong to the reading beside it.
   */
  forecast: Forecast | null;
  /** The reaches between here and the station that measures it. */
  trace: readonly SectionId[];
}

export function useConditions(source: RegsSource, section: SectionId | null): Async<Conditions> {
  return useAsync(
    async (): Promise<Conditions> => {
      const empty: Conditions = {
        station: null, stationName: null, discharge: null, level: null, percentile: null,
        standing: null, trust: null, fetchedAt: null, trace: [], forecast: null,
      };
      if (!section) return empty;
      const link = await source.gaugeForSection(section);
      // NO LINK IS THE REFUSAL. A station that drains far too much to describe this water
      // gets no `section_gauge` row at all, so `gaugeForSection` returns null — there is no
      // "none" band to test for, and testing for one implied the row existed and was
      // labelled. Returning its number anyway is the failure this path exists to prevent.
      if (!link) return empty;
      const [now, trace, series] = await Promise.all([
        source.gaugeNow(link.station),
        source.traceToGauge(section),
        // Asked for the STATION's own quantity, because this is about the run rather than
        // about a chart: which model, issued when, under whose disclaimer.
        source.gaugeSeries(link.station, "72h"),
      ]);
      return {
        station: link.station, stationName: link.name, trust: link.trust, trace,
        discharge: now?.value.discharge ?? null,
        level: now?.value.level ?? null,
        percentile: now?.value.percentile ?? null,
        standing: now?.value.standing ?? null,
        fetchedAt: now?.fetchedAt ?? null,
        forecast: series?.value.forecast ?? null,
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
export function useStationReading(
  source: RegsSource, station: StationId | null,
): Async<Conditions> {
  return useAsync(
    async (): Promise<Conditions> => {
      const empty: Conditions = {
        station: null, stationName: null, discharge: null, level: null, percentile: null,
        standing: null, trust: null, fetchedAt: null, trace: [], forecast: null,
      };
      if (!station) return empty;
      const [now, series] = await Promise.all([
        source.gaugeNow(station),
        source.gaugeSeries(station, "72h"),
      ]);
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
        fetchedAt: now.fetchedAt, forecast: series?.value.forecast ?? null,
      };
    },
    `reading:${station ?? ""}`,
    station !== null,
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
 * read. The panel needs both, and asking twice is free: `useAsync` keys on the same string,
 * so the second call is the same in-flight promise rather than a second fetch.
 */
export function useSeries(
  source: RegsSource, station: StationId | null, span: "72h" | "year",
  parameter?: Parameter, model?: string,
): Async<Series | null> {
  const got = useAsync(
    () => (station ? source.gaugeSeries(station, span, parameter, model)
                   : Promise.resolve(null)),
    `series:${station}:${span}:${parameter ?? "auto"}:${model ?? "auto"}`,
    station !== null,
  );
  return useMemo(() => (got.state === "ready"
    ? { state: "ready" as const, value: got.value?.value ?? null, error: null }
    : (got as Async<Series | null>)), [got]);
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
    const day = `${d.getUTCDate()} ${monthAbbr(d.getUTCMonth())}`;
    return hours <= 72 ? `${day} ${String(d.getUTCHours()).padStart(2, "0")}h` : day;
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
export function useDataFacts(source: RegsSource, feed?: GaugeFeed): Async<{
  counts: BundleCounts | null;
  /** Stations the LIVE FEED is publishing — not the same as the bundle's roster. */
  liveStations: number | null;
  /** When the feed was last built, ISO. Null when it could not be reached. */
  fetchedAt: string | null;
}> {
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
  }, `datafacts:${feed ? "feed" : "nofeed"}`, true);
}

/**
 * Whether the tiles and the bundle came from the same atlas.
 *
 * WHY THIS EXISTS. A section is an integer handle — an index into the atlas's
 * `section_handles.txt` — carried identically by the tile and by every section-keyed table
 * in the bundle. Pair a bundle with tiles from a DIFFERENT atlas and the two do not miss
 * each other: they agree on a number that means two different rivers. Every lookup
 * succeeds, and the answers are about the wrong water.
 *
 * It has already happened. The tiles were rebuilt with new handles while `bundle.sqlite`
 * was left behind, and the Conditions map painted every river as unmeasured — which is a
 * state this app draws on purpose, so it looked like a quiet feed rather than a broken
 * pair. Nothing errored anywhere.
 *
 * `ok` is deliberately three-valued. `null` means "not established yet" and must NOT be
 * treated as agreement: the sidecar is one fetch and the answer arrives a moment after the
 * map does, so colouring on an unproven pair is exactly the window this closes.
 */
export interface Vintage {
  /** true = same atlas · false = a mixed pair · null = not established yet. */
  ok: boolean | null;
  /** What the bundle says, for a message a person can act on. */
  bundle: string | null;
  /** What the tiles say. */
  tiles: string | null;
}

const UNKNOWN_VINTAGE: Vintage = { ok: null, bundle: null, tiles: null };

/**
 * Compare the bundle's `meta.section_handles` against the tile sidecar's.
 *
 * A SIDECAR AND NOT PMTILES METADATA, because both platforms have to read it: the web map
 * goes through the `pmtiles` protocol and the device map through maplibre-react-native, and
 * only one of those hands a page the archive header. A ~100-byte JSON file next to the
 * archive is readable by both with no library at all.
 *
 * A sidecar that will not load leaves this `null` rather than false. That is not the same
 * failure — an old deployment has no sidecar at all, and refusing to draw a map because a
 * metadata file 404'd would be worse than the bug this guards.
 */
export function useVintage(source: RegsSource, atlasUrl: string | undefined): Vintage {
  const got = useAsync(
    async (): Promise<Vintage> => {
      const info = await source.info();
      const bundle = info.sectionHandles;
      if (!atlasUrl) return { ok: null, bundle, tiles: null };
      const url = atlasUrl.replace(/[^/]*$/, "atlas.meta.json");
      const res = await fetch(url);
      if (!res.ok) return { ok: null, bundle, tiles: null };
      const meta = (await res.json()) as { section_handles?: string };
      const tiles = meta.section_handles ?? null;
      if (!bundle || !tiles) return { ok: null, bundle, tiles };
      return { ok: bundle === tiles, bundle, tiles };
    },
    `vintage:${atlasUrl ?? ""}`,
  );
  return got.state === "ready" ? got.value : UNKNOWN_VINTAGE;
}

/**
 * WHERE THE STATUS INDEX LIVES: beside the tiles, like `atlas.meta.json`, because it is
 * keyed by the tiles' own feature ids and is one set with them (`status_index.bin`, written by
 * `python -m pipeline.deliver.status_index`).
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
 * damaged or refused file) is null: "not asked", never "every water is base".
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
      });
    STATUS_INDEXES.set(key, got);
  }
  return got;
}

/**
 * THE DAY'S STATUS OF EVERY SECTION AND WATER — the index, once it has loaded and matched the
 * bundle. Null until then, and for good if it is missing or refused: every surface then draws
 * no status at all, which is what `statusOn` / `waterStatusOn` in @app/core return for it.
 */
export function useStatusIndex(source: RegsSource, atlasUrl: string | undefined):
    StatusIndex | null {
  const got = useAsync(
    async () => {
      if (!atlasUrl) return null;
      const info = await source.info();
      return loadStatusIndex(statusIndexUrl(atlasUrl), info.sectionHandles, fetch,
                             info.reachDigest);
    },
    `status-index:${atlasUrl ?? ""}`,
    !!atlasUrl,
  );
  return got.state === "ready" ? got.value : null;
}

/**
 * The map's per-feature values for the status colouring: every section on screen, on `on`.
 *
 * EVERY visible section gets a value, base and none included — feature-state is sticky, so a
 * section that was closed yesterday and is base today must be told so, not left red. A
 * section with no status (tidal, outside B.C.) is set to null: the mode's `missing`.
 * The same values for streams and lakes, because feature-state is per layer.
 */
export function statusData(index: StatusIndex | null, visible: readonly SectionKey[],
                           on: Date): Record<string, Record<string, Record<string, unknown>>> {
  const values: Record<string, Record<string, unknown>> = {};
  for (const s of visible) values[String(s)] = { status: statusOn(index, s, on) };
  return { stream: values, lake: values };
}
