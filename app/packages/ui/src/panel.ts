/**
 * The hooks that fetch a panel and hand it to the arithmetic.
 *
 * THE ARITHMETIC ITSELF IS IN `@app/data` — see `panel.ts` there. It has three callers and
 * one of them is not a screen (`captureSpot` freezes an answer into a saved spot), so it
 * cannot live behind a hook.
 */
import { useMemo, useState } from "react";
import { answerFrom, reading, type BasinMember, type DonorRow, type Horizon, type Panel,
         type PanelAnswer, type Parameter, type Quantity, type RegsSource, type SectionId,
         type StationId } from "@app/data";
// The field combines its gauges with the SAME probit transform the reaches use — see
// `basinStanding`. A second averaging rule here is how a catchment and the river inside it
// start disagreeing for reasons that are only arithmetic.
import { basinStanding, type BasinVote } from "@app/core";
import { CONDITIONS_WORDS, disagreeNote, donorBecause, donorFacts, donorKeyLabel, donorLastReport,
         gaugeChoiceLabel, donorShare,
         donorPinTitle, donorReads, donorReason, donorRowLabel, donorWeightLine,
         estimateHeadline, estimateRange, estimateSentence, freshness, HORIZON_LABELS,
         HORIZONS, horizonChipLabel, metresApart, noGaugeSentence, panelCamera,
         readingAgeLine, readingHeading, readingStandingLine, regulatedNote, SAME_PLACE_M,
         shortRecordNote, stageLine, unplacedNote, type GaugeTrace, type SectionKey,
         type Standing } from "@app/core";
import { useAsync } from "./async";
import { useConditions, useGaugeTrace, useStationReading } from "./hooks";

/** Re-exported so a screen has one import for the whole subject. */
export { answerFrom, HORIZONS } from "@app/data";
export type { DonorRow, Horizon, PanelAnswer, Quantity } from "@app/data";

type Index = Parameters<typeof answerFrom>[1];

/** The live feed as the panel hooks read it: one call for the day's index. */
export type PanelFeed = {
  index(): Promise<Index>;
  /** A station's last report from its own file (`GaugeFeed.lastReport`), for a quiet gauge the index has dropped. */
  lastReport?(station: StationId): Promise<string | null>;
};

/** The quiet donors whose last report the index does not carry — the ones worth asking their own file about. */
export function lastReportWanted(rows: readonly DonorRow[]): string[] {
  return rows.filter((r) => r.percentile === null && !r.observedAt).map((r) => r.station);
}

/**
 * THE LAST REPORTS, onto the rows (owner 2026-10-21): only a quiet row with none of its own takes one; the index's word
 * always stands. Returns the same array when there is nothing to fill.
 */
export function withLastReports(rows: readonly DonorRow[], got: ReadonlyMap<string, string | null>): DonorRow[] {
  const want = new Set(lastReportWanted(rows));
  if (![...want].some((s) => got.get(s))) return rows as DonorRow[];
  return rows.map((r) => {
    const at = want.has(r.station) ? got.get(r.station) : null;
    return at ? { ...r, observedAt: at } : r;
  });
}

/** `withLastReports`, fetched: one file per quiet, index-less donor, through the feed's own cache. */
export function useLastReports(feed: PanelFeed | undefined, panel: PanelAnswer): PanelAnswer {
  const want = lastReportWanted(panel.rows);
  const key = `last:${want.join(",")}`;
  const got = useAsync(
    async () => new Map(await Promise.all(
      want.map(async (s) => [s, (await feed!.lastReport!(s as StationId).catch(() => null)) ?? null] as const))),
    key, !!feed?.lastReport && want.length > 0);
  const map = got.state === "ready" ? got.value : null;
  return useMemo(() => {
    if (!map) return panel;
    const rows = withLastReports(panel.rows, map);
    return rows === panel.rows ? panel : { ...panel, rows };
  }, [panel, map]);
}

/*
 * PANELS ARE READ ONCE PER SECTION, EVER.
 *
 * They come out of the bundle, which does not change while the app is running — but the
 * viewport does, constantly, and the query key is the viewport. So every pan re-read
 * panels the client already had, over SQLite in WebAssembly on top of range requests, and
 * the colour arrived a beat behind the map.
 *
 * `null` is cached as firmly as a panel: "nothing qualifies here" is an answer and re-asking
 * for it on every pan is the same waste. The feed is NOT cached here — it is the half that
 * changes, and it is one small fetch the async layer already dedupes.
 *
 * Bounded, because a long session over a whole province would otherwise hold every panel
 * in the bundle. Oldest-first eviction: a reader who has panned away is unlikely to be
 * about to pan back onto the very first reaches they saw.
 */
const PANEL_CACHE_MAX = 60_000;
const panelCache = new Map<SectionId, Panel | null>();

async function cached(source: RegsSource,
                      sections: readonly SectionId[]): Promise<ReadonlyMap<SectionId, Panel>> {
  const out = new Map<SectionId, Panel>();
  const missing: SectionId[] = [];
  for (const s of sections) {
    const hit = panelCache.get(s);
    if (hit === undefined) missing.push(s);
    else if (hit !== null) out.set(s, hit);
  }
  if (missing.length) {
    const got = await source.panelsFor(missing);
    for (const s of missing) {
      const panel = got.get(s) ?? null;
      panelCache.set(s, panel);
      if (panel) out.set(s, panel);
    }
    // Evict in insertion order — Map iterates oldest first, so this is one pass.
    if (panelCache.size > PANEL_CACHE_MAX) {
      const over = panelCache.size - PANEL_CACHE_MAX;
      let n = 0;
      for (const k of panelCache.keys()) {
        panelCache.delete(k);
        if (++n >= over) break;
      }
    }
  }
  return out;
}

/** Test seam: the cache is module state and a test must be able to start from empty. */
export function clearPanelCache(): void {
  panelCache.clear();
}

/**
 * THE MAP, COLOURED THE WAY THE SHEET ANSWERS.
 *
 * THIS AND `usePanel` ARE ONE CALCULATION. Both fetch panels and hand them to `answerFrom`
 * — the same function, not an equivalent one — so a reach's colour and the number in its
 * sheet are the same arithmetic run over different numbers of sections. There is no second
 * implementation for the two to drift apart in.
 *
 * It replaces a `useStandings` that coloured a reach from `section_gauge`, the ONE station
 * matched to it, and said nothing whenever that station was quiet, out of record or simply
 * absent. The failure: the Harrison drawn as unmeasured grey for its whole length except
 * one short reach, while a tap on that same grey opened a sheet reading "very low for the
 * time of year, fairly confident, from 2 gauges". Two tables, one question, and the map had
 * the worse answer. That hook is deleted rather than left beside this one — a superseded
 * path that still compiles is a path something will be wired back into.
 *
 * THE THREE OUTCOMES ARE UNCHANGED, because the map's legend depends on them:
 *
 *   absent   nothing can speak for this reach   -> drawn as unmeasured water
 *   -0.01    a panel exists but answered today's question with nothing
 *   0..1     a real percentile
 *
 * Absent is never zero. Zero is the bottom of the scale and would paint every ungauged
 * creek as a river in drought.
 */
export function usePanelStandings(
  source: RegsSource,
  feed: { index(): Promise<Index> } | undefined,
  sections: readonly SectionId[],
  quantity: Quantity = "discharge",
  horizon: Horizon = 0,
): ReadonlyMap<SectionId, number> {
  // Keyed on the viewport's extent rather than its contents: a map that has not moved
  // re-renders constantly and the section list is a new array every time.
  const key = (sections.length
    ? `${sections.length}:${sections[0]}:${sections[sections.length - 1]}` : "")
    + `:${quantity}:${horizon}`;
  const got = useAsync(
    async (): Promise<ReadonlyMap<SectionId, number>> => {
      const out = new Map<SectionId, number>();
      if (!feed || !sections.length) return out;
      const [panels, lakes, idx] = await Promise.all([
        cached(source, sections),
        source.lakeStationsFor(sections),
        feed.index(),
      ]);
      if (!idx) return out;             // offline: colour nothing rather than colour wrong
      /*
       * A GAUGED LAKE IS COLOURED BY ITS OWN READING, and an ungauged one is not coloured
       * at all.
       *
       * There is no panel for a lake and there must not be: a panel carries a reading
       * between catchments on the argument that they share drainage, and a lake's stage is
       * set by its outlet and its own storage. So a station in the lake speaks for it
       * exactly, and nothing speaks for a lake without one.
       *
       * IN THE QUANTITY BEING ASKED FOR, like everything else here. A lake station usually
       * reports a level and not a discharge; under "Flow" such a lake has no answer and
       * must stay grey rather than borrow its own stage under a flow legend.
       */
      for (const [section, station] of lakes) {
        const row = idx.stations[station as string];
        if (!row) continue;                        // not transmitting: say nothing
        const p = reading(row, quantity, horizon);
        // A gauged lake with nothing to say today is the sentinel, not absence: somebody
        // measures here and today it cannot tell you.
        out.set(section, typeof p === "number" ? p : -0.01);
      }
      for (const [section, panel] of panels) {
        // ONE QUANTITY, THE READER'S. This used to ask each panel in whatever quantity its
        // own donors led with, which coloured the most water and made the ramp compare a
        // stage percentile against a discharge one — see `Quantity` in @app/data for the
        // measurement that retired it. A station that does not measure what was asked says
        // nothing, which is the honest answer and is drawn as such.
        const { answer } = answerFrom(panel, idx, quantity, horizon);
        out.set(section, answer.ok ? answer.value.percentile : -0.01);
      }
      return out;
    },
    `panelstandings:${key}`,
    sections.length > 0 && feed !== undefined,
  );
  const none = useMemo(() => new Map<SectionId, number>(), []);
  return got.state === "ready" ? got.value : none;
}

/**
 * THE PROVINCE AS A FIELD — one value per catchment, for z4–8.
 *
 * ONE PATHWAY WITH THE RIVERS. It reads the same feed index through the same `reading`
 * function, in the same quantity and at the same horizon, so a catchment and the river
 * inside it cannot disagree about what the water is doing. That was the failure worth
 * designing against: two joins to one feed is how the map and the sheet ended up answering
 * differently for the Harrison.
 *
 * WHAT IT DOES NOT SHARE is the join, because the questions are different. A section asks a
 * donor PANEL — several gauges of comparable catchment, weighted by measured error. A
 * catchment asks the one station that measures it or the one measuring the country it
 * drains into, and reports how far that reading travelled. Running the panel arithmetic
 * over a basin would give the field a precision it has not got.
 *
 * THE WHOLE TABLE, ONCE. 9,642 rows, cached for the session like the panels: a viewport at
 * z5 is a third of the province, so scoping it to the view would re-read most of it on
 * every pan to save nothing.
 */
export function useBasinStandings(
  source: RegsSource,
  feed: { index(): Promise<Index> } | undefined,
  enabled: boolean,
  quantity: Quantity = "discharge",
  horizon: Horizon = 0,
): ReadonlyMap<string, number> {
  const got = useAsync(
    async (): Promise<ReadonlyMap<string, number>> => {
      const out = new Map<string, number>();
      if (!feed) return out;
      const [basins, idx] = await Promise.all([cachedBasins(source), feed.index()]);
      if (!idx) return out;           // offline: colour nothing rather than colour wrong
      for (const [basin, members] of basins) {
        /*
         * EVERY GAUGE IN THE GROUP, not the biggest one. Electing a representative made the
         * group's colour hostage to that station: Chilliwack has six gauges and went grey
         * because the one named happened to be quiet. A group now stays coloured while ANY
         * of its gauges reports, and `basinStanding` decides how much each one counts.
         */
        const votes: BasinVote[] = [];
        for (const m of members) {
          const r = reading(idx.stations[m.station as string], quantity, horizon);
          if (typeof r === "number")
            votes.push({ percentile: r, areaKm2: m.areaKm2, years: m.years });
        }
        const p = basinStanding(votes);
        /*
         * A GROUP WITH NOTHING TO SAY IS LEFT OUT, not marked with the sentinel.
         *
         * On a reach the sentinel is worth drawing — "a gauge reports here and has no
         * record to rank it against" is a state you can tap and be told about. On a
         * 3,600 km2 region it is a purple blotch the size of a valley that a reader cannot
         * interrogate, and it reads as chaos across the province. Absent means the same
         * unmeasured grey a group with no gauge gets, which at this scale is the truthful
         * pairing: at a province on screen, "we cannot say" is one answer, not two.
         */
        if (typeof p === "number") out.set(basin, p);
      }
      return out;
    },
    `basins:${quantity}:${horizon}:${enabled}`,
    enabled && feed !== undefined,
  );
  const none = useMemo(() => new Map<string, number>(), []);
  return got.state === "ready" ? got.value : none;
}

/** The station-per-catchment table, read once. It comes from the bundle and cannot change. */
let basinCache: ReadonlyMap<string, readonly BasinMember[]> | null = null;
async function cachedBasins(source: RegsSource) {
  if (basinCache === null) basinCache = await source.basinMembers();
  return basinCache;
}

/** Test seam: the cache is module state and a test must be able to start from empty. */
export function clearBasinCache(): void {
  basinCache = null;
}

/** The panel for one section, joined against today's readings. */
export function usePanel(
  source: RegsSource,
  feed: { index(): Promise<Index> } | undefined,
  section: SectionId | null,
  quantity: Quantity = "discharge",
  horizon: Horizon = 0,
): PanelAnswer {
  const got = useAsync(
    async () => {
      if (!section) return null;
      const [panels, idx] = await Promise.all([
        // THE SAME FETCH THE MAP USES, cache and all. It called `source.panelsFor`
        // directly, which is a second door to one table — and a second door is a place the
        // two can differ. There is one now.
        cached(source, [section]),
        feed ? feed.index() : Promise.resolve(null),
      ]);
      return answerFrom(panels.get(section), idx, quantity, horizon);
    },
    `panel:${section ?? ""}:${quantity}:${horizon}`,
    section !== null,
  );
  const none = useMemo<PanelAnswer>(
    () => ({ answer: { ok: false, why: "no-station" }, rows: [], areaKm2: null,
             routesReady: false }), []);
  return got.state === "ready" && got.value ? got.value : none;
}

/**
 * The same answer, with each donor placed on the map.
 *
 * SEPARATE FROM `usePanel` AND NOT FOLDED INTO IT, because the routes are several walks
 * down the pointer table and the panel is one indexed read. The number and the table must
 * appear as soon as the reading does; the map catching up a moment later costs nothing,
 * whereas making the headline wait on a graph walk would be visible on every tap.
 *
 * Rows keep the weight ORDER `usePanel` produced, so the map's key, the table and the
 * arithmetic are all the same list in the same sequence.
 */
export function usePanelRoutes(source: RegsSource, section: SectionId | null,
                               panel: PanelAnswer): PanelAnswer {
  const got = useAsync(
    async () => (section ? await source.panelRoutes(section) : []),
    `routes:${section ?? ""}`,
    section !== null,
  );
  const routes = got.state === "ready" ? got.value : null;
  return useMemo<PanelAnswer>(() => {
    if (!routes) return panel;                    // still walking; `routesReady` stays false
    const by = new Map(routes.map((r) => [r.station as string, r]));
    return { ...panel, routesReady: true,
             rows: panel.rows.map((r) => ({ ...r, route: by.get(r.station) })) };
  }, [routes, panel]);
}

/* ============================================================ CONDITIONS VIEW ====
 *
 * P12a: what `DonorPanel` and `ConditionsPanel` draw, finished. The sentences are core's
 * (`conditions.ts`, pinned whole); the CHOICES — which station leads, which notes appear,
 * which day the range is for — are made here, so the components only draw.
 */

/** One donor row, every line finished. */
export interface DonorRowVM {
  station: string;
  name: string | null;
  /** Not reporting: drawn in the quiet tone, contributes nothing. */
  quiet: boolean;
  /** The row's chart is the one showing. */
  on: boolean;
  /** The weight bar's width, never below 1% so a real share stays visible. */
  bar: `${number}%`;
  weightLine: string;
  reads: string;
  /** A quiet gauge's last report, local and labelled — or that it is not known. Null for a reporting gauge. */
  lastReport: string | null;
  facts: string;
  /** Why it counts what it counts; null for a quiet gauge, which counts nothing. */
  because: string | null;
  /** The row's screen-reader name. */
  label: string;
}

/** A pin or key entry: `row` is the donor's index, null for "you are here". */
export interface DonorPinVM { lat: number; lon: number; row: number | null; title: string }
export interface DonorKeyVM { row: number | null; label: string }

export interface DonorMapVM {
  camera: { lon: number; lat: number; zoom: number };
  /** Remounts the map only when the frame genuinely changes (a map reads its camera once). */
  key: string;
  pins: readonly DonorPinVM[];
  keys: readonly DonorKeyVM[];
  unplaced: string | null;
  /** The reaches between here and the gauges, unioned. */
  chain: readonly SectionKey[];
}

export interface HorizonChipVM { day: Horizon; chip: string; label: string; on: boolean }

/**
 * THE HORIZON CHIPS, for every control that offers them — the map's corner (Conditions tab)
 * and the panel's header. One function over core's HORIZONS and words, so the two controls
 * cannot list different days or name one differently (P16's worry about a second copy).
 */
export function horizonChips(on: Horizon): HorizonChipVM[] {
  return HORIZONS.map((d) => ({ day: d, chip: HORIZON_LABELS[d].chip,
                                label: horizonChipLabel(d, d === on), on: d === on }));
}

/** The one horizon state the Shell holds and both controls set — see `useHorizon`. */
export interface HorizonState {
  horizon: Horizon;
  chips: readonly HorizonChipVM[];
  pick: (d: Horizon) => void;
}

/**
 * ONE HORIZON, TWO CONTROLS (owner 2026-10-20). The map's corner control and the panel's
 * header both draw `chips` and call `pick`, so choosing +10 on the map is what the panel
 * opens on, and stepping back to Now in the panel is what the map then paints. 0 is where
 * it opens: a forecast is what you ask for, never what you are shown without asking.
 */
export function useHorizon(initial: Horizon = 0): HorizonState {
  const [horizon, pick] = useState<Horizon>(initial);
  const chips = useMemo(() => horizonChips(horizon), [horizon]);
  return { horizon, chips, pick };
}

export type DonorVM =
  | {
      ok: false; heading: string; reason: string;
      /** The chips, kept when the refusal is about the CHOSEN DAY (`no-forecast`), so the
       *  reader can step back from it; null when another day would not change the answer. */
      horizons: readonly HorizonChipVM[] | null;
    }
  | {
      ok: true;
      heading: string;
      headline: string;
      /** "by then", drawn quieter after the headline, on a forecast; null for now. */
      headlineSuffix: string | null;
      sentence: string;
      range: string;
      horizons: readonly HorizonChipVM[];
      /** Null until the routes are in, or when no donor can be placed. */
      map: DonorMapVM | null;
      tapToChart: string;
      listLabel: string;
      rows: readonly DonorRowVM[];
      shortRecord: string | null;
      regulated: string | null;
      disagree: string | null;
      explainer: string;
    };

export interface DonorViewOptions {
  /** The CHOSEN day: today on a live sheet, the visit on a saved spot. Never the clock. */
  day: Date;
  horizon: Horizon;
  /** Where the reader is, for "you are here". */
  from: { lat: number; lon: number } | null;
  /** The route map's measured size; the camera fits each axis to its own dimension. */
  size: { width: number; height: number };
  selected: StationId | null;
  /** Rows are controls (the caller has a chart to steer). */
  pickable: boolean;
  /** The time zone times are said in — for tests; left out, the device's. */
  zone?: string;
}

/** The donor panel's view model, from an answer. Pure: a saved spot builds it from frozen data. */
export function donorView(value: PanelAnswer, o: DonorViewOptions): DonorVM {
  const { answer, rows } = value;
  if (!answer.ok)
    return { ok: false, heading: CONDITIONS_WORDS.noEstimate, reason: donorReason(answer.why),
             horizons: answer.why === "no-forecast" ? horizonChips(o.horizon) : null };
  const e = answer.value;

  const placed = rows.filter((r) => r.route?.lat != null && r.route?.lon != null);
  const camera = panelCamera(placed.map((r) => ({ lat: r.route!.lat, lon: r.route!.lon })),
                             o.from, o.size);
  // A gauge AT the tap absorbs "you are here" — two pins on one pixel read as one lost.
  const from = o.from;
  const atSpot = from
    ? placed.find((r) => metresApart(from, { lat: r.route!.lat!, lon: r.route!.lon! })
                         <= SAME_PLACE_M)
    : undefined;
  const showFrom = from && !atSpot ? from : null;
  const here = CONDITIONS_WORDS.youAreHere;
  const map: DonorMapVM | null = value.routesReady && camera ? {
    camera,
    key: `${camera.lon.toFixed(3)},${camera.lat.toFixed(3)},${camera.zoom.toFixed(1)}`,
    pins: [
      ...(showFrom ? [{ lat: showFrom.lat, lon: showFrom.lon, row: null, title: here }] : []),
      ...placed.map((r) => ({ lat: r.route!.lat!, lon: r.route!.lon!, row: rows.indexOf(r),
                              title: donorPinTitle(r, r === atSpot) })),
    ],
    keys: [
      ...(showFrom ? [{ row: null, label: here }] : []),
      ...placed.map((r) => ({ row: rows.indexOf(r), label: donorKeyLabel(r, r === atSpot) })),
    ],
    unplaced: unplacedNote(rows.length - placed.length),
    chain: [...new Set(rows.flatMap((r) => r.route?.path ?? []))],
  } : null;

  return {
    ok: true,
    heading: HORIZON_LABELS[o.horizon].heading,
    headline: estimateHeadline(e),
    headlineSuffix: o.horizon > 0 ? CONDITIONS_WORDS.byThen : null,
    sentence: estimateSentence(e, o.horizon),
    range: estimateRange(e, o.day, o.horizon, value.areaKm2),
    horizons: horizonChips(o.horizon),
    map,
    tapToChart: CONDITIONS_WORDS.tapToChart,
    listLabel: CONDITIONS_WORDS.donorList,
    rows: rows.map((r: DonorRow): DonorRowVM => {
      const on = o.selected != null && r.station === o.selected;
      return {
        station: r.station, name: r.route?.name ?? null, quiet: r.percentile === null, on,
        bar: `${Math.max(1, Math.round(r.weight * 100))}%`,
        weightLine: donorWeightLine(r),
        reads: donorReads(r, { ref: o.day.getTime(), horizon: o.horizon }),
        lastReport: r.percentile === null ? donorLastReport(r.observedAt, o.zone) : null,
        facts: donorFacts(r),
        because: r.percentile === null ? null : donorBecause(r),
        label: donorRowLabel(r, o.pickable, on),
      };
    }),
    shortRecord: shortRecordNote(rows),
    regulated: regulatedNote(rows),
    disagree: disagreeNote(e),
    explainer: CONDITIONS_WORDS.explainer,
  };
}

/** One choice in the chart's gauge switcher (owner 2026-10-21: the switch sits by the plot it controls). */
export interface GaugeChoiceVM {
  station: string;
  name: string | null;
  /** "35%", "<1%" or "quiet" — the same share the donor list and the map's key say. */
  share: string;
  quiet: boolean;
  on: boolean;
  /** The donor's index, so the switcher's dot is its pin's and its row's colour. */
  row: number;
  label: string;
}

/**
 * THE GAUGE SWITCHER, beside the chart: every donor in the panel's weight order, the one the chart shows marked. Empty
 * when there is nothing to switch between (one gauge, or none) — a single option is not a choice.
 */
export function gaugeChoices(rows: readonly DonorRow[], lead: StationId | null): GaugeChoiceVM[] {
  if (rows.length < 2) return [];
  return rows.map((r, i) => {
    const on = r.station === lead;
    return { station: r.station, name: r.route?.name ?? null, share: donorShare(r),
             quiet: r.percentile === null, on, row: i, label: gaugeChoiceLabel(r, on) };
  });
}

/** `donorView`, memoised for a component. */
export function useDonorView(value: PanelAnswer, o: DonorViewOptions): DonorVM {
  const { day, horizon, from, size, selected, pickable } = o;
  const t = day.getTime();
  const lat = from?.lat;
  const lon = from?.lon;
  const { width, height } = size;
  return useMemo(
    () => donorView(value, { day: new Date(t), horizon,
                             from: lat != null && lon != null ? { lat, lon } : null,
                             size: { width, height }, selected, pickable }),
    [value, t, horizon, lat, lon, width, height, selected, pickable]);
}

/** The lead station's own reading, finished. */
export interface ReadingVM {
  heading: string;
  figure: number;
  unit: string;
  /** Which quantity `figure` is. */
  quantity: Parameter;
  /** The stage beside a discharge, where the station has both. */
  stage: string | null;
  standing: string;
  age: string;
}

export interface ConditionsVM {
  /** The matched gauge is still being looked up. */
  loading: boolean;
  /** The day everything is for — the caller's, or today when it chose none. */
  day: Date;
  /** The donor answer with its routes, for `DonorPanel`. */
  panel: PanelAnswer;
  /** The reaches between here and every donor. */
  chain: readonly SectionKey[];
  /** Each of those reaches coloured by its own percentile, for the route map. */
  routeData: { stream: Record<string, { standing: number }>;
               lake: Record<string, { standing: number }> };
  /** The station the chart and the reading are about. */
  lead: StationId | null;
  select: (station: StationId) => void;
  /** Null when no gauge may speak for this water; draw `noGauge` instead. */
  reading: ReadingVM | null;
  noGauge: string;
  /** The single-station trace, only where the panel could not answer. */
  trace: GaugeTrace | null;
  /** The chart's gauge switcher — see `gaugeChoices`. */
  gauges: readonly GaugeChoiceVM[];
}

/**
 * WHAT THE WATER IS DOING, finished for the conditions panel.
 *
 * `day` is the chosen day (null: today, read once per mount); `horizon` is how far past it.
 * `opts` carries the live feed and the quantity the map is painting, so the sheet answers
 * the same question the colour does.
 */
export function useConditionsView(
  source: RegsSource, section: SectionId | null, day: Date | null, horizon: Horizon,
  opts: { feed: PanelFeed | undefined; quantity: Quantity },
): ConditionsVM {
  const { feed, quantity } = opts;
  const conditions = useConditions(source, section);
  const traced = useGaugeTrace(source, section);
  const panel = useLastReports(feed, usePanelRoutes(source, section,
                                                    usePanel(source, feed, section, quantity, horizon)));
  const chain = useMemo<readonly SectionKey[]>(
    () => [...new Set(panel.rows.flatMap((r) => r.route?.path ?? []))], [panel.rows]);
  const standings = usePanelStandings(source, feed, chain as readonly SectionId[], quantity,
                                      horizon);
  const routeData = useMemo(() => {
    const values = Object.fromEntries(
      [...standings].map(([sec, p]) => [sec, { standing: p * 100 }]));
    // `standing` is keyed by SECTION, and a lake section is a section.
    return { stream: values, lake: values };
  }, [standings]);

  const matched = conditions.state === "ready" ? conditions.value : null;
  /*
   * THE CHART FOLLOWS THE PANEL: the picked donor, else the heaviest reporting one (rows
   * are in weight order), else any donor, else the one station matched to this reach.
   */
  const [picked, select] = useState<StationId | null>(null);
  const lead: StationId | null = picked
    ?? panel.rows.find((r) => r.percentile !== null)?.station
    ?? panel.rows[0]?.station
    ?? matched?.station
    ?? null;
  const got = useStationReading(source, lead);
  const live = got.state === "ready" ? got.value : null;
  const name = panel.rows.find((r) => r.station === lead)?.route?.name
    ?? (lead === matched?.station ? matched?.stationName : null)
    ?? lead ?? "";
  const c = lead === matched?.station && !live?.fetchedAt ? matched : live;

  const today = useMemo(() => day ?? new Date(), [day]);
  let reading: ReadingVM | null = null;
  if (c && (c.discharge != null || c.level != null)) {
    const shown: Parameter = c.discharge != null ? "discharge" : "level";
    reading = {
      heading: readingHeading(name),
      figure: (c.discharge ?? c.level)!,
      unit: CONDITIONS_WORDS.unit[shown],
      quantity: shown,
      stage: c.discharge != null && c.level != null ? stageLine(c.level) : null,
      standing: readingStandingLine(c.percentile, c.standing as Standing | null),
      age: readingAgeLine(freshness(c.fetchedAt, Date.now()), c.observedAt),
    };
  }

  return {
    loading: conditions.state === "loading",
    day: today, panel, chain, routeData, lead, select, reading,
    noGauge: noGaugeSentence(),
    trace: !panel.answer.ok && traced.state === "ready" && traced.value.station
      ? traced.value : null,
    gauges: gaugeChoices(panel.rows, lead),
  };
}
