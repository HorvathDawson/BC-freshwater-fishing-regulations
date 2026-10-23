/**
 * The hooks that fetch a panel and hand it to the arithmetic.
 *
 * THE ARITHMETIC ITSELF IS IN `@app/data` — see `panel.ts` there. It has three callers and
 * one of them is not a screen (`captureSpot` freezes an answer into a saved spot), so it
 * cannot live behind a hook.
 */
import { useMemo, useRef, useState } from "react";
import { answerFrom, basinStandings, sectionStanding, type BasinMember, type DonorRow, type Horizon, type Panel,
         type PanelAnswer, type Parameter, type Quantity, type RegsSource, type SectionId,
         type StationId } from "@app/data";
import { CHANGE_POLICY, changeDay, changeLine, changeSparkWords, CONDITIONS_WORDS, disagreeNote, donorBecause, isAhead, type RunChange, donorFacts, donorKeyLabel, donorLastReport,
         donorSilent, gaugeChoiceLabel, donorShare, silentLine,
         donorPinTitle, donorReads, donorReason, donorRowLabel, donorWeightLine,
         estimateHeadline, estimateRange, estimateSentence, freshness, HORIZON_LABELS,
         HORIZONS, horizonChipLabel, metresApart, noGaugeSentence, panelCamera,
         readingAgeLine, readingHeading, readingStandingLine, regulatedNote, SAME_PLACE_M, tidalNote,
         shortRecordNote, stageLine, unplacedNote, type GaugeTrace, type SectionKey,
         type Standing } from "@app/core";
import { donorMapKey, donorMapLabel, hereMapLabel, ROUTE_MAP_WORDS,
         routeLegend } from "@app/core";
import { useAsync } from "./async";
import { useConditions, useGaugeTrace, useStationReading } from "./hooks";
import { pushedRoute, routeLabels, routeValues, type RouteColourVM } from "./routeColour";

import type { FlowParam } from "./mapModes";

/** Re-exported so a screen has one import for the whole subject. */
export { answerFrom, HORIZONS } from "@app/data";
export type { DonorRow, Horizon, PanelAnswer, Quantity } from "@app/data";

type Index = Parameters<typeof answerFrom>[1];

/** The live feed as the panel hooks read it: one call for the day's index. */
export type PanelFeed = {
  index(): Promise<Index>;
  /** A station's last report from its own file (`GaugeFeed.lastReport`), for a quiet gauge the index has dropped. */
  lastReport?(station: StationId): Promise<string | null | undefined>;
};

/**
 * The quiet donors whose last report the index does not carry — the ones worth asking their own file about. A file
 * that was read and holds nothing (null) marks its row `noData`, so the silent line can say the station "has never
 * reported"; a file that could not be read (undefined: a 404, offline) leaves the row as it was — not known.
 */
export function lastReportWanted(rows: readonly DonorRow[]): string[] {
  return rows.filter((r) => r.percentile === null && !r.observedAt).map((r) => r.station);
}

/**
 * THE LAST REPORTS, onto the rows (owner 2026-10-21): only a quiet row with none of its own takes one; the index's word
 * always stands. Returns the same array when there is nothing to fill.
 */
export function withLastReports(rows: readonly DonorRow[],
                                got: ReadonlyMap<string, string | null | undefined>): DonorRow[] {
  const want = new Set(lastReportWanted(rows));
  const heard = (s: string) => want.has(s) && got.get(s) !== undefined;
  if (![...want].some(heard)) return rows as DonorRow[];
  return rows.map((r) => {
    if (!heard(r.station)) return r;
    const at = got.get(r.station);
    return at ? { ...r, observedAt: at } : { ...r, noData: true as const };
  });
}

/** `withLastReports`, fetched: one file per quiet, index-less donor, through the feed's own cache. */
export function useLastReports(feed: PanelFeed | undefined, panel: PanelAnswer): PanelAnswer {
  const want = lastReportWanted(panel.rows);
  const key = `last:${want.join(",")}`;
  const got = useAsync(
    async () => new Map(await Promise.all(
      want.map(async (s) => [s, await feed!.lastReport!(s as StationId).catch(() => undefined)] as const))),
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
      /*
       * ONE QUANTITY, THE READER'S, and ONE READING (`sectionStanding` in @app/data): the per-tile class path the web map
       * takes computes its values with the same function, so the two maps cannot colour a section differently. A panel
       * wins over a lake station; a gauged lake with nothing to say today is the sentinel, not absence.
       */
      for (const section of new Set<SectionId>([...lakes.keys(), ...panels.keys()])) {
        const p = sectionStanding(panels.get(section), lakes.get(section), idx, quantity, horizon);
        if (p !== null) out.set(section, p);
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
      if (!feed) return new Map<string, number>();
      const [basins, idx] = await Promise.all([cachedBasins(source), feed.index()]);
      /*
       * EVERY GAUGE IN THE GROUP (`basinStanding`), and a group with nothing to say LEFT OUT rather than marked with
       * the sentinel — `basinStandings` in @app/data, the one reading the web map's per-tile path computes its field with.
       */
      return basinStandings(basins, idx, quantity, horizon);
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
  /**
   * The member's index in the panel's weight order (`PanelAnswer.rows`) — its TONE. The same index its pin, its route,
   * its key and its switch chip carry, so a row and its pin are one colour though silent members are not drawn.
   */
  row: number;
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

/**
 * A mark on the route map: `row` is the donor's index, null for "you are here".
 *
 * `kind` is the SHAPE (owner 2026-10-21: the spot must not look like a gauge); `label` is
 * what is written beside it on the map — a gauge's share, the spot's words; `rank` orders
 * the labels, heaviest gauge first, so a crowded valley keeps the label that matters most.
 */
export interface DonorPinVM {
  lat: number; lon: number; row: number | null; title: string;
  kind: "here" | "gauge"; label: string | null; rank: number;
}
export interface DonorKeyVM { row: number | null; label: string }

/** One donor's route, for the overlay: its chain of reaches, spot first, and both ends. */
export interface DonorRouteVM {
  row: number;
  /** Where the GAUGE sits: downstream is drawn solid, upstream dashed. */
  role: "up" | "down";
  path: readonly SectionKey[];
  from: { lat: number; lon: number } | null;
  to: { lat: number; lon: number };
}

export interface DonorMapVM {
  camera: { lon: number; lat: number; zoom: number };
  /** Remounts the map only when the frame genuinely changes (a map reads its camera once). */
  key: string;
  pins: readonly DonorPinVM[];
  keys: readonly DonorKeyVM[];
  unplaced: string | null;
  /** The reaches between here and the gauges, unioned. */
  chain: readonly SectionKey[];
  /** Each placed donor's route, in weight order — what the overlay copies out of the tiles. */
  routes: readonly DonorRouteVM[];
  /** The key's line samples: only the roles drawn. */
  legend: readonly { role: "up" | "down"; label: string }[];
  /** Every mark's extent [w, s, e, n] — what "fit" returns to. */
  bbox: readonly [number, number, number, number];
  words: { fit: string; fitLabel: string; larger: string; smaller: string };
}

export interface HorizonChipVM { day: Horizon; chip: string; label: string; on: boolean }

/**
 * THE HORIZON CHIPS, for every control that offers them — the map's corner (Conditions tab)
 * and the panel's header. One function over core's HORIZONS and words, so the two controls
 * cannot list different days or name one differently (P16's worry about a second copy).
 */
export function horizonChips(on: Horizon, quantity?: FlowParam): HorizonChipVM[] {
  // THE CHANGE CHIP IS FLOW ONLY (T52, owner: "never mix flow and level"): the publisher writes no change in level, so
  // under Level (or Temperature) it is not offered rather than offered and blank.
  return HORIZONS.filter((d) => d !== "change" || quantity === undefined || quantity === "discharge")
    .map((d) => ({ day: d, chip: HORIZON_LABELS[d].chip, label: horizonChipLabel(d, d === on), on: d === on }));
}

/**
 * THE HORIZON THE MAP AND PANEL ACT ON for this quantity: the chosen one, except Change outside Flow, which answers as
 * Now. The choice itself is kept, so switching back to Flow returns to Change — no effect, no reset.
 */
export function flowHorizon(h: Horizon, quantity: FlowParam): Horizon {
  return h === "change" && quantity !== "discharge" ? 0 : h;
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
      /** Every member is silent (no-record, no-forecast): the line naming them. Null with no members. */
      silent: string | null;
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
      /** Only the members that speak at this horizon; the silent ones are in `silent`. */
      rows: readonly DonorRowVM[];
      /** "Not reporting: …" — the members hidden from the table and the map, each with its last data time. */
      silent: string | null;
      shortRecord: string | null;
      regulated: string | null;
      /** The tide (T78): one sentence, Level only, where a reporting member's station is tidal; else null. */
      tidal: string | null;
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
  /**
   * The quantity this answer is in — the one the map is painting. Only the tide's sentence reads it (Level only,
   * T78); left out (a saved spot, which did not record it), the tide is not said.
   */
  quantity?: Quantity;
}

/** The donor panel's view model, from an answer. Pure: a saved spot builds it from frozen data. */
export function donorView(value: PanelAnswer, o: DonorViewOptions): DonorVM {
  const { answer, rows } = value;
  /*
   * SILENT MEMBERS ARE HIDDEN, AND NAMED (owner 2026-10-22). A member with no reading at this horizon is not a row and
   * not a pin; `silentLine` names it under the table with its last data time. Core decides which are silent (the same
   * rule `donorReads` words), against the CHOSEN day.
   */
  const ref = o.day.getTime();
  const silent = silentLine(rows, { ref, horizon: o.horizon, ...(o.zone ? { zone: o.zone } : {}) });
  if (!answer.ok)
    return { ok: false, heading: CONDITIONS_WORDS.noEstimate, reason: donorReason(answer.why, o.horizon),
             horizons: answer.why === "no-forecast" ? horizonChips(o.horizon, o.quantity) : null, silent };
  const e = answer.value;
  /*
   * A ROW'S TONE IS ITS INDEX IN THE PANEL'S WEIGHT ORDER, not its place among the rows shown. The gauge switch lists
   * every member by that index, so the switch's dot, the row and the pin are one colour; and silent members weigh 0
   * and sort last, so the speaking ones keep the first tones anyway — a hidden member never shifts a shown one's hue.
   */
  const tone = new Map(rows.map((r, i) => [r, i]));
  const shown = rows.filter((r) => !donorSilent(r, { ref, horizon: o.horizon }));
  const toneOf = (r: DonorRow) => tone.get(r)!;

  const placed = shown.filter((r) => r.route?.lat != null && r.route?.lon != null);
  const camera = panelCamera(placed.map((r) => ({ lat: r.route!.lat, lon: r.route!.lon })),
                             o.from, o.size);
  // A gauge AT the tap absorbs "you are here" — two pins on one pixel read as one lost.
  const from = o.from;
  const atSpot = from
    ? placed.find((r) => metresApart(from, { lat: r.route!.lat!, lon: r.route!.lon! })
                         <= SAME_PLACE_M)
    : undefined;
  const here = CONDITIONS_WORDS.youAreHere;
  /*
   * THE SPOT IS ALWAYS MARKED NOW, gauge at the spot or not. It used to be absorbed into a
   * gauge within `SAME_PLACE_M`, because both were the same teardrop and two on one pixel
   * read as one lost. The spot is a pin STANDING on the place and a gauge a dot lying on it
   * (owner 2026-10-21: different icons), so both show: the dot at the pin's point. The
   * absorbing words stay on the gauge's own title — it IS the gauge you are standing at.
   */
  const pts = [...placed.map((r) => ({ lat: r.route!.lat!, lon: r.route!.lon! })),
               ...(from ? [from] : [])];
  const drawn = placed.filter((r) => (r.route?.path.length ?? 0) > 0);
  const map: DonorMapVM | null = value.routesReady && camera ? {
    camera,
    key: `${camera.lon.toFixed(3)},${camera.lat.toFixed(3)},${camera.zoom.toFixed(1)}`,
    pins: [
      ...placed.map((r): DonorPinVM => {
        const row = toneOf(r);
        return { lat: r.route!.lat!, lon: r.route!.lon!, row, kind: "gauge",
                 title: donorPinTitle(r, r === atSpot), label: donorMapLabel(r, { ref, horizon: o.horizon }), rank: row };
      }),
      // LAST, so it draws over a gauge at the same place.
      ...(from ? [{ lat: from.lat, lon: from.lon, row: null, title: here, kind: "here" as const,
                    label: hereMapLabel(), rank: -1 }] : []),
    ],
    keys: [
      ...(from ? [{ row: null, label: here }] : []),
      ...placed.map((r) => ({ row: toneOf(r), label: donorMapKey(r, { ref, horizon: o.horizon }) })),
    ],
    unplaced: unplacedNote(shown.length - placed.length),
    chain: [...new Set(shown.flatMap((r) => r.route?.path ?? []))],
    routes: drawn.map((r) => ({ row: toneOf(r), role: r.role, path: r.route!.path,
                                from, to: { lat: r.route!.lat!, lon: r.route!.lon! } })),
    legend: routeLegend(drawn.map((r) => r.role)),
    bbox: [Math.min(...pts.map((p) => p.lon)), Math.min(...pts.map((p) => p.lat)),
           Math.max(...pts.map((p) => p.lon)), Math.max(...pts.map((p) => p.lat))],
    words: { fit: ROUTE_MAP_WORDS.fit, fitLabel: ROUTE_MAP_WORDS.fitLabel,
             larger: ROUTE_MAP_WORDS.larger, smaller: ROUTE_MAP_WORDS.smaller },
  } : null;

  return {
    ok: true,
    heading: HORIZON_LABELS[o.horizon].heading,
    headline: estimateHeadline(e, o.horizon),
    // "by then" is about a DAY; Change spans the window and its heading says so
    headlineSuffix: isAhead(o.horizon) && o.horizon !== "change" ? CONDITIONS_WORDS.byThen : null,
    sentence: estimateSentence(e, o.horizon),
    range: estimateRange(e, o.day, o.horizon, value.areaKm2),
    horizons: horizonChips(o.horizon, o.quantity),
    map,
    tapToChart: CONDITIONS_WORDS.tapToChart,
    listLabel: CONDITIONS_WORDS.donorList,
    rows: shown.map((r: DonorRow): DonorRowVM => {
      const on = o.selected != null && r.station === o.selected;
      return {
        station: r.station, row: toneOf(r), name: r.route?.name ?? null, quiet: r.percentile === null, on,
        bar: `${Math.max(1, Math.round(r.weight * 100))}%`,
        weightLine: donorWeightLine(r),
        reads: donorReads(r, { ref: o.day.getTime(), horizon: o.horizon }),
        lastReport: r.percentile === null ? donorLastReport(r.observedAt, o.zone) : null,
        facts: donorFacts(r),
        because: r.percentile === null ? null : donorBecause(r),
        label: donorRowLabel(r, o.pickable, on, { ref, horizon: o.horizon }),
      };
    }),
    silent,
    shortRecord: shortRecordNote(rows),
    regulated: regulatedNote(rows),
    tidal: o.quantity ? tidalNote(rows, o.quantity) : null,
    disagree: disagreeNote(e, o.horizon),
    explainer: o.horizon === "change" ? `${CONDITIONS_WORDS.changeExplainer} ${CONDITIONS_WORDS.explainer}`
      : CONDITIONS_WORDS.explainer,
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
  const { day, horizon, from, size, selected, pickable, quantity } = o;
  const t = day.getTime();
  const lat = from?.lat;
  const lon = from?.lon;
  const { width, height } = size;
  return useMemo(
    () => donorView(value, { day: new Date(t), horizon,
                             from: lat != null && lon != null ? { lat, lon } : null,
                             size: { width, height }, selected, pickable,
                             ...(quantity ? { quantity } : {}) }),
    [value, t, horizon, lat, lon, width, height, selected, pickable, quantity]);
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

/** A quiet donor at the head of the chart, in core's words. */
export interface QuietVM { heading: string; reads: string; lastReport: string }

/**
 * THE QUIET HEAD, at the horizon the sheet is on: a day ahead with no forecast is a missing forecast, as the silent
 * line under the table says it — never "Not reporting today" over a gauge that reported last night (review
 * 2026-10-22). `zone` is for tests.
 */
export function quietHead(name: string, row: DonorRow, ref: number, horizon: Horizon, zone?: string): QuietVM {
  return { heading: readingHeading(name), reads: donorReads(row, { ref, horizon }),
           lastReport: donorLastReport(row.observedAt, zone) };
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
  /**
   * Each of those reaches coloured by its own percentile, for the route map — through the colouring engine on a
   * per-tile map (`standing`), pushed elsewhere (`data`); see `routeColour.ts`.
   */
  route: RouteColourVM;
  /** The station the chart and the reading are about. */
  lead: StationId | null;
  select: (station: StationId) => void;
  /** Null when the lead gauge has no reading: draw `quiet` if it is set, else `noGauge`. */
  reading: ReadingVM | null;
  /**
   * The lead is one of the panel's donors and has no reading (a quiet gauge picked in the switch, or every donor
   * quiet): whose it is, what it reads today and when it last reported, from core. NOT the no-gauge refusal — the
   * panel's gauges speak for this water, one of them is only silent today (review 2026-10-21).
   */
  quiet: QuietVM | null;
  /** True where no donor leads and nothing reads: draw `noGauge`, the refusal. */
  refuse: boolean;
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
  opts: { feed: PanelFeed | undefined; quantity: Quantity;
          /** `coloursEachTile(Platform.OS)`: the route map colours through the engine. Omitted: pushed. */
          perTile?: boolean },
): ConditionsVM {
  const { feed, quantity } = opts;
  const perTile = opts.perTile ?? false;
  const conditions = useConditions(source, section);
  const traced = useGaugeTrace(source, section);
  const panel = useLastReports(feed, usePanelRoutes(source, section,
                                                    usePanel(source, feed, section, quantity, horizon)));
  const today = useMemo(() => day ?? new Date(), [day]);
  // THE ROUTES OF THE MEMBERS THAT SPEAK — the route map's own chain (`donorView`): a silent member has no pin, so its
  // route is not coloured either.
  const chain = useMemo<readonly SectionKey[]>(
    () => [...new Set(panel.rows.filter((r) => !donorSilent(r, { ref: today.getTime(), horizon }))
                                .flatMap((r) => r.route?.path ?? []))], [panel.rows, today, horizon]);
  const standings = usePanelStandings(source, feed, chain as readonly SectionId[], quantity,
                                      horizon);
  // THE ROUTE AS THE ENGINE'S CLASSES, kept while the route is. A horizon change re-asks the panel, which answers no rows
  // while it loads and then the same rows as new objects: the chain goes empty and comes back new-but-equal. New labels
  // would make the engine clear the route map twice; the last route of THIS section is kept through the gap instead,
  // and the values (null while the chain is empty: the engine leaves the old colours) relabel when they come.
  const steady = useRef<{ section: SectionId | null; chain: readonly SectionKey[] }>({ section, chain });
  if (chain.length || steady.current.section !== section) steady.current = { section, chain };
  const steadyKey = `${section ?? ""}:${steady.current.chain.join(",")}`;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const labels = useMemo(() => routeLabels(steady.current.chain as readonly SectionId[]), [steadyKey]);
  const route = useMemo<RouteColourVM>(() => (perTile
    ? { standing: { labels, values: routeValues(labels, standings, horizon) }, data: {} }
    : { standing: null, data: pushedRoute(standings, horizon) }), [perTile, labels, standings, horizon]);

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

  const row = reading ? undefined : panel.rows.find((r) => r.station === lead);
  // Said once the reading has been asked for and is not there — not while it is on its way.
  const quiet: QuietVM | null = row && got.state !== "loading" ? quietHead(name, row, today.getTime(), horizon) : null;

  return {
    loading: conditions.state === "loading",
    day: today, panel, chain, route, lead, select, reading, quiet, refuse: !reading && !row,
    noGauge: noGaugeSentence(),
    trace: !panel.answer.ok && traced.state === "ready" && traced.value.station
      ? traced.value : null,
    gauges: gaugeChoices(panel.rows, lead),
  };
}


/* ------------------------------------------------------------------ expected change (T52) ---- */

/** One run's change as the panel draws it: the model's line, and its days as a small line with the peak marked. */
export interface ChangeRunVM {
  model: string;
  /** "CLEVER: rises to about 23× today's flow on Mon (very high for the time of year), back down by Tue." */
  line: string;
  /** The day-by-day line (`changeSpark`): null when the run has fewer than two days. */
  spark: ChangeSparkVM | null;
}

/** The Change block under the reading, for the lead gauge. */
export interface ChangeVM {
  /** Every run that can say, in the publisher's order (CLEVER, COFFEE, ELF). Empty: `none` says why. */
  runs: readonly ChangeRunVM[];
  /** "No forecast is running for this gauge now." — when no run can say; null otherwise. */
  none: string | null;
  /** The Province's attribution, verbatim, from the feed (null when the feed carries none). */
  attribution: string | null;
}

/** A run's days as a line: each day's HIGHEST step against today, the peak marked. In a w × h box, top-left origin. */
export interface ChangeSparkVM {
  path: string;
  /** The marked day: the run's peak (or its trough, when the run falls). */
  mark: { x: number; y: number; label: string };
  /** The first and last day's names, at their x. */
  ends: readonly { x: number; label: string }[];
  /** For a screen reader: the line in words. */
  label: string;
}

/**
 * A run's days as a line, on a LOG scale of the ratio to its own today (a 23-fold spike and a 20% dip both read), each
 * day at its highest step for a rise and its lowest for a fall. Pure, so the shape is pinned in a test.
 */
export function changeSpark(c: RunChange, today: string, w: number, h: number): ChangeSparkVM | null {
  const base = c.base;
  const fall = c.dir === "fall";
  const pts = c.days.map(([d, lo, hi]) => ({ d, r: Math.max(1e-3, (fall ? lo : hi) / base) }));
  if (pts.length < 2 || !(base > 0)) return null;
  const logs = pts.map((p) => Math.log(p.r));
  const top = Math.max(0, ...logs), bottom = Math.min(0, ...logs);
  const span = top - bottom || 1;
  const x = (i: number) => Math.round((i / (pts.length - 1)) * w * 10) / 10;
  const y = (l: number) => Math.round((h - ((l - bottom) / span) * h) * 10) / 10;
  const path = pts.map((p, i) => `${i ? "L" : "M"}${x(i)},${y(Math.log(p.r))}`).join(" ");
  const markDay = fall ? c.trough.day : c.peak.day;
  const at = Math.max(0, pts.findIndex((p) => p.d === markDay));
  const first = pts[0]!.d, last = pts[pts.length - 1]!.d;
  const words = changeSparkWords(fall, markDay, first, last, today);
  return {
    path,
    mark: { x: x(at), y: y(logs[at]!), label: words.mark },
    ends: [{ x: 0, label: changeDay(first, today) }, { x: w, label: changeDay(last, today) }],
    label: words.label,
  };
}

/** The Change block for the lead gauge's runs (each `Forecast` with its `change`), on the reader's `today`. */
export function changeView(runs: readonly { model: string; change?: RunChange | null; series?: unknown }[],
                           today: Date, attribution: string | null, size = { width: 280, height: 44 }): ChangeVM {
  const day = isoDay(today);
  const said = runs.filter((r): r is typeof r & { change: RunChange } => !!r.change)
    .sort((a, b) => order(a.model) - order(b.model));
  return {
    runs: said.map((r) => ({ model: r.model, line: changeLine(r.model, r.change, day),
                             // a steady run has no day worth marking: its line says "little change", nothing more
                             spark: r.change.dir === "steady" ? null
                               : changeSpark(r.change, day, size.width, size.height) })),
    none: said.length ? null : CONDITIONS_WORDS.changeNone,
    attribution: said.length ? attribution : null,
  };
}

/** The publisher's model order (`CHANGE_POLICY.order`, generated), never typed a second time. */
const order = (m: string) => {
  const o: readonly string[] = CHANGE_POLICY.order;
  const i = o.indexOf(m);
  return i < 0 ? o.length : i;
};

const isoDay = (d: Date): string =>
  `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;

/** The Province's forecast attribution, as the live feed carries it (T52); null until it is in, or when absent. */
export function useForecastAttribution(feed: PanelFeed | undefined): string | null {
  const got = useAsync(async () => (feed ? (await feed.index())?.forecastAttribution ?? null : null),
                       "forecast-attribution", feed !== undefined);
  return got.state === "ready" ? got.value ?? null : null;
}
