/**
 * The hooks that fetch a panel and hand it to the arithmetic.
 *
 * THE ARITHMETIC ITSELF IS IN `@app/data` — see `panel.ts` there. It has three callers and
 * one of them is not a screen (`captureSpot` freezes an answer into a saved spot), so it
 * cannot live behind a hook.
 */
import { useMemo } from "react";
import { answerFrom, reading, type BasinMember, type Horizon, type Panel,
         type PanelAnswer, type Quantity, type RegsSource, type SectionId,
         type StationId } from "@app/data";
// The field combines its gauges with the SAME probit transform the reaches use — see
// `basinStanding`. A second averaging rule here is how a catchment and the river inside it
// start disagreeing for reasons that are only arithmetic.
import { basinStanding, type BasinVote } from "@app/core";
import { useAsync } from "./async";

/** Re-exported so a screen has one import for the whole subject. */
export { answerFrom, HORIZONS } from "@app/data";
export type { DonorRow, Horizon, PanelAnswer, Quantity } from "@app/data";

type Index = Parameters<typeof answerFrom>[1];

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
