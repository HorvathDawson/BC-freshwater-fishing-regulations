/**
 * THE PANEL READS: every panel fetched once per section through one cache, handed to the arithmetic — the route map's
 * standings (`usePanelStandings`) and the sheet's answer (`usePanel`, `useSpeaker`,
 * `usePanelRoutes`) are one calculation over different numbers of sections.
 */
import { useMemo } from "react";
import { answerFrom, sectionStanding, speakerFor, type Horizon, type Panel,
         type PanelAnswer, type Quantity, type WaterSource, type SectionId, type Speaker } from "@app/data";
import { useAsync, type Async } from "../async";

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

async function cached(source: WaterSource,
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
  source: WaterSource,
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
       * ONE QUANTITY, THE READER'S, and ONE READING (`sectionStanding` in @app/data): the class path the whole map takes
       * computes its values with the same function, so the route and the map cannot colour a section differently. A panel
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

/** The panel for one section, joined against today's readings. */
export function usePanel(
  source: WaterSource,
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
 * WHO SPEAKS FOR THIS SECTION, as the map decides it (`speakerFor` in @app/data, T105): its panel through the SAME
 * cached fetch `usePanelStandings` colours from, else the station in its lake, else nothing. `name` is a lake
 * station's name (a panel's donors carry theirs on their routes); null where the bundle has none.
 */
export function useSpeaker(source: WaterSource, section: SectionId | null):
  Async<{ speaker: Speaker | null; name: string | null }> {
  return useAsync(
    async () => {
      if (!section) return { speaker: null, name: null };
      const [panels, lakes] = await Promise.all([cached(source, [section]), source.lakeStationsFor([section])]);
      const speaker = speakerFor(panels.get(section), lakes.get(section));
      const name = speaker?.kind === "lake"
        ? (await source.gaugePoints()).find((g) => g.station === speaker.station)?.name ?? null : null;
      return { speaker, name };
    },
    `speaker:${section ?? ""}`,
    section !== null,
  );
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
export function usePanelRoutes(source: WaterSource, section: SectionId | null,
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
