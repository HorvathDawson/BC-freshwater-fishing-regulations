/**
 * The donor panel for a spot, turned into an answer.
 *
 * DECIDES NOTHING ITSELF (rule 25): the arithmetic is `estimate` in core, shared with the
 * pipeline and pinned to it by a test. This is the fetch, the join against today's
 * readings, and the ordering the screen displays in — which is the SAME ordering the
 * arithmetic used, so the table and the number above it can never disagree.
 */
import { useMemo } from "react";
import { estimate, trustFor, weightFactors, type Answer, type TrustClass } from "@app/core";
import type { Panel, PanelRoute, RegsSource, SectionId, StationId } from "@app/data";
import { useAsync } from "./async";

/** One row of the table under the answer — a donor, and what it contributed. */
export interface DonorRow {
  station: StationId;
  role: "up" | "down";
  /** 0–1, this donor's own percentile today. Null when it is not reporting. */
  percentile: number | null;
  /** Its share of the answer, 0–1, normalised across the panel. */
  weight: number;
  /** How many times bigger the larger of the two catchments is. Always ≥ 1. */
  areaRatio: number;
  /** The DONOR's own catchment, km². Half of the ratio, and worth saying out loud: a
   *  reader can check a claim about size against two numbers and cannot against one. */
  areaKm2: number;
  trust: TrustClass;
  years: number;
  /** A dam governs this station's water — see `PanelMember.regulated`. */
  regulated: boolean;
  /**
   * Why this donor is worth what it is worth, before normalising — the three factors the
   * model multiplies. Carried so a screen can show the working rather than assert a
   * percentage: `share × role × record`, then divided by the panel's total.
   */
  factors: { share: number; role: number; record: number };
  /** Where it is and how the water reaches it. Absent until `usePanelRoutes` resolves. */
  route?: PanelRoute;
}

export interface PanelAnswer {
  answer: Answer;
  rows: readonly DonorRow[];
  /** The TARGET's catchment, km². The other half of every ratio in `rows`. */
  areaKm2: number | null;
  /**
   * Whether `route` on the rows has been RESOLVED — true even when it resolved to nothing.
   *
   * A caller that frames a map around the donors has to tell "no routes yet" from "no
   * routes at all", and the rows cannot: both look like `route: undefined`. Framing on the
   * first is how a map ends up locked to a camera fitted around a single point, because a
   * map reads its opening camera once and never again.
   */
  routesReady: boolean;
}

/**
 * How far ahead the map and the sheet are looking. 0 is now.
 *
 * The values are the days the publisher ranks a forecast for — see HORIZONS in
 * `pipeline/gauges/feed/publish.py`. Beyond about five the model's own bounds are wide
 * enough that the answer is "normal for the season" whatever it says, which the
 * climatology already gives you without a forecast.
 */
export type Horizon = 0 | 1 | 3 | 5;

/** What the map or the sheet is asking about. `both` = each station's own quantity. */
export type Quantity = "discharge" | "level" | "both";
export const HORIZONS: readonly Horizon[] = [0, 1, 3, 5];

type Ahead = Record<string, { discharge?: number; level?: number; model?: string }>;

type Index = { stations: Record<string, { percentile: number | null;
                                          discharge?: number | null;
                                          level?: number | null;
                                          parameter?: "discharge" | "level";
                                          /** Forecast percentiles, keyed by days ahead. */
                                          ahead?: Ahead }> } | null;

/**
 * One station's percentile for the day being asked about, in one quantity.
 *
 * NOW AND AHEAD ARE THE SAME KIND OF NUMBER and are read the same way — a percentile for
 * the date, ranked against the envelope for THAT date. That is what makes a forecast
 * horizon a parameter of this function rather than a separate screen: everything
 * downstream, the weights, the combine, the interval, the words, is identical.
 *
 * A horizon the model does not reach is null, never today's value: a map that silently
 * shows today when asked for Friday is worse than one that shows nothing.
 */
function reading(row: Index extends null ? never : NonNullable<Index>["stations"][string],
                 quantity: Quantity, horizon: Horizon): number | null {
  /*
   * "BOTH" IS NOT A THIRD QUANTITY — it is "whichever this station actually measures".
   *
   * 237 BC stations measure stage and never discharge, and a lake station almost always
   * reports a level. Asking every one of them for a discharge colours the most water it is
   * possible to leave grey, for no reason: the publisher already chose each station's own
   * quantity and computed the percentile against the matching envelope. `parameter` says
   * which one that was, so under "both" the answer is simply the station's own.
   *
   * It is still never MIXED. One dot, one quantity, named — what "both" refuses to do is
   * pick the same quantity for every station.
   */
  const q: "discharge" | "level" =
    quantity === "both" ? ((row.parameter ?? "discharge") === "level" ? "level" : "discharge")
                        : quantity;
  if (horizon === 0) {
    // `percentile` is the station's own default and `parameter` says which quantity it is
    // about, so it stands in for that one only — a level percentile read as a flow is
    // arithmetic across two units.
    return row[q] ?? ((row.parameter ?? "discharge") === q ? row.percentile ?? null : null);
  }
  return row.ahead?.[String(horizon)]?.[q] ?? null;
}

/**
 * Build the answer and its working from a panel and today's index.
 *
 * Exported and pure so it can be tested without a database or a network — the hook below
 * is only the fetching around it.
 */
export function answerFrom(panel: Panel | undefined, index: Index,
                           quantity: Quantity = "discharge",
                           horizon: Horizon = 0): PanelAnswer {
  if (!panel)
    return { answer: { ok: false, why: "no-station" }, rows: [], areaKm2: null,
             routesReady: false };
  if (!index)
    return { answer: { ok: false, why: "offline" }, rows: [], areaKm2: panel.areaKm2,
             routesReady: false };

  const contributions = [];
  const raw: (DonorRow & { _w: number })[] = [];
  for (const m of panel.members) {
    const row = index.stations[m.station];
    const own = row ? reading(row, quantity, horizon) : null;
    const ratio = panel.areaKm2 && m.areaKm2
      ? Math.max(panel.areaKm2, m.areaKm2) / Math.min(panel.areaKm2, m.areaKm2) : Infinity;
    const share = Number.isFinite(ratio) ? 1 / ratio : 0;
    const f = weightFactors(share, m.role, m.years);
    // A donor that is not reporting weighs NOTHING, but its factors are still real and are
    // still shown: "this gauge would have carried 60% of the answer and is quiet today" is
    // the most useful thing the table can say on a bad day.
    const w = own === null ? 0 : f.share * f.role * f.record;
    raw.push({ station: m.station, role: m.role, percentile: own, weight: 0,
               areaRatio: ratio, areaKm2: m.areaKm2, trust: trustFor(ratio).klass,
               years: m.years, regulated: m.regulated, factors: f, _w: w });
    if (own !== null && panel.areaKm2)
      contributions.push({ percentile: own, role: m.role, areaKm2: m.areaKm2,
                           years: m.years });
  }
  const total = raw.reduce((a, r) => a + r._w, 0) || 1;
  const rows = raw
    .map(({ _w, ...r }) => ({ ...r, weight: _w / total }))
    .sort((a, b) => b.weight - a.weight);
  return { answer: estimate(panel.areaKm2, contributions), rows,
           areaKm2: panel.areaKm2, routesReady: false };
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
  quantity: Quantity = "both",
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
        source.panelsFor(sections),
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
        // "both" asks each panel in the quantity its own donors lead with, which is the
        // publisher's choice per station and is what colours the most water. Asking for
        // one quantity colours only the stations that measure it and says nothing about
        // the rest, rather than quietly answering with the other.
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

/** The panel for one section, joined against today's readings. */
export function usePanel(
  source: RegsSource,
  feed: { index(): Promise<Index> } | undefined,
  section: SectionId | null,
  quantity: "discharge" | "level" = "discharge",
  horizon: Horizon = 0,
): PanelAnswer {
  const got = useAsync(
    async () => {
      if (!section) return null;
      const [panels, idx] = await Promise.all([
        source.panelsFor([section]),
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
