/**
 * A donor panel plus today's readings → one answer.
 *
 * IN `@app/data` AND NOT IN THE HOOKS, because three different callers need it and one of
 * them is not a screen: the map colours from it, the sheet reads from it, and `captureSpot`
 * freezes it into a saved spot. A spot recorded from a second implementation is a spot that
 * disagrees with the app that produced it — which is exactly what happened, twice, before
 * this moved.
 *
 * DECIDES NOTHING ITSELF (rule 25): the arithmetic is `estimate` in core, shared with the
 * pipeline and pinned to it by `tools/one-formula.test.ts`. This is the join and the
 * ordering the screen displays in — which is the SAME ordering the arithmetic used, so the
 * table and the number above it can never disagree.
 */
import { estimate, trustFor, weightFactors, type Answer, type TrustClass } from "@app/core";
import type { Panel, PanelRoute, SectionId, StationId } from "./index";

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
  /** On the same blue line as this reach — see `PanelMember.sameRiver`. */
  sameRiver: boolean;
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

/**
 * What the map or the sheet is asking about — ONE physical quantity, province-wide.
 *
 * There used to be a third value, `both`, meaning "whichever quantity this station itself
 * measures". Within one station that was sound and it coloured the most water. Across the
 * FIELD it was not: the map paints every station on one ramp, so a discharge percentile at
 * one gauge sat beside a stage percentile at the next with nothing saying which was which.
 *
 * MEASURED, on the 358 stations publishing both on 2026-09-04: the two percentiles differ
 * by a median of 11.6 points, agree within 5 points only 28% of the time, and disagree by
 * more than 25 points at 84 stations. 08LB024 read p5 by discharge and p95 by level — the
 * lowest flows on record or the highest, for the same river on the same day. A reader
 * comparing two rivers was sometimes comparing two different questions.
 *
 * So the mode is now the reader's, not the publisher's. What it costs is honest and small:
 * 61 stations report level only and go grey under Flow, 12 report discharge only and go
 * grey under Level. Grey means "no reading of the thing you asked about", which is true.
 */
export type Quantity = "discharge" | "level";
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
export function reading(row: NonNullable<Index>["stations"][string] | null | undefined,
                 quantity: Quantity, horizon: Horizon): number | null {
  if (!row) return null;
  const q: "discharge" | "level" = quantity;
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

  /*
   * ONE QUANTITY FOR THE WHOLE PANEL, decided here rather than per donor.
   *
   * Combining donors means averaging their percentiles, and averaging a level percentile
   * with a discharge one produces a number that is not a claim about anything: a stage is
   * about one cross-section and moves when the channel does; a discharge is about the whole
   * river. The panel therefore asks every donor the same question, and a donor that cannot
   * answer it is absent rather than substituted.
   *
   * Measured on the Harrison: a lake gauge's level at the 40th percentile averaged with the
   * river's discharge at the 6th, disagreeing by more than MAX_USEFUL_SPREAD, so the panel
   * refused and the map drew "no baseline" — while the SAME reach coloured perfectly at
   * +1 day, because the forecast block carries discharge only and the level could not
   * intrude. Two different answers for one reach, an artefact of mixing units.
   *
   * THE QUANTITY ASKED FOR, AND NO SUBSTITUTE. This used to prefer discharge whenever any
   * donor had it and fall back to level otherwise, which was the right call while the
   * caller could say "both" — but it also meant a reader who asked for Level could be
   * answered in discharge without being told.
   *
   * DISCHARGE IS STILL THE ONLY TRANSFERABLE ONE, and that is a fact about the method, not
   * a preference: this carries a reading BETWEEN catchments by area ratio, and a stage does
   * not travel — two rivers at the same depth are not at the same anything. So a Level
   * panel answers only where the donors themselves report a level, and says nothing
   * elsewhere. Less water is coloured under Level than under Flow; the alternative is
   * answering a question that was not asked.
   */
  const chosen: "discharge" | "level" = quantity;

  const contributions = [];
  const raw: (DonorRow & { _w: number })[] = [];
  for (const m of panel.members) {
    const row = index.stations[m.station];
    const own = row ? reading(row, chosen, horizon) : null;
    const ratio = panel.areaKm2 && m.areaKm2
      ? Math.max(panel.areaKm2, m.areaKm2) / Math.min(panel.areaKm2, m.areaKm2) : Infinity;
    const share = Number.isFinite(ratio) ? 1 / ratio : 0;
    const f = weightFactors(share, m.role, m.years, m.sameRiver);
    // A donor that is not reporting weighs NOTHING, but its factors are still real and are
    // still shown: "this gauge would have carried 60% of the answer and is quiet today" is
    // the most useful thing the table can say on a bad day.
    const w = own === null ? 0 : f.share * f.role * f.record;
    raw.push({ station: m.station, role: m.role, percentile: own, weight: 0,
               areaRatio: ratio, areaKm2: m.areaKm2, trust: trustFor(ratio).klass,
               years: m.years, regulated: m.regulated, sameRiver: m.sameRiver,
               factors: f, _w: w });
    if (own !== null && panel.areaKm2)
      contributions.push({ percentile: own, role: m.role, areaKm2: m.areaKm2,
                           years: m.years, sameRiver: m.sameRiver });
  }
  const total = raw.reduce((a, r) => a + r._w, 0) || 1;
  const rows = raw
    .map(({ _w, ...r }) => ({ ...r, weight: _w / total }))
    .sort((a, b) => b.weight - a.weight);
  return { answer: estimate(panel.areaKm2, contributions), rows,
           areaKm2: panel.areaKm2, routesReady: false };
}

