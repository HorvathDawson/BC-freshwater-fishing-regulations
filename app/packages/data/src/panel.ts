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
import { answering, CORE_MEMBERS, estimate, isAhead, leaderOf, leadShares, trustFor, weightFactors, type Answer,
         type Contribution, type TrustClass } from "@app/core";
import type { Panel, PanelRoute, SectionId, StationId } from "./index";

/** One row of the table under the answer — a donor, and what it contributed. */
export interface DonorRow {
  station: StationId;
  role: "up" | "down";
  /** 0–1, this donor's own percentile today. Null when it is not reporting. */
  percentile: number | null;
  /** Its share of the answer, 0–1, normalised across the panel — with this water's own gauge leading where one may
   *  (`lead`, core's `leadShares`), exactly as the answer above the table was computed. */
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
  /**
   * When the station last reported, as the feed's index says (ISO). Absent where the index does not carry the
   * station: a quiet gauge then says its last report is not known rather than inventing one (owner 2026-10-21).
   */
  observedAt?: string;
  /**
   * `false` only: the feed's index lists ECCC's real-time stations (`listed`) and this one is not among them, so the
   * row says "not a real-time gauge" rather than "not reporting". Absent when it is listed, or when an older index
   * carries no list (every station then reads as listed — today's words).
   */
  realtime?: false;
  /**
   * `true` only: the station's own file was asked (`withLastReports` in @app/ui) and holds no data at all, so the
   * silent line says it "has never reported". Absent where nobody asked, or the file said when (`observedAt`).
   */
  noData?: true;
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
 * How far ahead the map and the sheet are looking. 0 is now. DEFINED IN CORE beside the
 * words for each day (`HORIZON_LABELS`), so the list and its labels are one table.
 */
export { HORIZONS, type Horizon } from "@app/core";
import type { Horizon } from "@app/core";

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

type Ahead = Record<string, { discharge?: number; level?: number; model?: string }>;

/**
 * A station's EXPECTED CHANGE over the next ten days (T52, the feed's `change`, `pipeline/gauges/feed/change.py`): the
 * largest move its lead forecast run expects, read day by day. FLOW ONLY. `discharge` is the ratio to today's flow
 * CODED for the donor pathway (core `changeRatio` decodes it); the rest is what the run said, for the words.
 */
export interface StationChange {
  discharge: number;
  model: string;
  dir: "rise" | "fall" | "both" | "steady";
  ratio: number;
  day: string;
  shape: "spike" | "step";
  back?: string;
}

type Index = { stations: Record<string, { percentile: number | null;
                                          discharge?: number | null;
                                          level?: number | null;
                                          parameter?: "discharge" | "level";
                                          /** The station's last observation (ISO), from the feed. */
                                          observedAt?: string | null;
                                          /** Forecast percentiles, keyed by days ahead. */
                                          ahead?: Ahead;
                                          /** The expected change over the next ten days (T52). Flow only. */
                                          change?: StationChange }>;
               /** The Province's forecast attribution, verbatim, when any run answered a change (T52). */
               forecastAttribution?: string;
               /** ECCC's real-time station ids on the publishing run; absent on an older feed (= all listed). */
               listed?: readonly string[] } | null;

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
  // THE CHANGE VIEW (T52) reads the station's expected change — flow only, never a level: a ratio of stages is not a
  // ratio of water, and the publisher writes none.
  if (horizon === "change") return q === "discharge" ? row.change?.discharge ?? null : null;
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

  const contributions = contributionsOf(panel, index, chosen, horizon);
  const { answer, stoodBack } = answering(panel.areaKm2, contributions);
  const raw: (DonorRow & { _w: number })[] = [];
  const listed = index.listed ? new Set(index.listed) : null;
  let at = 0;   // this member's index in `contributions`, which holds the reporting members in panel order
  for (const m of panel.members) {
    const row = index.stations[m.station];
    const own = row ? reading(row, chosen, horizon) : null;
    const back = own !== null && panel.areaKm2 ? stoodBack.has(at++) : false;
    const ratio = panel.areaKm2 && m.areaKm2
      ? Math.max(panel.areaKm2, m.areaKm2) / Math.min(panel.areaKm2, m.areaKm2) : Infinity;
    const share = Number.isFinite(ratio) ? 1 / ratio : 0;
    const f = weightFactors(share, m.role, m.years, m.sameRiver);
    // A donor that is not reporting weighs NOTHING, but its factors are still real and are
    // still shown: "this gauge would have carried 60% of the answer and is quiet today" is
    // the most useful thing the table can say on a bad day.
    // A reserve member that stood back (core's `answering`) weighs nothing either: the answer above left it out.
    const w = own === null || back ? 0 : f.share * f.role * f.record;
    raw.push({ station: m.station, role: m.role, percentile: own, weight: 0,
               areaRatio: ratio, areaKm2: m.areaKm2, trust: trustFor(ratio).klass,
               years: m.years, regulated: m.regulated, sameRiver: m.sameRiver,
               factors: f, _w: w,
               ...(row?.observedAt ? { observedAt: row.observedAt } : {}),
               ...(listed && !listed.has(m.station) ? { realtime: false as const } : {}) });
  }
  /*
   * A DAY AHEAD THAT NO RUN REACHES is said as that. With no donor reading at the horizon
   * the combine would refuse with "no-station" — "no gauge close enough", which is false:
   * the gauges are there, their forecasts stop short (the +10 day often does, every day
   * ahead does out of season). Only for a horizon past now, and only where the panel could
   * otherwise answer (it has gauges and an area).
   */
  /*
   * EVERY MEMBER SILENT TODAY is said as that, too (owner 2026-10-22, "chosen live"): the panel is a ranked reserve of
   * every station that could describe this water, live or not, and which of them answers is the feed's business. With
   * none reporting the asked quantity the gauges are still there — "no-station" ("no gauge close enough") would be
   * false — so it is "no-record": none has a reading to rank. Nothing outside the panel is borrowed.
   */
  if (!contributions.length && panel.areaKm2 && panel.members.length) {
    return { answer: { ok: false, why: isAhead(horizon) ? "no-forecast" : "no-record" },
             rows: raw.map(({ _w, ...r }) => ({ ...r, weight: 0 })),
             areaKm2: panel.areaKm2, routesReady: false };
  }
  // THE SAME SHARES `estimate` USES: the usual weights of the members REPORTING now (a quiet one weighs 0, so the rest
  // re-normalise among themselves), with the leading own gauge — the best-ranked own one reporting — raised to its
  // share. A table that showed the plain weights under a led answer would contradict the number above it.
  const ws = raw.map((r) => r._w);
  const shares = leadShares(ws, leaderOf(panel.members.map((m) => m.lead ?? 0), ws));
  const rows = raw
    .map(({ _w, ...r }, i) => ({ ...r, weight: shares[i]! }))
    .sort((a, b) => b.weight - a.weight);
  return { answer, rows,
           areaKm2: panel.areaKm2, routesReady: false };
}


/**
 * What a panel's donors contribute to `estimate` for one quantity and horizon: every member reporting a reading, while
 * the section has a target area. THE ONE RULE for it — `answerFrom` (the sheet, and the rows under the answer) and
 * `panelStanding` (the map's per-class values) both take their contributions from here.
 */
export function contributionsOf(panel: Panel, index: NonNullable<Index>, quantity: Quantity,
                                horizon: Horizon): Contribution[] {
  const out: Contribution[] = [];
  if (!panel.areaKm2) return out;
  panel.members.forEach((m, i) => {
    const row = index.stations[m.station];
    const own = row ? reading(row, quantity, horizon) : null;
    // past the core, a member is reserve: it answers, and stands back rather than silence the core (`answering`)
    if (own !== null)
      out.push({ percentile: own, role: m.role, areaKm2: m.areaKm2, years: m.years, sameRiver: m.sameRiver,
                 ...(m.lead ? { lead: m.lead } : {}), ...(i >= CORE_MEMBERS ? { reserve: true } : {}) });
  });
  return out;
}

/**
 * THE MAP'S READING OF A PANEL: the percentile `answerFrom` answers, or -0.01 where it answers with nothing — the same
 * contributions and the same `estimate`, without building the rows a sheet shows (the map asks ~18.5k classes per feed
 * or horizon, and the rows were most of that cost). Held to `answerFrom` on every section of the fixture release
 * (`conformance/src/release.test.ts`).
 */
export function panelStanding(panel: Panel, index: NonNullable<Index>, quantity: Quantity, horizon: Horizon): number {
  // `answerFrom`'s "no-forecast" (a horizon no donor reaches) is a reason for the sheet; on the map it is the same
  // -0.01 any refusal is, which `estimate` gives for no contributions anyway
  const a = estimate(panel.areaKm2, contributionsOf(panel, index, quantity, horizon));
  return a.ok ? a.value.percentile : -0.01;
}
