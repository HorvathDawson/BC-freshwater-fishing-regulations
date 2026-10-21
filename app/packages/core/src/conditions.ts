/**
 * THE CONDITIONS SENTENCES — what the donor panel and the gauge reading under it say.
 *
 * These were composed inline in `DonorPanel` and `ConditionsPanel` (P12a), which put the
 * wording where only a fragment of it could be tested, and a fragment you interpolated
 * yourself cannot fail (AGENTS 33). Every sentence is finished here and pinned whole in
 * `conditions.test.ts`; the components draw the strings and decide nothing (AGENTS 25).
 *
 * NO DATA TYPES FROM `@app/data` — core sits below it. A donor row is taken structurally
 * (`DonorFacts`), which `DonorRow` satisfies as it stands.
 */
import { catchmentLabel, confidenceWord, inTen, ordinal, percentileOrdinal, plainStanding,
         seasonPhrase, standing, standingWord, type Standing } from "./flow";
import { interval, type Estimate, type NoEstimate } from "./trust";

/* ------------------------------------------------------------------- horizons ---- */

/**
 * How far ahead the map and the sheet are looking. 0 is now.
 *
 * The values are the days the publisher ranks a forecast for — see HORIZONS in
 * `pipeline/gauges/feed/publish.py`. Beyond about five the model's own bounds are wide
 * enough that the answer is "normal for the season" whatever it says. In core so the words
 * for each day can be derived from the one list; `@app/data` re-exports both.
 */
export type Horizon = 0 | 1 | 3 | 5;
export const HORIZONS: readonly Horizon[] = [0, 1, 3, 5];

export interface HorizonLabel {
  /** The chip: "Now", "+3d". */
  chip: string;
  /** The panel's one-line heading. */
  heading: string;
  /** The chip's screen-reader name. */
  accessible: string;
}

const days = (d: number) => (d === 1 ? "day" : "days");

function horizonLabel(d: Horizon): HorizonLabel {
  if (d === 0)
    return { chip: "Now", heading: "ESTIMATE FOR THIS SPOT", accessible: "Conditions now" };
  return { chip: `+${d}d`, heading: `FORECAST · ${d} ${days(d).toUpperCase()} AHEAD`,
           accessible: `Forecast ${d} ${days(d)} ahead` };
}

/** Every horizon's words, derived from HORIZONS so a new day cannot ship unnamed. */
export const HORIZON_LABELS: Readonly<Record<Horizon, HorizonLabel>> =
  Object.fromEntries(HORIZONS.map((d) => [d, horizonLabel(d)])) as Record<Horizon, HorizonLabel>;

/** The chip's screen-reader name, saying which one is on. */
export function horizonChipLabel(d: Horizon, on: boolean): string {
  return HORIZON_LABELS[d].accessible + (on ? ", showing" : "");
}

/* ------------------------------------------------------------------ the words ---- */

/** Fixed words on the conditions surfaces, in one table rather than spelled in JSX. */
export const CONDITIONS_WORDS = {
  noEstimate: "NO ESTIMATE",
  tapToChart: "TAP A GAUGE TO SEE ITS CHART",
  donorList: "The gauges behind this estimate",
  youAreHere: "you are here",
  byThen: "by then",
  readingWater: "Reading the water",
  /** The one fold under the donor list (P16): the working's working and the credits. */
  fold: "Notes and sources",
  loadingRecord: "Loading the record",
  controls: { quantity: "Quantity", span: "Span", model: "Forecast model" },
  quantity: { discharge: "Flow", level: "Level" },
  span: { "72h": "Last 72 hours", year: "This year" },
  /** The ONE place the app decides what a quantity is measured in. */
  unit: { discharge: "m³/s", level: "m" },
  explainer: "No gauge sits exactly here, so the reading is carried from the nearest ones. A "
    + "gauge counts for more when it drains a piece of country the same size as this one, "
    + "when it sits upstream, and when it has a long record. The percentage beside each is "
    + "how much of the answer above came from it.",
} as const;

export type ChartSpan = keyof typeof CONDITIONS_WORDS.span;

/* --------------------------------------------------------------- no estimate ---- */

const WHY: Record<NoEstimate, string> = {
  "no-station": "No gauge on this water is close enough in size to speak for it.",
  "no-record": "The gauges here are reporting, but none has enough history to rank today "
             + "against.",
  "regulated": "The only gauges within reach are on regulated water, where a reading "
             + "describes a release schedule rather than rainfall.",
  "too-uncertain": "The gauges that can speak for this spot disagree too much to tell a "
                 + "low river from a high one.",
  "offline": "The live readings did not arrive.",
};

/** Why there is no answer, in words a reader can act on. */
export function donorReason(why: NoEstimate): string {
  return WHY[why];
}

/* -------------------------------------------------------------- a donor row ---- */

/** What a donor row must carry to be described. `DonorRow` in `@app/data` satisfies it. */
export interface DonorFacts {
  station: string;
  role: "up" | "down";
  percentile: number | null;
  weight: number;
  areaRatio: number;
  areaKm2: number;
  years: number;
  regulated: boolean;
  factors: { share: number; role: number; record: number };
  route?: { path: readonly unknown[]; name?: string | null } | undefined;
}

const ROLE: Record<"up" | "down", string> = { up: "upstream", down: "downstream" };

/** "155× apart" reads better than a ratio nobody converts in their head. */
export function sizeApart(ratio: number): string {
  if (!Number.isFinite(ratio)) return "—";
  if (ratio < 1.5) return "same size";
  return `${ratio < 10 ? ratio.toFixed(1) : Math.round(ratio)}× apart`;
}

/**
 * What a donor contributed, in words.
 *
 * A station that is not reporting says so rather than showing 0%: it IS in the panel, and
 * "0%" reads as "not in the panel". A real but tiny share reads "<1%" for the same reason.
 */
export function donorShare(r: Pick<DonorFacts, "weight" | "percentile">): string {
  if (r.percentile === null) return "quiet";
  const pc = r.weight * 100;
  return pc > 0 && pc < 1 ? "<1%" : `${Math.round(pc)}%`;
}

/** The labelled line beside the weight bar. */
export function donorWeightLine(r: DonorFacts): string {
  return r.percentile === null ? "not counted today" : `${donorShare(r)} of this estimate`;
}

/** What the gauge itself reads, in the headline's words. */
export function donorReads(r: DonorFacts): string {
  return r.percentile === null ? "Not reporting today." : `Reads ${inTen(r.percentile)}.`;
}

/** "upstream · drains 120 km² · 2.4× apart · dam-controlled · 2 reaches away". */
export function donorFacts(r: DonorFacts): string {
  const bits = [ROLE[r.role], `drains ${catchmentLabel(r.areaKm2)}`, sizeApart(r.areaRatio)];
  if (r.regulated) bits.push("dam-controlled");
  const n = r.route?.path.length ?? 0;
  if (n === 1) bits.push("on this reach");
  else if (n > 1) bits.push(`${n - 1} ${n === 2 ? "reach" : "reaches"} away`);
  return bits.join(" · ");
}

/**
 * WHY THIS DONOR WEIGHS WHAT IT WEIGHS — the model's three factors (`weightFactors`, the
 * function the estimate used), so this restates the arithmetic without being able to
 * disagree with it.
 */
export function donorBecause(r: DonorFacts): string {
  // "How informative, against the best possible donor" — said as a comparison, because a
  // bare percentage of an abstract quantity is not something a reader can check.
  const bits = [r.factors.share >= 0.99
    ? "it drains almost exactly the same country"
    : `it is ${Math.round(r.factors.share * 100)}% as informative as a gauge on this very water`];
  if (r.factors.role < 1) bits.push("it sits downstream, so it carries extra water");
  // A short record is coarse, not wrong; saying the discount is what makes it checkable.
  if (r.factors.record < 1)
    bits.push(`it has only ${r.years} ${r.years === 1 ? "year" : "years"} of record, `
              + `so it counts ${Math.round(r.factors.record * 100)}%`);
  return `Counts this much because ${bits.join("; ")}.`;
}

/** The row's screen-reader name; the weight is drawn as a bar, invisible to a reader. */
export function donorRowLabel(r: DonorFacts, pickable: boolean, on: boolean): string {
  const said = r.percentile === null
    ? "not reporting today, so it counts for nothing"
    : `${Math.round(r.weight * 100)} per cent of the answer`;
  const base = `${r.station}, ${ROLE[r.role]}, ${sizeApart(r.areaRatio)}, ${said}`;
  return pickable ? `${base}. ${on ? "Showing" : "Show"} this gauge's chart` : base;
}

const hereName = (r: DonorFacts, atSpot: boolean) =>
  atSpot ? `${CONDITIONS_WORDS.youAreHere} · ${r.station}` : r.station;

/** The donor's pin on the route map. `atSpot`: it absorbed the "you are here" marker. */
export function donorPinTitle(r: DonorFacts, atSpot: boolean): string {
  return `${hereName(r, atSpot)} · ${donorShare(r)} of the answer`;
}

/** The donor's entry in the map's key. */
export function donorKeyLabel(r: DonorFacts, atSpot: boolean): string {
  return `${hereName(r, atSpot)} · ${donorShare(r)}`;
}

/* ----------------------------------------------------------------- estimate ---- */

/** The headline: what the water is doing, before any number. */
export function estimateHeadline(e: Estimate): string {
  return plainStanding(standing(e.percentile));
}

/** "Lower than 8 days in 10 at this time of year — fairly confident, from 2 gauges nearby." */
export function estimateSentence(e: Estimate, horizon: Horizon): string {
  const w = inTen(e.percentile);
  const gauges = e.donors === 1 ? "one gauge" : `${e.donors} gauges`;
  return `${w.charAt(0).toUpperCase()}${w.slice(1)} — ${confidenceWord(e.plusMinus)}, `
    + `${horizon === 0 ? "from" : "forecast from"} ${gauges} nearby.`;
}

/**
 * An interval endpoint, clamped to 1–99: the envelope stops at p10/p90 because the
 * extremes do not interpolate, so a range may not claim the record's own ends.
 */
const bound = (n: number) => ordinal(Math.max(1, Math.min(99, Math.round(n))));

const DAY_MS = 86_400_000;

/**
 * "3rd–27th percentile for early September · this spot drains 50 km²".
 *
 * `day` is the CHOSEN day — today on a live sheet, the visit on a saved spot — never the
 * clock read here; the horizon moves it forward to the day the forecast is for.
 */
export function estimateRange(e: Estimate, day: Date, horizon: Horizon,
                              areaKm2: number | null): string {
  const [lo, hi] = interval(e);
  const base = `${bound(lo)}–${bound(hi)} percentile for `
    + seasonPhrase(new Date(day.getTime() + horizon * DAY_MS));
  return areaKm2 != null ? `${base} · this spot drains ${catchmentLabel(areaKm2)}` : base;
}

/** Some donors had no route to draw. Null when every one was placed. */
export function unplacedNote(missing: number): string | null {
  if (missing <= 0) return null;
  return `${missing} of these gauges could not be placed on the map. They still count `
    + "toward the answer.";
}

/** A short record, said once, when the answer actually leans on one. */
export function shortRecordNote(rows: readonly DonorFacts[]): string | null {
  const voting = rows.filter((r) => r.percentile !== null && r.weight > 0);
  const lean = voting.reduce((a, r) => a + (r.years < 10 ? r.weight : 0), 0);
  if (!voting.length || lean < 0.5) return null;
  const fewest = Math.min(...voting.filter((r) => r.years < 10).map((r) => r.years));
  return `Most of this rests on a gauge with only ${fewest} ${fewest === 1 ? "year" : "years"} `
    + "of record. That is few enough that \"normal for this week\" is itself a rough idea, "
    + "which is part of why the range above is wide.";
}

/** A dam, said once — only about a number being shown, so only for a reporting gauge. */
export function regulatedNote(rows: readonly DonorFacts[]): string | null {
  return rows.some((r) => r.regulated && r.percentile !== null)
    ? "This water is controlled by a dam. The reading is real, but it follows a release "
      + "schedule as much as the weather — a low river here can mean the gate is shut "
      + "rather than that the country is dry."
    : null;
}

/** The donors disagree by more than 25 points. */
export function disagreeNote(e: Estimate): string | null {
  if (!(e.spread > 25)) return null;
  return `These gauges disagree by ${Math.round(e.spread)} points. That is a catchment doing `
    + "more than one thing, and the range above is the honest answer rather than their "
    + "average.";
}

/* ---------------------------------------------------------- a gauge's reading ---- */

/** "AT VEDDER RIVER AT VEDDER CROSSING" — whose reading it is, before the number. */
export function readingHeading(name: string): string {
  return `AT ${name.toUpperCase()}`;
}

/** The reading ranked with core's band word, or the admission there is nothing to rank by. */
export function readingStandingLine(percentile: number | null,
                                    band: Standing | null): string {
  if (percentile == null) return "There is a reading here, but no record to compare it against.";
  return `${standingWord(band ?? "no-record")} — ${percentileOrdinal(percentile)} percentile `
    + "for the date";
}

/** The stage, beside a discharge. */
export function stageLine(level: number): string {
  return `${level} ${CONDITIONS_WORDS.unit.level} stage`;
}

/** The refusal where no gauge may speak for the water. */
export function noGaugeSentence(): string {
  return "No gauge is entitled to speak for this water. A station draining a far larger "
    + "watershed would have given a number, and the number would have been wrong.";
}

/* ------------------------------------------------------------------ the chart ---- */

/** What the chart is of, for its accessible label. */
export function chartLabel(span: ChartSpan): string {
  return span === "72h" ? "the last 72 hours" : "the whole year against its record";
}

/** The year chart explains its bands; the 72-hour one needs no caption. */
export function chartCaption(span: ChartSpan): string | undefined {
  if (span !== "year") return undefined;
  return "Bands are this station's whole record for each five-day period. The bold line is "
    + "this year to date, which the feed accumulates day by day — it starts a month long "
    + "and grows, because the published daily record lags a year behind. The thin lines "
    + "are the last complete years out of that record. The frame ends a month past today, "
    + "where the longest forecast does.";
}

/** Why the chart is empty. */
export function noChartSentence(span: ChartSpan): string {
  return span === "year"
    ? "This station has no envelope in this quantity, so there is no season to draw it "
      + "against."
    : "This station published no recent readings.";
}

export interface ForecastRunFacts { model: string; horizonDays: number }

/**
 * When the run was made, beside what it says — IN LABELLED UTC. It printed the ISO
 * string's own clock unlabelled, which for an offset time is not UTC and not said to be
 * anything. A time that does not parse is left out rather than guessed.
 */
export function forecastRunLine(run: ForecastRunFacts & { issuedAt: string }): string {
  const t = Date.parse(run.issuedAt);
  const when = Number.isFinite(t)
    ? ` issued ${new Date(t).toISOString().replace("T", " ").slice(0, 16)} UTC` : "";
  return `${run.model} run${when} · ${run.horizonDays}-day outlook`;
}

/** The forecast's label on the chart. */
export function forecastChartLabel(run: ForecastRunFacts): string {
  return `${run.model} · ${run.horizonDays} days`;
}

/** A model in the picker. */
export function forecastModelOption(run: ForecastRunFacts): string {
  return `${run.model} · ${run.horizonDays}d`;
}
