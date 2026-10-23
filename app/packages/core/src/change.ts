/**
 * EXPECTED CHANGE (T52) — the words and the scale for the last horizon, "Change".
 *
 * Owner 2026-10-22: "even if it is a quick spike then drop we want to show this river will have a big change within the
 * CLEVER forecast (it could also use multiple forecasts)". The publisher reads every forecast day of every run for its
 * largest rise and fall over the next ten days (`pipeline/gauges/feed/change.py`) and carries, per station, the ratio to
 * today's flow CODED as a percentile-shaped number — Φ(log2(ratio) / scale) — so a section's expected change rides the
 * donor panels like every other horizon (weights, own-gauge lead, reserve, refusals). This file decodes that number and
 * says it; the thresholds are the publisher's (`CHANGE_POLICY`, generated), never typed here a second time.
 *
 * FLOW ONLY. A ratio of stages is not a ratio of water, and the owner's rule is never to mix the two: under Level the
 * Change chip is greyed "Flow only" and a tap moves to Flow (T130, `horizonChips` / `pickHorizon` in @app/ui).
 */
import { CHANGE_POLICY } from "./gauge-policy.generated";
import { monthAbbr, plainStanding, standing, weekdayAbbr } from "./flow";
import { normCdf, probit } from "./trust";

export { CHANGE_POLICY };

/**
 * WITHIN THIS FACTOR EITHER WAY IS "LITTLE CHANGE" in words, and the legend's middle bracket. App-side only: the
 * publisher's thresholds say what is BIG (2x, half, a quarter of the record); this says what is not worth a word.
 */
export const STEADY_RATIO = 1.25;

/** The ratio the publisher coded, back out of the donor pathway's 0–1 number. */
export function changeRatio(coded: number): number {
  const p = Math.min(1 - CHANGE_POLICY.codeFloor, Math.max(CHANGE_POLICY.codeFloor, coded));
  return 2 ** (probit(p) * CHANGE_POLICY.scale);
}

/** A ratio as the publisher codes it — the inverse of `changeRatio`, for the scale's stops. */
export function codedChange(ratio: number): number {
  const p = normCdf(Math.log2(ratio) / CHANGE_POLICY.scale);
  return Math.min(1 - CHANGE_POLICY.codeFloor, Math.max(CHANGE_POLICY.codeFloor, p));
}

/**
 * THE SCALE'S STOPS, by the ratio each colour names: a big fall (a quarter, half), steady, a big rise (twice, four
 * times). The map's `standing` mode carries one stop at each (`layers.source.json`, offset into its own domain — see
 * `toRamp` in @app/ui), and a test holds the two equal.
 */
export const CHANGE_STOP_RATIOS: readonly number[] =
  [CHANGE_POLICY.fallRatio ** 2, CHANGE_POLICY.fallRatio, 1, CHANGE_POLICY.riseRatio, CHANGE_POLICY.riseRatio ** 2];

export type ChangeKind = "big-rise" | "rise" | "steady" | "fall" | "big-fall";

export function changeKind(ratio: number): ChangeKind {
  if (ratio >= CHANGE_POLICY.riseRatio) return "big-rise";
  if (ratio >= STEADY_RATIO) return "rise";
  if (ratio <= CHANGE_POLICY.fallRatio) return "big-fall";
  if (ratio <= 1 / STEADY_RATIO) return "fall";
  return "steady";
}

const HEADLINE: Record<ChangeKind, string> = {
  "big-rise": "A big rise expected",
  rise: "Rising",
  steady: "Little change expected",
  fall: "Falling",
  "big-fall": "A big fall expected",
};

/** The headline for a section's expected change. */
export function changeHeadline(ratio: number): string {
  return HEADLINE[changeKind(ratio)];
}

const round5 = (n: number) => Math.round(n / 5) * 5;

/** "3×" — whole numbers to 20, then fives, and past a hundredfold no figure the model's own bounds would support. */
function times(r: number): string {
  return `${r < 20 ? Math.round(r) : round5(r)}×`;
}

/**
 * A ratio to today's flow, in words: "about 3× today's flow", "about 40% more than today's flow", "about 60% of
 * today's flow", "about today's flow".
 */
export function ratioWords(r: number): string {
  if (r >= 100) return "more than 100× today's flow";
  if (r >= CHANGE_POLICY.riseRatio) return `about ${times(r)} today's flow`;
  if (r >= STEADY_RATIO) return `about ${round5((r - 1) * 100)}% more than today's flow`;
  if (r > 1 / STEADY_RATIO) return "about today's flow";
  return `about ${Math.max(5, round5(r * 100))}% of today's flow`;
}

/** "0.6×", "1.3×", "3×" — the short form, for a range. */
export function ratioShort(r: number): string {
  if (r >= 100) return "100×+";
  return r >= 2 ? `${Math.round(r)}×` : `${(Math.round(r * 10) / 10).toFixed(1)}×`;
}

/**
 * A gauge dot's label under Change (T126: the dots follow the horizon): the station's own ratio to today's flow, short —
 * "3× today", "0.5× today", "1.0× today". The colour already says rising or falling; the label says how much, in the
 * same short form the panel's ranges use. Pass the station's own `ratio` (the index row's `change.ratio`): the coded
 * number the colour rides is clamped (`codeFloor`), so decoded it tops out near 36×.
 */
export function changeDotLabel(ratio: number, lowWater = false): string {
  return `${ratioShort(ratio)} today${lowWater ? ` · ${CHANGE_LOW_WATER_MARK}` : ""}`;
}

/**
 * THE LOW-WATER CAVEAT ON THE MAP (T157; T144 left it on the panel only): a gauge whose change is ELF's alone colours the
 * Change map like any other, so its dot carries this mark and the legend under Change says what it means.
 */
export const CHANGE_LOW_WATER_MARK = "low-water model";


const dayNumber = (iso: string): number => {
  const [y, m, d] = iso.split("-").map(Number) as [number, number, number];
  return Date.UTC(y, m - 1, d) / 86_400_000;
};

/**
 * A forecast day as a reader names it from `today` (both "YYYY-MM-DD", calendar days, never instants): "today",
 * "tomorrow", "Sat" within the week, "Sat 31 Oct" a week or more out (a weekday alone would be last week's or next).
 */
export function changeDay(iso: string, today: string): string {
  const n = dayNumber(iso) - dayNumber(today);
  if (n === 0) return "today";
  if (n === 1) return "tomorrow";
  const [, m, d] = iso.split("-").map(Number) as [number, number, number];
  const wd = weekdayAbbr(new Date(dayNumber(iso) * 86_400_000).getUTCDay());
  return n > 1 && n < 7 ? wd : `${wd} ${d} ${monthAbbr(m - 1)}`;
}

const on = (iso: string, today: string) => {
  const w = changeDay(iso, today);
  return w === "today" || w === "tomorrow" ? w : `on ${w}`;
};
const by = (iso: string, today: string) => `by ${changeDay(iso, today)}`;

/** One end of a run's change, as the feed's station file carries it (`forecasts[model].change.peak` / `.trough`). */
export interface ChangeEnd {
  day: string;
  ratio: number;
  /** The model's own flow that day (m³/s), its highest for a peak, its lowest for a trough. */
  value: number;
  /** The day's percentile for the date (the model's flow rescaled to the gauge), where the station has a record. */
  percentile?: number;
  /** That percentile less today's, where both are known. */
  points?: number;
  /** The first day back within half of the move — a spike's end; null for a step. */
  back: string | null;
  /**
   * A station's combined change only (T144 review): the last day the models that made this end all speak — COFFEE's
   * fifth — when it is not back by then. Past it the line is the other model alone, so a drop there is a voice leaving,
   * not the river coming back; the words say the move is still on where that model stops.
   */
  until?: string;
}

/** A run's expected change (`forecasts[model].change`). */
export interface RunChange {
  dir: "rise" | "fall" | "both" | "steady";
  /** The model's own flow now (m³/s): every ratio is against this, never against the gauge. */
  base: number;
  peak: ChangeEnd;
  trough: ChangeEnd;
  /** `[day, lowest, highest]` per forecast day, in the model's own m³/s. */
  days: readonly (readonly [string, number, number])[];
}

/**
 * THE STATION'S CHANGE (T144, the feed's station-file `change`): which models make it is decided by the publisher
 * (`change_source` in pipeline/gauges/feed/change.py) where the runs live — CLEVER and COFFEE combined day by day where
 * both reach (equal weights, the geometric mean of their ratios), else the one that does, else ELF marked `lowWater`.
 * Its days are ratios to today (`base` 1). This file only words it.
 */
export interface SourceChange extends RunChange {
  models: readonly string[];
  /** Only the low-water model (ELF) spoke: the words carry `CHANGE_LOW_WATER`. */
  lowWater: boolean;
  /** Each model's last day in the window: COFFEE runs five days, CLEVER ten. */
  reach: Readonly<Record<string, string>>;
}

/** Who the station's change is from, as its line opens: "CLEVER and COFFEE together", "CLEVER", "ELF". */
export function changeSourceName(models: readonly string[]): string {
  return models.length > 1 ? `${models.slice(0, -1).join(", ")} and ${models[models.length - 1]} together`
    : models[0] ?? "";
}

/** The station's change as the Change block's first line: "CLEVER and COFFEE together: rises to about 160× …". */
export function changeSourceLine(c: SourceChange, today: string): string {
  const stops = (day: string) => c.models.filter((m) => c.reach[m] === day).join(" and ");
  return changeLine(changeSourceName(c.models), c, today, stops);
}

/**
 * THE LOW-WATER CAVEAT (owner 2026-10-23: "maybe have a warning for it being a low water forecast"). ELF is an empirical
 * recession extrapolation with no weather in it (BC River Forecast Centre, ELF technical reference 2023: forecasts of a
 * rise "are not recommended … because the model has no meteorological data input"), so it is said whenever ELF alone
 * makes a station's change.
 */
export const CHANGE_LOW_WATER = "ELF is a low-water model: it carries the river's recent fall forward and has no "
  + "rain or snowmelt in it, so it cannot see a rise coming.";

/**
 * THE SECTION'S LOW-WATER CAVEAT (T157; T144 said it only on the charted gauge's Change block): the gauges whose change
 * is ELF's alone among those COUNTED in this spot's answer, named, then `CHANGE_LOW_WATER`. "This change" when every
 * counted gauge is one, "Part of this change" otherwise. Null when none is.
 */
export function changeLowWaterNote(stations: readonly string[], all: boolean): string | null {
  if (!stations.length) return null;
  const who = stations.length === 1 ? stations[0]!
    : `${stations.slice(0, -1).join(", ")} and ${stations[stations.length - 1]}`;
  return `${all ? "This change" : "Part of this change"} comes from ELF alone (${who}). ${CHANGE_LOW_WATER}`;
}

/**
 * Where a combined change becomes one model's alone: "COFFEE runs to Mon; after that, CLEVER alone." Null when every
 * model reaches the same last day, or one model speaks.
 */
export function changeReachNote(c: SourceChange, today: string): string | null {
  if (c.models.length < 2) return null;
  const ends = c.models.map((m) => ({ m, day: c.reach[m] })).filter((e): e is { m: string; day: string } => !!e.day);
  const last = Math.max(...ends.map((e) => dayNumber(e.day)));
  const short = ends.filter((e) => dayNumber(e.day) < last);
  if (!short.length) return null;
  const rest = ends.filter((e) => dayNumber(e.day) === last).map((e) => e.m);
  return `${short.map((e) => `${e.m} runs to ${changeDay(e.day, today)}`).join("; ")}; after that, `
    + `${rest.join(" and ")} alone.`;
}

/** "(high for the time of year)" when the move is big against the record; nothing otherwise. */
function forTheDate(e: ChangeEnd): string {
  if (e.percentile === undefined || e.points === undefined) return "";
  if (Math.abs(e.points) < CHANGE_POLICY.bigPoints) return "";
  return ` (${plainStanding(standing(e.percentile)).toLowerCase()})`;
}

/**
 * STILL MOVING WHERE THE FORECAST STOPS (T52 review): an extreme on the last day the run was read for is no step — the
 * forecast ends while it is still rising (or falling), so whether it holds is not something it says. The publisher calls
 * this shape `open`; here it is read off the run's own days.
 */
const LAST_SHOWN = { rise: "still rising on the last day shown", fall: "still falling on the last day shown" } as const;

/** Who stops on a day (`SourceChange.reach`), for an end that is still on where they do: "COFFEE". */
type Stops = (day: string) => string;

/** "still up when COFFEE's forecast ends" — an end not back by the day a joined model stops (`ChangeEnd.until`). */
const stillOn = (rise: boolean, until: string, stops: Stops) =>
  `still ${rise ? "up" : "down"} when ${stops(until)}'s forecast ends`;

function riseWords(e: ChangeEnd, today: string, last: string | undefined, tail = true, stops?: Stops): string {
  if (e.back) return `rises to ${ratioWords(e.ratio)} ${on(e.day, today)}${forTheDate(e)}, back down ${by(e.back, today)}`;
  const head = `rises to ${ratioWords(e.ratio)} ${by(e.day, today)}${forTheDate(e)}`;
  if (!tail) return head;
  if (e.until && stops) return `${head}, ${stillOn(true, e.until, stops)}`;
  return e.day === last ? `${head}, ${LAST_SHOWN.rise}` : `${head} and stays up`;
}

function fallWords(e: ChangeEnd, today: string, last: string | undefined, tail = true, stops?: Stops): string {
  if (e.back) return `falls to ${ratioWords(e.ratio)} ${on(e.day, today)}${forTheDate(e)}, back up ${by(e.back, today)}`;
  const head = `falls to ${ratioWords(e.ratio)} ${by(e.day, today)}${forTheDate(e)}`;
  if (tail && e.until && stops) return `${head}, ${stillOn(false, e.until, stops)}`;
  return tail && e.day === last ? `${head}, ${LAST_SHOWN.fall}` : head;
}

/**
 * ONE MODEL'S EXPECTED CHANGE, as the panel says it, the model named first:
 *   "CLEVER: rises to about 23× today's flow on Mon (very high for the time of year), back down by Tue."
 *   "ELF: falls to about 60% of today's flow by Sun 1 Nov (low for the time of year)."
 *   "COFFEE: little change expected."
 * `today` is the reader's calendar day ("YYYY-MM-DD"); the run's days are the Centre's (Pacific).
 */
export function changeLine(model: string, c: RunChange, today: string, stops?: Stops): string {
  const last = c.days.length ? c.days[c.days.length - 1]![0] : undefined;
  const said = c.dir === "rise" ? riseWords(c.peak, today, last, true, stops)
    : c.dir === "fall" ? fallWords(c.trough, today, last, true, stops)
    : c.dir === "both"
      ? (dayNumber(c.peak.day) <= dayNumber(c.trough.day)
        ? `${riseWords({ ...c.peak, back: null }, today, last, false)}, then ${fallWords(c.trough, today, last, true, stops)}`
        : `${fallWords({ ...c.trough, back: null }, today, last, false)}, then ${riseWords(c.peak, today, last, true, stops)}`)
      : "little change expected";
  return `${model}: ${said}.`;
}

/** The section's expected change, in one sentence after the headline (`estimateSentence` under Change). */
export function changeSentence(ratio: number, confidence: string, donors: number): string {
  const gauges = donors === 1 ? "one gauge" : `${donors} gauges`;
  const what = changeKind(ratio) === "steady"
    ? "Staying near today's flow"
    : `At its ${ratio >= 1 ? "highest" : "lowest"}, ${ratioWords(ratio)}`;
  return `${what} — ${confidence}, forecast from ${gauges} nearby.`;
}

/** What one donor expects, for its row (`donorReads` under Change). */
export function changeReads(ratio: number): string {
  return changeKind(ratio) === "steady" ? "Expects little change."
    : `Expects ${ratioWords(ratio)} at its ${ratio >= 1 ? "highest" : "lowest"}.`;
}

/** "Between 2× and 9× today's flow over the next 10 days" — the estimate's interval, decoded. */
export function changeRange(lo: number, hi: number): string {
  const a = ratioShort(changeRatio(lo)), b = ratioShort(changeRatio(hi));
  const span = a === b ? `About ${a}` : `Between ${a} and ${b}`;
  return `${span} today's flow over the next ${CHANGE_POLICY.days} days`;
}

/** The marked day under a model's line: its peak, or its lowest day when the run falls. */
export const CHANGE_MARK = { peak: "Peak", low: "Lowest" } as const;

/**
 * The words of a run's day line (`changeSpark` in @app/ui): the marked day ("Peak Mon", "Lowest Sun 1 Nov") and the
 * line's screen-reader name. `fall`: the line is each day's lowest and the mark the trough.
 */
export function changeSparkWords(fall: boolean, markDay: string, first: string, last: string,
                                 today: string): { mark: string; label: string } {
  return {
    mark: `${fall ? CHANGE_MARK.low : CHANGE_MARK.peak} ${changeDay(markDay, today)}`,
    label: `Each day's ${fall ? "lowest" : "highest"} forecast flow against today's, from ${changeDay(first, today)} `
      + `to ${changeDay(last, today)}`,
  };
}

const LEGEND_TITLE = `Expected change, next ${CHANGE_POLICY.days} days`;

/**
 * WHERE THE CHANGE COMES FROM ELF (T157): the low-water model's alone at some gauges (T144: no flood model reaches
 * them), which carries the river's recent fall forward and cannot see a rise coming. Said under the Change legend,
 * the meaning of the dots' `CHANGE_LOW_WATER_MARK`.
 */
const LEGEND_LOW_WATER = `"${CHANGE_LOW_WATER_MARK}": forecast by ELF alone, which has no rain in it and cannot `
  + "see a rise coming.";

/** The Change legend's words; its middle bracket is "little change" (`STEADY_RATIO` either way). */
export const CHANGE_LEGEND = {
  title: LEGEND_TITLE,
  /** the bar's screen-reader name */
  describe: `${LEGEND_TITLE}: colours from falling to rising; steady is little change`,
  low: "Falling",
  mid: "Steady",
  high: "Rising",
  steady: [codedChange(1 / STEADY_RATIO), codedChange(STEADY_RATIO)] as const,
  lowWater: LEGEND_LOW_WATER,
} as const;

/** Where a gauge mark sits on the Change legend, for a screen reader: "near 3× today's flow". */
export function changeMarkWords(at: number): string {
  return `near ${ratioShort(changeRatio(at))} today's flow`;
}

/**
 * THE CAVEAT the owner's own words imply (a spike, "then drop"): each gauge is read for its own largest move, so the
 * gauges behind a spot may peak on different days. Said once, under the answer.
 */
export const CHANGE_EXPLAINER = `Each gauge's forecast is read day by day for its largest rise or fall over the next `
  + `${CHANGE_POLICY.days} days, against its own forecast for today. The figure for this spot combines them the way `
  + "every estimate here does, so the gauges behind it may peak on different days.";
