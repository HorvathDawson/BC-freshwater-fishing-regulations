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
import { localStamp, shortDay, shortStamp } from "./day";
import { byCadence } from "./staleness";
import { CHANGE_EXPLAINER, CHANGE_MARK, CHANGE_POLICY, changeHeadline, changeRange, changeRatio, changeReads,
         changeSentence } from "./change";

/* ------------------------------------------------------------------- horizons ---- */

/**
 * How far ahead the map and the sheet are looking. 0 is now.
 *
 * The values are the days the publisher ranks a forecast for — see HORIZONS in
 * `pipeline/gauges/feed/publish.py`. Beyond about five the model's own bounds are wide
 * enough that the answer is often "normal for the season"; TEN DAYS is there because the
 * owner wants it for planning (2026-10-20), and where no run reaches it the panel says
 * `no-forecast` and the map draws no reading — never the +5 value. In core so the words
 * for each day can be derived from the one list; `@app/data` re-exports both.
 *
 * KEYED BY DAY, NEVER BY POSITION. The feed's `ahead` block is `{"1": …, "10": …}` and is
 * read by `String(day)`; nothing indexes a horizon by its place in this list.
 *
 * AND ONE THAT IS NO DAY: "change" (T52, owner 2026-10-22: "a last option next to +10 … about expected change"), the
 * largest move each gauge's forecast expects over the next ten days, read day by day (`./change`). It is "ahead" like
 * the days — a forecast, refused as `no-forecast` — but it has no date, so ask `isAhead` / `aheadDays`, never `> 0`.
 */
export type Horizon = 0 | 1 | 3 | 5 | 10 | "change";
export const HORIZONS: readonly Horizon[] = [0, 1, 3, 5, 10, "change"];

/** The expected-change horizon. */
export const CHANGE: Horizon = "change";

/** A forecast rather than now: every horizon but 0, the Change view included. */
export const isAhead = (h: Horizon): boolean => h !== 0;

/** How many days ahead the answer is FOR — 0 for now and for Change, whose answer spans the window, not one day. */
export const aheadDays = (h: Horizon): number => (typeof h === "number" ? h : 0);

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
  if (d === "change")
    return { chip: "Change", heading: `EXPECTED CHANGE · NEXT ${CHANGE_POLICY.days} DAYS`,
             accessible: `Expected change over the next ${CHANGE_POLICY.days} days` };
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
  /** after a forecast run past `STALE_AFTER.forecast` — the Data block's "Likely out of date", in a line */
  runOld: "likely out of date",
  noEstimate: "NO ESTIMATE",
  tapToChart: "TAP A GAUGE TO SEE ITS CHART",
  donorList: "The gauges behind this estimate",
  youAreHere: "you are here",
  byThen: "by then",
  readingWater: "Reading the water",
  /** The one fold under the donor list (P16): the working's working and the credits. */
  fold: "Notes and sources",
  loadingRecord: "Loading the record",
  controls: { quantity: "Quantity", span: "Span", model: "Forecast model", gauge: "Gauge" },
  /** The gauge switcher's kicker, right above the chart it switches (owner 2026-10-21). */
  chartGauge: "CHART THIS GAUGE",
  /** The chart's zoom controls (owner 2026-10-21: "look at data better"). */
  zoom: { in: "Zoom in", out: "Zoom out", fit: "Whole chart", group: "Chart zoom" },
  /** How to work the chart, said once under it — touch and pointer each in their own words. */
  zoomHint: {
    touch: "Pinch to zoom, drag to move along, tap to read a value. Double-tap for the whole chart.",
    pointer: "Scroll to zoom, drag to move along, point to read a value. Double-click for the whole chart.",
  },
  quantity: { discharge: "Flow", level: "Level" },
  span: { "72h": "Last 72 hours", year: "This year" },
  /** The ONE place the app decides what a quantity is measured in. */
  unit: { discharge: "m³/s", level: "m" },
  /** The Change block's kicker over the lead gauge's own forecasts, one line per model (T52). */
  changeKicker: "WHAT THE FORECASTS EXPECT HERE",
  /** The marked day under a model's line: its peak, or its lowest day when the run falls. */
  changeMark: CHANGE_MARK,
  /** Under Change, when no run for the charted gauge can say (T52). */
  changeNone: "No forecast is running for this gauge now.",
  /** Under Change (T52): what the figure is, and that the gauges behind it may peak on different days. */
  changeExplainer: CHANGE_EXPLAINER,
  explainer: "No gauge sits exactly here, so the reading is carried from the nearest ones. A "
    + "gauge counts for more when it drains a piece of country the same size as this one, "
    + "when it sits upstream, and when it has a long record. The percentage beside each is "
    + "how much of the answer above came from it.",
} as const;

export type ChartSpan = keyof typeof CONDITIONS_WORDS.span;

/* --------------------------------------------------------------- no estimate ---- */

const WHY: Record<NoEstimate, string> = {
  "no-station": "No gauge on this water is close enough in size to speak for it.",
  // Every gauge in the panel quiet in the asked quantity, or reporting with no record to rank against (owner
  // 2026-10-22, "chosen live": the panel holds every station that could speak here; the feed says which do today).
  "no-record": "None of the gauges that speak for this spot has a reading today that can be "
             + "ranked against its record.",
  "regulated": "The only gauges within reach are on regulated water, where a reading "
             + "describes a release schedule rather than rainfall.",
  "too-uncertain": "The gauges that can speak for this spot disagree too much to tell a "
                 + "low river from a high one.",
  "offline": "The live readings did not arrive.",
  // A day ahead past every forecast run for these gauges — the +10 day often is, and every
  // forecast day is out of season. Not "no gauge close enough", which would be false.
  "no-forecast": "No forecast for the gauges here reaches this far ahead.",
};

/**
 * Under Change, no gauge here has a forecast RUNNING (T52): the models are seasonal, so this is the normal out-of-season
 * answer — never "steady", which would be a forecast nobody made.
 */
const NO_CHANGE_FORECAST = "No forecast is running for the gauges here now. The River Forecast Centre's models are "
  + "seasonal, so out of season there is no expected change to show.";

/** Why there is no answer, in words a reader can act on — at the horizon asked about. */
export function donorReason(why: NoEstimate, horizon: Horizon = 0): string {
  return why === "no-forecast" && horizon === "change" ? NO_CHANGE_FORECAST : WHY[why];
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
  /** `tidal`: the tide or a tidal river's backwater moves this station's level (`PanelRoute.tidal`, T78). */
  route?: { path: readonly unknown[]; name?: string | null; tidal?: boolean } | undefined;
  /** When the station last reported, from the feed's index (ISO), where the feed says. */
  observedAt?: string | null;
  /**
   * `false`: ECCC does not list this station as real-time on the feed's last run (the index's `listed`), so it is
   * "not a real-time gauge", not "not reporting". Absent (or true) where it is listed, or the index carries no list.
   */
  realtime?: boolean;
  /**
   * `true`: the station's own file was read and holds no data at all (`withLastReports`), so it "has never reported"
   * in the live feed. Absent where nobody asked, or the file said when — then the last report is `observedAt`.
   */
  noData?: boolean;
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

const DAY = 86_400_000;

/**
 * What the gauge itself reads, in the headline's words.
 *
 * A gauge with no percentile is not always silent: the feed may hold a reading from today that cannot be ranked (no
 * record in the quantity asked for). Calling that "not reporting" beside "last reported this evening" contradicts
 * itself, so with `ref` (the day the answer is for) a report within a day of it is said as what it is. A day ahead
 * with no forecast is a missing FORECAST, not a quiet gauge.
 */
export function donorReads(r: DonorFacts, o: { ref?: number; horizon?: Horizon } = {}): string {
  if (r.percentile !== null)
    return o.horizon === "change" ? changeReads(changeRatio(r.percentile)) : `Reads ${inTen(r.percentile)}.`;
  // Not on ECCC's real-time list at all: it is not silent today, it never speaks live — on any day asked about.
  if (r.realtime === false) return "Not a real-time gauge.";
  if (o.horizon === "change") return "No forecast running for this gauge now.";
  if (o.horizon) return "No forecast for this gauge that far ahead.";
  if (reportingUnranked(r, o.ref)) return "Reporting, but its reading cannot be ranked today.";
  return "Not reporting today.";
}

/**
 * A MEMBER THAT REPORTS BUT CANNOT BE RANKED at this horizon — `donorReads`' "Reporting, but …" branch, and the one
 * percentile-less member that stays a row. Every word about a member (its row label, its pin, its key) asks this, so
 * the map never says "not reporting" beside a row that says it reports (review 2026-10-22).
 */
export function donorUnranked(r: Pick<DonorFacts, "percentile" | "realtime" | "observedAt">,
                              o: { ref?: number; horizon?: Horizon } = {}): boolean {
  return r.percentile === null && r.realtime !== false && !o.horizon && reportingUnranked(r, o.ref);
}

/** No percentile, but a report within a day of `ref`: it speaks, its reading just cannot be ranked. */
function reportingUnranked(r: Pick<DonorFacts, "observedAt">, ref: number | undefined): boolean {
  const t = r.observedAt ? Date.parse(r.observedAt) : NaN;
  return ref !== undefined && Number.isFinite(t) && Math.abs(ref - t) < DAY;
}

/**
 * A SILENT MEMBER: no reading at the horizon asked about (owner 2026-10-22). Not on the real-time list, no forecast
 * that far ahead, or not reporting — every `donorReads` branch but a reading and "Reporting, but … cannot be ranked",
 * which is a gauge that speaks today and stays a row. A silent member is not drawn as a row or a pin; `silentLine`
 * names it instead.
 */
export function donorSilent(r: DonorFacts, o: { ref?: number; horizon?: Horizon } = {}): boolean {
  if (r.percentile !== null) return false;
  if (r.realtime === false || o.horizon) return true;
  return !reportingUnranked(r, o.ref);
}

const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;

/**
 * When a station last had data, SHORT, for inside a line: "14 Oct, 09:00 PDT", local and labelled (P16); a day-only
 * report (a daily mean) is that calendar day, "14 Oct"; a year other than `ref`'s is named. Null when it is not known.
 */
export function lastDataStamp(observedAt: string | null | undefined, ref: number, zone?: string): string | null {
  if (!observedAt || !Number.isFinite(Date.parse(observedAt))) return null;
  return ISO_DAY.test(observedAt) ? shortDay(observedAt, ref, zone) : shortStamp(Date.parse(observedAt), ref, zone);
}

/** One silent member, as the line says it: its name (its station id when the name is not in yet) and when. */
function silentMember(r: DonorFacts, ref: number, zone?: string): string {
  const name = r.route?.name || r.station;
  const at = lastDataStamp(r.observedAt, ref, zone);
  const last = at ? `last ${at}` : r.noData ? "has never reported" : "last report not known";
  if (r.realtime === false) return `${name} (not a real-time gauge${at ? `; ${last}` : ""})`;
  return `${name} (${last})`;
}

/**
 * THE SILENT MEMBERS, IN ONE LINE under the gauge table (owner 2026-10-22: "hidden but mentioned with a last data
 * time"): "Not reporting: Liard River at Upper Crossing (last 14 Oct, 09:00 PDT) · Coal River (not a real-time
 * gauge)". On a day ahead the members are silent because no forecast reaches it, and the line says that instead.
 * Null when every member speaks. `ref` is the instant the answer is for; `zone` is for tests.
 */
export function silentLine(rows: readonly DonorFacts[],
                           o: { ref: number; horizon?: Horizon; zone?: string }): string | null {
  const silent = rows.filter((r) => donorSilent(r, o));
  if (!silent.length) return null;
  const kicker = o.horizon === "change" ? "No forecast running"
    : o.horizon ? "No forecast this far ahead" : "Not reporting";
  return `${kicker}: ${silent.map((r) => silentMember(r, o.ref, o.zone)).join(" · ")}`;
}

/**
 * WHEN A QUIET GAUGE LAST SPOKE (owner 2026-10-21), local and labelled like every other time on the screen (P16).
 * Unknown is said, never left blank: a blank reads as "never".
 */
export function donorLastReport(observedAt: string | null | undefined, zone?: string): string {
  // A DAY WITH NO CLOCK (the feed's daily mean, when nothing finer survives) names that calendar day: parsed as an
  // instant it is UTC midnight, which a Pacific reader would be told was the evening before.
  if (observedAt && /^\d{4}-\d{2}-\d{2}$/.test(observedAt) && Number.isFinite(Date.parse(observedAt)))
    return `Last reported on ${observedAt}.`;
  const t = observedAt ? Date.parse(observedAt) : NaN;
  return Number.isFinite(t) ? `Last reported ${localStamp(t, zone)}.`
    : "When it last reported is not known.";
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
export function donorRowLabel(r: DonorFacts, pickable: boolean, on: boolean,
                              o: { ref?: number; horizon?: Horizon } = {}): string {
  const said = r.percentile === null
    ? `${r.realtime === false ? "not a real-time gauge"
        : donorUnranked(r, o) ? "reporting but not ranked today" : "not reporting today"}, so it counts for nothing`
    : `${Math.round(r.weight * 100)} per cent of the answer`;
  const base = `${r.station}, ${ROLE[r.role]}, ${sizeApart(r.areaRatio)}, ${said}`;
  return pickable ? `${base}. ${on ? "Showing" : "Show"} this gauge's chart` : base;
}

/**
 * A choice in the chart's gauge switcher, for a screen reader: "Chart 08MH016, Chilliwack River at outlet of
 * Chilliwack Lake, 35% of this estimate" — "Showing …" for the one on the chart.
 */
export function gaugeChoiceLabel(r: DonorFacts, on: boolean): string {
  const name = r.route?.name ? `, ${r.route.name}` : "";
  const said = r.percentile === null ? "not counted today" : `${donorShare(r)} of this estimate`;
  return `${on ? "Showing" : "Chart"} ${r.station}${name}, ${said}`;
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

/** The headline: what the water is doing, before any number. Under Change, what it is expected to do. */
export function estimateHeadline(e: Estimate, horizon: Horizon = 0): string {
  return horizon === "change" ? changeHeadline(changeRatio(e.percentile)) : plainStanding(standing(e.percentile));
}

/** "Lower than 8 days in 10 at this time of year — fairly confident, from 2 gauges nearby." */
export function estimateSentence(e: Estimate, horizon: Horizon): string {
  if (horizon === "change") return changeSentence(changeRatio(e.percentile), confidenceWord(e.plusMinus), e.donors);
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
  const base = horizon === "change" ? changeRange(lo / 100, hi / 100)
    : `${bound(lo)}–${bound(hi)} percentile for ` + seasonPhrase(new Date(day.getTime() + aheadDays(horizon) * DAY_MS));
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

/** The tide, in one sentence — `tidalNote`'s words. */
export const TIDAL_NOTE = "One of these gauges is tidal: its level rises and falls with the tide and with water "
  + "backed up from downstream, not only with what this river carries.";

/**
 * THE TIDE, said once (T78; owner 2026-10-22: "In the Level view, check and disclose tidal or backwater influence
 * where the record shows it"). LEVEL ONLY — the Flow view never mixes in a stage — and, like the dam, only about a
 * number being shown: a reporting member whose station the release flags tidal (`gauge.tidal`, HYDAT's tidal remarks
 * + data/curated/gauges/tidal.json). The flag rides on the member's route, so the line appears once routes are in.
 */
export function tidalNote(rows: readonly DonorFacts[], quantity: "discharge" | "level"): string | null {
  return quantity === "level" && rows.some((r) => r.route?.tidal === true && r.percentile !== null)
    ? TIDAL_NOTE : null;
}

/** The donors disagree by more than 25 points. */
export function disagreeNote(e: Estimate, horizon: Horizon = 0): string | null {
  if (!(e.spread > 25)) return null;
  if (horizon === "change")
    return "These gauges expect different things. The range above is the honest answer rather than their average.";
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
 * When the run was made, beside what it says — LOCAL AND LABELLED (P16), in the same form as
 * the reading's "observed" (`localStamp`). It printed the ISO string's own clock unlabelled,
 * then labelled UTC; neither is the reader's clock. A time that does not parse is left out
 * rather than guessed. `zone` is for tests; left out, it is the device's.
 *
 * A run older than the forecast's line in THE ONE staleness table (`STALE_AFTER.forecast`, staleness.ts — a day and
 * a half: the models issue daily) says "likely out of date" after it, the same words the Data block's badge uses.
 * `now` is for tests; left out, it is the device's clock.
 */
export function forecastRunLine(run: ForecastRunFacts & { issuedAt: string },
                                zone?: string, now: number = Date.now()): string {
  const t = Date.parse(run.issuedAt);
  const when = Number.isFinite(t) ? ` issued ${localStamp(t, zone)}` : "";
  const old = Number.isFinite(t) && byCadence("forecast", now - t) === "stale" ? ` · ${CONDITIONS_WORDS.runOld}` : "";
  return `${run.model} run${when} · ${run.horizonDays}-day outlook${old}`;
}

/* ------------------------------------------------------------- the read-out ---- */

/** A chart value as a reader reads it: three figures, no false precision. */
export function chartFigure(v: number): string {
  const a = Math.abs(v);
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}

export interface ReadoutFacts {
  /** The sample's instant (epoch ms). */
  t: number;
  value: number;
  unit: string;
  /** A measurement, or a model's forecast — never said the same way. */
  kind: "reading" | "forecast";
  /** A daily series names the day only; an hourly one the minute. */
  daily: boolean;
  /** The record's median for that date, where the chart has a band. */
  median?: number | null;
}

/**
 * THE VALUE UNDER THE FINGER: "2026-10-20 at 14:00 PDT · 4.12 m³/s · median 10.6", or for a forecast step
 * "… · 4.12 m³/s forecast". Local and labelled (P16); a daily mean says its day and no clock.
 */
export function chartReadoutLine(f: ReadoutFacts, zone?: string): string {
  // A DAILY SAMPLE NAMES A CALENDAR DAY, stamped "YYYY-MM-DD" (UTC midnight): it is said as that day, never pushed
  // into the evening before by a time zone.
  const when = f.daily ? new Date(f.t).toISOString().slice(0, 10) : localStamp(f.t, zone);
  const what = `${chartFigure(f.value)} ${f.unit}${f.kind === "forecast" ? " forecast" : ""}`;
  const norm = f.median != null && Number.isFinite(f.median) ? ` · median ${chartFigure(f.median)}` : "";
  return `${when} · ${what}${norm}`;
}

/** The forecast's label on the chart. */
export function forecastChartLabel(run: ForecastRunFacts): string {
  return `${run.model} · ${run.horizonDays} days`;
}

/** A model in the picker. */
export function forecastModelOption(run: ForecastRunFacts): string {
  return `${run.model} · ${run.horizonDays}d`;
}
