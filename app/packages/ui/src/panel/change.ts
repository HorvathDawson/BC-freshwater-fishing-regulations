import { CHANGE_LOW_WATER, CHANGE_POLICY, changeDay, changeLine, changeReachNote, changeSourceLine, changeSourceName,
         changeSparkWords, CONDITIONS_WORDS, type RunChange, type SourceChange } from "@app/core";
import type { Horizon } from "@app/data";

/* ------------------------------------------------------------------ expected change (T52) ---- */

/** One run's change as the panel draws it: the model's line, and its days as a small line with the peak marked. */
export interface ChangeRunVM {
  model: string;
  /** "CLEVER: rises to about 23× today's flow on Mon (very high for the time of year), back down by Tue." */
  line: string;
  /** The day-by-day line (`changeSpark`): null when the run has fewer than two days. */
  spark: ChangeSparkVM | null;
  /** One model's own line under a combined change (T144): drawn quieter, it is what was joined, not the answer. */
  part: boolean;
}

/** The Change block under the reading, for the lead gauge. */
export interface ChangeVM {
  /**
   * The station's change first (T144: "CLEVER and COFFEE together", one model, or ELF), then each combined model's own
   * line; on a feed from before T144, every run in the publisher's order. Empty: `none` says why.
   */
  runs: readonly ChangeRunVM[];
  /** The low-water caveat when ELF alone makes the change (`CHANGE_LOW_WATER`); null otherwise. */
  caveat: string | null;
  /** Where a combined change becomes one model's alone ("COFFEE runs to Mon; after that, CLEVER alone."); or null. */
  note: string | null;
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
  /**
   * WHERE A JOINED MODEL STOPS (T157): the line is broken there (a gap in `path`, never a fall drawn across it) and a
   * tick stands at `x`, between the model's last day and the next. Empty for one model's line.
   */
  handovers: readonly { x: number; model: string }[];
  /** For a screen reader: the line in words. */
  label: string;
}

/**
 * A run's days as a line, on a LOG scale of the ratio to its own today (a 23-fold spike and a 20% dip both read), each
 * day at its highest step for a rise and its lowest for a fall. Pure, so the shape is pinned in a test.
 *
 * A STATION'S JOINED CHANGE (T144) is CLEVER and COFFEE together up to COFFEE's fifth day and CLEVER alone after it, so
 * the joined value can drop on the day after with neither model forecasting a fall (T144 review: a voice leaving, not
 * the river). The line is broken at each such handover (`SourceChange.reach`, a model's last day before the window's
 * end) and the place marked (`handovers`), so the drawing says what the words say ("still up when COFFEE's forecast
 * ends") instead of a cliff.
 */
export function changeSpark(c: RunChange | SourceChange, today: string, w: number, h: number): ChangeSparkVM | null {
  const base = c.base;
  const fall = c.dir === "fall";
  const pts = c.days.map(([d, lo, hi]) => ({ d, r: Math.max(1e-3, (fall ? lo : hi) / base) }));
  if (pts.length < 2 || !(base > 0)) return null;
  const logs = pts.map((p) => Math.log(p.r));
  const top = Math.max(0, ...logs), bottom = Math.min(0, ...logs);
  const span = top - bottom || 1;
  const x = (i: number) => Math.round((i / (pts.length - 1)) * w * 10) / 10;
  const y = (l: number) => Math.round((h - ((l - bottom) / span) * h) * 10) / 10;
  // the days after which a joined model stops: the next day starts a new stretch of the line (a move, not a draw)
  // (a run's own change may carry a `reach` and no `models`: one model, nothing joined)
  const joined = "models" in c && Array.isArray(c.models) && c.models.length > 1;
  const reach = joined && "reach" in c ? c.reach : {};
  // (`at`: the model's last day drawn, the last day on or before its reach; a model reaching past the line has none)
  const stops = Object.entries(reach).map(([model, day]) => ({ model, day, at: pts.findIndex((p) => p.d > day) - 1 }))
    .filter((s) => s.at >= 0).sort((a, b) => a.at - b.at || a.model.localeCompare(b.model));
  const breaks = new Set(stops.map((s) => s.at + 1));
  const path = pts.map((p, i) => `${i && !breaks.has(i) ? "L" : "M"}${x(i)},${y(Math.log(p.r))}`).join(" ");
  const markDay = fall ? c.trough.day : c.peak.day;
  const at = Math.max(0, pts.findIndex((p) => p.d === markDay));
  const first = pts[0]!.d, last = pts[pts.length - 1]!.d;
  const words = changeSparkWords(fall, markDay, first, last, today);
  return {
    path,
    mark: { x: x(at), y: y(logs[at]!), label: words.mark },
    ends: [{ x: 0, label: changeDay(first, today) }, { x: w, label: changeDay(last, today) }],
    handovers: stops.map((s) => ({ x: Math.round(((x(s.at) + x(s.at + 1)) / 2) * 10) / 10, model: s.model })),
    label: words.label + stops.map((s) => `; ${s.model}'s forecast ends ${changeDay(s.day, today)}`).join(""),
  };
}

/**
 * WHICH RUN THE CHART DRAWS (T144 review). The reader's pick wins. Under Change, with no pick, it is the first model
 * the publisher's change is made of (`source.models[0]`: CLEVER where it runs, else COFFEE, else ELF) — owner
 * 2026-10-23, "no elf": the freshest run, the default everywhere else, is often ELF, which drew "ELF · 30 days" under
 * "CLEVER and COFFEE together". Elsewhere, or with no station change (a feed from before T144), undefined: freshest.
 */
export function changeChartModel(picked: string | null, horizon: Horizon,
                                 source: SourceChange | null | undefined): string | undefined {
  if (picked) return picked;
  return horizon === "change" ? source?.models[0] : undefined;
}

/**
 * The Change block for the charted gauge, on the reader's `today`.
 *
 * WHICH MODELS SPEAK IS THE PUBLISHER'S (T144, `source`, the station file's `change`): its line first, with its days
 * as the line; then, where it combines CLEVER and COFFEE, each one's own line (no day line) so the reader sees what was
 * joined; ELF only when it is the source, and then with the low-water caveat. A feed from before T144 has no `source`:
 * every run is listed as it stands, as it was.
 */
export function changeView(runs: readonly { model: string; change?: RunChange | null; series?: unknown }[],
                           today: Date, attribution: string | null, size = { width: 280, height: 44 },
                           source: SourceChange | null = null): ChangeVM {
  const day = isoDay(today);
  // a steady change has no day worth marking: its line says "little change", nothing more
  const spark = (c: RunChange | SourceChange) => (c.dir === "steady" ? null : changeSpark(c, day, size.width, size.height));
  const said = runs.filter((r): r is typeof r & { change: RunChange } => !!r.change)
    .filter((r) => !source || (source.models.length > 1 && source.models.includes(r.model)))
    .sort((a, b) => order(a.model) - order(b.model));
  const lines: ChangeRunVM[] = [
    ...(source ? [{ model: changeSourceName(source.models), line: changeSourceLine(source, day), spark: spark(source),
                    part: false }] : []),
    ...said.map((r) => ({ model: r.model, line: changeLine(r.model, r.change, day),
                          spark: source ? null : spark(r.change), part: !!source })),
  ];
  return {
    runs: lines,
    caveat: source?.lowWater ? CHANGE_LOW_WATER : null,
    note: source ? changeReachNote(source, day) : null,
    none: lines.length ? null : CONDITIONS_WORDS.changeNone,
    attribution: lines.length ? attribution : null,
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
