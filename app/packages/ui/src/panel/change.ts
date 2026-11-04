import { CHANGE_LOW_WATER, CHANGE_POLICY, changeDay, changeLine, changeModelsNote, changeReachNote, changeSourceLine,
         changeSourceName, changeSparkWords, CONDITIONS_WORDS, ratioShort, type RunChange, type SourceChange } from "@app/core";
import type { Horizon } from "@app/data";

/* ------------------------------------------------------------------ expected change (T52) ---- */

/** One run's change as the panel draws it: the model's line, and its days as a small line with the peak marked. */
export interface ChangeRunVM {
  model: string;
  /** "CLEVER: rises to about 23× today's flow on Mon (very high for the time of year), back down by Tue." */
  line: string;
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
  /** (T367) the models behind a station's change, in one quiet line ("Models: CLEVER rises to 4× (Tue) · COFFEE …") */
  models: string | null;
  /** (T367) ONE chart: the station's change bold, each model's own thin, on one ×today scale; null under two days */
  chart: ChangeChartVM | null;
}

/**
 * (T367, owner 2026-11-04: the two small graphs were "hard to read") THE CHANGE CHART: every line on one LOG scale of
 * the ratio to today, gridlines at the round ratios in range (½×, 1×, 2×, 4× …, 1× always), the days along the foot,
 * the station's line bold with its peak (or low) marked, each model's own line thin. In a w × h box, top-left origin.
 */
export interface ChangeChartVM {
  series: readonly { model: string; path: string; bold: boolean }[];
  grid: readonly { y: number; label: string }[];
  days: readonly { x: number; label: string }[];
  mark: { x: number; y: number; label: string } | null;
  /** (T157) where a joined model stops: the bold line breaks there (never a fall drawn across it), a tick at `x` */
  handovers: readonly { x: number; model: string }[];
  /** For a screen reader: what the chart shows. */
  label: string;
}

/** The chart over a lead change (bold) and the models' own (thin); the days are the lead's. */
export function changeChart(lead: { model: string; change: RunChange }, parts: readonly { model: string; change: RunChange }[],
                            today: string, w: number, h: number): ChangeChartVM | null {
  const c = lead.change;
  const fall = c.dir === "fall";
  const days = c.days.map(([d]) => d);
  if (days.length < 2 || !(c.base > 0)) return null;
  const ratiosOf = (r: RunChange) => new Map(r.days.map(([d, lo, hi]) => [d, Math.max(1e-3, (fall ? lo : hi) / r.base)]));
  const all = [lead, ...parts].filter((p) => p.change.base > 0).map((p) => ({ ...p, at: ratiosOf(p.change) }));
  const logs = all.flatMap((p) => [...p.at.values()].map(Math.log));
  const top = Math.max(Math.log(2), ...logs), bottom = Math.min(Math.log(0.5), ...logs);
  const span = top - bottom || 1;
  const x = (i: number) => Math.round((i / (days.length - 1)) * w * 10) / 10;
  const y = (l: number) => Math.round((h - ((l - bottom) / span) * h) * 10) / 10;
  // (T157) the lead's line breaks after each joined model's last day drawn (a gap, never a fall drawn across it)
  const reach = "models" in c && Array.isArray((c as SourceChange).models) && (c as SourceChange).models.length > 1
    ? (c as SourceChange).reach : {};
  const stops = Object.entries(reach).map(([model, d]) => ({ model, day: d, at: days.findIndex((e) => e > d) - 1 }))
    .filter((s) => s.at >= 0).sort((a, b) => a.at - b.at || a.model.localeCompare(b.model));
  const breaks = new Set(stops.map((s) => s.at + 1));
  const series = all.map((p, k) => ({
    model: p.model, bold: k === 0,
    path: days.map((d, i) => (p.at.has(d)
      ? `${i && p.at.has(days[i - 1]!) && !(k === 0 && breaks.has(i)) ? "L" : "M"}${x(i)},${y(Math.log(p.at.get(d)!))}` : ""))
      .filter(Boolean).join(" "),
  })).filter((s) => s.path);
  const grid = [0.125, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128].filter((r) => Math.log(r) >= bottom && Math.log(r) <= top)
    .map((r) => ({ y: y(Math.log(r)), label: ratioShort(r) }));
  const markDay = fall ? c.trough.day : c.peak.day;
  const mi = days.indexOf(markDay);
  const lr = all[0]!.at.get(markDay);
  const words = changeSparkWords(fall, markDay, days[0]!, days[days.length - 1]!, today);
  const mid = Math.floor((days.length - 1) / 2);
  return {
    series, grid,
    days: [0, mid, days.length - 1].map((i) => ({ x: x(i), label: changeDay(days[i]!, today) })),
    mark: c.dir !== "steady" && mi >= 0 && lr !== undefined ? { x: x(mi), y: y(Math.log(lr)), label: words.mark } : null,
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
  const said = runs.filter((r): r is typeof r & { change: RunChange } => !!r.change)
    .filter((r) => !source || (source.models.length > 1 && source.models.includes(r.model)))
    .sort((a, b) => order(a.model) - order(b.model));
  // (T367) the station's line says what will happen; who says it is the note under it, so it opens on the words
  const lines: ChangeRunVM[] = source
    ? [{ model: changeSourceName(source.models), line: changeSourceLine(source, day, source.models.length < 2),
         part: false }]
    : said.map((r) => ({ model: r.model, line: changeLine(r.model, r.change, day), part: false }));
  const lead = source ? { model: changeSourceName(source.models), change: source as RunChange }
    : said[0] ? { model: said[0].model, change: said[0].change } : null;
  const parts = said.filter((r) => !lead || r.model !== lead.model).map((r) => ({ model: r.model, change: r.change }));
  return {
    runs: lines,
    caveat: source?.lowWater ? CHANGE_LOW_WATER : null,
    note: source ? changeReachNote(source, day) : null,
    none: lines.length ? null : CONDITIONS_WORDS.changeNone,
    attribution: lines.length ? attribution : null,
    models: source ? changeModelsNote(said.map((r) => ({ model: r.model, change: r.change })), day) : null,
    chart: lead ? changeChart(lead, source ? parts : [], day, size.width, size.height) : null,
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
