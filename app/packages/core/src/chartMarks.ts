/**
 * (T604, owner 2026-11-10, build 25: "overlapping text and not obvious what current day is") THE LABELS OF THE CHART'S
 * VERTICAL DAY MARKS, laid out once. A chart can carry up to three rules: TODAY (where the solid reading line ends),
 * the CONDITIONS DAY (the horizon chip picked) and the REGULATIONS DAY (the map's day picker, T582). Each is labelled,
 * and two rules a day apart sit closer together than their words are wide, so the old rule (every label centred or
 * right-aligned on its rule, same height) printed them over one another.
 *
 * Pure and shared: `buildHydrograph` calls this and the shape carries the result, so the web chart and the phone chart
 * (one component over one shape) draw the same boxes and nothing else decides where a label goes. A label goes at the
 * first place, in preference order, where its measured box (a) stays inside the plot, (b) meets no label already placed
 * and (c) preferably covers no mark's rule: the right of its rule, then the left, on row 0, then the same two on row 1, and on.
 * Marks are placed in importance order (today first) so the strongest label keeps the best place. A label with no
 * place at all is returned `placed: false` and the rule is still drawn; it is never drawn over another.
 */

export type DayMarkKind = "today" | "conditions" | "regs";

export interface DayMarkIn { readonly x: number; readonly text: string; readonly kind: DayMarkKind }

export interface LabelBox { readonly x0: number; readonly x1: number; readonly y0: number; readonly y1: number }

export interface DayMarkOut extends DayMarkIn {
  readonly placed: boolean;
  /** the text's anchor point: "start" runs right from `tx`, "end" runs left from it */
  readonly anchor: "start" | "end";
  readonly tx: number;
  /** the text's baseline */
  readonly ty: number;
  readonly row: number;
  readonly box: LabelBox;
}

export const MARK_ROWS = 3;
/** the gap between a rule and its label, and the least gap between two labels */
const RULE_GAP = 3;
const LABEL_GAP = 4;
const IMPORTANCE: Record<DayMarkKind, number> = { today: 0, conditions: 1, regs: 2 };

/** A text's drawn width, a little over (em fractions of a sans face; capitals wider; the bold today label wider again). */
export function markTextWidth(text: string, fontSize: number, bold = false): number {
  let em = 0;
  for (const ch of text) em += ch === " " ? 0.3 : ch >= "A" && ch <= "Z" ? 0.7 : /[0-9]/.test(ch) ? 0.6 : 0.58;
  return Math.ceil(em * fontSize * (bold ? 1.1 : 1.04)) + 1;
}

const meets = (a: LabelBox, b: LabelBox, gap: number) =>
  a.x0 < b.x1 + gap && b.x0 < a.x1 + gap && a.y0 < b.y1 && b.y0 < a.y1;

export interface MarkLayoutIn {
  readonly plot: { readonly x: number; readonly width: number };
  /** baseline of row 0, and the distance between rows */
  readonly top: number;
  readonly rowHeight: number;
  readonly fontSize: number;
  readonly marks: readonly DayMarkIn[];
}

export function layoutDayMarks(inp: MarkLayoutIn): DayMarkOut[] {
  const { plot, top, rowHeight, fontSize } = inp;
  const lo = plot.x, hi = plot.x + plot.width;
  const order = inp.marks.map((m, i) => ({ m, i })).sort((a, b) => IMPORTANCE[a.m.kind] - IMPORTANCE[b.m.kind] || a.m.x - b.m.x);
  const out = new Array<DayMarkOut>(inp.marks.length);
  const taken: LabelBox[] = [];
  for (const { m, i } of order) {
    const w = markTextWidth(m.text, fontSize, m.kind === "today");
    let chosen: DayMarkOut | null = null;
    // first a place that covers no mark's rule; only when there is none, one that crosses a rule (a thin dashed line
    // under a word reads; a dropped label does not)
    for (const clear of [true, false]) for (let row = 0; row < MARK_ROWS && !chosen; row++) {
      const ty = top + row * rowHeight;
      for (const anchor of ["start", "end"] as const) {
        const tx = anchor === "start" ? m.x + RULE_GAP : m.x - RULE_GAP;
        const x0 = anchor === "start" ? tx : tx - w, x1 = anchor === "start" ? tx + w : tx;
        const box = { x0, x1, y0: ty - fontSize, y1: ty + 2 };
        if (x0 < lo || x1 > hi) continue;
        if (taken.some((t) => meets(box, t, LABEL_GAP))) continue;
        if (clear && inp.marks.some((o) => o !== m && o.x > x0 - 2 && o.x < x1 + 2)) continue;
        chosen = { ...m, placed: true, anchor, tx, ty, row, box };
        break;
      }
    }
    if (!chosen) chosen = { ...m, placed: false, anchor: "start", tx: m.x + RULE_GAP, ty: top,
                            row: 0, box: { x0: m.x, x1: m.x, y0: top, y1: top } };
    else taken.push(chosen.box);
    out[i] = chosen;
  }
  return out;
}
