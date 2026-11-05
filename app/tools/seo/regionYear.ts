/**
 * T406 part 2 (the cheap version, owner 2026-11-05; a PROPOSAL): "when is it open across this region", folded from the
 * year strips the water pages already draw (`keep.strip`, the game-fish strip: bit 1 = closed). No new season logic:
 * each part's strip says, per book day, closed or open; a region's week is the share of its parts' days that are open,
 * streams and lakes apart (a wetland is counted with the lakes). Drawn as one small inline SVG on the region hub.
 */
import { MONTHS as CORE_MONTHS } from "@app/core";
import { esc } from "./html";
import type { WaterModel } from "./model";

const WEEKS = 52, YEAR = 365;
/** the book year (a non-leap year, as the strips): each month's first day index, its letter and short name — read from
 *  core's MONTHS and the date arithmetic, never a second calendar table (AGENTS dataflow gate) */
const DAY_MS = 86_400_000;
const MONTH_START = CORE_MONTHS.map((_, m) => Math.round((Date.UTC(2001, m, 1) - Date.UTC(2001, 0, 1)) / DAY_MS));
const dayWord = (day: number) => { const d = new Date(Date.UTC(2001, 0, 1 + day)); return `${CORE_MONTHS[d.getUTCMonth()]!.slice(0, 3)} ${d.getUTCDate()}`; };
const weekOf = (day: number) => Math.min(WEEKS - 1, Math.floor(day / 7));

/** One row of the bar: per week, open and closed part-days (days with no frame are neither). */
export interface YearRow { readonly label: string; readonly parts: number; readonly open: readonly number[]; readonly closed: readonly number[] }

/** The region's rows: streams, then lakes; a row with no part that has a strip is left out. */
export function regionYear(ws: readonly WaterModel[]): YearRow[] {
  const rows: YearRow[] = [];
  for (const [label, kinds] of [["Streams", ["stream"]], ["Lakes", ["lake", "wetland"]]] as const) {
    const open = new Array<number>(WEEKS).fill(0), closed = new Array<number>(WEEKS).fill(0);
    let parts = 0;
    for (const m of ws) {
      if (!(kinds as readonly string[]).includes(m.kind)) continue;
      for (const p of m.parts) {
        if (!p.year) continue;
        let day = 0;
        for (const r of p.year) {
          for (let k = 0; k < r.days && day < YEAR; k++, day++) {
            if (r.closed === true) closed[weekOf(day)]!++; else if (r.closed === false) open[weekOf(day)]!++;
          }
        }
        if (day !== YEAR) throw new Error(`regionYear: ${m.id} part ${p.partIx}'s strip covers ${day} days, not ${YEAR}`);
        parts++;
      }
    }
    if (parts) rows.push({ label, parts, open, closed });
  }
  return rows;
}

const pct = (o: number, c: number) => (o + c ? Math.round((100 * o) / (o + c)) : null);

/** A row in words: open in every week, or its lowest week. */
export function rowWords(r: YearRow): string {
  const shares = r.open.map((o, w) => pct(o, r.closed[w]!));
  const known = shares.filter((s): s is number => s !== null);
  if (!known.length) return `${r.label}: no season on record.`;
  const low = Math.min(...known), at = shares.indexOf(low), high = Math.max(...known);
  if (low === 100) return `${r.label} (${r.parts}): every one open all year.`;
  return `${r.label} (${r.parts}): ${high}% open at most; fewest open in the week of ${dayWord(at * 7)}, ${low}%.`;
}

const ROW_H = 28, GAP = 18, W = 364;

/** The card: a bar per row (each week's column: open share at the bottom, closed above), month letters, the words. */
export function yearCardHtml(name: string, rows: readonly YearRow[]): string {
  if (!rows.length) return "";
  const h = rows.length * (ROW_H + GAP);
  const bars = rows.map((r, i) => {
    const y0 = i * (ROW_H + GAP) + 12;
    const cols = r.open.map((o, w) => {
      const c = r.closed[w]!, share = o + c ? o / (o + c) : 0, oh = +(share * ROW_H).toFixed(2);
      const x = w * 7;
      return (o + c ? `<rect class="c" x="${x}" y="${y0}" width="7" height="${ROW_H}"/>` : "") +
        (oh > 0 ? `<rect class="o" x="${x}" y="${(y0 + ROW_H - oh).toFixed(2)}" width="7" height="${oh}"/>` : "");
    }).join("");
    return `<text x="0" y="${y0 - 3}">${esc(r.label)}</text>${cols}`;
  }).join("");
  const months = MONTH_START.map((s, m) => `<text x="${s + 1}" y="${h + 8}">${CORE_MONTHS[m]!.slice(0, 1)}</text>`).join("");
  const words = rows.map(rowWords);
  return `<section class="card yb" aria-labelledby="yb-h"><h2 id="yb-h">When waters are open in ${esc(name)}</h2>` +
    `<svg viewBox="0 0 ${W} ${h + 10}" role="img" aria-label="${esc(words.join(" "))}">${bars}${months}</svg>` +
    `<p class="small">Each column is a week: the share of ${rows.map((r) => r.label.toLowerCase()).join(" and ")} open to fishing (lower part) and closed (upper part), ` +
    `from each water's season strip. Fish by fish dates are on each water's page.</p>` +
    `<ul class="small">${words.map((w) => `<li>${esc(w)}</li>`).join("")}</ul></section>`;
}
