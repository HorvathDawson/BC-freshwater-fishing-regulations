/**
 * (T1167, owner 2026-11-25 "Banner only, but louder") WHAT THE KEEP TAB SAYS OF THE PAGE'S IN-SEASON NOTICES, composed
 * once (`inSeasonKeepOf`): the pointer while one is in force ("See the in-season notice above"), where the page draws it
 * (on the game-fish strip's status line when the page draws that strip, else at the top of the Keep tab, above whatever
 * answers there: the salmon block, a closed banner, the trout table), and the whole-year bars' marks of a dated range.
 * The answer and the bars' own colours stay the book's.
 */
import { INSEASON_WORDS, inSeasonDates, type InSeasonBannerVM } from "./inseason";
import { bookIndex } from "./regs/strip";
import { addDays } from "./inseasonWindow";
import type { KeepVM } from "./regs/model";

/**
 * (T1167, owner 2026-11-25 "Banner only, but louder") THE KEEP STRIP'S POINTER while a notice is IN FORCE on the page:
 * "See the in-season notice above" (the banners sit above the answer). The answer and the map's colour stay the book's;
 * an upcoming notice points nowhere (its banner says it is coming). None in force: null.
 */
function inSeasonStatusLine(banners: readonly InSeasonBannerVM[]): string | null {
  const n = banners.filter((b) => b.phase === "in force" && b.text.trim()).length;
  return n ? INSEASON_WORDS.statusSee(n) : null;
}

/** (T1167) what the whole-year bars mark: each dated notice's range as book-year runs (fractions of the 365 days, a range
 *  over the new year in two), and the legend's words */
export interface InSeasonYearVM { readonly runs: readonly { readonly at: number; readonly len: number }[]; readonly label: string }

const BOOK = 365;
const dayIx = (iso: string) => bookIndex({ month: Number(iso.slice(5, 7)), day: Number(iso.slice(8, 10)) });

/**
 * (T1167) "Whole-year calendars mark the window": the ranges of the page's DATED notices (upcoming or in force) on the
 * book year every bar draws (365 days, Feb 29 read as Feb 28), merged where they overlap; a range of a year or more is
 * the whole bar. No dated notice: null.
 */
function inSeasonYearMarks(banners: readonly InSeasonBannerVM[]): InSeasonYearVM | null {
  const dated = banners.filter((b) => b.window && b.text.trim());
  if (!dated.length) return null;
  const spans: [number, number][] = [];
  for (const b of dated) {
    const { from, to } = b.window!;
    const a = dayIx(from), z = dayIx(to);
    if (addDays(from, BOOK - 1) <= to) spans.push([0, BOOK]);
    else if (a <= z && to.slice(0, 4) === from.slice(0, 4)) spans.push([a, z + 1]);
    else spans.push([a, BOOK], [0, z + 1]);
  }
  spans.sort((x, y) => x[0] - y[0]);
  const merged: [number, number][] = [];
  for (const [a, z] of spans) {
    const l = merged[merged.length - 1];
    if (l && a <= l[1]) l[1] = Math.max(l[1], z); else merged.push([a, z]);
  }
  const one = dated.length === 1 ? inSeasonDates(dated[0]!.window!.from, dated[0]!.window!.to) : null;
  return { runs: merged.map(([a, z]) => ({ at: a / BOOK, len: (z - a) / BOOK })),
           label: one ? INSEASON_WORDS.yearMark(one) : INSEASON_WORDS.yearMarks };
}

/** the Keep tab's in-season words and marks, once per page */
export interface InSeasonKeepVM {
  /** "See the in-season notice above"; null: none in force */
  readonly pointer: string | null;
  /** (T1167, owner 2026-11-26) the caution mark the pointer wears beside it (`INSEASON_WORDS.warn`); null: no pointer */
  readonly pointerWarn: string | null;
  /** where the pointer is drawn: the game-fish strip's status line, or the top of the Keep tab (a page with no strip) */
  readonly pointerOn: "strip" | "tab";
  /** the dated ranges on the year bars; null: none */
  readonly marks: InSeasonYearVM | null;
}

/** (T1167) THE KEEP TAB'S IN-SEASON VM for a page (`keep`) and its notices on the page's day (`banners`). */
export function inSeasonKeepOf(keep: Pick<KeepVM, "strip" | "order">, banners: readonly InSeasonBannerVM[]): InSeasonKeepVM {
  const pointer = inSeasonStatusLine(banners);
  return { pointer, pointerWarn: pointer ? INSEASON_WORDS.warn : null, pointerOn: keep.strip && keep.order.includes("strip") ? "strip" : "tab",
           marks: inSeasonYearMarks(banners) };
}
