/**
 * Every English string a regs presenter may emit that is NOT data. Grouped by the page_v36 / prototype function that
 * says it. A function word takes its data as parameters and returns a string; ALL_WORDS holds each constant and each
 * function's FIXED fragments, so the no-hand-facts test can subtract data strings and assert the rest ⊆ ALL_WORDS.
 *
 * Day and month words are not duplicated here: MONTHS and WEEKDAYS are the generated calendar's; WD3 and MONTH1 are
 * derived from them.
 */
import { MONTHS, WEEKDAYS } from "../calendar.generated";
import type { Line } from "./line";

export const WORDS = Object.freeze({
  // tabs (golden header.tabs)
  TAB_KEEP: "What you can keep", TAB_GEAR: "Gear", TAB_METHODS: "Methods", TAB_LICENCE: "Licence",
  // header
  TODAY: "Today", MAP: "Map", PARTS: "Parts",
  // rich text (line.ts plain()): `${BOOK} ${PAGE}<printed> ${LINK_OUT}`
  BOOK: "Rule book",
  PAGE: "p.",
  LINK_OUT: "↗",
  TERMS_MANY: "What these words mean",   // plain() of a term run with 2+ ids
  TERM_ONE: "ⓘ",
  // describe() (T06 fills: FROM_STREAMS " from streams", FROM_LAKES " from lakes", NO_FISHING_FOR "No fishing for ", …)
  // missing / refusal (T03/T06 fill: MISSING "Missing from the answers file:", NO_WORDS "(no words in the answers for ", …)
} as const);

/** "Mon" … "Sun" (Monday first, as the calendar). */
export const WD3: readonly string[] = WEEKDAYS.map((w) => w.slice(0, 3));
/** "J", "F", … — the strip's month initials. */
export const MONTH1: readonly string[] = MONTHS.map((m) => m[0]!);

export const ALL_WORDS: ReadonlySet<string> = new Set([
  ...(Object.values(WORDS) as unknown[]).filter((v): v is string => typeof v === "string"),   // function words add their fragments
  ...WD3, ...MONTH1,
]);

/** The words of a Line that are not data: text and link words, and the fixed words of term and book runs. Numbers are
 *  data and are left out; a book run's page number is too. (The no-hand-facts helper.) */
export function wordsIn(l: Line): string[] {
  const out: string[] = [];
  for (const r of l) {
    if (r.t === "text" || r.t === "link") out.push(r.s);
    else if (r.t === "term") out.push(r.ids.length >= 2 ? WORDS.TERMS_MANY : WORDS.TERM_ONE);
    else if (r.t === "book" && !r.bare) out.push(WORDS.BOOK);
  }
  return out;
}
