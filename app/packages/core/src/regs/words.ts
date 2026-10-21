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
  // species words (names.ts: page v36 join, lcNames, lc, writtenName)
  AND: " and ", OR: " or ",
  MORE: (n: number) => ` + ${n} more`,
  /** page v36 lc(): a name starting with one of these proper nouns keeps its capital. A match key, never said. */
  LC_KEEP: Object.freeze(["Dolly", "Arctic", "Nooksack", "Salish", "Cultus", "Enos", "Coastal", "Westslope", "Misty", "Vananda",
    "Paxton", "Hadley", "Morrison", "Rocky", "Speckled", "Charlotte"]) as readonly string[],
  TROUT: "trout", FISH: "fish",
  // time words (time.ts: page v36 timeCond)
  MIN_BEFORE: "min before ", MIN_AFTER: "min after ",
  // describe() (describe.ts: page v36 describe, bandTxt)
  FROM_STREAMS: " from streams", FROM_LAKES: " from lakes",
  NO_FISHING_FOR: "No fishing for ",
  ALL_GAME: "all game fish",      // the written name describe() replaces, only when it is the WHOLE name …
  ANY_GAME: "any game fish",      // … with this
  RELEASE_EVERY: "Release every ", NO_LIMIT_ON: "No limit on ", KEEP: "Keep ", A_DAY: " a day", SHARED: ", shared",
  NONE: ", none ", NONE_BARE: "none ",
  INSIDE: "Inside the day’s total: ", AT_MOST: "at most ", RELEASE_ANY: "release any ",
  PER: " a ", A_MONTH: "month", A_YEAR: "year", SMALLER: ". Smaller fish don’t count",
  UNDER: (b: number) => `under ${b} cm`,
  OVER: (a: number) => `over ${a} cm`,
  CM: " cm",
  // rule notes (rules.ts: page v36 RULES decode)
  UNCERTAIN: "Uncertain placement: ", NO_REASON: "no reason given", CURATOR: "Curator still to settle: ",
  // missing / refusal (T03 fills: MISSING "Missing from the answers file:", …)
  NO_WORDS: (key: string) => `(no words in the answers for ${key})`,
  MISSING_RULE: (ix: number) => `(rule ${ix}: missing from the page’s data)`,
  MISSING_DATA: (ix: number) => `Missing from the page’s data: rule ${ix}`,
  // T13 the year strip's status line (page v36 waterStrip0; prototype buildKeep "Closed from" → span rewrite)
  STRIP_OPEN: "Open", STRIP_OPEN_EXCEPT: "Open, except some parts", STRIP_CLOSED: "Closed",
  TODAY_LC: "today",
  ON_DAY: (md: string) => `on ${md}`,
  WEEKDAY_TAG: (wd3: string) => ` (${wd3})`,
  OPENS: (md: string) => `Opens ${md}`,
  CLOSED_FROM: (md: string) => `Closed from ${md}`,
  CLOSED_SPAN: (span: string) => `Closed ${span}`,
  TROUT_CLOSED: "closed", TROUT_RELEASE: "release only",
  TROUT_LINE: (state: string, tail: string) => `Trout ${state}${tail}`,
  ALL_YEAR: " all year",
  UNTIL: (md: string) => ` until ${md}`,
  // T14 the closed banner (page v36 closedBanner / mergedHtml / tidalHtml; prototype closechain rewrite)
  CLOSED_ALL_YEAR: "Closed all year",
  CLOSED_OPENS_AGAIN: (md: string) => `Closed · opens again ${md}`,
  NO_FISHING_RUN: (span: string) => `No fishing for any game fish ${span}.`,
  TOGETHER: " Together these closures cover it:",
  OPENS_AGAIN_LINK: (md: string) => `Opens again ${md}. See the rules from then ›`,
  SOURCE: "Source",
  FROM_STREAMS_CAP: "From streams, ",
  MERGED: (n: number, sections: number) => `Closed all year on ${n} stretches (${sections} sections)`,
  DOT: " · ", SECTION: "section", SECTIONS: "sections",
  TIDAL_WATER: "Tidal water",
  // T16 the header (prototype hdr / placeGo; page v36 subtitle)
  DATE_ARIA: (date: string) => `Change date, now ${date}`,
  MAP_ARIA: (full: string) => `View on map: where these rules apply${full ? `, ${full}` : ""}`,
  PARTS_ARIA: (full: string) => `Other parts${full ? ` — now ${full}` : ""}`,
  SUB_CLOSED_ALL_YEAR: "closed all year",
} as const);

/** page v36 RANK[rank].t: the level a rule speaks at (the part's rank). */
export const RANK_LEVEL: Readonly<Record<string, string>> = Object.freeze({
  "-1": "Federal / parks", "0": "This water", "1": "From downstream", "2": "Named area", "3": "Region", "4": "Province",
});

/** "Mon" … "Sun" (Monday first, as the calendar). */
export const WD3: readonly string[] = WEEKDAYS.map((w) => w.slice(0, 3));
/** "J", "F", … — the strip's month initials. */
export const MONTH1: readonly string[] = MONTHS.map((m) => m[0]!);

/** A function word's fixed fragments: what is left of its output once the data put into it is taken out. */
function fragments(f: (x: never) => string): string[] {
  const HOLE = "\u0000";
  return f(HOLE as never).split(HOLE).filter((s) => s.length > 0);
}

export const ALL_WORDS: ReadonlySet<string> = new Set([
  ...(Object.values(WORDS) as unknown[]).filter((v): v is string => typeof v === "string"),
  ...(Object.values(WORDS) as unknown[]).filter((v): v is (x: never) => string => typeof v === "function").flatMap(fragments),
  ...WD3, ...MONTH1,
  ...Object.values(RANK_LEVEL),
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
