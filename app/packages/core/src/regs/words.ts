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
  // T15 standing.ts: page v36 inheritedHtml / whereHtml / possessionHtml; prototype buildKeep (.inh, section.nofish, .posrow)
  INHERITED_LABEL: "By another water’s rule",
  WHERE_NOT: "Where you can’t fish",
  NO_FISHING_COLON: "No fishing: ",
  POSSESSION: "Possession",
  // T15b caveats.ts: page v36 inPartHtml / sideHtml / anglerClosures / timedHtml / uncertainHtml (+ momentTxt, answerTxt, daysTxt, friendlyWhy)
  INPART_HOT: "Fishing is different in some spots today",
  INPART_BOAT: "Boat rules in some spots",
  INPART_SOME: "Some spots have their own rules",
  INPART_IC: "ⓘ",
  CALM_HOT: "Everywhere else, follow this page.",
  CALM_BOAT: (noun: string) => "Fishing rules are the same everywhere on this " + noun + ".",
  WATER_LAKE: "lake", WATER_RIVER: "river",   // page v36: PLACE.kind === "lake" ? "lake" : "river"
  CALM_LATER: "Not in force today. The map can’t draw these places yet, so check where you are.",
  WHERE: (p: string) => "Where: " + p,
  WHERE_SIGNS: "Where: some spots, marked by buoys and signs. Look for signs.",
  MAP_IN_REGS: "(map $1 in the regulations)",
  OTHER_DATES: (n: number) => "Other dates (" + n + ")",
  SIDE_TITLE: "⚠ On one side of the channel only",
  HALF_ONLY: (side: string) => side + " half only",
  TIMED_TITLE: "⚠ At certain times",
  ANGLER_TITLE: "⚠ Closed to some anglers",
  UNCERTAIN_TITLE: "? Check: rules for parts of this water that couldn’t be placed on the map",
  UNPLACED_WHY: "the regulations don’t say exactly where",
  TODAY_PAREN: (m: string) => "today (" + m + ")",
  NO_RULE: "no rule", ANSWER_NOLIMIT: "no limit", ANSWER_RELEASE: "release", ANSWER_CLOSED: "closed",
  OTHER_HOURS: "other hours",
  MISSING_CLOSER: (when: string) => "Missing from the answers file: the closure that closes it " + when,
  // T19b plainrow.ts: page v36 rowHtml / valueHtml (the pill), summaryHtml (!pool), runTxt, closedRuns, weekHtml, namedIn
  PILL_RELEASE: "↩ Release", PILL_CLOSED: "⊘ Closed", PILL_NOLIMIT: "No limit",
  SUM_CLOSED: (until: string) => "Don’t fish for them" + until + ". If one bites, carefully release it.",
  SUM_RELEASE: (until: string) => "You can fish for them, but release every one" + until + ".",
  UNTIL: (d: string) => " until " + d,
  NEVER_KEEP: "No keeping at any time of year. ",
  ALL_YEAR_SENT: "All year.",
  WHOLE_CLOSED: (r: string) => "The whole water is closed " + r + ".",
  CLOSED_RANGE: (r: string) => "Closed " + r + ".",
  NEXT_YEAR: " next year",
  KEEP_UNTIL_THEN: (until: string, then: string, next: string) => "You can keep them until " + until + ". Then " + then + " from " + next + ".",
  NOW_UNTIL: (now: string, until: string) => now + " until " + until + ".",
  THEN_WHOLE: (r: string) => " Then the whole water is closed " + r + ".",
  THEN_RANGE: (then: string, r: string) => " Then " + then + " " + r + ".",
  KEEP_FROM: (next: string) => " You can keep them from " + next + ".",
  NO_KEEP_TAIL: " No keeping at any time of year.",
  NAMES_SEP: " · ",
  WHICH_FISH: (n: number) => "Which fish (" + n + ")",
  TODAY_MARK: "(today)",
  // T19b decided.ts: page v36 ladderHtml / rowSources / levelHead / byName / shortLabel ("How this was decided")
  DECIDED: "How this was decided",
  CHECK: "✓ ",
  RES_CLOSED: "Closed here.", RES_RELEASE: "Release every one here.", RES_NOLIMIT: "No daily limit here.",
  RES_POOL: (daily: string) => daily + " a day here",
  RES_COUNT_TOWARD: (take: string, scope: string) => ", and they count toward " + take + " a day in " + scope,
  RES_ALL_OF: (scope: string) => ", for all of " + scope + " together",
  RULES_NOT_APPLY: (n: number) => "Rules that don’t apply here (" + n + ")",
  DOESNT_APPLY: "Doesn’t apply",
  REPLACED_BY: (by: string) => "Replaced by " + by,
  RECORD_IT: "Record it", STOP_FISHING: "Stop fishing", TAG_CLOSED: "Closed", TAG_RELEASE: "Release",
  BY_WATER: (noun: string) => "this " + noun + "’s rule",
  BY_REGION: (name: string) => "the " + name + " rule",
  REGION: "region", BY_BC: "a B.C.-wide rule", BY_ABOVE: "a rule above",
  THE_REGION: "The region", THIS_WATER: "This water",
  MISSING_SUBSET: (key: string, fish: string) => "Missing from the answers file: the sentence for " + key + " said for " + fish + " (display.rules[].subsets)",
  // page v36 shortLabel's rewrites (the label shortened for the ladder; the exact words stay in the rule sheet)
  SHORT_A_DAY: " a day", SHORT_CM_PLUS: "$1 cm+", SHORT_MAX_OVER: ": max $1 over $2 cm", SHORT_HOLD: "hold up to 2 days’ limits", SHORT_COLON: ": ",
  // ── T17 group head (grouphead.ts: page v36 rowTitle, scopeOf, scopeBadge, rowHtml; prototype buildKeep rootLine) ──
  OF_THE: "of the", SHARE_ARROW: "↳ ",
  SCOPE_HERE: "This water", SCOPE_WIDE: (s: string) => `${s}-wide`, SCOPE_ALL_STREAMS: (s: string) => `All ${s} streams`,
  SCOPE_MISSING: "scope missing", PLURAL_S: "s",
  SEPARATE_FROM: "Separate from your ",
  HOW_TELL: "How to tell ›", BC: "B.C.", THE_REGION: "the region", THE_AREA: "the area",
  PROTECTED: "Protected species", TROUT_TITLE: "Trout", DOT_SEP: " · ",
  // prototype shortOf (form b's "↳ 2 of them from streams"; T28's pin lines)
  SHORT_ACROSS: " a day across ", SHORT_DAY: " a day", SHORT_OF_THEM_FROM: " of them from ", SHORT_DAY_FROM: " a day from ",
  SHORT_STREAMS: "streams", SHORT_DAY_UNDER: " a day under ", SHORT_UNDER: " under ", SHORT_PLUS: "plus up to ",
  SHORT_OF_THEM: " of them", SHORT_OF_THOSE: "of those, ", SHORT_OVER: "over ", COMMA: ", ", COLON_SP: ": ",
  OPEN_PAREN: " (", CLOSE_PAREN: ")",
} as const);

/** page v36 RANK[rank].t: the level a rule speaks at (the part's rank). */
export const RANK_LEVEL: Readonly<Record<string, string>> = Object.freeze({
  "-1": "Federal / parks", "0": "This water", "1": "From downstream", "2": "Named area", "3": "Region", "4": "Province",
});
/** "How this was decided" tables (page v36 ladderHtml SAYS / LOSES, levelHead, the four questions), verbatim. */
export const LADDER_Q: readonly (readonly [string, readonly string[]])[] = Object.freeze([
  Object.freeze(["What you can keep", Object.freeze(["governs", "contains", "narrows", "also"])] as const),
  Object.freeze(["Size", Object.freeze(["floor"])] as const),
  Object.freeze(["Other limits", Object.freeze(["limit"])] as const),
  Object.freeze(["Other rules", Object.freeze(["season", "possession_cap", "duty", "possession"])] as const),
]);
export const SAYS: Readonly<Record<string, string>> = Object.freeze({ governs: "Daily limit", contains: "Daily limit", narrows: "Lower daily limit",
  also: "Also applies", floor: "Size", limit: "Extra cap", season: "Yearly limit", possession_cap: "Possession limit", duty: "Must do",
  possession: "Carry limit" });
export const LOSES: Readonly<Record<string, string>> = Object.freeze({ replaced: "Replaced", agrees: "Same as another rule",
  falls: "Not needed here", moot: "Doesn’t matter today", lifted: "Replaced" });
/** page v36 levelHead for ranks 4, 3, 2, 1, -1 (rank 0 is the water's name). */
export const LEVEL_HEAD: Readonly<Record<string, string>> = Object.freeze({ "4": "All of B.C.", "3": "The region", "2": "A named area",
  "1": "Downstream waters", "-1": "Federal law" });
/** page v36 runTxt's NOW / THEN tables. */
export const RUN_NOW: Readonly<Record<string, string>> = Object.freeze({ release: "Release only", closed: "Closed", keep: "You can keep them", none: "No rule" });
export const RUN_THEN: Readonly<Record<string, string>> = Object.freeze({ release: "release only", closed: "closed", keep: "you can keep them", none: "no rule" });

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
  // (T19b) the ladder / run tables
  ...LADDER_Q.map(([q]) => q), ...[SAYS, LOSES, LEVEL_HEAD, RUN_NOW, RUN_THEN].flatMap((t) => Object.values(t)),
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
