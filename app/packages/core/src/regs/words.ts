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
  NEVER_KEEP: "No keeping at any time of year. ",
  THROUGH_YEAR: "Through the year",   // page v36 stripHtml's label; the group's year window (prototype UR5 .gyear)
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
  HOW_TELL: "How to tell ›", BC: "B.C.", THE_REGION_LC: "the region", THE_AREA: "the area",
  PROTECTED: "Protected species", TROUT_TITLE: "Trout", DOT_SEP: " · ",
  // prototype shortOf (form b's "↳ 2 of them from streams"; T28's pin lines)
  SHORT_ACROSS: " a day across ", SHORT_DAY: " a day", SHORT_OF_THEM_FROM: " of them from ", SHORT_DAY_FROM: " a day from ",
  SHORT_STREAMS: "streams", SHORT_DAY_UNDER: " a day under ", SHORT_UNDER: " under ", SHORT_PLUS: "plus up to ",
  SHORT_OF_THEM: " of them", SHORT_OF_THOSE: "of those, ", SHORT_OVER: "over ", COMMA: ", ", COLON_SP: ": ",
  OPEN_PAREN: " (", CLOSE_PAREN: ")",
  // ── T28 pin bar (pins.ts: prototype pinUpdate's form a group line, the total in brackets under the share) ──
  PIN_OF_TOTAL: (total: string) => `(of ${total})`,
  // ── T18 tree (tree.ts: prototype buildKeep kt, dutyWords, fishBtn) ──
  STEELHEAD_STAMP: "Steelhead stamp ›",
  // prototype stampLine (round UR7): "You need <document ›> <when>" — the document and its when are the store's (rows 31 stamps)
  YOU_NEED: "You need ", DOC_LINK: (name: string) => `${name} ›`,
  CAPTION: (c: string) => `${c}\u00a0›`,   // U+00A0 keeps the › on the caption's line (T20)
  THE_NAME: (name: string) => `the ${name}`,
  // ── T19 tail + other fish (tail.ts, others.ts: prototype nk, worked, Other fish; page v36 noRulesLine, rowsHtml) ──
  HOW_IT_ADDS_UP: "How it adds up", SEE_EVERY_LINE: "See every regulation line ›", OTHER_FISH: "Other fish",
  NO_LIMIT: "no limit", CLOSED_LC: "closed", RELEASE_LC: "release", SIZE_HINT: "· size limit", COLON: ":",
  STEEL_POSSIBLE: "Steelhead rules apply here; steelhead may not be present in this water.",
  STEEL_KNOWN: "Steelhead are known to be in this water.",
  STEEL_NO_RULES: "Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout.",
  CLOSED_MARK: "✕",
  // page v36 levelTag (level.ts): the level a rule speaks at, by the part's rank (Region N from the rule's own entry id)
  LEVEL_TAG_BC: "B.C. rule", LEVEL_TAG_REGION: (n: string) => `Region ${n} rule`, LEVEL_TAG_REGION_ANY: "Region rule",
  LEVEL_TAG_AREA: "Named area rule", LEVEL_TAG_DOWN: "Downstream rule", LEVEL_TAG_FED: "Federal or park rule",
  LEVEL_TAG_WATER: "This water’s rule",
  // ── T36 sheets (sheets.ts: prototype sheetHtml / glossBody / head; page v36 openRule, adiposeHtml, openSheet titles) ──
  SHEET_RULE: "Rule", SHEET_EXACT: "The exact words", SHEET_TECH: "Technical detail", SHEET_NOTE: "⚑ ",
  SHEET_BOOK_WORDS: "In the book’s words", SHEET_RULE_BOOK_WORDS: "In the rule book’s words", SHEET_FOR_EXAMPLE: "For example:",
  SHEET_SOURCE_LINK: "Source ›",
  SHEET_HATCH_TITLE: "Hatchery or wild?", SHEET_HATCH_SUB: "The adipose fin",
  SHEET_HATCH_WILD: "Wild", SHEET_HATCH_WILD_CAP: " · adipose fin there",
  SHEET_HATCH_CLIPPED: "Hatchery", SHEET_HATCH_CLIPPED_CAP: " · clipped, a healed scar instead",
  SHEET_HATCH_LOOK: "Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.",
  // ── T21 the Keep composition (keep.ts: page v36 renderCard's outside-B.C. foot) ──
  OUTSIDE_BC: "This part of the water is outside B.C.: no B.C. rules apply.",
  /** a fish picture's accessible name (prototype fishBtn aria-label) */
  LOOKS_LIKE: (name: string) => `What a ${name} looks like`,
  /** the water screen while a tab's presenter is not built yet (T30–T35), and when the store cannot answer */
  TAB_PENDING: (tab: string) => `${tab}: not in the app yet. Check the B.C. Freshwater Fishing Regulations Synopsis before you fish.`,
  REGS_LOADING: "Loading the regulations",
  REGS_UNAVAILABLE: (why: string) => `Regulations unavailable: ${why}`,
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

// ── T30 the Gear tab (prototype dayLine, closedUntil, levelOf, learnChipWords, itemSrc, gnote, buildGear GLAB; the level
//    words are WORDS.LEVEL_TAG_*, level.ts). Verbatim. The chip words are RANK_LEVEL (page v36 RANK[rank].t). ──
/** prototype buildGear GLAB: the gear lines in draw order — key, label, the summary part kinds each line draws. */
export const GEAR_LINES: readonly (readonly ["lines" | "hook" | "weight", string, readonly ("lines" | "attach" | "hook" | "weight")[]])[] = Object.freeze([
  Object.freeze(["lines", "Lines", Object.freeze(["lines"] as const)] as const),
  Object.freeze(["hook", "Hooks", Object.freeze(["attach", "hook"] as const)] as const),
  Object.freeze(["weight", "Weight on the line", Object.freeze(["weight"] as const)] as const),
]);
export const GEAR = Object.freeze({
  CHIP_NOW: "Now", CHIP_NEXT: "Next", IN_FORCE: "· in force today", ALL_YEAR: "All year",
  SOURCES: "Sources", SOURCES_ARIA: "Sources: ", SOURCES_RULES: "rules", SRC_ARROW: "›",
  CLOSED_UNTIL: (mon3: string, d: number) => `No fishing here until ${mon3} ${d}.`,
  CLOSED_ALL_YEAR: "No fishing here on any day of the year.",
  /** prototype levelOf's split of the rule sheet's sub (level.ts ruleSub) */
  RULE_SUB: " rule · ", SUB_SEP: " · ",
  /** prototype learnChipWords: the chip word that is never learned (a match key) */
  CHIP_NOT_LEARNED: "Region",
});

// ── T31 the Bait item (page v36 ELEM, gearParts bait WHY; prototype yesNo, buildGear bait cell). Verbatim. ──
/** page v36 ELEM: the gear frame's element and method codes as the page names them. */
export const ELEM: Readonly<Record<string, string>> = Object.freeze({ roe: "Roe (fish eggs)", invertebrate: "Water insects and crayfish", worms: "Worms",
  fin_fish: "Fish or fish parts", dead_fin_fish: "dead fish", live_fin_fish: "live fish", artificial_fly: "flies", artificial_lure: "lures",
  angling: "Angling (rod and line)", fly_fishing: "Fly fishing", ice_fishing: "Ice fishing", set_lining: "Set lines",
  spear_fishing: "Spear or bow", crayfish_trapping: "Crayfish traps", netting: "Nets", snagging: "Snagging",
  chumming: "Chumming", downrigger: "Downrigger", light: "Light to attract fish" });
export const BAIT = Object.freeze({
  LABEL: "Bait",
  /** + the bait_ban ⓘ as its own run (page v36 gearParts: the note, a space, the ⓘ) */
  BAN_NOTE: "No natural bait of any kind while the ban runs. ",
  /** page v36 gearParts WHY */
  WHY: Object.freeze({ bait_ban: "bait ban", on_streams: "on streams", on_lakes: "on lakes", not_on_streams: "not on streams",
    not_on_lakes: "not on lakes", banned_roe_ok: "banned · roe is still OK", allowed: "allowed" }) as Readonly<Record<string, string>>,
  ALLOWED: "allowed",
  EXCEPTED: (elem: string) => `allowed · the ${elem} ban excepts it`,
  CARRY: (kg: number) => `carry up to ${kg} kg`,
  STILL_OK: (w: string) => `${w} is still OK`,
  SEP: " · ",
});
/** prototype yesNo: the two column heads and marks. */
export const YESNO = Object.freeze({ OK: "Allowed", NO: "Not allowed", OK_MARK: "✓", NO_MARK: "✕" } as const);

// ── T32 the Methods tab (page v36 SLOT_LABEL, MUST, GROUPWORD, MOMENT_HEAD, WAYWHY, countTxt, tgtTxt, gearParts ways /
//    always cards, renderKit's always tile; prototype buildGear ways / boats / reference, WAYS). Verbatim. ──
export const METHODS = Object.freeze({
  WAYS: "Ways to fish", BOATS: "Boats", REFERENCE: "Reference", HANDLING: "Handling fish",
  RULES_EVERYWHERE: (n: number) => `${n} rules that apply everywhere`,
  NOT_GAME: " — not for trout or other game fish",
  NOT_FOR: (t: string) => `not for ${t}`, FOR: (t: string) => `${t} allowed`,
  MUST_BE: (xs: string) => `must be ${xs}`, WITH_DEVICE: (d: string, xs: string) => `with a ${d}: ${xs}`,
  SLOT_COUNT: (slot: string, n: string) => `${slot}: ${n}`,
  CRAYFISH_ONLY: "release anything in the trap that isn’t a crayfish",
  CAUGHT_HEAD: "When you catch a fish",
  OR: " or ", LIST: ", ", SEP: " · ",
  WAYWHY: Object.freeze({ fly_fishing_only: "fly fishing only: nothing but the fly on the line, no float, sinker or attractor",
    not_allowed_here: "not allowed here", not_lawful: "not a lawful way to sport fish", fly_fishing_only_here: "fly fishing only here" }) as Readonly<Record<string, string>>,
  SLOT_LABEL: Object.freeze({ lines_per_angler: "Lines", terminal_attachments_per_line: "Hook, lure or fly per line", hooks_per_line: "Hooks per line",
    flies_per_line: "Flies per line", points_per_hook: "Hook points", hook_gap_mm: "Hook gap", weight_per_line_kg: "Weight on the line",
    bait_possession_kg: "Bait you can carry", light_to_hook_mm: "Light to hook" }) as Readonly<Record<string, string>>,
  MUST: Object.freeze({ quick_release_to_line: "attached to your line by a quick-release", submerged: "submerged", attached_to_line: "attached to the line" }) as Readonly<Record<string, string>>,
  GROUPWORD: Object.freeze({ ALL_GAME_FISH: "game fish", SALMON: "salmon", PROTECTED_SPECIES: "protected species", ALL_FIN_FISH: "any fish" }) as Readonly<Record<string, string>>,
  MOMENT_HEAD: Object.freeze({ release: "When you release a fish", keep: "When you keep a fish", carry: "Carrying fish", never: "Never", also: "Also" }) as Readonly<Record<string, string>>,
  COUNT: Object.freeze({ NO_LIMIT: "no limit", AT_LEAST: "at least ", UP_TO: "up to ", SINGLE_POINT: "Single point only (no trebles)",
    MISSING: "(clause missing)",
    POINTS: (n: string) => `${n} points`, CM: (n: string) => `${n} cm`, KG: (n: string) => `${n} kg`,
    WITHIN_M: (m: number) => `within ${m} m of the hook` }),
});
/** Match keys of the Methods tab (never said): page v36's "not for game fish" limited way, and its crayfish trap rule. */
export const METHODS_TEST = Object.freeze({ NOT_GAME: /not for game fish/, CRAYFISH: /other than crayfish/i });

/** Every string and function-word fragment of a (nested, frozen) word table (a function of n parameters gets n holes). */
function deepWords(t: object): string[] {
  const HOLE = "\u0000";
  return Object.values(t).flatMap((v: unknown): string[] => typeof v === "string" ? (v.length ? [v] : [])
    : typeof v === "function" ? String((v as (...x: string[]) => string)(...Array<string>(v.length).fill(HOLE))).split(HOLE).filter((s) => s.length > 0)
    : v && typeof v === "object" && !(v instanceof RegExp) ? deepWords(v) : []);
}

/** "Mon" … "Sun" (Monday first, as the calendar). */
export const WD3: readonly string[] = WEEKDAYS.map((w) => w.slice(0, 3));
/** "J", "F", … — the strip's month initials. */
export const MONTH1: readonly string[] = MONTHS.map((m) => m[0]!);

/**
 * (T33) The Licence tab's words (licence.ts), verbatim from page v36 licParts / AXES / whoTxt / actTxt / priceTxt / srcOf /
 * pathHtml / reqCard / otherCard (@ 7fb6809f) and the prototype's licence() / render() (regs v38). Data strings (licence
 * names, requirement labels, period / waiver sentences, prices) are never here.
 */
export const LIC = Object.freeze({
  QUESTION: "Who is fishing?", Q_TERM: "under_16",
  AXES: Object.freeze({
    residency: Object.freeze([["resident", "B.C. resident"], ["non_resident", "Canadian, other province"], ["non_resident_alien", "From outside Canada"]] as const),
    age: Object.freeze([["16_plus", "16 or older"], ["under_16", "Under 16"]] as const),
    guidance: Object.freeze([["non_guided", "Not guided"], ["guided", "Guided"]] as const),
    status: Object.freeze([["none", "No status"], ["indian_bc_resident", "Status First Nations, living in B.C."], ["metis", "Métis"],
      ["disabled", "Disabled"], ["aged_65_plus", "65 or older"]] as const),
  }),
  /** page v36 whoTxt: the status word mid-sentence, and the empty `who` */
  WHO_STATUS_FN: "a status First Nations person living in B.C.", EVERY_ANGLER: "every angler",
  // actTxt
  ACT_FISHING: "To fish here", ACT_TARGETING: (sp: string) => `To fish for ${sp}`, ACT_RETAINING: (sp: string) => `To keep ${sp}`,
  ACT_RECORDED: "When you keep a fish you must record", ACT_GUIDING: "To guide anglers",
  LEN_RANGE: (a: number, b: number) => `${a}–${b} cm`, LEN_OVER: (a: number) => `over ${a} cm`, LEN_UNDER: (b: number) => `under ${b} cm`,
  // priceTxt
  PER_YEAR: (p: string) => `$${p} a year`, PER_DAY: (p: string) => `$${p} a day`, PER_8_DAYS: (p: string) => `$${p} for 8 days`, DOT: " · ",
  // licParts whenTxt / alsoTxt
  NEEDED_AT_ALL: "Needed to fish at all", WHATEVER: ", whatever you fish for",
  WHILE_CLASSIFIED: (period: string) => `Needed while it’s classified: ${period}`,
  ONLY_IF: (act: string) => `Only if you ${act}`, EVEN_RELEASE: ", even to release them",
  OTHER_TIME: (act: string) => `at any other time only if you ${act}`, EITHER: ". Either one does", ALSO_SEP: "; ",
  // designation classbox
  CLASS_HEAD: (c: string) => `Class ${c} Classified Water`, CLASS_TERM: "classified_waters",
  UNIT: (u: string) => `Licence unit: ${u}`,
  STAMP_DURING: (dates: string) => `. Steelhead Stamp needed ${dates}, whatever you fish for`, IN_FORCE: " (in force today)",
  STAMP_OTHER_TIME: "; at any other time only to fish for steelhead",
  STAMP_WAIVED: ". Steelhead Stamp not needed here unless you fish for steelhead", STOP: ".", SEP: ", ",
  SOURCE: "Source", FLAG: "?",
  CONTESTED: "Check: part of this water is also marked “not a Classified Water”.",
  EXEMPT_FROM: (docs: string) => `You’re exempt from: ${docs}.`,
  // none needed
  NONE_HEAD: "No licence needed to fish here", NONE_U16: "Anglers under 16 who live in B.C. don’t need a basic angling licence.",
  NONE_EXEMPT: "You’re exempt from the basic angling licence.", NONE_ANY: "No licence rule applies to this angler.",
  NONE_TAIL: "The fishing rules and limits still apply.",
  // under 16
  YOUTH_HEAD: "Under 16: no licence needed if you fish with a licensed adult", YOUTH_TERM: "under_16",
  YOUTH_WITH: (who: string) => `Fish with someone ${who} who holds what this fishing needs`, YOUTH_DOCS: (docs: string) => ` (${docs})`,
  YOUTH_COUNTS_S: " What you keep counts toward their limit.",
  YOUTH_BUY: (docs: string) => ` Or buy ${docs} yourself to have your own limit.`, YOUTH_THE: (docs: string) => `the ${docs}`,
  YOUTH_ADULT_NEEDS: "what a 16+ angler needs", PLUS: " + ",
  YOUTH_NOTHING: "Nothing, if you fish with a licensed adult (16 or older)",
  YOUTH_COUNTS: "What you keep counts toward their limit", YOUTH_HOLD: "They must hold what this fishing needs",
  YOUTH_OWN: "Or, for your own limit: what a 16+ angler buys",
  // the needs list (prototype render: the "Also, only if…" split; the kokanee words)
  YOU_NEED: "You need", ONLY_IF_HEAD: "Also, only if…",
  PRICE_NOTE: "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence",
  NOT_KOKANEE: "(not kokanee)",
  // the paper licence
  PAPER_LEAD: (a: string, fish: string) => `Keep ${a} ${fish}? `, A: "a", AN: "an", PAPER_STRONG: "Carry your paper licence",
  PAPER_TAIL: " (16 and over) and record it right away.",
  ADULT: "adult ",
  // not yet mapped
  NYM_HEAD: "In one part of this water", NYM_SAYS: (part: string, need: string, doing: string) => `${part}: you need ${need} ${doing}.`,
  NYM_DOING: "to fish", NYM_NOTE: "That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water.",
  NOTHING: "Nothing is required of this angler here.",
  // folds and cards (otherCard / reqCard / pathHtml)
  GUIDING: "If you are guiding other anglers", FOR: (who: string) => `For ${who}:`, NOT_WHO: (who: string) => ` (not ${who})`, OR: "or",
  SOURCES_HEAD: "Where each one comes from",
  PATH_EXEMPT: "Nothing to buy: you’re exempt",
  PATH_WITH: (who: string) => `be with someone who is ${who} and holds what this fishing needs`,
  PATH_COUNTS: " (fish you keep count toward their limit)", PATH_AS: (who: string) => `meet the rules as ${who}`,
  PATH_ALT: " (also accepted here)",
  DISPLACED: "Not valid here: a federal or park authority’s requirement replaces provincial licences.",
  FREED: "Not for you: you don’t need the licence this is about", NOT_IF: (who: string) => `Not needed if you are ${who}.`,
  // the source line (srcOf; the prototype's licence source button)
  SRC: Object.freeze({ province: "Province", on_designation: "Classified water", superior: "Federal / parks", water: "This water" }),
  SRC_MORE: (n: number) => ` +${n}`, SRC_SEP: " · ", SRC_GO: " ›", SOURCES: "Sources", SRC_ARIA: (who: string) => `Sources: ${who}`,
  // tidal (page v36 licParts on a tidal key)
  TIDAL: "Tidal water",
  MISSING_LIC: (ix: number) => `(licensing record ${ix}: missing from the page’s data)`,
});
/** LIC's sayable words: every string, every function's fixed fragments (each argument a hole), the axes' labels (their
 *  values and the glossary ids are keys, never said). */
const licWords = (): string[] => {
  const HOLE = "\u0000", out: string[] = [];
  const walk = (v: unknown): void => {
    if (typeof v === "string") out.push(v);
    else if (typeof v === "function") out.push(...String((v as (...a: string[]) => string)(...Array<string>(v.length).fill(HOLE))).split(HOLE).filter(Boolean));
    else if (v && typeof v === "object") Object.values(v).forEach(walk);
  };
  const { AXES, Q_TERM: _q, CLASS_TERM: _c, YOUTH_TERM: _y, ...rest } = LIC;
  walk(rest);
  Object.values(AXES).forEach((xs) => xs.forEach(([, t]) => out.push(t)));
  return out;
};

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
  // (T30) the Gear tab
  ...GEAR_LINES.map(([, label]) => label), ...deepWords(GEAR),
  // (T31) the Bait item
  ...[ELEM, BAIT, YESNO].flatMap(deepWords),
  // (T32) the Methods tab
  ...deepWords(METHODS),
  // (T33) the licence words
  ...licWords(),
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
