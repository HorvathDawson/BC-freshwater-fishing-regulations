/**
 * The type scale, in one place.
 *
 * Three faces, and they carry different jobs, so the
 * app must not reach for a raw `fontFamily` anywhere else:
 *
 *   display  Bricolage Grotesque — names and headings. Tight tracking, heavy weights.
 *   text     Archivo             — everything you read in sentences.
 *   figure   JetBrains Mono      — numbers that line up in a column, and station ids.
 *
 * The families are named, not loaded, here: `apps/mobile` loads them (expo-font) so this
 * file stays free of platform imports. If a face fails to load, React Native falls back to
 * the system face and the layout still holds, because every style below sets a size and a
 * weight rather than relying on the face's own metrics.
 */
export const FACE = {
  display: "BricolageGrotesque_700Bold",
  displayHeavy: "BricolageGrotesque_800ExtraBold",
  text: "Archivo_400Regular",
  textMedium: "Archivo_500Medium",
  textSemi: "Archivo_600SemiBold",
  textBold: "Archivo_700Bold",
  /** Archivo 800 (T22, tokens.md §2.2): the regs page's date-day word, size chip, pin numbers, ✓/✕ marks. */
  textHeavy: "Archivo_800ExtraBold",
  figure: "JetBrainsMono_500Medium",
} as const;

/** What `useFonts` must be given. Kept beside the names so the two cannot drift. */
export const FACES_REQUIRED = Object.values(FACE);

export interface TextStyle {
  fontFamily: string; fontSize: number; lineHeight?: number;
  letterSpacing?: number; textTransform?: "uppercase";
}

export const TYPE = {
  /** A water's name at the top of its sheet. */
  title:    { fontFamily: FACE.displayHeavy, fontSize: 30, lineHeight: 34, letterSpacing: -0.7 },
  /** A water's name in a list. */
  name:     { fontFamily: FACE.display, fontSize: 21, lineHeight: 25, letterSpacing: -0.35 },
  /** A screen header. */
  screen:   { fontFamily: FACE.displayHeavy, fontSize: 23, lineHeight: 27, letterSpacing: -0.4 },
  /** Section headings inside a sheet. */
  section:  { fontFamily: FACE.textBold, fontSize: 11, lineHeight: 14, letterSpacing: 1.4,
              textTransform: "uppercase" },
  body:     { fontFamily: FACE.text, fontSize: 14.5, lineHeight: 21 },
  bodyStrong: { fontFamily: FACE.textSemi, fontSize: 14.5, lineHeight: 21 },
  small:    { fontFamily: FACE.text, fontSize: 12.5, lineHeight: 18 },
  micro:    { fontFamily: FACE.textMedium, fontSize: 11, lineHeight: 14, letterSpacing: 0.3 },
  /** The word in a status pill. Never smaller than this — it is the answer. */
  pill:     { fontFamily: FACE.textBold, fontSize: 12, lineHeight: 14, letterSpacing: 0.7,
              textTransform: "uppercase" },
  /** Readings, percentiles, coordinates. */
  figure:   { fontFamily: FACE.figure, fontSize: 13, lineHeight: 17 },
  figureBig:{ fontFamily: FACE.figure, fontSize: 19, lineHeight: 23 },
  /**
   * The tab label. UPPERCASE AND TRACKED, at the app's heaviest text weight.
   *
   * It was sentence case at 11/0.2 — the same treatment as `micro`, which is a caption.
   * A tab bar is the app's one permanent piece of furniture and it was the quietest thing
   * on the screen. Uppercase with real tracking is what the rest of this design does to say
   * "this is a label, not a sentence" (see `section` and `pill`), and it is what makes the
   * bar read as printed rather than typed.
   */
  tab:      { fontFamily: FACE.textBold, fontSize: 9.5, lineHeight: 12, letterSpacing: 1.1,
              textTransform: "uppercase" },

  // --- the Regulations page (T22; tokens.md §2.3, copied exactly). Sizes are the prototype's rem × 16; no entry carries a
  // weight (the weight IS the face). ---
  /** THE answer: the strip's "Open"/"Closed" (.wsstat, --t1 800 /1, tracking -0.5). */
  answer:      { fontFamily: FACE.displayHeavy, fontSize: 26, lineHeight: 26, letterSpacing: -0.5 },
  /** A big numeral INSIDE a sentence (.tn/.kn/.ghn at --t1): NO lineHeight — the outer Text's lineHeight is the line box (§2.4). */
  numeral:     { fontFamily: FACE.displayHeavy, fontSize: 26 },
  /** A head numeral inside the group's total line (.ghn/.gln/.ghn2 at --t2, line-height 1). */
  numeralMid:  { fontFamily: FACE.displayHeavy, fontSize: 17 },
  /** The lead: a group's name (.rtitle --t2/1.2), a section head (.sech h2, tracking -0.2 via `sectionHead`). */
  lead:        { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 20.4 },
  sectionHead: { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 20.4, letterSpacing: -0.2 },
  /** The water's name in the regs header (.ttl b --t2/1.15, two lines max). */
  headerName:  { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 19.55 },
  /** A tier header (.tier .tl, display face --t2/1.25) and the size/count head (--t3/1.25). */
  tier:        { fontFamily: FACE.displayHeavy, fontSize: 17, lineHeight: 21.25 },
  tierSmall:   { fontFamily: FACE.displayHeavy, fontSize: 15, lineHeight: 18.75 },
  /** Body, --t3/1.42. regsBody* are 15 px; the app's TYPE.body (14.5/21) is NOT the prototype's body. */
  regsBody:     { fontFamily: FACE.text,      fontSize: 15, lineHeight: 21.3 },
  regsBodySemi: { fontFamily: FACE.textSemi,  fontSize: 15, lineHeight: 20.25 },   // .kt .ks (1.35)
  regsBodyBold: { fontFamily: FACE.textBold,  fontSize: 15, lineHeight: 21.3 },    // values (.orow b, .posrow b), .golink
  regsChip:     { fontFamily: FACE.textHeavy, fontSize: 15, lineHeight: 19.5 },    // .szc (1.3); [data-size-rule] uses textBold
  duty:         { fontFamily: FACE.textBold,  fontSize: 15, lineHeight: 20.25 },   // .k28 .duty (--t3/1.35, 700)
  /** (T34) a Gear line's value (.gline .gval: 700 --t2/1.35) and a changes line's chip (.chg .chgi: 700 .69rem/1.6, caps, .4). */
  value:        { fontFamily: FACE.textBold,  fontSize: 17, lineHeight: 22.95 },
  chip:         { fontFamily: FACE.textBold,  fontSize: 11.04, lineHeight: 17.66, letterSpacing: 0.4, textTransform: "uppercase" },
  /** Meta, --t4. `meta` = /1.42 (notes, legend, captions); the /1.3 forms for labels and short lines. */
  meta:        { fontFamily: FACE.text,      fontSize: 12.48, lineHeight: 17.72 },
  metaSemi:    { fontFamily: FACE.textSemi,  fontSize: 12.48, lineHeight: 16.22 },
  metaBold:    { fontFamily: FACE.textBold,  fontSize: 12.48, lineHeight: 16.22 },
  metaHeavy:   { fontFamily: FACE.textHeavy, fontSize: 12.48, lineHeight: 14.98 },  // the date pill's "Today"/"Fri" (1.2)
  /** ALL-CAPS kickers only (.lab, .scope, .bkh, .tkick, .nofish h3): --t4 700, tracking 1. */
  kicker:      { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 16.22, letterSpacing: 1, textTransform: "uppercase" },
  /** The four header tabs (.segm[role=tablist] button: 700 --t4, tracking .2, sentence case; line-height normal → 15). */
  regsTab:     { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 15, letterSpacing: 0.2 },
  /** The pin bar: the group line (--t3 700 /1.3), a tier line (--t4 700 /1.3), the form-a sub-line (--t4 600 /1.3). */
  pinGroup:    { fontFamily: FACE.textBold, fontSize: 15, lineHeight: 19.5 },
  pinLine:     { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 16.22 },
  pinSub:      { fontFamily: FACE.textSemi, fontSize: 12.48, lineHeight: 16.22 },
  /** The strip's month initials (.smonths: JetBrains Mono .625rem/1). */
  month:       { fontFamily: FACE.figure, fontSize: 10, lineHeight: 10 },
} as const satisfies Record<string, TextStyle>;

/**
 * (T40) How far a capped Text follows the OS text size (`maxFontSizeMultiplier`): 2×. Capped are the Regulations page's
 * numbers (a 26 px numeral at 3× would outgrow its line) and the water's header (the name, the date pill, Map, the place
 * label, the tabs: the fixed rows above the one scroll). Every other Text follows the reader's setting all the way.
 */
export const CAPPED_SCALE = 2;
