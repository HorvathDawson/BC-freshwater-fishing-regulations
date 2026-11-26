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
 *
 * (T1201) THE ONE RULE FOR TEXT SIZE. Body and reading text follows the system text size in full. A label inside a
 * fixed-size control (a rail cell, the tab bar, a chip, a badge, a chart tick) follows it only to `LABEL_SCALE` and is
 * ALWAYS one line, whole: never wrapped, never cut. A control GROWS before its label shrinks: the control takes a
 * `minWidth` from `labelMinWidth` (a safe over-estimate of the label's width at that scale) and the label sets
 * `labelProps(scale)` (one line, the cap). `tools/system-text-labels.test.ts` lays the rail and the bar out in Chrome at
 * 1, 1.15 and 1.3 and fails on a wrapped or cut label. The page's numbers and header keep their own cap, `CAPPED_SCALE`.
 */
export const FACE = {
  display: "BricolageGrotesque_700Bold",
  displayHeavy: "BricolageGrotesque_800ExtraBold",
  text: "Archivo_400Regular",
  textMedium: "Archivo_500Medium",
  textSemi: "Archivo_600SemiBold",
  textBold: "Archivo_700Bold",
  /** Archivo 800 (T22, tokens.md §2.2): the regs page's date-day word, size chip, ✓/✕ marks. */
  textHeavy: "Archivo_800ExtraBold",
  figure: "JetBrainsMono_500Medium",
} as const;

/** What `useFonts` must be given. Kept beside the names so the two cannot drift. */
export const FACES_REQUIRED = Object.values(FACE);

export interface TextStyle {
  fontFamily: string; fontSize: number; lineHeight?: number;
  letterSpacing?: number; textTransform?: "uppercase";
}

/** (T283) the map legend's words, one size in every map mode (it was 11.5, hard to read on a phone): the legend text floor. */
export const LEGEND_TEXT = 15;

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
  /** (T260) the line under a row or a control: `small` at 11.5 (what eighteen screens typed out as `...TYPE.small, fontSize: 11.5`). */
  caption:  { fontFamily: FACE.text, fontSize: 11.5, lineHeight: 18 },
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
  /** (T531) a year bar's legend: full ink, larger than the meta text it was */
  legend:      { fontFamily: FACE.textSemi,  fontSize: 14, lineHeight: 20 },
  meta:        { fontFamily: FACE.text,      fontSize: 12.48, lineHeight: 17.72 },
  metaSemi:    { fontFamily: FACE.textSemi,  fontSize: 12.48, lineHeight: 16.22 },
  metaBold:    { fontFamily: FACE.textBold,  fontSize: 12.48, lineHeight: 16.22 },
  metaHeavy:   { fontFamily: FACE.textHeavy, fontSize: 12.48, lineHeight: 14.98 },  // the date pill's "Today"/"Fri" (1.2)
  /** ALL-CAPS kickers only (.lab, .scope, .bkh, .tkick, .nofish h3): --t4 700, tracking 1. */
  kicker:      { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 16.22, letterSpacing: 1, textTransform: "uppercase" },
  /** The four header tabs (.segm[role=tablist] button: 700 --t4, tracking .2, sentence case; line-height normal → 15). */
  regsTab:     { fontFamily: FACE.textBold, fontSize: 12.48, lineHeight: 15, letterSpacing: 0.2 },
  /** The strip's month initials (.smonths: JetBrains Mono .625rem/1). */
  month:       { fontFamily: FACE.figure, fontSize: 10, lineHeight: 10 },
} as const satisfies Record<string, TextStyle>;

/**
 * (T816) Each face's vertical metrics per em, from its font file (hhea ascent / descent, OS/2 cap height; `type.lineBox.test.ts`
 * reads the files, so these cannot drift from them). iOS sets a line whose `lineHeight` is shorter than the face's
 * ascent + descent with the descent on the box's bottom edge and draws nothing above the box's top: what the box lacks is
 * cut off the top of the glyphs. The figure face at 30 or 32 px in `figureBig`'s 23 px box lost the top third of "18:00"
 * and most of "8°C" on an iPhone (Android and the web draw the overflow, so they showed it whole).
 */
export const FACE_METRICS = {
  BricolageGrotesque: { ascent: 0.93, descent: 0.27, cap: 0.66 },
  Archivo:            { ascent: 0.878, descent: 0.21, cap: 0.686 },
  JetBrainsMono:      { ascent: 1.02, descent: 0.3, cap: 0.73 },
} as const;

/**
 * (T816) The shortest line box that keeps a capital or a figure whole on iOS: its cap height plus the descent below it (the
 * ascent's empty headroom above the capitals may be cut; the letters may not). Every TYPE entry, and every size a screen
 * gives one, stays at or above it (`type.lineBox.test.ts`, over every Text style in the source).
 */
export function lineBoxFloor(fontFamily: string, fontSize: number): number {
  const m = FACE_METRICS[fontFamily.split("_")[0] as keyof typeof FACE_METRICS];
  if (!m) throw new Error(`no metrics for the face "${fontFamily}" (FACE_METRICS, type.ts)`);
  return fontSize * (m.cap + m.descent);
}

/**
 * (T816) A TYPE entry at another size, its line box scaled with it (the entry's own ratio), so a bigger size never sits in
 * the smaller size's box: `typeAt("figureBig", 30)`, not figureBig spread with a fontSize of 30 (which kept its 23 px box).
 */
export function typeAt<K extends keyof typeof TYPE>(key: K, fontSize: number): TextStyle {
  const t: TextStyle = TYPE[key];
  return t.lineHeight === undefined ? { ...t, fontSize }
    : { ...t, fontSize, lineHeight: Math.round(t.lineHeight * fontSize / t.fontSize * 100) / 100 };
}

/**
 * (T40) How far a capped Text follows the OS text size (`maxFontSizeMultiplier`): 2×. Capped are the Regulations page's
 * numbers (a 26 px numeral at 3× would outgrow its line) and the water's header (the name, the date pill, Map, the place
 * label, the tabs: the fixed rows above the one scroll). Every other Text follows the reader's setting all the way.
 */
export const CAPPED_SCALE = 2;

/**
 * (T1201) How far a label in a fixed-size control follows the OS text size (`maxFontSizeMultiplier`): 1.3, what a phone's
 * own tab bar holds. Past it the label stops growing; below it the control grows to hold it (`labelMinWidth`).
 */
export const LABEL_SCALE = 1.3;

/** (T1201) the props every fixed-size-control label spreads: one line, whole, and the cap on its growth */
export const LABEL_PROPS = { numberOfLines: 1, maxFontSizeMultiplier: LABEL_SCALE } as const;

/**
 * (T1201) The width a control must give a label so it is not cut or wrapped at `scale` (1 to `LABEL_SCALE`): an UPPERCASE
 * label's glyphs run to `em` each (default 0.72, the heaviest weight; measured: "CONDITIONS" 0.65, "SALMON" 0.72), plus its letter
 * spacing (which the OS does not scale), plus the control's own horizontal padding. An over-estimate by design: the
 * control grows a few px more than the word needs, never less.
 */
export function labelMinWidth(text: string, t: { fontSize: number; letterSpacing?: number }, scale: number, padding = 0, em = 0.72): number {
  return Math.ceil(text.length * (em * t.fontSize * scale + (t.letterSpacing ?? 0)) + padding);
}
