/**
 * The type scale, in one place.
 *
 * Riffle (`design/riffle.html`) settles three faces and they carry different jobs, so the
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
  tab:      { fontFamily: FACE.textSemi, fontSize: 11, lineHeight: 13, letterSpacing: 0.2 },
} as const satisfies Record<string, TextStyle>;
