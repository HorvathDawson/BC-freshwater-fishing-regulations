/**
 * T166 — TIDAL WATER, TAPPED: what the sheet says about the `tidal_water` tile layer (the DFO tidal-waters polygon plus
 * the footprints of stream pieces removed below a tidal boundary). The words are the K7 tidal note's, whole, so the map
 * and the water page say the same thing; a test pins every finished line (AGENTS.md rule 33).
 *
 * Nothing here is a regulation of this app: the freshwater regulations stop at tidal water, and the federal (DFO) tidal
 * rules govern it.
 */
export const TIDAL_SHEET_WORDS = {
  title: "Tidal water — DFO rules",
  body: [
    "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial "
      + "rule, quota, closure, gear rule, licence or stamp.",
    "See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport "
      + "Fishing Licence is required.",
  ],
} as const;

export interface TidalSheetVM {
  readonly title: string;
  /** Whole paragraphs, in order. */
  readonly body: readonly string[];
}

/** The sheet for a tap on tidal water: the same words everywhere on it. */
export function tidalSheet(): TidalSheetVM {
  return { title: TIDAL_SHEET_WORDS.title, body: TIDAL_SHEET_WORDS.body };
}
