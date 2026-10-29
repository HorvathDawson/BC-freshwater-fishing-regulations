/**
 * (T180) THE WORDS OF THE SHARED FURNITURE — the sheet every screen opens, the credits block, the loading fish — that
 * were typed in their components (T60's `KNOWN_COPY`). One table each, so no component types a word of its own.
 */

/** (T41 Q4) ONE close word for every sheet, and the scrim's name aloud. */
export const SHEET_WORDS = { done: "Done", close: "Close" } as const;

/** The credits block's default label (`Credits`). */
export const CREDITS_WORDS = { label: "Sources" } as const;

/** The loading fish's default name aloud (`FishSpinner`). */
export const SPINNER_WORDS = { loading: "Loading" } as const;

/** (T224) The water title's line under a name that is not the official one: where it came from. */
export const WATER_TITLE_WORDS = { nameSource: (source: string) => `Name from ${source}` } as const;

/**
 * (T244, owner 2026-10-29) THE SPLASH'S LINE: the short pun under the loading fish while the app opens. One line per
 * opening, picked by `splashLineFor` and never cycled while it is on screen — the same rule as the search placeholders
 * (`SEARCH_BAR.placeholders`, `./search`). Each is about waters and the rules and none promises fish.
 */
export const SPLASH_WORDS = {
  lines: [
    "Wading into the rules…",
    "Getting our lines straight…",
    "Reading between the lines…",
    "Hooked on the fine print…",
    "All going swimmingly…",
    "Casting about for the rules…",
  ],
} as const;

/** The splash line for one opening: `seed` (any number; the app passes a random one, a test a fixed one) picks it. */
export function splashLineFor(seed: number): string {
  const lines = SPLASH_WORDS.lines;
  const i = Number.isFinite(seed) ? Math.abs(Math.floor(seed)) % lines.length : 0;
  return lines[i]!;
}
