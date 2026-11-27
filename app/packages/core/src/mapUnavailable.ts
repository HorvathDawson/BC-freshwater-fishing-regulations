/**
 * (T1005, crash kind 1bd22eea73a9dfb8: 5 browsers, every web version 0.8.1 → 0.8.5) A BROWSER THAT CANNOT DRAW THE MAP.
 * MapLibre needs WebGL; with hardware acceleration off, a blocked GPU or an old one, its constructor throws "Failed to
 * initialize WebGL" (as JSON with the browser's reason, when the browser gives one). That threw out of the map's effect:
 * an uncaught error whose message the crash scrubber reduced to quoted dots, and a blank map with nothing said. Now the
 * map says why (these words) and reports one stable message the crash server can class as the environment.
 */
export const MAP_UNAVAILABLE_WORDS = {
  title: "This browser cannot draw the map",
  body: "The map needs WebGL, which is turned off or not available in this browser. Turn on hardware acceleration in "
    + "the browser's settings, or open the map in another browser. Search and the regulations pages still work.",
} as const;

/**
 * (T1252) THE SITE'S FAULT, NOT THE BROWSER'S: the web map's GL worker file did not load (a deploy without it, a CDN
 * error), so no tile can draw. Never the WebGL words above (they send the reader to the browser's settings). The
 * owner's words (2026-11-27, tickets/T1252 `## Owner`: "that sounds good").
 */
export const MAP_WORKER_FAILED_WORDS = {
  title: "The map could not load",
  body: "Reload the page; search and the regulations pages still work.",
} as const;

/** Why a map cannot draw: this browser (no WebGL) or the site (GL's worker did not load). */
export type MapUnavailableReason = "webgl" | "worker";

/** The words a map that cannot draw says, for its reason. */
export function mapUnavailableWords(reason: MapUnavailableReason): { readonly title: string; readonly body: string } {
  return reason === "worker" ? MAP_WORKER_FAILED_WORDS : MAP_UNAVAILABLE_WORDS;
}

/**
 * Is `e` MapLibre's "no WebGL" error? (T1252) Since GL 6 (WebGL2 required) its constructor throws a
 * `GPUInitializationError` ("WebGL2 is required to display this map…") carrying the browser's reason as `statusMessage`;
 * GL 5's "Failed to initialize WebGL" (bare, or as JSON) is never thrown now.
 */
export function isNoWebGL(e: unknown): e is Error {
  return e instanceof Error && e.name === "GPUInitializationError";
}

/**
 * The error reported for it: `Failed to initialize WebGL` (the stable words the crash server classes as the environment,
 * workers/feeds `crashes.ts`), then the browser's own reason (`statusMessage`) unquoted — so the scrubber keeps it — and
 * cut to 160 characters. Named `MapUnavailable`, the cause kept.
 */
export function noWebGLError(e: Error): Error {
  const reason = (e as Error & { statusMessage?: unknown }).statusMessage;
  const why = (typeof reason === "string" ? reason.trim() : "").replace(/['"`]/g, "").slice(0, 160);
  const out = new Error(`Failed to initialize WebGL${why ? ` (${why})` : ""}`, { cause: e });
  out.name = "MapUnavailable";
  return out;
}
