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

/** Is `e` MapLibre's "no WebGL" error: the bare words, or the JSON that carries them with the context's details? */
export function isNoWebGL(e: unknown): e is Error {
  return e instanceof Error && /Failed to initialize WebGL/.test(e.message);
}

/**
 * The error reported for it: `Failed to initialize WebGL`, then the browser's own reason (`statusMessage`) unquoted —
 * so the scrubber keeps it — and cut to 160 characters. Named `MapUnavailable`, the cause kept.
 */
export function noWebGLError(e: Error): Error {
  // read from the JSON's text (no parse to fail): the context's type and the browser's reason, when it gave them
  const field = (k: string) => new RegExp(`"${k}"\\s*:\\s*"([^"]*)"`).exec(e.message)?.[1]?.trim() ?? "";
  let why = [field("type"), field("statusMessage")].filter(Boolean).join(": ");
  why = why.replace(/['"`]/g, "").slice(0, 160);
  const out = new Error(`Failed to initialize WebGL${why ? ` (${why})` : ""}`, { cause: e });
  out.name = "MapUnavailable";
  return out;
}
