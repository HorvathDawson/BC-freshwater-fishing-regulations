/**
 * (T418) A STYLE ASSET THAT WOULD NOT LOAD — the sprite, a glyph range, a source's data (the `outside` mask) — is
 * REPORTED, once per asset a run, not only logged. MapLibre Native says it only in its own log (`LogManager`, an
 * `[ERROR] [Style]` line the binding prints with `console.error`), never through an event the map can catch, so the
 * map reads its log for exactly these lines. The line is still printed (the handler returns false): the report adds to
 * the log, it does not replace it.
 *
 * On a phone every one of them is a file of the installed release (`releaseTiles`): one missing is a defect of the app
 * (0.5.0 pruned the set on screen during an in-session update, T418), never the network's — so it is never the
 * environment's and always reported. What the reader sees is the map itself (no labels, no icons, no mask).
 */
import { reportOnce } from "@app/core";

/** One MapLibre Native log event, as `LogManager.onLog` hands it over. */
export interface NativeLogEvent {
  readonly level: string;
  readonly tag: string;
  readonly message: string;
}

const FAILED = /Failed to load (sprite|glyph range \S+ for font stack ([^:]+)|source ([^:\s]+))/;

/**
 * THE ASSET A LOG LINE SAYS FAILED, or null for any other line: its once-a-run key (`sprite`, `glyphs:<font stack>`,
 * `source:<id>`) and the error to report. Only error-level lines are read.
 */
export function styleAssetFailure(e: NativeLogEvent): { key: string; error: Error } | null {
  if (e.level !== "error") return null;
  const m = FAILED.exec(e.message);
  if (!m) return null;
  const key = m[1] === "sprite" ? "sprite" : m[2] !== undefined ? `glyphs:${m[2].trim()}` : `source:${m[3]}`;
  return { key, error: new Error(`map: a style asset failed to load (${key}): ${e.message.trim()}`) };
}

/** What `LogManager` offers: one handler for the whole app (the binding's own, replaced). */
interface LogManagerLike { onLog(h: (e: NativeLogEvent) => boolean): void }

let watching = false;
/** Install the watch once (the first native map mounted): every later map shares MapLibre's one log. */
export function watchStyleAssets(log: LogManagerLike): void {
  if (watching) return;
  watching = true;
  log.onLog((e) => {
    const f = styleAssetFailure(e);
    if (f) reportOnce(`map-style-asset:${f.key}`, f.error);
    return false;            // and printed as before
  });
}
