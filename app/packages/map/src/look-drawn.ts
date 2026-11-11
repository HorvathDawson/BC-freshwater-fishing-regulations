/**
 * (T566) WHEN HAS THE MAP DRAWN THE LOOK IT WAS HANDED — `MapProps.lookKey` / `onLookDrawn`. Platform-free, so both
 * sequences are tested as plain code (`look-drawn.test.ts`).
 *
 * The owner (Pixel 9, build 24): pressing Trout / Salmon / Runs gave "layers [that] stay wrong for half a second or so".
 * Measured (web, 4x CPU: 2-3 frames, up to ~235 ms per switch; the phone holds its old picture until the new style draws
 * whole, T451): a colouring change is a data-driven paint change, so every tile on screen is laid out again, and until a
 * tile's new layout lands it is drawn from its OLD bucket, whose colours are the previous mode's. The key under the map had
 * already changed. So the map says when the new look is on screen, and the caller keeps its key on what the map drew.
 *
 *   web    `webLookWatch`: armed right after the look's paint is set; done at the first `render` after which the atlas
 *          has every tile laid out (`isSourceLoaded`: a tile reloading for the new paint is not loaded). A repaint is
 *          asked for, so a look that changed no paint (no reload) is said at the next frame, never left waiting.
 *   phone  `NativeLookTracker`: a key handed with a NEW style waits for that style to load and then for a full frame
 *          (the frame the binding's held picture lifts at); a key handed with the style unchanged is drawn already.
 */

/** The slice of a GL JS map the web watch reads. */
export interface LookWatchMap {
  on(type: "render" | "sourcedata", fn: () => void): unknown;
  off(type: "render" | "sourcedata", fn: () => void): unknown;
  getSource(id: string): unknown;
  isSourceLoaded(id: string): boolean;
  triggerRepaint(): void;
}

/** The atlas: the source every colouring paints. */
const ATLAS = "atlas";

/**
 * Call `done` once every atlas tile is laid out for the paint just set; returns the cancel. Asked after each frame AND as
 * each tile lands (`sourcedata`), so the caller's key can change in the same frame the last tile is drawn new, not one
 * after — but never before the first frame since the paint was set: the reload for it starts in that frame, and until
 * then the tiles on screen still count as loaded.
 */
export function webLookWatch(m: LookWatchMap, done: () => void): () => void {
  let live = true, framed = false;
  const stop = () => { live = false; m.off("render", onRender); m.off("sourcedata", onTile); };
  const check = () => {
    if (!live || !framed) return;
    if (m.getSource(ATLAS) !== undefined && !m.isSourceLoaded(ATLAS)) return;
    stop();
    done();
  };
  const onRender = () => { framed = true; check(); };
  const onTile = () => check();
  m.on("render", onRender);
  m.on("sourcedata", onTile);
  m.triggerRepaint();
  return () => { if (live) stop(); };
}

/** The phone's sequence: style handed → style loaded → a full frame. */
export class NativeLookTracker {
  /** the key waiting to be said, and whether it waits for a style load first */
  private pending: { key: string; needsLoad: boolean } | null = null;
  private said: string | null = null;

  /**
   * A render of the map with `key`; `styleChanged`: the style handed to the binding is a new one in this render. Returns
   * the key to say AT ONCE — one handed with the style on screen unchanged and nothing waiting (the picture is already
   * that look; an idle map draws no frame to say it at) — else null.
   */
  handed(key: string, styleChanged: boolean): string | null {
    if (styleChanged) { this.pending = { key, needsLoad: true }; return null; }
    if (this.pending) { this.pending = { ...this.pending, key }; return null; }
    if (key === this.said) return null;
    this.said = key;
    return key;
  }

  /** The binding loaded the newest style handed. */
  styleLoaded(): void {
    if (this.pending) this.pending = { ...this.pending, needsLoad: false };
  }

  /** Is the map waiting on a frame to say a key (`onDidFinishRenderingFrameFully` is listened to only then)? */
  waiting(): boolean {
    return this.pending !== null;
  }

  /** A fully rendered frame (every tile in): the key now drawn, if one waited for it, else null. */
  fullFrame(): string | null {
    if (!this.pending || this.pending.needsLoad) return null;
    const { key } = this.pending;
    this.pending = null;
    this.said = key;
    return key;
  }
}
