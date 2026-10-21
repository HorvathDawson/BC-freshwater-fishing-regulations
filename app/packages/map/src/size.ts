/**
 * KEEP THE CANVAS THE SIZE OF ITS BOX.
 *
 * MapLibre 5 sizes its canvas on creation and then observes the container — but it DROPS its observer's first callback
 * (`initialResizeEventCaptured`), so a box that changes between construction and that callback is never seen. A map
 * mounted inside a flex box that is still settling — search's preview map, remounted on the way back from the eye —
 * keeps the first size it saw: a 486-px canvas in a 374-px box. Every fit then centres on the canvas, not the box, and the water runs
 * off the bottom edge; the ⤢ control refits against the same wrong canvas. So the canvas follows the container, and a
 * fit re-measures before it frames. Pure over a tiny interface so it can be tested without layout (jsdom has none).
 */

/** The three things on a MapLibre map these helpers touch. */
export interface Sized {
  getContainer(): HTMLElement;
  getCanvas(): HTMLCanvasElement;
  resize(): unknown;
}

type ROCtor = new (cb: () => void) => { observe(el: Element): void; disconnect(): void };

/** The canvas's CSS size differs from its container's — every fit would frame the wrong box. */
export function canvasStale(m: Sized): boolean {
  const el = m.getContainer();
  const c = m.getCanvas();
  const w = parseFloat(c.style.width);
  const h = parseFloat(c.style.height);
  return Math.round(w) !== el.clientWidth || Math.round(h) !== el.clientHeight;
}

/** Resize a stale canvas now; true when it was. Called before every fit. */
export function syncSize(m: Sized): boolean {
  if (!canvasStale(m)) return false;
  m.resize();
  return true;
}

/**
 * Watch the container; when it changes size, resize the canvas (if MapLibre has not already) and call `resized` (the
 * caller refits a subject the reader has not moved away from). The change is judged against the size the CANVAS was
 * built at, so the first observation counts, and a resize MapLibre did first still refits. Returns the cleanup.
 * Without a ResizeObserver it does nothing.
 */
export function followSize(m: Sized, resized: () => void,
                           RO: ROCtor | undefined = (globalThis as { ResizeObserver?: ROCtor }).ResizeObserver,
                           ): () => void {
  if (!RO) return () => {};
  const c = m.getCanvas();
  let last = `${Math.round(parseFloat(c.style.width))}x${Math.round(parseFloat(c.style.height))}`;
  const ro = new RO(() => {
    const el = m.getContainer();
    const now = `${el.clientWidth}x${el.clientHeight}`;
    if (now === last) return;
    last = now;
    syncSize(m);
    resized();
  });
  ro.observe(m.getContainer());
  return () => ro.disconnect();
}
