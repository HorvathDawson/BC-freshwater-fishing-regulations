/**
 * The map's canvas follows its CONTAINER, not the window.
 *
 * Review finding (2026-10-20): after search → eye → back, search remounts and the preview map was created while its
 * flex box was still settling. MapLibre 5 observes its container but DROPS the first ResizeObserver callback
 * (`initialResizeEventCaptured`), so a box that changed between construction and that callback is never seen: the
 * canvas kept the first size (400×486 in a 374-tall box), the fit centred on that canvas, and a long lake ran off the
 * bottom edge — the ⤢ control refit against the same stale canvas. jsdom has no layout, so these drive the helpers with fakes whose
 * sizes we set.
 */
import { describe, expect, it, vi } from "vitest";
import { canvasStale, followSize, syncSize } from "./size";

function fakeMap(box: { w: number; h: number }, canvas: { w: number; h: number }) {
  const m = {
    getContainer: () => ({ clientWidth: box.w, clientHeight: box.h }) as unknown as HTMLElement,
    getCanvas: () => ({ style: { width: `${canvas.w}px`, height: `${canvas.h}px` } }) as unknown as HTMLCanvasElement,
    resize: vi.fn(() => { canvas.w = box.w; canvas.h = box.h; }),
  };
  return m;
}

class FakeRO {
  static last: FakeRO | null = null;
  cb: () => void;
  observed: unknown[] = [];
  disconnected = false;
  constructor(cb: () => void) { this.cb = cb; FakeRO.last = this; }
  observe(el: unknown) { this.observed.push(el); }
  disconnect() { this.disconnected = true; }
}

describe("the map canvas follows its container", () => {
  it("a canvas taller than its box is stale; a matching one is not", () => {
    expect(canvasStale(fakeMap({ w: 400, h: 374 }, { w: 400, h: 486 }))).toBe(true);
    expect(canvasStale(fakeMap({ w: 400, h: 374 }, { w: 400, h: 374 }))).toBe(false);
    expect(canvasStale(fakeMap({ w: 399, h: 374 }, { w: 400, h: 374 }))).toBe(true);
  });

  it("syncSize resizes a stale canvas (so a fit after it frames the box you see) and leaves a good one alone", () => {
    const stale = fakeMap({ w: 400, h: 374 }, { w: 400, h: 486 });
    expect(syncSize(stale)).toBe(true);
    expect(stale.resize).toHaveBeenCalledTimes(1);
    expect(canvasStale(stale)).toBe(false);
    const good = fakeMap({ w: 400, h: 374 }, { w: 400, h: 374 });
    expect(syncSize(good)).toBe(false);
    expect(good.resize).not.toHaveBeenCalled();
  });

  it("followSize: the container settling after mount resizes the canvas and tells the caller (who refits)", () => {
    const box = { w: 400, h: 486 };
    const m = fakeMap(box, { w: 400, h: 486 });
    const resized = vi.fn();
    const stop = followSize(m, resized, FakeRO as never);
    const ro = FakeRO.last!;
    expect(ro.observed).toHaveLength(1);
    ro.cb(); // the first observation, nothing changed
    expect(m.resize).not.toHaveBeenCalled();
    expect(resized).not.toHaveBeenCalled();
    box.h = 374; ro.cb(); // the flex box settles shorter
    expect(m.resize).toHaveBeenCalledTimes(1);
    expect(resized).toHaveBeenCalledTimes(1);
    stop();
    expect(ro.disconnected).toBe(true);
  });

  it("followSize: the FIRST observation is not dropped — a box that changed before it arrived still resizes + refits", () => {
    // MapLibre's own observer swallows its first callback; that is the window this bug lived in
    const box = { w: 400, h: 486 };
    const m = fakeMap(box, { w: 400, h: 486 });
    const resized = vi.fn();
    followSize(m, resized, FakeRO as never);
    box.h = 374; FakeRO.last!.cb();
    expect(m.resize).toHaveBeenCalledTimes(1);
    expect(resized).toHaveBeenCalledTimes(1);
  });

  it("followSize refits when the box changed even if MapLibre already resized the canvas (its observer ran first)", () => {
    const box = { w: 400, h: 486 };
    const canvas = { w: 400, h: 486 };
    const m = fakeMap(box, canvas);
    const resized = vi.fn();
    followSize(m, resized, FakeRO as never);
    box.h = 374; canvas.h = 374; // MapLibre's own resize got there first
    FakeRO.last!.cb();
    expect(m.resize).not.toHaveBeenCalled();
    expect(resized).toHaveBeenCalledTimes(1);
    FakeRO.last!.cb(); // and the same size again is not a change
    expect(resized).toHaveBeenCalledTimes(1);
  });

  it("followSize without a ResizeObserver is a no-op, not a crash", () => {
    const m = fakeMap({ w: 1, h: 1 }, { w: 1, h: 1 });
    expect(() => followSize(m, () => {}, undefined)()).not.toThrow();
  });
});
