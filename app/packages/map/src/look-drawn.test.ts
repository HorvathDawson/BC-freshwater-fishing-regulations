/**
 * (T566) THE MAP SAYS A LOOK IS DRAWN ONLY WHEN IT IS (`look-drawn.ts`). Measured before: a Trout / Salmon / Runs press
 * left the map drawing the previous colouring for 2-3 frames on the web (4x CPU, up to ~235 ms) and 66-101 ms on the
 * emulator (the held picture), under a key that had already changed. Pinned here as the sequence of renderer events,
 * frame by frame — no clocks, no sleeps.
 */
import { describe, expect, it } from "vitest";
import { NativeLookTracker, webLookWatch, type LookWatchMap } from "./look-drawn";

/** A GL JS map's slice: `frames` scripts, per render, whether the atlas has every tile laid out. */
function fakeMap(atlas: boolean) {
  const listeners = { render: new Set<() => void>(), sourcedata: new Set<() => void>() };
  let loaded = true, repaints = 0;
  const m: LookWatchMap = {
    on: (t, fn) => listeners[t].add(fn), off: (t, fn) => listeners[t].delete(fn),
    getSource: (id) => (atlas && id === "atlas" ? {} : undefined),
    isSourceLoaded: () => loaded,
    triggerRepaint: () => { repaints++; },
  };
  return { m, frame: (laidOut: boolean) => { loaded = laidOut; for (const f of [...listeners.render]) f(); },
           tile: (laidOut: boolean) => { loaded = laidOut; for (const f of [...listeners.sourcedata]) f(); },
           listening: () => listeners.render.size + listeners.sourcedata.size, repaints: () => repaints };
}

describe("the web says a look is drawn at the first frame with every atlas tile laid out for it (T566)", () => {
  it("not while the atlas is laid out again for the new paint — the frames drawn from the old buckets", () => {
    const f = fakeMap(true);
    const said: string[] = [];
    webLookWatch(f.m, () => said.push("salmon"));
    f.frame(false); f.frame(false); f.frame(false);         // tiles reloading: the previous colouring on screen
    expect(said).toEqual([]);
    f.frame(true);                                          // the first frame every tile is in the new colouring
    expect(said).toEqual(["salmon"]);
    f.frame(true);
    expect(said).toEqual(["salmon"]);                       // once
    expect(f.listening()).toBe(0);
  });

  it("a tile landing says it as the last tile is in — but never before the first frame since the paint was set", () => {
    const f = fakeMap(true);
    const said: string[] = [];
    webLookWatch(f.m, () => said.push("runs"));
    f.tile(true);                                           // before the frame that starts the reload: tiles still "loaded"
    expect(said).toEqual([]);
    f.frame(false);                                         // the reload starts: old buckets on screen
    f.tile(false);
    expect(said).toEqual([]);
    f.tile(true);                                           // the last tile laid out: said before the frame that draws it
    expect(said).toEqual(["runs"]);
    expect(f.listening()).toBe(0);
  });

  it("asks for a frame, so a look that changed no paint (nothing reloads) is said at the next one", () => {
    const f = fakeMap(true);
    const said: string[] = [];
    webLookWatch(f.m, () => said.push("map"));
    expect(f.repaints()).toBe(1);
    f.frame(true);
    expect(said).toEqual(["map"]);
  });

  it("a map with no atlas source (nothing to lay out) is drawn at its next frame", () => {
    const f = fakeMap(false);
    const said: string[] = [];
    webLookWatch(f.m, () => said.push("x"));
    f.frame(false);
    expect(said).toEqual(["x"]);
  });

  it("a watch cancelled by a newer look says nothing", () => {
    const f = fakeMap(true);
    const said: string[] = [];
    const cancel = webLookWatch(f.m, () => said.push("old"));
    cancel();
    f.frame(true);
    expect(said).toEqual([]);
    expect(f.listening()).toBe(0);
  });
});

describe("the phone says a look is drawn at the first full frame after its style loaded (T566)", () => {
  it("a new style: not at the hand-over, not at a full frame of the OLD style, but after the load", () => {
    const t = new NativeLookTracker();
    expect(t.handed("trout", true)).toBeNull();
    expect(t.fullFrame()).toBeNull();
    t.styleLoaded();
    expect(t.fullFrame()).toBe("trout");
    expect(t.handed("salmon", true)).toBeNull();
    expect(t.waiting()).toBe(true);
    expect(t.fullFrame()).toBeNull();                       // the held picture of trout: not salmon yet
    t.styleLoaded();
    expect(t.fullFrame()).toBe("salmon");
    expect(t.waiting()).toBe(false);
  });

  it("a key handed with the style unchanged is the picture already on screen: said at once", () => {
    const t = new NativeLookTracker();
    t.handed("trout", true); t.styleLoaded(); t.fullFrame();
    expect(t.handed("trout", false)).toBeNull();            // nothing new
    expect(t.handed("trout-again", false)).toBe("trout-again");
  });

  it("a newer key while a style is still loading waits for that load and is the one said", () => {
    const t = new NativeLookTracker();
    t.handed("trout", true); t.styleLoaded(); t.fullFrame();
    t.handed("salmon", true);
    t.handed("runs", true);
    t.styleLoaded();
    expect(t.fullFrame()).toBe("runs");
  });
});
