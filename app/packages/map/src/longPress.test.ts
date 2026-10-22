/**
 * The web map's long press (owner 2026-10-21), driven by a fake clock: one script per case, the whole log compared, so a
 * press that fires twice, fires after a pan, or fires on a pinch fails with the full picture.
 */
import { describe, expect, it } from "vitest";
import { longPressWatch, type LongPressClock } from "./longPress";

function rig() {
  const log: string[] = [];
  let pending: { fn: () => void; ms: number } | null = null;
  const t = { now: 0 };
  const clock: LongPressClock = {
    set: (fn, ms) => { pending = { fn, ms }; log.push(`timer ${ms}`); return 1; },
    clear: () => { pending = null; log.push("clear"); },
    now: () => t.now,
  };
  const w = longPressWatch((x, y) => log.push(`fire ${x},${y}`), clock);
  const tick = () => { const p = pending; pending = null; t.now += 500; p?.fn(); };
  return { w, log, tick, t };
}

describe("longPressWatch", () => {
  it("one finger held still: fires once, where it went down, and the click after it is swallowed", () => {
    const { w, log, tick } = rig();
    w.down(100, 200, 1);
    w.move(104, 203, 1);                    // 5 px: still a press
    tick();
    w.context(110, 210);                    // Chrome's own long press, after ours: not a second spot
    const fired = w.fired();
    w.up();
    expect([...log, fired]).toEqual(["timer 500", "fire 100,200", true]);
  });

  it("a pan past the slop, a second finger, a lift or the map moving: nothing fires", () => {
    const cases = [
      (w: ReturnType<typeof rig>["w"]) => w.move(108, 200, 1),
      (w: ReturnType<typeof rig>["w"]) => w.move(100, 200, 2),
      (w: ReturnType<typeof rig>["w"]) => w.up(),
    ].map((end) => {
      const { w, log, tick } = rig();
      w.down(100, 200, 1);
      end(w);
      tick();
      return [...log, w.fired()];
    });
    expect(cases).toEqual([["timer 500", "clear", false], ["timer 500", "clear", false],
                           ["timer 500", "clear", false]]);
  });

  it("a mouse is never timed; its right click (contextmenu) fires, and every right click is a new press", () => {
    const { w, log, tick } = rig();
    w.down(10, 20, 0);
    tick();
    w.context(10, 20);
    w.up();
    w.down(30, 40, 0);
    w.context(30, 40);
    expect(log).toEqual(["fire 10,20", "fire 30,40"]);
  });

  it("two fingers down at once are a pinch: no timer at all", () => {
    const { w, log } = rig();
    w.down(100, 200, 2);
    expect([...log, w.fired()]).toEqual([false]);
  });

  it("a new press clears the last one's `fired`, so its tap is a tap again", () => {
    const { w, tick } = rig();
    w.down(1, 1, 1);
    tick();
    const after = w.fired();
    w.down(1, 1, 1);
    expect([after, w.fired()]).toEqual([true, false]);
  });

  it("the mouse a phone makes up after a touch is not a new press, so a long press's click stays swallowed", () => {
    const { w, tick, t } = rig();
    w.down(5, 5, 1);
    tick();
    t.now += 1500;                          // held well past the press
    w.up(true);
    t.now += 300;                           // the emulated mousedown that follows the touch
    w.down(5, 5, 0);
    const emulated = w.fired();
    t.now += 1000;                          // a real mouse, a second later: a new press
    w.down(5, 5, 0);
    expect([emulated, w.fired()]).toEqual([true, false]);
  });
});
