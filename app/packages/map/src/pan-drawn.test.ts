/** (T420/T401) The pan probe's "streams drawn" line: frames missing tiles during a move, and rest → first full frame. */
import { describe, expect, it, vi } from "vitest";
import { drawnTracker } from "./native/panProbe";

const lines = () => { const out: Record<string, unknown>[] = []; return { out, log: (l: string) => out.push(JSON.parse(l.slice("[drawn] ".length))) }; };

describe("drawnTracker (T420/T401)", () => {
  it("counts partial frames during the move and times rest → the first full frame", () => {
    const { out, log } = lines();
    const d = drawnTracker(log);
    d.moving(1000); d.moving(1005);                       // the binding says will-change twice for one gesture
    d.frame(true, 1016); d.frame(false, 1032); d.frame(false, 1048);
    d.settled(1100);
    d.frame(false, 1150); d.frame(true, 1420);
    expect(out).toEqual([{ t: 1420, ms: 320, frames: 3, partial: 2, moveMs: 100, never: false }]);
  });
  it("a move whose last frame was full is drawn at rest (0 ms); a settle with no move says nothing", () => {
    const { out, log } = lines();
    const d = drawnTracker(log);
    d.settled(10);
    d.moving(100); d.frame(true, 116); d.settled(200);
    expect(out).toEqual([{ t: 200, ms: 0, frames: 1, partial: 0, moveMs: 100, never: false }]);
  });
  it("never full within 8 s, or moved again first: `never`", () => {
    vi.useFakeTimers();
    const { out, log } = lines();
    const d = drawnTracker(log);
    d.moving(0); d.frame(false, 16); d.settled(100);
    vi.advanceTimersByTime(8000);
    d.moving(9000); d.frame(false, 9016); d.settled(9100);
    d.moving(9200);
    vi.useRealTimers();
    expect(out.map((o) => [o.ms, o.never])).toEqual([[null, true], [null, true]]);
  });
});
