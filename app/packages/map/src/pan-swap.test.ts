/** (T451) The pan probe's style-swap line: styles handed per load, frames drawn before the labels land, feature-state kept. */
import { describe, expect, it } from "vitest";
import { swapTracker } from "./native/panProbe";

const lines = () => { const out: Record<string, unknown>[] = []; return { out, log: (l: string) => out.push(JSON.parse(l.slice("[swap] ".length))) }; };

describe("swapTracker (T451)", () => {
  it("one line per load: the hand-overs before it, the frames drawn until the labels land, and kept", () => {
    const { out, log } = lines();
    const s = swapTracker(log);
    s.handed("modes", 100); s.handed("values", 110);           // a switch that handed two styles
    s.loaded(300); s.kept(true);
    s.frame(316); s.frame(332);
    s.acked(340);
    expect(out).toEqual([{ t: 300, handed: 2, why: ["modes", "values"], loadMs: 200, bare: 2, bareMs: 40, kept: true }]);
  });
  it("a load whose labels never land is closed by the next hand-over (bareMs null)", () => {
    const { out, log } = lines();
    const s = swapTracker(log);
    s.handed("modes", 0); s.loaded(50); s.frame(60);
    s.handed("values", 70);
    expect(out).toEqual([{ t: 50, handed: 1, why: ["modes"], loadMs: 50, bare: 1, bareMs: null, kept: null }]);
  });
});
