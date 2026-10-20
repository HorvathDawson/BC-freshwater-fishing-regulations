/**
 * A saved spot keeps a trace with NO section handles (AGENTS 5, owner decision 2026-10-20).
 *
 * `freezeTrace` is the one place a live trace becomes a stored one: it keeps every fact the screen shows and the
 * number of reaches, and drops the handles, which are valid only against the atlas that produced them. The words
 * and the camera must be the same for both, or a saved spot says something the app never said.
 */
import { describe, expect, it } from "vitest";
import { freezeTrace, onTheReach, reachCount, routeCamera, traceSentence, tracePath,
         type GaugeTrace } from "./trace";
import type { SectionKey } from "./section";

const live = (path: number[], over: Partial<GaugeTrace> = {}): GaugeTrace => ({
  station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", trust: "good",
  path: path as unknown as SectionKey[], metres: null, reachMagnitude: 40, gaugeMagnitude: 80,
  lon: -121.96, lat: 49.09, ...over,
});

describe("freezeTrace", () => {
  it("keeps every fact and the reach count, and no section handle", () => {
    expect(freezeTrace(live([11, 12, 13]))).toEqual({
      station: "08MH001", stationName: "Chilliwack River at Vedder Crossing", trust: "good",
      reaches: 3, metres: null, reachMagnitude: 40, gaugeMagnitude: 80, lon: -121.96, lat: 49.09,
    });
  });

  it("says the same sentence and frames the same camera as the live trace", () => {
    for (const path of [[], [7], [7, 8, 9, 10, 11]]) {
      const t = live(path), f = freezeTrace(t);
      expect(traceSentence(f)).toBe(traceSentence(t));
      expect(onTheReach(f)).toBe(onTheReach(t));
      expect(routeCamera(f)).toEqual(routeCamera(t));
      expect(reachCount(f)).toBe(path.length);
    }
  });

  it("has nothing to highlight once frozen", () => {
    expect(tracePath(freezeTrace(live([1, 2])))).toEqual([]);
    expect(tracePath(live([1, 2]))).toEqual([1, 2]);
  });

  it("sentence for a frozen trace on the gauge's own reach is whole", () => {
    expect(traceSentence(freezeTrace(live([5])))).toBe("The gauge sits on this reach.");
  });
});
