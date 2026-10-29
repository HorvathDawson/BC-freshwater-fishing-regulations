/**
 * A saved spot keeps a trace with NO section handles (AGENTS 5, owner decision 2026-10-20).
 *
 * `freezeTrace` is the one place a live trace becomes a stored one: it keeps every fact the screen shows and the
 * number of reaches, and drops the handles, which are valid only against the atlas that produced them. The words
 * and the camera must be the same for both, or a saved spot says something the app never said.
 */
import { describe, expect, it } from "vitest";
import { freezeTrace, metresOfBucket, onTheReach, reachCount, relOfCode, routeCamera, shareWord, traceSentence, tracePath,
         whereWord,
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

// ---- the single-station trace's words, pinned whole (AGENTS 33; P12a) ----
const trace = (over: Partial<GaugeTrace> = {}): GaugeTrace => ({
  station: "08MH001", stationName: "VEDDER", trust: null, path: ["a" as never, "b" as never],
  metres: 12_300, reachMagnitude: null, gaugeMagnitude: null, lon: null, lat: null, ...over,
});

describe("traceSentence, all four branches", () => {
  it("no station may speak", () => {
    expect(traceSentence(trace({ station: null })))
      .toBe("No station drains enough of this water to speak for it. One that did not would "
            + "still give you a number, and the number would be wrong.");
  });
  it("the gauge is on this reach", () => {
    expect(traceSentence(trace({ path: ["a" as never] }))).toBe("The gauge sits on this reach.");
  });
  it("downstream, distance unknown", () => {
    expect(traceSentence(trace({ metres: null }))).toBe("This reach drains through 08MH001.");
  });
  it("downstream, distance known", () => {
    expect(traceSentence(trace())).toBe("Water here flows 12.3 km down to 08MH001.");
    expect(traceSentence(trace({ metres: 450 }))).toBe("Water here flows 450 m down to 08MH001.");
  });
});

describe("shareWord boundaries", () => {
  it("is null without both magnitudes", () => {
    expect(shareWord(trace())).toBeNull();
    expect(shareWord(trace({ reachMagnitude: 3 }))).toBeNull();
  });
  it("keeps two decimals below one per cent, one at and above it", () => {
    expect(shareWord(trace({ reachMagnitude: 3, gaugeMagnitude: 10_000 }))).toBe("0.03 %");
    expect(shareWord(trace({ reachMagnitude: 999, gaugeMagnitude: 100_000 }))).toBe("1.00 %");
    expect(shareWord(trace({ reachMagnitude: 1, gaugeMagnitude: 100 }))).toBe("1.0 %");
    expect(shareWord(trace({ reachMagnitude: 24, gaugeMagnitude: 1000 }))).toBe("2.4 %");
  });
});

// ---- (T93) where the station stands, as the release says it (`section_gauge.rel` / `dist_b`, decoded) ----
describe("the release's encoding", () => {
  it("rel is an index into GAUGE_RELS; null stays null", () => {
    expect(relOfCode(0)).toBe("on");
    expect(relOfCode(3)).toBe("down_trunk");
    expect(relOfCode(null)).toBeNull();
    expect(relOfCode(99)).toBeNull();
  });
  it("dist_b decodes to the bucket's step (shed.dist_bucket's inverse)", () => {
    expect([0, 1, 2, 3, 8, null].map((b) => metresOfBucket(b))).toEqual([0, 750, 1000, 1500, 11391, null]);
  });
});

describe("the relationship's words", () => {
  it("a station downstream on this water: the distance row says which way, and 'about'", () => {
    expect(whereWord(trace({ rel: "down", metres: 11_391 }))).toBe("about 11 km downstream");
    expect(traceSentence(trace({ rel: "down", metres: 11_391 }))).toBe("Water here flows about 11 km down to 08MH001.");
  });
  it("downstream on the river this flows into", () => {
    expect(whereWord(trace({ rel: "down_trunk", metres: 3_375 })))
      .toBe("about 3.4 km downstream, on the river this flows into");
    expect(whereWord(trace({ rel: "down_trunk", metres: 750 }))).toBe("under 1 km downstream, on the river this flows into");
  });
  it("a station UPSTREAM (the Vedder below 08MH001): never 'drains through' or 'flows down to'", () => {
    expect(whereWord(trace({ rel: "up", metres: 5_063 }))).toBe("about 5.1 km upstream");
    expect(traceSentence(trace({ rel: "up", metres: 5_063 })))
      .toBe("08MH001 is about 5.1 km upstream, on this water: water here has already passed it.");
    // the reach that starts at the station: no "0 m"
    expect(whereWord(trace({ rel: "up", metres: 0 }))).toBe("at the top of this reach");
    expect(traceSentence(trace({ rel: "up", metres: 0 })))
      .toBe("08MH001 is just upstream, on this water: water here has already passed it.");
    // a one-reach walk is not "on the reach" when the release says the station is upstream
    expect(onTheReach(trace({ rel: "up", metres: 0, path: ["a" as never] }))).toBe(false);
  });
  it("on the reach is the release's word when it has one", () => {
    expect(onTheReach(trace({ rel: "on", metres: 0 }))).toBe(true);
    expect(whereWord(trace({ rel: "on", metres: 0 }))).toBe("on the reach");
  });
  it("a release cut before T93 (no rel): the bare distance, or nothing", () => {
    expect(whereWord(trace())).toBe("12.3 km");
    expect(whereWord(trace({ metres: null }))).toBeNull();
  });
  it("freezes with the spot", () => {
    const f = freezeTrace(trace({ rel: "up", metres: 5_063 }));
    expect(f.rel).toBe("up");
    expect(traceSentence(f)).toBe(traceSentence(trace({ rel: "up", metres: 5_063 })));
  });
});
