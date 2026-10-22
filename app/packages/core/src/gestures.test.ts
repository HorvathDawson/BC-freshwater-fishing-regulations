/**
 * Every gesture threshold, at, below and above its edge (G1). Whole tables: one row per case, compared as a whole,
 * so a moved threshold fails with the full picture rather than the first row that differs.
 */
import { describe, expect, it } from "vitest";
import { EDGE_BACK, LONG_PRESS, MAP_GESTURES, RESIST, SHEET_DRAG, SWIPE, TAP_SLOP, classifySwipe, edgeBackCommits,
         edgeBackEnabled, edgeBackStarts, follow, isTap, longPressHolds, sheetDismiss, viewSwipeStarts } from "./gestures";

const table = <A extends unknown[], R>(f: (...a: A) => R, rows: readonly A[]) =>
  rows.map((a) => [...a, f(...a)]);

describe("the numbers (the ticket's, not re-derived)", () => {
  it("are the owner's", () => {
    expect({ TAP_SLOP, EDGE_BACK, SWIPE, SHEET_DRAG, RESIST }).toEqual({
      TAP_SLOP: 8,
      EDGE_BACK: { zone: 24, commit: 80, flick: 500 },
      SWIPE: { lock: 2, distance: 60, flick: 500, flickMin: 20 },
      SHEET_DRAG: { fraction: 1 / 3, flick: 800 },
      RESIST: { factor: 0.3, max: 32 },
    });
  });
  it("the map: pan, pinch, double-tap and wheel zoom; no rotate, no tilt; the tap slop", () => {
    expect(MAP_GESTURES).toEqual({ pan: true, pinch: true, doubleTapZoom: true, wheel: true,
                                   rotate: false, tilt: false, tapSlop: 8 });
  });
  it("a long press (owner 2026-10-21, a spot dropped on the map): held 500 ms, still a tap's slop", () => {
    expect(LONG_PRESS).toEqual({ ms: 500, slop: 8 });
  });
});

describe("a long press holds while one finger stays within the tap slop", () => {
  it("moved, at and past the slop; a second finger is a pinch, never a press", () => {
    expect(table(longPressHolds, [[0, 0, 1], [7.9, 0, 1], [8, 0, 1], [0, 0, 2], [5, 5, 1], [6, 6, 1], [0, 0, 0]]))
      .toEqual([[0, 0, 1, true], [7.9, 0, 1, true], [8, 0, 1, false], [0, 0, 2, false], [5, 5, 1, true],
                [6, 6, 1, false], [0, 0, 0, false]]);
  });
});

describe("a tap is a press that moved under 8 px", () => {
  it("below, at and above the slop, along both axes and the diagonal", () => {
    expect(table(isTap, [[0, 0], [7.9, 0], [8, 0], [8.1, 0], [0, -7.9], [0, -8], [5, 5], [6, 6]]))
      .toEqual([[0, 0, true], [7.9, 0, true], [8, 0, false], [8.1, 0, false], [0, -7.9, true], [0, -8, false],
                [5, 5, true], [6, 6, false]]);
  });
});

describe("the left-edge back swipe", () => {
  it("is on only where the platform has no system back", () => {
    expect(table(edgeBackEnabled, [["ios"], ["android"], ["web"], ["windows"]]))
      .toEqual([["ios", true], ["android", false], ["web", false], ["windows", false]]);
  });
  it("starts only inside the 24 px zone", () => {
    expect(table(edgeBackStarts, [[-1], [0], [23.9], [24], [25]]))
      .toEqual([[-1, false], [0, true], [23.9, true], [24, false], [25, false]]);
  });
  it("commits past 80 px, or on a rightward flick", () => {
    expect(table(edgeBackCommits, [[79, 0], [80, 0], [81, 0], [30, 499], [30, 500], [30, 501], [0, 900],
                                   [-10, 900], [200, -900]]))
      .toEqual([[79, 0, false], [80, 0, true], [81, 0, true], [30, 499, false], [30, 500, true], [30, 501, true],
                [0, 900, false], [-10, 900, false], [200, -900, true]]);
  });
});

describe("a view swipe", () => {
  it("never begins inside either edge zone", () => {
    expect(table(viewSwipeStarts, [[23, 390], [24, 390], [200, 390], [366, 390], [367, 390]]))
      .toEqual([[23, 390, false], [24, 390, true], [200, 390, true], [366, 390, true], [367, 390, false]]);
  });
  it("by distance: under, at and over 60 px; finger left is next, right is prev", () => {
    expect(table(classifySwipe, [[-59, 0, 0], [-60, 0, 0], [-61, 0, 0], [59, 0, 0], [60, 0, 0], [61, 0, 0]]))
      .toEqual([[-59, 0, 0, null], [-60, 0, 0, "next"], [-61, 0, 0, "next"],
                [59, 0, 0, null], [60, 0, 0, "prev"], [61, 0, 0, "prev"]]);
  });
  it("direction lock: horizontal must beat vertical two to one, or it is a scroll", () => {
    expect(table(classifySwipe, [[-100, 49, 0], [-100, 50, 0], [-100, 51, 0], [100, -51, 0], [0, 300, 0],
                                 [-100, 51, -2000]]))
      .toEqual([[-100, 49, 0, "next"], [-100, 50, 0, "next"], [-100, 51, 0, null], [100, -51, 0, null],
                [0, 300, 0, null], [-100, 51, -2000, null]]);
  });
  it("by flick: 500 px/s the same way, once it has moved 20 px", () => {
    expect(table(classifySwipe, [[-30, 0, -499], [-30, 0, -500], [-30, 0, -501], [-19, 0, -900], [-20, 0, -900],
                                 [-30, 0, 900], [30, 0, 500], [30, 0, -500]]))
      .toEqual([[-30, 0, -499, null], [-30, 0, -500, "next"], [-30, 0, -501, "next"], [-19, 0, -900, null],
                [-20, 0, -900, "next"], [-30, 0, 900, null], [30, 0, 500, "prev"], [30, 0, -500, null]]);
  });
});

describe("a sheet dragged down", () => {
  it("closes past a third of its height, or on a downward flick; never on an upward drag", () => {
    expect(table(sheetDismiss, [[99, 0, 300], [100, 0, 300], [101, 0, 300], [20, 799, 300], [20, 800, 300],
                                [20, 801, 300], [0, 2000, 300], [-50, 2000, 300], [150, -900, 300],
                                [500, 0, 0], [5, 900, 0]]))
      .toEqual([[99, 0, 300, false], [100, 0, 300, true], [101, 0, 300, true], [20, 799, 300, false],
                [20, 800, 300, true], [20, 801, 300, true], [0, 2000, 300, false], [-50, 2000, 300, false],
                [150, -900, 300, true], [500, 0, 0, false], [5, 900, 0, true]]);
  });
});

describe("a dragged view follows the finger, or resists at an end", () => {
  it("free moves one-for-one; held back moves 0.3 of it, capped at 32 px", () => {
    expect(table(follow, [[50, true], [-50, true], [50, false], [-50, false], [100, false], [110, false],
                          [-500, false], [0, false]]))
      .toEqual([[50, true, 50], [-50, true, -50], [50, false, 15], [-50, false, -15], [100, false, 30],
                [110, false, 32], [-500, false, -32], [0, false, 0]]);
  });
});
