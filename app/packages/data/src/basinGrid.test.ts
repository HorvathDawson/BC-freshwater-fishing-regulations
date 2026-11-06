/**
 * (T376) The watershed groups by name and by place. The row bytes are the ones `pipeline/tests/test_basin_grid.py` pins
 * the writer to (`PINNED`), so the pipeline's encoder and this decoder read one format.
 */
import { describe, expect, it } from "vitest";
import { decodeBasinGrid, decodeBasinRow, type BasinFrame } from "./basinGrid";

describe("decodeBasinRow: (run, ix) varint pairs", () => {
  it("reads the pipeline's pinned rows", () => {
    const a = decodeBasinRow(Uint8Array.from([3, 0, 2, 2, 1, 1]), 6);
    expect([...a.ends]).toEqual([3, 5, 6]);
    expect([...a.ix]).toEqual([0, 2, 1]);
    const b = decodeBasinRow(Uint8Array.from([0xc8, 0x01, 5]), 200);
    expect([...b.ends]).toEqual([200]);
    expect([...b.ix]).toEqual([5]);
  });
  it("refuses a row that does not cover the grid, or ends inside a run", () => {
    expect(() => decodeBasinRow(Uint8Array.from([3, 0, 2, 2, 1, 1]), 7)).toThrow("covers 6 cells, not 7");
    expect(() => decodeBasinRow(Uint8Array.from([3]), 3)).toThrow("inside a run");
    expect(() => decodeBasinRow(Uint8Array.from([0x83]), 3)).toThrow("inside a varint");
  });
});

// two rows of six half-degree cells from (0, 0): Chilliwack west of lon 1, Lower Fraser to lon 2, nothing past it
const FRAME: BasinFrame = { w: 0, s: 0, dlon: 0.5, dlat: 0.5, cols: 6, rows: 2 };
const ROW = Uint8Array.from([2, 1, 2, 2, 2, 0]);
const NAMES = [{ ix: 1, id: "CHWK", name: "Chilliwack River" }, { ix: 2, id: "LFRA", name: "Lower Fraser" }];

describe("decodeBasinGrid: the group a point is in", () => {
  const g = decodeBasinGrid(FRAME, [{ row: 0, b: ROW }, { row: 1, b: ROW }], NAMES);
  it("answers the group holding the point's cell, with its name as sourced", () => {
    expect(g.at(0.2, 0.2)).toEqual({ id: "CHWK", name: "Chilliwack River" });
    expect(g.at(0.99, 0.9)).toEqual({ id: "CHWK", name: "Chilliwack River" });
    expect(g.at(1, 0.9)).toEqual({ id: "LFRA", name: "Lower Fraser" });
    expect(g.at(1.7, 0.6)).toEqual({ id: "LFRA", name: "Lower Fraser" });
  });
  it("null in a cell no group holds and outside the grid", () => {
    expect(g.at(2.2, 0.2)).toBeNull();
    expect(g.at(-0.1, 0.2)).toBeNull();
    expect(g.at(0.2, 1.0)).toBeNull();
    expect(g.at(3, -1)).toBeNull();
  });
  it("throws on a point that is not one, rather than answering no group", () => {
    expect(() => g.at(Number.NaN, 0.2)).toThrow("is not a point");
  });
  it("(T417) mostIn: the group with the largest share of the box, not the one under its centre", () => {
    // centre on the sea (lon 2.25), Lower Fraser covers part of the box: Lower Fraser, never none
    expect(g.mostIn([1.6, 0, 2.9, 0.9])).toEqual({ id: "LFRA", name: "Lower Fraser" });
    // centre in Chilliwack (lon 0.9) but Lower Fraser holds more of the box: Lower Fraser
    expect(g.at(0.9, 0.5)).toEqual({ id: "CHWK", name: "Chilliwack River" });
    expect(g.mostIn([0.6, 0, 1.2, 1])?.id).toBe("CHWK");   // CHWK cell 1, LFRA cell 2: equal, centre (0.9) wins
    expect(g.mostIn([0.7, 0, 1.9, 1])).toEqual({ id: "LFRA", name: "Lower Fraser" });
    // only the sea, or outside the grid: none
    expect(g.mostIn([2.1, 0, 2.9, 0.9])).toBeNull();
    expect(g.mostIn([10, 10, 11, 11])).toBeNull();
    // a box that is one point is that point's cell
    expect(g.mostIn([0.2, 0.2, 0.2, 0.2])).toEqual(g.at(0.2, 0.2));
  });
  it("(T417) mostIn weighs a cell by its ground area: a row nearer the pole counts for less", () => {
    // row 0 (lat 0–30): one A cell; row 2 (lat 60–90): two B cells — more cells, less ground (cos 75° ≈ 0.26)
    const f: BasinFrame = { w: 0, s: 0, dlon: 30, dlat: 30, cols: 2, rows: 3 };
    const names = [{ ix: 1, id: "A", name: "A" }, { ix: 2, id: "B", name: "B" }];
    const g2 = decodeBasinGrid(f, [{ row: 0, b: Uint8Array.from([1, 1, 1, 0]) }, { row: 1, b: Uint8Array.from([2, 0]) },
                                  { row: 2, b: Uint8Array.from([2, 2]) }], names);
    expect(g2.mostIn([0, 0, 59, 89])!.id).toBe("A");
  });
  it("(T417) mostIn throws on a box that is not one", () => {
    expect(() => g.mostIn([Number.NaN, 0, 1, 1])).toThrow("is not a box");
    expect(() => g.mostIn([2, 0, 1, 1])).toThrow("is not a box");
  });
  it("names every group by id", () => {
    expect([...g.basins.keys()]).toEqual(["CHWK", "LFRA"]);
  });
  it("refuses a missing row, a row outside the frame, and an ix with no name", () => {
    expect(() => decodeBasinGrid(FRAME, [{ row: 0, b: ROW }], NAMES)).toThrow("row 1 is missing");
    expect(() => decodeBasinGrid(FRAME, [{ row: 0, b: ROW }, { row: 2, b: ROW }], NAMES)).toThrow("outside the frame");
    expect(() => decodeBasinGrid(FRAME, [{ row: 0, b: ROW }, { row: 1, b: ROW }], NAMES.slice(0, 1)))
      .toThrow("ix 2 names no basin");
  });
});
