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
