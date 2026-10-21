/**
 * The vector-tile reader under the route overlay: it must hand back exactly the features
 * asked for, by id, with their geometry intact — and skip everything else unread.
 */
import { describe, expect, it } from "vitest";
import { decodeGeometry, decodeTile } from "./mvt";
import { commands, encodeTile } from "./mvt.fixture";

const LINE: [number, number][] = [[10, 20], [30, 25], [30, 60]];
const RING: [number, number][] = [[0, 0], [100, 0], [100, 100], [0, 100], [0, 0]];

describe("reading a vector tile", () => {
  it("round-trips a line and a closed ring through the geometry commands", () => {
    expect(decodeGeometry(commands([LINE], false))).toEqual([LINE]);
    expect(decodeGeometry(commands([RING], true))).toEqual([RING]);
  });

  it("returns only the asked-for layers and ids, each whole", () => {
    const tile = encodeTile({
      stream: [{ id: 7, type: 2, parts: [LINE] }, { id: 8, type: 2, parts: [LINE] }],
      lake: [{ id: 9, type: 3, parts: [RING] }],
      park: [{ id: 7, type: 3, parts: [RING] }],
    }, 512);
    expect(decodeTile(tile, new Set(["stream", "lake"]), (id) => id !== 8)).toEqual([
      { name: "stream", extent: 512, features: [{ id: 7, type: 2, rings: [LINE] }] },
      { name: "lake", extent: 512, features: [{ id: 9, type: 3, rings: [RING] }] },
    ]);
  });

  it("asks about an id before decoding it, and never about a skipped layer", () => {
    const tile = encodeTile({ stream: [{ id: 1, type: 2, parts: [LINE] }],
                              park: [{ id: 2, type: 3, parts: [RING] }] });
    const asked: number[] = [];
    decodeTile(tile, new Set(["stream"]), (id) => { asked.push(id); return false; });
    expect(asked).toEqual([1]);
  });

  it("reads a section handle past 2^31 without wrapping", () => {
    const big = 2 ** 40 + 3;
    const tile = encodeTile({ stream: [{ id: big, type: 2, parts: [LINE] }] });
    expect(decodeTile(tile, new Set(["stream"]), () => true)[0]!.features[0]!.id).toBe(big);
  });
});
