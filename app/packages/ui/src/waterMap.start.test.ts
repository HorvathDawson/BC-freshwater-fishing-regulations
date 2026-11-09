/**
 * (T517) THE MAP STARTS ON THE ENTRY HOLDING THE FRAME'S PART — whichever piece of a joined entry the frame names. Pieces
 * of one run the live salmon reads alike are one entry (`mergeNav`); a frame on its LATER piece (a tap there, a reload of
 * its address, a flow into it) found -1 (`partAt` matched only the entry's first piece), reported "not in its group" and
 * opened at the mouth: the Squamish's part 3, the Somass 4 + 5, the Chehalis, Vedder, Chilliwack and Pallant 0 + 1.
 * Pure: a hand-built group, no release.
 */
import { afterEach, describe, expect, it } from "vitest";
import { mergeNav, setFailureSink, waterGroup, type EndsTable, type ItemNavRows } from "@app/core";
import { waterMapStart, waterMapStartIx, waterMapView } from "./waterMap";

const RUN = "Estuary sign → Canyon";
const ENDS: EndsTable = { mouth: ["Mouth"], est: ["Estuary sign"], bridge: ["Bridge"], canyon: ["Canyon"] };
const piece = (partIx: number, lo: string, hi: string) => ({ partIx, label: RUN, grp: 0, run: 0, lo, hi });
const RIVER: ItemNavRows = {
  ord: 1, item: "gnis:1", name: "Test River", kind: "stream", tidal: [],
  parts: [{ partIx: 0, label: "Estuary", lo: "mouth", hi: "est", grp: 1, run: 0 }, piece(1, "est", "bridge"), piece(2, "bridge", "canyon")],
  geo: [{ partIx: 0, box: [0, 0, 1, 1], mid: [0.5, 0.5] }, { partIx: 1, box: [1, 0, 2, 1], mid: [1.5, 0.5] },
        { partIx: 2, box: [2, 0, 3, 1], mid: [2.5, 0.5] }],
  marks: [{ lon: 1, lat: 0.5, token: "est", down: 0, up: [1] }, { lon: 2, lat: 0.5, token: "bridge", down: 1, up: [2] }],
  sections: [{ sid: 10, partIx: 0 }, { sid: 11, partIx: 1 }, { sid: 12, partIx: 2 }],
  exit: null,
};

let reports: unknown[] = [];
let off: () => void = () => {};
afterEach(() => { off(); reports = []; });

describe("(T517) a frame on a later piece of a joined entry opens that entry", () => {
  it("parts 1 and 2 read alike are one entry: the frame on part 2 opens it, with nothing reported", () => {
    off = setFailureSink((e) => { reports.push(e); });
    const g = mergeNav(waterGroup([RIVER], ENDS), () => "alike");
    expect(g.parts.map((p) => p.pieces)).toEqual([[0], [1, 2]]);
    expect([waterMapStartIx(g, "gnis:1", 1), waterMapStartIx(g, "gnis:1", 2), waterMapStartIx(g, "gnis:1", 0)]).toEqual([1, 1, 0]);
    const vm = waterMapView(g, waterMapStart(waterMapStartIx(g, "gnis:1", 2)))!;
    expect([vm.where, vm.current.name, vm.selected]).toEqual(["Part 2 of 2 · Test River", RUN, [11, 12]]);
    expect(reports).toEqual([]);
  });

  it("a part the group does not hold THROWS (useWaterMap: the map failed, shown unavailable and reported) — never the mouth", () => {
    const g = mergeNav(waterGroup([RIVER], ENDS), () => "alike");
    expect(() => waterMapStartIx(g, "gnis:1", 7)).toThrow("water map: part 7 of gnis:1 is not in its group");
  });

  it("no part on the frame (a water opened whole, its part not read yet): the mouth — the documented rule", () => {
    expect(waterMapStartIx(mergeNav(waterGroup([RIVER], ENDS), null), "gnis:1", null)).toBe(0);
  });

  it("a state off the group's parts is never clamped onto one: the view throws", () => {
    const g = waterGroup([RIVER], ENDS);
    expect(() => waterMapView(g, waterMapStart(3))).toThrow("water map: part 3 of a group of 3");
  });
});
