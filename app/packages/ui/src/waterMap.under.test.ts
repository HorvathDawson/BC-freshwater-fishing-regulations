/**
 * (T1140, owner 2026-11-25 evening: "Drop the repeat (Recommended)") THE MAP BAR'S NAME UNDER ITS WATER'S LINE: a part
 * named "<water> <words>" (the Squamish's "Squamish River inside Baynes Island Ecological Reserve", the shipped `near:`
 * token's composition) is shown by its words under "SQUAMISH RIVER"; the full name stays the screen reader's label
 * (`current.name`) and the parts list's. A part named by its ends is shown as it is.
 */
import { describe, expect, it } from "vitest";
import { waterGroup, type ItemNavRows } from "@app/core";
import { waterMapStart, waterMapView } from "./waterMap";

const INSIDE = "near:inside:area:BAYNES ISLAND ECOLOGICAL RESERVE";
const ENDS = { [INSIDE]: ["Inside Baynes Island Ecological Reserve"], tidal: ["Tidal boundary"], source: ["Source"] };
const SQUAMISH: ItemNavRows = {
  ord: 1, item: "gnis:25671", name: "Squamish River", kind: "stream", tidal: [],
  parts: [{ partIx: 1, label: "", grp: 1, run: 0, lo: INSIDE, hi: INSIDE, side: true },
          { partIx: 2, label: "", grp: 0, run: 0, lo: "tidal", hi: "source" }],
  geo: [1, 2].map((partIx) => ({ partIx, box: [partIx, 0, partIx + 1, 1] as const, mid: [partIx + 0.5, 0.5] as const })),
  marks: [{ lon: 1, lat: 0.5, token: null, down: 1, up: [2] }],
  sections: [{ sid: 1, partIx: 1 }, { sid: 2, partIx: 2 }], exit: null,
};

describe("(T1140) the bar drops the water's name it already says", () => {
  it("the Squamish's reserve part: 'Inside Baynes Island Ecological Reserve' under 'Squamish River'; the full name kept", () => {
    const g = waterGroup([SQUAMISH], ENDS);
    const vm = waterMapView(g, waterMapStart(0))!;
    expect([vm.where, vm.shown, vm.current.name, vm.list!.rows[0]!.name]).toEqual(["Squamish River",
      "Inside Baynes Island Ecological Reserve", "Squamish River inside Baynes Island Ecological Reserve",
      "Squamish River inside Baynes Island Ecological Reserve"]);
    const up = waterMapView(g, waterMapStart(1))!;
    expect([up.shown, up.current.name]).toEqual(["Tidal boundary → Source", "Tidal boundary → Source"]);
  });
});
