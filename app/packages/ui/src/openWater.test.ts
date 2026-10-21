/**
 * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21): the same tap from the Conditions map and from the
 * Map tab produce routes that are equal except `view` (and the tab underneath, which is where each came from).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { openTap, openWater } from "./openWater";
import { HOME, reduce, waterFrame, type Route, type RouteAction } from "./route";

const VEDDER = "gnis:3062" as ItemId;
const AT = { lat: 49.1, lon: -121.9 };
const source = { itemForSection: async (s: SectionId) => (s === (77 as SectionId) ? VEDDER : null) };
const condMap: Route = reduce(HOME, { type: "tab", tab: "conditions" });
const mapTab: Route = HOME;

describe("openTap: a map tap, resolved in one place", () => {
  it("the same tap from either map: equal routes but for `view` and the tab each returns to", async () => {
    const fromCond = reduce(condMap, (await openTap(source, 77 as SectionId, AT, "conditions"))!);
    const fromMap = reduce(mapTab, (await openTap(source, 77 as SectionId, AT, "regs"))!);
    expect({ cond: fromCond, map: fromMap }).toEqual({
      cond: { mode: "water", from: condMap, stack: [{ item: VEDDER, part: null, tapSid: 77, at: AT, view: "conditions" }] },
      map: { mode: "water", from: mapTab, stack: [{ item: VEDDER, part: null, tapSid: 77, at: AT, view: "regs" }] },
    });
    // and nothing else differs: swap the view and the two frames are the same frame
    expect({ ...waterFrame(fromCond)!, view: "regs" }).toEqual(waterFrame(fromMap));
    // back returns each to its own map, exactly
    expect([reduce(fromCond, { type: "back" }), reduce(fromMap, { type: "back" })]).toEqual([condMap, mapTab]);
  });

  it("an unnamed reach opens on Conditions only; from the Map tab nothing opens (it has no page of rules)", async () => {
    expect([await openTap(source, 501 as SectionId, AT, "conditions"), await openTap(source, 501 as SectionId, AT, "regs")])
      .toEqual([{ type: "openWater", item: null, tapSid: 501, at: AT, part: null, view: "conditions" }, null]);
  });

  it("a tap on the water already on screen (its own map) opens nothing; another water opens on top", async () => {
    expect([await openTap(source, 77 as SectionId, AT, "regs", VEDDER),
            await openTap(source, 77 as SectionId, AT, "regs", "gnis:1" as ItemId)])
      .toEqual([null, { type: "openWater", item: VEDDER, tapSid: 77, at: AT, part: null, view: "regs" }]);
  });
});

describe("openWater: the one builder", () => {
  it("a water by id opens on its Regulations unless told; nothing tapped", () => {
    const rows: RouteAction[] = [openWater({ item: VEDDER }), openWater({ item: VEDDER, view: "conditions" })];
    expect(rows).toEqual([
      { type: "openWater", item: VEDDER, tapSid: null, at: null, part: null, view: "regs" },
      { type: "openWater", item: VEDDER, tapSid: null, at: null, part: null, view: "conditions" },
    ]);
  });
});
