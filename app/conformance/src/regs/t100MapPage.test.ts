/**
 * (T100) the one golden rewrite for the map's page (t100MapPage.ts): which stored pages are a map's, and the exact count of
 * golden lines it rewrote over the paired store — a store that stops adding the map page (display.py `book_pages`)
 * rewrites nothing and the pin fails.
 */
import { describe, expect, it } from "vitest";
import { parityGate } from "./golden";
import { mapPages, T100_REWRITES, T100_RULES } from "./t100MapPage";

const APPLIES = "This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Shuswap Lake. Not yet mapped: "
  + "this applies only in one part of this water — entire area north of Albas — which is not drawn on the map yet; see map B on p.28 of the regulations.";

describe("mapPages", () => {
  it("a page after the row's that the rule's applies sentence cites as a map is T100's", () => {
    expect(mapPages([[32, 34], [28, 30]], APPLIES)).toEqual([[28, 30]]);
  });
  it("mutations: the store drops the map page, the sentence cites no map, the map page is the only one → nothing", () => {
    expect(mapPages([[32, 34]], APPLIES)).toEqual([]);
    expect(mapPages([[8, 10], [22, 24]], "This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: X.")).toEqual([]);
    expect(mapPages([[28, 30]], APPLIES)).toEqual([]);
    expect(mapPages([[32, 34], [29, 31]], APPLIES)).toEqual([]);
    expect(mapPages([[32, 34], [28, 30]], "… which is not drawn on the map yet; see the maps on p.28 of the regulations.")).toEqual([[28, 30]]);
  });
});

const gate = await parityGate();
describe.skipIf(gate.skip)("t100Ruled over the paired store", () => {
  it("rewrote exactly Shuswap's map-cited lines in x4p_1010: its 5 in-part caveat rows (maps A ×2, B, C, and Salmon Arm Bay's \"the map on p.28\"), and their 5 rule sheets (pages, book line, why-it-applies line)", () => {
    expect(T100_REWRITES).toEqual({ x4p_1010: { keep: 5, sheets: 5, applies: 5 } });
  });
  it("the map-cited rules the golden's states reach, and their map pages: Shuswap's seven, p.28 — r1–r5 and the two pier lifts r5x/r11x (“see the maps on p.28”), which the golden draws no line of (no golden state reaches Canim p.42 or François p.51; Mahood's outlet rules cite no map)", () => {
    const shuswap = "r3:shuswap_lake_see_maps_on_page_28_includes_little_shuswap_lak@3-26::shuswap_lake.r";
    expect(T100_RULES).toEqual(Object.fromEntries(["1", "2", "3", "4", "5", "5x", "11x"].map((i) => [`${shuswap}${i}`, [28]])));
  });
});
