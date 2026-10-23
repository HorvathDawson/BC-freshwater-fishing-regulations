/**
 * P15: the water map's group read through the regs store (`navRows`, `siblingOrds`) on THE COMMITTED REGS FIXTURE
 * RELEASE — the Chilliwack, Vedder, Cowichan, Kootenay Lake's three closure areas and the Zymoetz, as the release's
 * `part_geo` / `part_mark` / `part_section` / `water_exit` tables hold them (the same rows `core/src/waterNav.test.ts`
 * copies). The fixture's core holds only its 43 waters, so a mouth into a water it dropped is no exit here.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { crossDown, crossUp, markLabel, navSections, partName, stepsDown, stepsUp, type WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore, RegsInputsError, siblingItems, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";

let st: RegsStore;
beforeAll(async () => { st = await openRegsStore(openRegsFixture()); });

const names = (g: WaterGroup) => g.parts.map((_, i) => partName(g, i));
const marks = (g: WaterGroup) => g.marks.map((m) => `${markLabel(g, m)}: ${m.down} → ${m.up.join(",")}`);
const steps = (g: WaterGroup) =>
  g.parts.map((_, i) => `${stepsDown(g, i).join(",") || "-"}|${stepsUp(g, i).join(",") || "-"}`);

describe("the water map's group from the fixture release", () => {
  it("Chilliwack: four parts by rank, its three boundaries, the mouth into the Vedder at its part 0", async () => {
    const g = await loadWaterGroup(st, "gnis:8634");
    expect(names(g)).toEqual([
      "Vedder Crossing Bridge → Tamihi Rapids Bridge",
      "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence",
      "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary",
      "Chilliwack River Ecological Reserve boundary → Source"]);
    expect(marks(g)).toEqual([
      "Chilliwack River Ecological Reserve boundary: 2 → 3", "Tamihi Rapids Bridge: 0 → 1",
      "fishing boundary signs 100 m below the Slesse Creek confluence: 1 → 2"]);
    expect(steps(g)).toEqual(["-|1", "0|2", "1|3", "2|-"]);
    expect(g.exit).toEqual({ item: "gnis:3062", name: "Vedder River", part: 0 });
    expect(navSections(g, 0).selected).toEqual([60210, 60211, 60212, 62453, 62571, 62823, 1235779, 1235780, 1235825]);
    expect(g.parts[0]!.box).toEqual([-121.966394, 49.071523, -121.836034, 49.097385]);
  });

  it("Vedder: one part, named by its water", async () => {
    const g = await loadWaterGroup(st, "gnis:3062");
    expect([names(g), steps(g), navSections(g, 0).selected]).toEqual([
      ["Vedder River"], ["-|-"], [62410, 313837, 313838, 313839, 1957095]]);
  });

  it("Cowichan: four parts in a line", async () => {
    const g = await loadWaterGroup(st, "gnis:12227");
    expect(marks(g)).toEqual(["CNR Mile 66 Trestle: 0 → 1", "CNR Bridge (mile 70.2): 1 → 2", "Greendale Trestle: 2 → 3"]);
    expect([steps(g), g.exit]).toEqual([["-|1", "0|2", "1|3", "2|-"], null]);
  });

  it("KLWA: opened from any closure area, the lake's three items in the prototype's order", async () => {
    for (const id of ["wbk:-20", "wbk:-21", "wbk:-22"]) {
      const g = await loadWaterGroup(st, id);
      expect(names(g)).toEqual([
        "Kootenay Lake — Lower West Arm", "Kootenay Lake — Upper West Arm", "Kootenay Lake — Main Body"]);
      expect([marks(g), steps(g)]).toEqual([
        ["Lower West Arm / Upper West Arm: 0 → 1", "Upper West Arm / Main Body: 1 → 2"], ["-|1", "0|2", "1|-"]]);
    }
  });

  it("Zymoetz: ▲ below the canyon fork offers the main stem first, then the side channel", async () => {
    const g = await loadWaterGroup(st, "gnis:3137");
    expect(names(g)).toEqual([
      "Mouth → Zymoetz Canyon lower entrance = transmission-line sign",
      "Zymoetz Canyon lower entrance = transmission-line sign → Zymoetz Canyon upper entrance",
      "Side channel", "Zymoetz Canyon upper entrance → Limonite Creek confluence",
      "A lake → McDonell Lake outlet", "Limonite Creek confluence → Source"]);
    // ▼ from the source is a fork: the main stem, then "A lake → McDonell Lake outlet" — its only way in (P15 review)
    expect(steps(g)).toEqual(["-|1", "0|3,2", "1|3", "1,2|5", "3|5", "3,4|-"]);
  });

  it("water_continue: the Vedder goes on from the Chilliwack — ▲ from the Vedder, ▼ from the Chilliwack's lowest part", async () => {
    const [ved, chw] = await Promise.all([loadWaterGroup(st, "gnis:3062"), loadWaterGroup(st, "gnis:8634")]);
    expect([ved.prev, ved.next, crossUp(ved, 0), crossDown(ved, 0)]).toEqual([
      { item: "gnis:8634", name: "Chilliwack River", part: 1, at: 0 }, null,
      { item: "gnis:8634", name: "Chilliwack River", part: 1, at: 0 }, null]);
    expect([chw.next, chw.prev, chw.parts.map((_, i) => crossDown(chw, i)?.item ?? "-")]).toEqual([
      { item: "gnis:3062", name: "Vedder River", part: 0, at: 0 }, null, ["gnis:3062", "-", "-", "-"]]);
  });

  it("no other fixture water crosses: the Cowichan, the Zymoetz and the KLWA lake", async () => {
    for (const id of ["gnis:12227", "gnis:3137", "wbk:-20"]) {
      const g = await loadWaterGroup(st, id);
      expect([id, g.next, g.prev]).toEqual([id, null, null]);
    }
  });

  it("selected is exactly the part's part_section rows, for every part of every fixture water", async () => {
    const w = await st.water();
    for (const id of ["gnis:8634", "gnis:3062", "gnis:12227", "wbk:-21", "gnis:3137"]) {
      const g = await loadWaterGroup(st, id);
      for (let i = 0; i < g.parts.length; i += 1) {
        const p = g.parts[i]!;
        const ord = await st.itemOrd(p.item);
        const rows = await w.all("SELECT sid FROM part_section WHERE ord = ? AND part_ix = ? ORDER BY sid", ord, p.partIx);
        const { selected, water } = navSections(g, i);
        expect([id, i, selected]).toEqual([id, i, rows.map((r) => Number(r.sid))]);
        expect(selected.length).toBeGreaterThan(0);
        // the rest of the water is every other part's, and never the selected part's
        expect(water.filter((s) => selected.includes(s))).toEqual([]);
        expect(selected.length + water.length).toBe(g.parts.reduce((k, q) => k + q.sids.length, 0));
      }
    }
  });

  it("(T58) siblingItems: a closure area -> its lake's three areas; a river -> itself; a water not held -> none", async () => {
    const all = await Promise.all(["wbk:-20", "wbk:-21", "wbk:-22", "gnis:8634", "gnis:0"].map((id) => siblingItems(st, id)));
    const klwa = ["wbk:-20", "wbk:-21", "wbk:-22"];
    expect(all).toEqual([klwa, klwa, klwa, ["gnis:8634"], []]);
  });

  it("a water the store does not hold is refused, by name", async () => {
    await expect(loadWaterGroup(st, "gnis:0")).rejects.toThrow(new RegsInputsError("water map: no water gnis:0 in the regs store"));
  });
});
