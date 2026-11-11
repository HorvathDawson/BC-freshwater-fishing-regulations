/**
 * (T369) The one reading of a frame (`frameView.ts`), the address round trip of everything a view is defined by
 * (`routeSearch` → `parseStartLink`: browse tab, open spot, day, horizon, the tap), and a release applied with a water
 * open (`carry.ts`).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture, openRegsFixtureWithLines, regsFixtureLines, sectionNames } from "@app/data/release/node";
import { frameBasinAsk } from "./basin";
import { carryRoute, entryAt } from "./carry";
import { frameHere, frameSection, type FrameNav } from "./frameView";
import { openWater } from "./openWater";
import { HOME, reduce, type Route, type WaterRoute } from "./route";
import { parseStartLink, routeSearch } from "./startLink";

const NAV: FrameNav = {
  sections: [{ sid: 1, partIx: 0 }, { sid: 2, partIx: 0 }, { sid: 3, partIx: 1 }, { sid: 4, partIx: 2 }, { sid: 5, partIx: 2 }],
  geo: [{ partIx: 0, midSid: 2, mid: [-122, 49], box: [-122.1, 48.9, -121.9, 49.1] },
        { partIx: 1, midSid: 3, mid: [-121.05, 49.95], box: [-121.1, 49.9, -121, 50] },
        { partIx: 2, midSid: 5, mid: [-120.95, 50.05], box: [-121, 50, -120.9, 50.1] }],
};
const W = "gnis:1" as ItemId;
const f = (o: Partial<{ part: number | null; tapSid: number | null; at: { lat: number; lon: number } | null }>) =>
  ({ item: W, part: null, tapSid: null, at: null, ...o }) as Parameters<typeof frameSection>[0];

describe("(T369) the one reading of a frame", () => {
  it("the reach: the tap in the part, else the part's middle — never the water's first section", () => {
    expect([
      frameSection(f({ part: 0, tapSid: 1 }), 0, NAV),            // the tap, in its part
      frameSection(f({ part: 0 }), 0, NAV),                       // the part's middle
      frameSection(f({ part: 2 }), 2, NAV),                       // another part's middle
      frameSection(f({ part: 1 }), 1, NAV),
      frameSection(f({ part: 1, tapSid: 1 }), 1, NAV),            // a tap of another part is not this part's reach
      frameSection({ item: null, part: null, tapSid: 9 as SectionId, at: null }, null, null),
    ]).toEqual([1, 2, 5, 3, 3, 9]);
  });

  it("\"you are here\": the tap's point, else the middle's own point (owner 2026-11-04: at the link's spot)", () => {
    const at = { lat: 49.05, lon: -122.05 };
    expect([frameHere(f({ part: 0, tapSid: 1, at }), 0, NAV), frameHere(f({ part: 0 }), 0, NAV),
            frameHere(f({ part: 2 }), 2, NAV), frameHere(f({ part: 0, tapSid: 1 }), 0, NAV)])
      .toEqual([at, { lon: -122, lat: 49 }, { lon: -120.95, lat: 50.05 }, null]);
  });
});

describe("(T369 D5/D10/D11) the address keeps what defines the view, and gives it back", () => {
  const NOW = new Date(2026, 9, 22, 9, 30);
  const back = (r: Route, extra: Parameters<typeof routeSearch>[3]) => parseStartLink(routeSearch(r, "", null, extra, NOW), NOW);
  it("browse: the tab, an open spot, the day when not today, the Conditions horizon — each back from its address", () => {
    const runs = reduce(HOME, { type: "tab", tab: "runs" });
    const spot = reduce(reduce(HOME, { type: "tab", tab: "spots" }), { type: "openSpot", id: "s-1" });
    const day = new Date(2030, 6, 1, 12);
    const out = [back(runs, { day: null, horizon: 0 }), back(spot, { day, horizon: 3 }),
                 back(HOME, { day: null, horizon: "change" })];
    expect(out.map((l) => l && { route: l.route, day: l.day?.toDateString() ?? null, horizon: l.horizon })).toEqual([
      { route: runs, day: null, horizon: null },
      { route: spot, day: day.toDateString(), horizon: 3 },
      { route: HOME, day: null, horizon: "change" }]);
    // today and Now are unsaid; the Map tab with nothing open is no link at all
    expect([routeSearch(HOME, "", null, { day: NOW, horizon: 0 }, NOW), back(HOME, { day: null, horizon: 0 })]).toEqual(["", null]);
  });

  it("a tapped water says it was tapped, and where (its place is the tap's); a bad point throws", () => {
    const tapped = reduce(HOME, openWater({ item: W, part: 0, tapSid: 1 as SectionId, at: { lat: 49.123456, lon: -122.5 } }));
    const noPoint = reduce(HOME, openWater({ item: W, part: 0, tapSid: 1 as SectionId }));
    expect([routeSearch(tapped, "", { blk: 7, m: 50 }), routeSearch(noPoint, "", { blk: 7, m: 50 })])
      .toEqual(["?regs=gnis:1&blk=7&m=50&here=49.12346,-122.50000", "?regs=gnis:1&blk=7&m=50&tap=1"]);
    expect(parseStartLink("?regs=gnis:1&blk=7&m=50&here=49.12346,-122.50000")!.tap).toEqual({ at: { lat: 49.12346, lon: -122.5 } });
    expect(() => parseStartLink("?regs=gnis:1&blk=7&m=50&here=north")).toThrow("start link: here=north is not a lat,lon point");
  });
});

describe("(T369 D8, owner 2026-11-04) a new release with a water open: the same place on the water", () => {
  it("the point's part: the box that holds it, the nearest middle", () => {
    expect([entryAt(NAV, { lon: -122, lat: 49 }), entryAt(NAV, { lon: -120.96, lat: 50.04 }), entryAt(NAV, { lon: -100, lat: 60 })])
      .toEqual([{ part: 0 }, { part: 2 }, { part: 2 }]);
  });

});

// (T667) its own block: the test above runs on invented geometry (NAV); this one on the regs fixture release, whose
// sections and parts are named, never numbered
describe("(T369 D8, owner 2026-11-04) a new release with a water open: carried into the same release", () => {
  it("carried into the same release, every named frame stays on its part (its tap dropped); an unnamed reach is left", async () => {
    const st = await openRegsStore(openRegsFixture());
    const names = sectionNames(openRegsFixtureWithLines());
    const C = "gnis:19986" as ItemId;
    // the Coquitlam's part from the Lougheed Highway bridge up to the watershed area, and a section on it (the Coquitlam's
    // stream key from 7028 m); the unnamed reach is the fixture's own, never a typed handle
    const upper = await names.part(C, "coquitlam_river__lougheed_highway_bridge", "area:Coquitlam Watershed [13480215]");
    const onUpper = await names.stream(356364584, 7028) as SectionId;
    const unnamed = regsFixtureLines().unnamed as SectionId;
    const r: WaterRoute = { mode: "water", from: HOME, stack: [
      { item: C, part: upper, tapSid: onUpper, at: null, view: "conditions" },
      { item: C, part: 0, tapSid: null, at: null, view: "map" },
      { item: null, part: null, tapSid: unnamed, at: null, view: "regs" }] };
    const a = await carryRoute(r, st, st);
    expect(a).toEqual({ type: "carry", stack: [
      { item: C, part: upper, tapSid: null, at: null, view: "conditions" },
      { item: C, part: 0, tapSid: null, at: null, view: "map" }] });
    expect(reduce(r, a)).toEqual({ ...r, stack: (a as { stack: unknown }).stack });
    expect(reduce(r, { type: "carry", stack: [] })).toBe(HOME);
  });
});

describe("(T584, T580 F5) one part, one watershed group whatever the way in", () => {
  // the group is asked at the frame's own point: a tap on the part's middle, a part step (which clears the tap), a search, a
  // deep link and a reload all name the same (section, here) and so the same ask
  const middle = { lon: -122, lat: 49 };
  const ask = (key: ReturnType<typeof f>, raw: { tapSid: SectionId | null; at: { lat: number; lon: number } | null }) =>
    frameBasinAsk({ item: W, ...raw }, frameHere(key, key.part, NAV));
  it("tap / step / search / deep link / reload of part 0 ask the same place", () => {
    const tap = ask(f({ part: 0, tapSid: 2, at: { lat: middle.lat, lon: middle.lon } }), { tapSid: 2 as SectionId, at: { lat: 49, lon: -122 } });
    const step = ask(f({ part: 0 }), { tapSid: null, at: null });
    const search = ask(f({ part: 0 }), { tapSid: null, at: null });
    const reload = ask(f({ part: 0 }), { tapSid: null, at: null });
    expect([tap, step, search, reload]).toEqual(Array(4).fill({ item: W, tapSid: null, at: middle }));
  });
  it("a step is not asked at the water's middle: the raw ask would be, the frame's is the part's", () => {
    const raw = { item: W, tapSid: null, at: null };
    expect(frameBasinAsk(raw, frameHere(f({ part: 2 }), 2, NAV))).toEqual({ item: W, tapSid: null, at: { lon: -120.95, lat: 50.05 } });
    expect(frameBasinAsk(raw, null)).toBe(raw);               // no point in the frame: the water's own point, as before
  });
});
