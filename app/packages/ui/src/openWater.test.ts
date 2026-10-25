/**
 * ONE WATER SCREEN, WHICHEVER MAP YOU TAPPED (owner rule 2026-10-21): the same tap from the Conditions map and from the
 * Map tab produce routes that are equal except `view` (and the tab underneath, which is where each came from).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, WaterSource, SectionId } from "@app/data";
import { openTap, openWater, waterOpener } from "./openWater";
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

  it("an unnamed reach (T53): the same frame from either map, Regulations in front from the Map tab", async () => {
    const [cond, map] = [await openTap(source, 501 as SectionId, AT, "conditions"),
                         await openTap(source, 501 as SectionId, AT, "regs")];
    expect([cond, map]).toEqual([
      { type: "openWater", item: null, tapSid: 501, at: AT, part: null, view: "conditions" },
      { type: "openWater", item: null, tapSid: 501, at: AT, part: null, view: "regs" },
    ]);
    const fromCond = reduce(condMap, cond!);
    const fromMap = reduce(mapTab, map!);
    // both open (neither map is a dead end for it), the same frame but for `view`, and back returns each to its own map
    expect([waterFrame(fromCond), waterFrame(fromMap)]).toEqual([
      { item: null, part: null, tapSid: 501, at: AT, view: "conditions" },
      { item: null, part: null, tapSid: 501, at: AT, view: "regs" },
    ]);
    expect([reduce(fromCond, { type: "back" }), reduce(fromMap, { type: "back" })]).toEqual([condMap, mapTab]);
  });

  it("whatever the tapped section resolves to, both maps open a frame and the frames differ at most in `view`",
     async () => {
    for (const sid of [77, 501] as SectionId[]) {
      const a = waterFrame(reduce(condMap, (await openTap(source, sid, AT, "conditions"))!));
      const b = waterFrame(reduce(mapTab, (await openTap(source, sid, AT, "regs"))!));
      expect(a).not.toBeNull();
      expect(b).not.toBeNull();
      expect({ ...a!, view: null }).toEqual({ ...b!, view: null });
    }
  });

  it("a tap on the water already on screen (its own map) opens nothing; another water opens on top", async () => {
    expect([await openTap(source, 77 as SectionId, AT, "regs", VEDDER),
            await openTap(source, 77 as SectionId, AT, "regs", "gnis:1" as ItemId)])
      .toEqual([null, { type: "openWater", item: VEDDER, tapSid: 77, at: AT, part: null, view: "regs" }]);
  });
});

describe("openWater: the one builder", () => {
  it("an unnamed reach has Regulations and Conditions (T53): asked for either, that is in front; asked for Map, Regulations", () => {
    const rows = (["regs", "conditions", "map", undefined] as const)
      .map((view) => (openWater({ item: null, tapSid: 501 as SectionId, at: AT, view }) as { view?: string }).view);
    expect(rows).toEqual(["regs", "conditions", "regs", "regs"]);
  });

  it("a water by id opens on its Regulations unless told; nothing tapped", () => {
    const rows: RouteAction[] = [openWater({ item: VEDDER }), openWater({ item: VEDDER, view: "conditions" })];
    expect(rows).toEqual([
      { type: "openWater", item: VEDDER, tapSid: null, at: null, part: null, view: "regs" },
      { type: "openWater", item: VEDDER, tapSid: null, at: null, part: null, view: "conditions" },
    ]);
  });
});

describe("(T58) the opener: a split lake's areas, and a late lookup dropped", () => {
  const LAKE_A = "wbk:-21" as ItemId, LAKE_B = "wbk:-22" as ItemId, TRIB = "gnis:11481" as ItemId;
  const AREAS = [LAKE_A, LAKE_B];
  /** A lookup the test answers by hand, in the order asked: section 1 is lake area B, 2 the tributary, 3 the Vedder. */
  function slow() {
    const pending: (() => void)[] = [];
    const items: Record<number, ItemId> = { 1: LAKE_B, 2: TRIB, 3: VEDDER };
    const src = { itemForSection: (s: SectionId) => new Promise<ItemId | null>((ok) => { pending.push(() => ok(items[s] ?? null)); }) };
    return { src, answer: async (i: number) => { pending[i]!(); await new Promise((r) => setTimeout(r, 0)); } };
  }
  const groupOf = async (item: ItemId) => (AREAS.includes(item) ? AREAS : [item]);
  /** The opener over a route the test moves: what it dispatched (applied to the route) and the route after. */
  function app(start: Route, src: Pick<WaterSource, "itemForSection">, held = false) {
    let route = start;
    const sent: RouteAction[] = [];
    const go = (a: RouteAction) => { sent.push(a); route = reduce(route, a); };
    // `held`: the route as the opener sees it never moves — the mutation pin's opener, blind to a back
    const o = waterOpener(src, go, { now: () => (held ? start : route), group: groupOf });
    return { o, sent, go, route: () => route };
  }
  const onTrib: Route = reduce(reduce(HOME, openWater({ item: LAKE_A, part: 0 })),
                               openWater({ item: TRIB, tapSid: 2 as SectionId }));
  const depth = (r: Route) => (r.mode === "water" ? r.stack.length : 0);

  it("in water mode a tap and a flow carry the water's areas; from browse mode nothing is asked", async () => {
    const s = slow();
    const inWater = app(onTrib, s.src);
    const t = inWater.o.tap(1 as SectionId, null, "regs", TRIB);
    await s.answer(0); await t;
    const browse = app(HOME, s.src);
    const b = browse.o.tap(1 as SectionId, null, "regs");
    await s.answer(1); await b;
    const f = app(onTrib, s.src);
    await f.o.flow(LAKE_B, (g) => openWater({ item: LAKE_B, part: 0, group: g }));
    expect({ tap: inWater.sent, browse: browse.sent, flow: f.sent,
             after: [inWater.route(), f.route()].map((r) => [depth(r), waterFrame(r)]) }).toEqual({
      tap: [{ type: "openWater", item: LAKE_B, tapSid: 1, at: null, part: null, view: "regs", group: AREAS }],
      browse: [{ type: "openWater", item: LAKE_B, tapSid: 1, at: null, part: null, view: "regs" }],
      flow: [{ type: "openWater", item: LAKE_B, tapSid: null, at: null, part: 0, view: "regs", group: AREAS }],
      // area A was held under the tributary: the lake is gone back to, not stacked again
      after: [[1, { item: LAKE_B, part: null, tapSid: 1, at: null, view: "regs", group: AREAS }],
              [1, { item: LAKE_B, part: 0, tapSid: null, at: null, view: "regs", group: AREAS }]],
    });
  });

  it("a lake whose areas cannot be read still opens (as a water alone)", async () => {
    const s = slow();
    const a = app(onTrib, s.src);
    const o = waterOpener(s.src, a.go, { now: a.route, group: async () => { throw new Error("store gone"); } });
    const t = o.tap(1 as SectionId, null, "regs", TRIB);
    await s.answer(0); await t;
    expect([a.sent, depth(a.route())]).toEqual([
      [{ type: "openWater", item: LAKE_B, tapSid: 1, at: null, part: null, view: "regs" }], 3]);
  });

  it("a lookup that finishes after back / × is dropped; of two taps the later wins", async () => {
    const s = slow();
    const backed = app(onTrib, s.src);
    const t1 = backed.o.tap(3 as SectionId, null, "regs", TRIB);
    backed.go({ type: "back" });
    await s.answer(0); await t1;
    const closed = app(onTrib, s.src);
    const t2 = closed.o.tap(3 as SectionId, null, "regs", TRIB);
    closed.go({ type: "close" });
    await s.answer(1); await t2;
    const twice = app(HOME, s.src);
    const [t3, t4] = [twice.o.tap(3 as SectionId, null, "regs"), twice.o.tap(1 as SectionId, null, "regs")];
    // the first tap's answer comes first, while the second is still asked: it is dropped, the second opens
    await s.answer(2); await s.answer(3); await Promise.all([t3, t4]);
    expect({ backed: backed.sent, closed: closed.sent, twice: twice.sent.map((a) => (a as { item: ItemId }).item) }).toEqual({
      backed: [{ type: "back" }], closed: [{ type: "close" }], twice: [LAKE_B],
    });
  });

  it("MUTATION PIN: an opener blind to the route (it never sees the back) opens the stale water after it", async () => {
    const s = slow();
    const blind = app(onTrib, s.src, true);
    const t = blind.o.tap(3 as SectionId, null, "regs", TRIB);
    blind.go({ type: "back" });
    await s.answer(0); await t;
    expect(blind.sent.map((a) => a.type)).toEqual(["back", "openWater"]);
  });
});

describe("(T203) a search result ALWAYS lands on the water's Map, the whole water shown", () => {
  it("from the Map tab and from Conditions alike: the Map view, `whole`, search consumed; a part step drops `whole`", () => {
    const sent: RouteAction[] = [];
    waterOpener(source, (a) => sent.push(a)).searched(VEDDER);
    expect(sent).toEqual([{ type: "openWater", item: VEDDER, tapSid: null, at: null, part: null, view: "map", whole: true,
                            fromSearch: true }]);
    for (const base of [mapTab, condMap]) {
      const r = reduce({ ...base, search: true } as Route, sent[0]!);
      expect([waterFrame(r)?.view, waterFrame(r)?.whole]).toEqual(["map", true]);
      // back lands on the map searched over, search closed
      expect(reduce(r, { type: "back" })).toEqual({ ...base, search: false });
      const stepped = reduce(r, { type: "part", part: 1 });
      expect([waterFrame(stepped)?.part, waterFrame(stepped)?.whole]).toEqual([1, undefined]);
    }
  });

  it("`whole` is only ever carried by a Map view", () => {
    expect(openWater({ item: VEDDER, view: "regs", whole: true })).not.toHaveProperty("whole");
  });
});
