import { describe, expect, it } from "vitest";
import { HOME, reduce } from "./route";
import { openWater } from "./openWater";
import { parseStartLink, routeSearch, slugShard, slugShardPath, SLUG_SHARDS, WATER_PATH } from "./startLink";
import type { ItemId } from "@app/data";

const NOW = new Date(2026, 9, 22, 9, 30);   // Thu Oct 22 2026, local

describe("parseStartLink (T41: the web test deep link)", () => {
  it("no `regs` → no link: the app opens on the map as always", () => {
    expect([parseStartLink("", NOW), parseStartLink("?foo=1", NOW)]).toEqual([null, null]);
  });

  it("the whole link → the water route (through openWater), the day at local noon, the regs start, who, theme", () => {
    const l = parseStartLink("?regs=gnis:3062&part=1&date=2026-10-10&tab=gear&sheet=pos:pos&age=u16&res=non_resident&theme=dark&scroll=900", NOW)!;
    expect({ ...l, day: l.day?.toString() }).toEqual({
      route: reduce(HOME, openWater({ item: "gnis:3062" as ItemId, part: 1, view: "regs" })),
      day: new Date(2026, 9, 10, 12).toString(),
      regs: { tab: "gear", sheet: "pos:pos", scroll: 900 },
      who: { age: "u16", residency: "non_resident" },
      theme: "dark",
      arrive: "gnis:3062",
    });
  });

  it("defaults: part from the store (null), today (null), Keep at the top, no sheet, the profile and theme untouched", () => {
    expect(parseStartLink("?regs=wbk:-22", NOW)).toEqual({
      route: reduce(HOME, openWater({ item: "wbk:-22" as ItemId, part: null, view: "regs" })),
      day: null, regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, theme: null, arrive: "wbk:-22",
    });
  });

  it("(T269) `show`: the map (no water open), arriving at that water; a bad id throws; the first route write drops it", () => {
    expect(parseStartLink("?show=gnis:3062", NOW)).toEqual({
      route: HOME, day: null, regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, theme: null, arrive: "gnis:3062" });
    expect(() => parseStartLink("?show=Vedder", NOW)).toThrow("start link: show=Vedder is not an item id");
    expect(routeSearch(HOME, "?show=gnis:3062&theme=dark")).toBe("?theme=dark");
  });

  it("today's date is null (Today follows midnight); age alone keeps the default residency, res alone the default age", () => {
    expect([parseStartLink("?regs=gnis:1&date=2026-10-22", NOW)!.day,
            parseStartLink("?regs=gnis:1&age=u16", NOW)!.who, parseStartLink("?regs=gnis:1&res=non_resident_alien", NOW)!.who])
      .toEqual([null, { age: "u16", residency: "resident" }, { age: "adult", residency: "non_resident_alien" }]);
  });

  it("a URL-encoded sheet key is read whole", () => {
    expect([parseStartLink("?regs=gnis:1&sheet=rule%3Azp%3Aconduct%3A%3Ar2", NOW)!.regs.sheet,
            parseStartLink("?regs=gnis:1&sheet=licsrc:u16_res", NOW)!.regs.sheet])
      .toEqual(["rule:zp:conduct::r2", "licsrc:u16_res"]);
  });

  it("a bad value throws, naming it — a typo never opens some other page silently", () => {
    const bad = ["?regs=Vedder", "?regs=gnis:1&part=-1", "?regs=gnis:1&date=2026-13-01", "?regs=gnis:1&date=2026-02-30",
                 "?regs=gnis:1&tab=cards", "?regs=gnis:1&sheet=nope", "?regs=gnis:1&age=old", "?regs=gnis:1&res=x",
                 "?regs=gnis:1&theme=blue", "?regs=gnis:1&scroll=up"];
    expect(bad.map((s) => { try { parseStartLink(s, NOW); return "no throw"; } catch (e) { return (e as Error).message; } })).toEqual([
      "start link: regs=Vedder is not an item id", "start link: part=-1 is not a part index",
      "start link: date=2026-13-01 is not a YYYY-MM-DD day", "start link: date=2026-02-30 is not a YYYY-MM-DD day",
      "start link: tab=cards is not a tab", "start link: sheet=nope is not a sheet key", "start link: age=old is not adult|u16",
      "start link: res=x is not resident|non_resident|non_resident_alien", "start link: theme=blue is not light|dark",
      "start link: scroll=up is not a scroll offset"]);
  });
});

describe("(T269) a water page's address", () => {
  it("the slug shard is a stable hash in range, the same for the build and the app; the water path is /water/<slug>", () => {
    expect([slugShard("gnis:3062"), slugShard("gnis:3062")]).toEqual([slugShard("gnis:3062"), slugShard("gnis:3062")]);
    const ids = Array.from({ length: 2000 }, (_, i) => `gnis:${i * 37}`);
    const used = new Set(ids.map(slugShard));
    expect([...used].every((n) => n >= 0 && n < SLUG_SHARDS)).toBe(true);
    expect(used.size).toBe(SLUG_SHARDS);                       // spread over every shard
    expect(slugShard("")).toBe(0x811c9dc5 % SLUG_SHARDS);      // FNV-1a's offset basis: pinned, a change re-shards every page
    expect(slugShardPath("gnis:3062")).toMatch(/^\/water-slugs\/\d+\.json$/);
    expect(["/water/vedder-river", "/water/vedder-river/", "/water/", "/region/2"].map((p) => WATER_PATH.exec(p)?.[1] ?? null))
      .toEqual(["vedder-river", "vedder-river", null, null]);
  });
});

describe("(T221) the address follows the route: routeSearch", () => {
  it("the water in front as a start link; its other keys kept; browse and an unnamed reach say no water", () => {
    const r = reduce(HOME, { type: "openWater", item: "gnis:4688" as never, part: 2, view: "map" });
    expect(routeSearch(r, "?theme=dark&tab=gear")).toBe("?theme=dark&regs=gnis:4688&part=2&view=map");
    expect(parseStartLink(routeSearch(r))!.route).toEqual(r);
    expect(routeSearch(HOME, "?regs=gnis:1&date=2026-07-01")).toBe("?date=2026-07-01");
    const reach = reduce(HOME, { type: "openWater", item: null, tapSid: 5 as never, view: "regs" });
    expect(routeSearch(reach)).toBe("");
  });
});
