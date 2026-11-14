import { describe, expect, it, vi } from "vitest";
import { HOME, reduce, waterFrame } from "./route";
import { openWater } from "./openWater";
import { holdsMapStatus, pageRegion, parseStartLink, routeSearch, slugShard, slugShardPath, SLUG_SHARDS, START_NOTICE_REACH_GONE,
         startNoticeLine, WATER_PATH } from "./startLink";
import type { ItemId } from "@app/data";

const NOW = new Date(2026, 9, 22, 9, 30);   // Thu Oct 22 2026, local

describe("parseStartLink (T41: the web test deep link)", () => {
  it("no `regs` → no link: the app opens on the map as always", () => {
    expect([parseStartLink("", NOW), parseStartLink("?foo=1", NOW)]).toEqual([null, null]);
  });

  it("the whole link → the water route (through openWater), the day at local noon, the regs start, who, theme", () => {
    const l = parseStartLink("?regs=gnis:3062&blk=356364584&m=3451&date=2026-10-30&tab=gear&sheet=pos:pos&age=u16&res=non_resident&theme=dark&scroll=900", NOW)!;
    expect({ ...l, day: l.day?.toString() }).toEqual({
      // (T369) the water whole until the place is resolved in the release (`resolveStartLink`)
      route: reduce(HOME, openWater({ item: "gnis:3062" as ItemId, part: null, view: "regs" })),
      day: new Date(2026, 9, 30, 12).toString(),
      regs: { tab: "gear", sheet: "pos:pos", scroll: 900 },
      who: { age: "u16", residency: "non_resident" },
      theme: "dark",
      arrive: "gnis:3062",
      place: { blk: 356364584, m: 3451, view: "regs" },
      notice: null, horizon: null, tap: null,
    });
  });

  it("defaults: part from the store (null), today (null), Keep at the top, no sheet, the profile and theme untouched", () => {
    expect(parseStartLink("?regs=wbk:-22", NOW)).toEqual({
      route: reduce(HOME, openWater({ item: "wbk:-22" as ItemId, part: null, view: "regs" })),
      day: null, regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, theme: null, arrive: "wbk:-22",
      place: null, notice: null, horizon: null, tap: null,
    });
  });

  it("(T369) `part=` is not a link key (no older link form is read: owner 2026-11-03); the tools' `ix=` is a part; a place alone is a reach's", () => {
    expect(parseStartLink("?regs=gnis:19986&part=2&view=map", NOW)).toEqual(parseStartLink("?regs=gnis:19986&view=map", NOW));
    const tool = parseStartLink("?regs=gnis:3137&ix=2&view=map", NOW)!;
    expect([waterFrame(tool.route), tool.notice]).toEqual([
      waterFrame(reduce(HOME, openWater({ item: "gnis:3137" as ItemId, part: 2, view: "map" }))), null]);
    const reach = parseStartLink("?blk=356364485&m=257011&view=conditions", NOW)!;
    expect([reach.route, reach.place, reach.arrive, reach.notice]).toEqual([
      HOME, { blk: 356364485, m: 257011, view: "conditions" }, null, null]);
    expect(startNoticeLine({ notice: "place-gone", route: HOME })).toBe(START_NOTICE_REACH_GONE);
  });

  it("(T269) `show`: the map (no water open), arriving at that water; a bad id throws; the first route write drops it", () => {
    expect(parseStartLink("?show=gnis:3062", NOW)).toEqual({
      route: HOME, day: null, regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, theme: null, arrive: "gnis:3062",
      place: null, notice: null, horizon: null });
    expect(() => parseStartLink("?show=Vedder", NOW)).toThrow("start link: show=Vedder is not an item id");
    expect(routeSearch(HOME, "?show=gnis:3062&theme=dark")).toBe("?theme=dark");
  });

  it("today's date is null (Today follows midnight); age alone keeps the default residency, res alone the default age", () => {
    expect([parseStartLink("?regs=gnis:1&date=2026-10-22", NOW)!.day,
            parseStartLink("?regs=gnis:1&age=u16", NOW)!.who, parseStartLink("?regs=gnis:1&res=non_resident_alien", NOW)!.who])
      .toEqual([null, { age: "u16", residency: "resident" }, { age: "adult", residency: "non_resident_alien" }]);
  });

  it("(T620 G12) a day already gone by opens as today, and the start says so; today and later are kept", () => {
    const old = parseStartLink("?regs=gnis:1&date=2026-10-10", NOW)!;
    expect([old.day, old.notice, startNoticeLine(old)]).toEqual([null, "day-past", "The day this link names has passed, so today is shown."]);
    const browse = parseStartLink("?date=2026-10-21", NOW)!;
    expect([browse.day, browse.notice]).toEqual([null, "day-past"]);
    expect(parseStartLink("?regs=gnis:1&date=2026-10-22", NOW)!.notice).toBeNull();
    expect(parseStartLink("?regs=gnis:1&date=2026-10-23", NOW)!.day).toEqual(new Date(2026, 9, 23, 12));
  });

  it("a URL-encoded sheet key is read whole", () => {
    expect([parseStartLink("?regs=gnis:1&sheet=rule%3Azp%3Aconduct%3A%3Ar2", NOW)!.regs.sheet,
            parseStartLink("?regs=gnis:1&sheet=srcs:gl%7Chook", NOW)!.regs.sheet])
      .toEqual(["rule:zp:conduct::r2", "srcs:gl|hook"]);
  });

  it("a bad value throws, naming it — a typo never opens some other page silently", () => {
    const bad = ["?regs=Vedder", "?regs=gnis:1&ix=-1", "?regs=gnis:1&blk=1", "?regs=gnis:1&blk=x&m=1", "?blk=1&m=-3", "?regs=gnis:1&date=2026-13-01", "?regs=gnis:1&date=2026-02-30",
                 "?regs=gnis:1&tab=cards", "?regs=gnis:1&sheet=nope", "?regs=gnis:1&age=old", "?regs=gnis:1&res=x",
                 "?regs=gnis:1&theme=blue", "?regs=gnis:1&scroll=up"];
    expect(bad.map((s) => { try { parseStartLink(s, NOW); return "no throw"; } catch (e) { return (e as Error).message; } })).toEqual([
      "start link: regs=Vedder is not an item id", "start link: ix=-1 is not a part index",
      "start link: blk=1 is not a place without m", "start link: blk=x is not a stream key",
      "start link: m=-3 is not a distance in metres",
      "start link: date=2026-13-01 is not a YYYY-MM-DD day", "start link: date=2026-02-30 is not a YYYY-MM-DD day",
      "start link: tab=cards is not a tab", "start link: sheet=nope is not a sheet key", "start link: age=old is not adult|u16",
      "start link: res=x is not resident|non_resident|non_resident_alien", "start link: theme=blue is not light|dark",
      "start link: scroll=up is not a scroll offset"]);
  });

  it("(T698, owner 2026-11-12 \"Remove the screen\") sheet=parts opens no Parts sheet: refused, said, never ignored", () => {
    expect(() => parseStartLink("?regs=gnis:11318&sheet=parts", NOW)).toThrow("start link: sheet=parts is not a sheet key");
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
    // (T369) the part is a PLACE (made from the release: place.ts), never its index
    expect(routeSearch(r, "?theme=dark&tab=gear&ix=1", { blk: 356364584, m: 3451 }))
      .toBe("?theme=dark&regs=gnis:4688&blk=356364584&m=3451&view=map");
    expect(routeSearch(r)).toBe("?regs=gnis:4688&view=map");
    expect(routeSearch(HOME, "?regs=gnis:1&date=2026-07-01")).toBe("?date=2026-07-01");
    const reach = reduce(HOME, { type: "openWater", item: null, tapSid: 5 as never, view: "conditions" });
    expect([routeSearch(reach), routeSearch(reach, "", { blk: 356364485, m: 257011 })])
      .toEqual(["", "?blk=356364485&m=257011&view=conditions"]);
  });
});

// (T345, owner 2026-11-03) a reload of the Coquitlam's Map view drew every river "Base rules" blue: the map's status was
// held for a Regulations page that the Map view never mounts, so its start was never applied and the hold never ended
describe("(T256, T345) the map's status waits for the regulations page only while that page is in front", () => {
  it("a start on the water's Map or Conditions view holds nothing; one on its Regulations holds until applied", () => {
    const map = parseStartLink("?regs=gnis:19986&ix=2&view=map", NOW)!;
    const cond = parseStartLink("?regs=gnis:19986&ix=2&view=conditions", NOW)!;
    const regs = parseStartLink("?regs=gnis:19986&ix=2", NOW)!;
    // the old rule (`!!regsStart && route.mode === "water"`) held all three, and only the page's start released it
    expect([map, cond, regs].map((l) => !!l.regs && l.route.mode === "water")).toEqual([true, true, true]);
    expect([holdsMapStatus(map.route, map.regs), holdsMapStatus(cond.route, cond.regs),
            holdsMapStatus(regs.route, regs.regs), holdsMapStatus(regs.route, null)]).toEqual([false, false, true, false]);
    // the reader leaves the page for the Map view before it applied its start: the hold ends there too
    expect(holdsMapStatus(reduce(regs.route, { type: "view", view: "map" }), regs.regs)).toBe(false);
    expect(holdsMapStatus(HOME, regs.regs)).toBe(false);
  });
});

describe("(T304) pageRegion: the region a water's page names (cift:region)", () => {
  it("reads every bundle region as written", () => {
    for (const r of ["1", "2", "3", "4", "5", "6", "7a", "7b", "8"]) expect(pageRegion(r)).toBe(r);
  });
  it("throws, naming what it got, on a page with none or not a region (the start reads the whole files and reports it)", () => {
    expect(() => pageRegion(null)).toThrow('start link: this water\'s page names no region (no <meta name="cift:region">)');
    expect(() => pageRegion(undefined)).toThrow("names no region");
    for (const bad of ["", "7", "7A", " 2", "9", "2,5"])
      expect(() => pageRegion(bad)).toThrow(`start link: this water's page names region "${bad}", not a region`);
  });
});

describe("(T624) a water page's address, and the slug reader both Shares use", () => {
  it("waterPageAddress drops regs and keeps the rest, as the address bar writes it", async () => {
    const { waterPageAddress } = await import("./startLink");
    expect(waterPageAddress("?regs=gnis:1&view=conditions&date=2030-07-01&horizon=3", "x-river")).toBe("/water/x-river?view=conditions&date=2030-07-01&horizon=3");
    expect(waterPageAddress("?regs=gnis:1", "x-river")).toBe("/water/x-river");
  });
  it("slugReader reads the shard from its origin once; a water not in the shard is null", async () => {
    const { slugReader, slugShardPath } = await import("./startLink");
    const fetcher = vi.fn(async (_u: string) => new Response(JSON.stringify({ "gnis:1": "x-river" })));
    const r = slugReader("https://site.test", fetcher);
    expect(await r.of("gnis:1")).toBe("x-river");
    expect(r.known("gnis:1")).toBe("x-river");
    expect(await r.of("gnis:2")).toBeNull();
    expect(fetcher.mock.calls[0]![0]).toBe(`https://site.test${slugShardPath("gnis:1")}`);
  });
  it("(T716) a site that serves its page for every path (200, text/html) has no shards: null, nothing reported; a body that claims JSON and is not one is reported", async () => {
    const { slugReader } = await import("./startLink");
    const { setFailureSink } = await import("@app/core");
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const html = slugReader("https://old.test", async () => new Response("<!doctype html>", { headers: { "content-type": "text/html; charset=utf-8" } }));
      expect([await html.of("gnis:1"), got.length]).toEqual([null, 0]);
      const broken = slugReader("https://bad.test", async () => new Response("<!doctype html>", { headers: { "content-type": "application/json" } }));
      expect([await broken.of("gnis:1"), got.length]).toEqual([null, 1]);
    } finally { undo(); }
  });
});
