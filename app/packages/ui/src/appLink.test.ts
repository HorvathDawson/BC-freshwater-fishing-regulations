import { describe, expect, it, vi } from "vitest";
import { setFailureSink } from "@app/core";
import { itemOfWaterPage, parseAppLink, resolveAppLink } from "./appLink";
import { parseStartLink, startForFrame } from "./startLink";
import { HOME, waterFrame } from "./route";

const NOW = new Date(2026, 9, 22, 9, 30);   // Thu Oct 22 2026, local
/** no day keys (today, Now) and no page keys (Keep at the top, the session's angler, not whole) */
const D0 = { day: null, past: false, horizon: null };
const P0 = { regs: { tab: "keep", sheet: null, scroll: 0 }, who: null, whole: false };
/** (T1049) the same on a water's Map or Conditions view: no page start */
const PN = { ...P0, regs: null };

const page = (item: string) => `<head><meta name="cift:item" content="${item}"><meta name="cift:region" content="2"></head>`;

describe("(T625) a shared link opened by the installed app", () => {
  it("reads a water page and the start link on our hosts, view default Regulations", () => {
    expect(parseAppLink("https://canifishthis.ca/water/vedder-river", NOW)).toEqual({ slug: "vedder-river", item: null, view: "regs", part: null, theme: null, days: D0, page: P0, problems: [] });
    expect(parseAppLink("https://staging.canifishthis.ca/water/vedder-river/?view=conditions&date=2030-07-01&horizon=3", NOW))
      .toEqual({ slug: "vedder-river", item: null, view: "conditions", part: null, theme: null,
        days: { day: new Date(2030, 6, 1, 12), past: false, horizon: 3 }, page: PN, problems: [] });
    expect(parseAppLink("https://canifishthis.ca/?regs=gnis:3062&view=map", NOW)).toEqual({ slug: null, item: "gnis:3062", view: "map", part: null, theme: null, days: D0, page: PN, problems: [] });
    expect(parseAppLink("https://canifishthis.ca/", NOW)).toEqual({ slug: null, item: null, view: "regs", part: null, theme: null, days: D0, page: P0, problems: [] });
  });

  it("is not ours: another host or scheme is null; a link of ours it cannot read throws, naming it", () => {
    expect(parseAppLink("https://example.com/water/x")).toBeNull();
    expect(parseAppLink("canifishthis://water/x")).toBeNull();
    expect(() => parseAppLink("https://canifishthis.ca/?blk=12&m=300")).toThrow(/blk, m, which the app does not open yet/);
    expect(() => parseAppLink("https://canifishthis.ca/about")).toThrow(/not a water page/);
    expect(() => parseAppLink("https://canifishthis.ca/water/x?view=bogus")).toThrow(/view=bogus/);
    expect(() => parseAppLink("https://canifishthis.ca/?regs=sid:7:ab")).toThrow(/regs=/);
    expect(() => parseAppLink("https://canifishthis.ca/water/x?regs=gnis:1")).toThrow(/twice/);
  });

  it("(T605) a region page's `/?fit=w,s,e,n` opens the map framed on that box; a bad box, or one beside a water, throws", async () => {
    expect(parseAppLink("https://canifishthis.ca/?fit=-118.82,48.999,-114.074,52.412", NOW))
      .toEqual({ slug: null, item: null, view: "regs", part: null, theme: null, days: D0, page: P0, problems: [], fit: [-118.82, 48.999, -114.074, 52.412] });
    expect(await resolveAppLink("https://staging.canifishthis.ca/?fit=-118.82,48.999,-114.074,52.412", async () => { throw new Error("no fetch"); }, NOW))
      .toEqual({ fit: [-118.82, 48.999, -114.074, 52.412], ...D0 });
    expect(() => parseAppLink("https://canifishthis.ca/?fit=-114,49,-118,52")).toThrow("app link: fit=-114,49,-118,52 is not a box with west < east and south < north");
    expect(() => parseAppLink("https://canifishthis.ca/?fit=a")).toThrow("is not a w,s,e,n box in degrees");
    expect(() => parseAppLink("https://canifishthis.ca/?regs=gnis:1&fit=-118,49,-114,52")).toThrow(/a box beside a water/);
    expect(() => parseAppLink("https://canifishthis.ca/water/x?fit=-118,49,-114,52")).toThrow(/a box beside a water/);
  });

  it("(T1034, T1035) the link's part and theme are carried to the open, never read and dropped; a bad one throws", async () => {
    const fetcher = async (_u: string) => new Response(page("gnis:3062"));
    expect(await resolveAppLink("https://canifishthis.ca/?regs=gnis:3062&view=map&part=2&theme=dark", fetcher, NOW))
      .toEqual({ item: "gnis:3062", view: "map", part: 2, theme: "dark", ...D0, ...PN });
    expect(await resolveAppLink("https://canifishthis.ca/water/vedder-river?part=0&theme=light", fetcher, NOW))
      .toEqual({ item: "gnis:3062", view: "regs", part: 0, theme: "light", ...D0, ...P0 });
  });

  it("(T1034, T1035) a bad part or theme drops THAT key, is reported, and the water still opens", async () => {
    const seen: string[] = [];
    setFailureSink((e) => seen.push(String((e as Error).message)));
    try {
      expect(await resolveAppLink("https://canifishthis.ca/?regs=gnis:3062&part=-1&theme=blue&view=map", async () => { throw new Error("no fetch"); }, NOW))
        .toEqual({ item: "gnis:3062", view: "map", part: null, theme: null, ...D0, ...PN });
      expect(seen).toEqual(["app link: part=-1 is not a part number: opened without a part", "app link: theme=blue is not light|dark: the theme is unchanged"]);
    } finally { setFailureSink(null); }
  });

  it("a water's item comes from its own page (cift:item), the same authority the web start link reads", () => {
    expect(itemOfWaterPage(page("gnis:3062"))).toBe("gnis:3062");
    expect(() => itemOfWaterPage("<head></head>")).toThrow(/names no item/);
    expect(() => itemOfWaterPage(page("garbage"))).toThrow(/names no item/);
  });

  it("resolves a slug through the page on the link's own host; ?regs= needs no fetch; the bare link opens nothing", async () => {
    const fetcher = vi.fn(async (_u: string) => new Response(page("gnis:3062")));
    expect(await resolveAppLink("https://staging.canifishthis.ca/water/vedder-river?view=conditions", fetcher, NOW))
      .toEqual({ item: "gnis:3062", view: "conditions", part: null, theme: null, ...D0, ...PN });
    expect(fetcher).toHaveBeenCalledWith("https://staging.canifishthis.ca/water/vedder-river");
    expect(await resolveAppLink("https://canifishthis.ca/?regs=gnis:5", fetcher, NOW)).toEqual({ item: "gnis:5", view: "regs", part: null, theme: null, ...D0, ...P0 });
    expect(await resolveAppLink("https://canifishthis.ca/", fetcher)).toBeNull();
    expect(await resolveAppLink("https://elsewhere.test/water/x", fetcher)).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("a page that cannot be read throws, never opening some other water", async () => {
    await expect(resolveAppLink("https://canifishthis.ca/water/gone", async () => new Response("", { status: 404 }))).rejects.toThrow(/answered 404/);
  });
});

describe("(T1038) the web start link's other keys, carried by the phone as the web reads them", () => {
  const W = "https://canifishthis.ca/?regs=gnis:3062";
  /** what the phone opens from `W&<q>`, beside what the web's start link opens from `?regs=gnis:3062&<q>` */
  const both = (q: string) => {
    const app = parseAppLink(`${W}&${q}`, NOW)!;
    const web = parseStartLink(`?regs=gnis:3062&${q}`, NOW)!;
    return { app: { ...app.days, ...app.page }, web: { day: web.day, past: web.notice === "day-past", horizon: web.horizon ?? null,
                                                       regs: web.regs, who: web.who, whole: !!waterFrame(web.route)?.whole } };
  };
  it.each([
    ["date", "date=2026-12-24", { day: new Date(2026, 11, 24, 12) }],
    ["date (gone by: today, said)", "date=2026-10-01", { past: true }],
    ["date (today: null)", "date=2026-10-22", {}],
    ["horizon", "horizon=3", { horizon: 3 }],
    ["horizon change", "horizon=change", { horizon: "change" }],
    ["tab", "tab=gear", { regs: { tab: "gear", sheet: null, scroll: 0 } }],
    ["sheet", "sheet=pos:pos", { regs: { tab: "keep", sheet: "pos:pos", scroll: 0 } }],
    ["scroll", "tab=licence&scroll=900", { regs: { tab: "licence", sheet: null, scroll: 900 } }],
    ["age", "age=u16", { who: { age: "u16", residency: "resident" } }],
    ["res", "res=non_resident", { who: { age: "adult", residency: "non_resident" } }],
    ["whole", "view=map&whole=1", { whole: true, regs: null }],
  ])("%s lands where the web lands", (_k, q, want) => {
    const { app, web } = both(q);
    expect(app).toEqual({ ...D0, ...P0, ...want });
    expect(app).toEqual(web);
    expect(parseAppLink(`${W}&${q}`, NOW)!.problems).toEqual([]);
  });

  it("a bad value throws, naming it, as the web's start link does (neither opens some other page)", () => {
    for (const [q, msg] of [["date=2026-13-01", "date=2026-13-01 is not a YYYY-MM-DD day"], ["horizon=99", "horizon=99 is not 0|"],
                            ["tab=bogus", "tab=bogus is not a tab"], ["sheet=parts", "sheet=parts is not a sheet key"],
                            ["scroll=-4", "scroll=-4 is not a scroll offset"], ["age=old", "age=old is not adult|u16"],
                            ["res=alien", "res=alien is not resident|"]] as const) {
      expect(() => parseAppLink(`${W}&${q}`, NOW)).toThrow(`app link: ${msg}`);
      expect(() => parseStartLink(`?regs=gnis:3062&${q}`, NOW)).toThrow(`start link: ${msg}`);
    }
  });

  it("a key that does nothing on its link is said, never dropped without a word; the box's link carries its day", async () => {
    const at = (u: string) => parseAppLink(u, NOW)!;
    expect(at(`${W}&whole=1`).problems).toEqual(["app link: whole=1 frames the whole water only as whole=1 on view=map with no part: dropped"]);
    expect(at(`${W}&view=map&part=1&whole=1`).page.whole).toBe(false);
    expect(at("https://canifishthis.ca/?date=2026-12-24&tab=gear").problems).toEqual([
      "app link: tab= does nothing on a link that names no water: dropped"]);
    const box = at("https://canifishthis.ca/?fit=-118,49,-114,52&date=2026-12-24&horizon=3&age=u16");
    expect([box.days, box.problems]).toEqual([{ day: new Date(2026, 11, 24, 12), past: false, horizon: 3 },
                                              ["app link: age= does nothing on a link that names no water: dropped"]]);
    const web = parseStartLink("?fit=-118,49,-114,52&date=2026-12-24&horizon=3", NOW)!;
    expect({ day: web.day, horizon: web.horizon }).toEqual({ day: box.days.day, horizon: box.days.horizon });
    expect(await resolveAppLink("https://canifishthis.ca/?fit=-118,49,-114,52&date=2026-12-24&horizon=3", async () => { throw new Error("no fetch"); }, NOW))
      .toEqual({ fit: [-118, 49, -114, 52], day: new Date(2026, 11, 24, 12), past: false, horizon: 3 });
  });

  it("a water page's link carries them through its page's item", async () => {
    expect(await resolveAppLink("https://canifishthis.ca/water/vedder-river?view=map&whole=1&date=2026-12-24&tab=gear&age=u16", async () => new Response(page("gnis:3062")), NOW))
      .toEqual({ item: "gnis:3062", view: "map", part: null, theme: null, day: new Date(2026, 11, 24, 12), past: false, horizon: null,
                 regs: null, who: { age: "u16", residency: "resident" }, whole: true });
  });
});

describe("(T1038) a link with a day and no water or box: the map on that day, as the web's browse start", () => {
  const none = async () => { throw new Error("no fetch"); };
  it.each([
    ["date", "date=2026-12-24", { day: new Date(2026, 11, 24, 12) }],
    ["horizon", "horizon=3", { horizon: 3 }],
    ["a day gone by (today, said)", "date=2026-10-01", { past: true }],
  ])("%s opens the map there, as the web does", async (_k, q, want) => {
    const got = await resolveAppLink(`https://canifishthis.ca/?${q}`, none, NOW);
    expect(got).toEqual({ home: true, ...D0, ...want });
    const web = parseStartLink(`?${q}`, NOW)!;
    expect([web.route, { day: web.day, past: web.notice === "day-past", horizon: web.horizon }])
      .toEqual([HOME, { day: (got as { day: Date | null }).day, past: (got as { past: boolean }).past, horizon: (got as { horizon: unknown }).horizon }]);
  });
  it("(T1038 review) a water's own keys on it (view, part, theme) are said, never dropped without a word; a box's link too", async () => {
    const seen: string[] = [];
    setFailureSink((e) => seen.push(String((e as Error).message)));
    try {
      expect(await resolveAppLink("https://canifishthis.ca/?date=2026-12-24&view=map&part=1&theme=dark", none, NOW))
        .toEqual({ home: true, ...D0, day: new Date(2026, 11, 24, 12) });
      expect(seen).toEqual(["view", "part", "theme"].map((k) => `app link: ${k}= does nothing on a link that names no water: dropped`));
      seen.length = 0;
      await resolveAppLink("https://canifishthis.ca/?fit=-118,49,-114,52&theme=light", none, NOW);
      expect(seen).toEqual(["app link: theme= does nothing on a link that names no water: dropped"]);
    } finally { setFailureSink(null); }
  });
  it("naming today and Now opens nothing more, as the bare link (the web's start is no link then either)", async () => {
    expect(await resolveAppLink("https://canifishthis.ca/?date=2026-10-22", none, NOW)).toBeNull();
    expect(parseStartLink("?date=2026-10-22", NOW)).toBeNull();
  });
});

describe("(T1038) startForFrame: a link's start is its own water's page's", () => {
  const S = { tab: "gear" as const, sheet: null, scroll: 0 };
  it("handed to that water only; the web's (no water named) to the first page; none is none", () => {
    expect([startForFrame(S, "gnis:1" as never, "gnis:1" as never), startForFrame(S, "gnis:1" as never, "gnis:2" as never),
            startForFrame(S, null, "gnis:2" as never), startForFrame(null, null, "gnis:2" as never)]).toEqual([S, null, S, null]);
  });
});

describe("(T1049) a phone link to a water's Map or Conditions keeps no page start, as the web's", () => {
  it.each(["map", "conditions"])("view=%s: no start; its tab, sheet and scroll are said", (view) => {
    const q = `view=${view}&tab=gear&sheet=pos:pos&scroll=40`;
    const app = parseAppLink(`https://canifishthis.ca/?regs=gnis:3062&${q}`, NOW)!;
    const web = parseStartLink(`?regs=gnis:3062&${q}`, NOW)!;
    expect([app.page.regs, web.regs]).toEqual([null, null]);
    expect(app.problems).toEqual(["tab", "sheet", "scroll"].map((k) => `app link: ${k}= does nothing on view=${view}: dropped`));
    expect(web.dropped).toEqual(app.problems.map((p) => p.replace("app link", "start link")));
  });
});
