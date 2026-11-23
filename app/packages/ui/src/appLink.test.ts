import { describe, expect, it, vi } from "vitest";
import { itemOfWaterPage, parseAppLink, resolveAppLink } from "./appLink";

const page = (item: string) => `<head><meta name="cift:item" content="${item}"><meta name="cift:region" content="2"></head>`;

describe("(T625) a shared link opened by the installed app", () => {
  it("reads a water page and the start link on our hosts, view default Regulations", () => {
    expect(parseAppLink("https://canifishthis.ca/water/vedder-river")).toEqual({ slug: "vedder-river", item: null, view: "regs", part: null, theme: null });
    expect(parseAppLink("https://staging.canifishthis.ca/water/vedder-river/?view=conditions&date=2030-07-01&horizon=3"))
      .toEqual({ slug: "vedder-river", item: null, view: "conditions", part: null, theme: null });
    expect(parseAppLink("https://canifishthis.ca/?regs=gnis:3062&view=map")).toEqual({ slug: null, item: "gnis:3062", view: "map", part: null, theme: null });
    expect(parseAppLink("https://canifishthis.ca/")).toEqual({ slug: null, item: null, view: "regs", part: null, theme: null });
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
    expect(parseAppLink("https://canifishthis.ca/?fit=-118.82,48.999,-114.074,52.412"))
      .toEqual({ slug: null, item: null, view: "regs", part: null, theme: null, fit: [-118.82, 48.999, -114.074, 52.412] });
    expect(await resolveAppLink("https://staging.canifishthis.ca/?fit=-118.82,48.999,-114.074,52.412", async () => { throw new Error("no fetch"); }))
      .toEqual({ fit: [-118.82, 48.999, -114.074, 52.412] });
    expect(() => parseAppLink("https://canifishthis.ca/?fit=-114,49,-118,52")).toThrow("app link: fit=-114,49,-118,52 is not a box with west < east and south < north");
    expect(() => parseAppLink("https://canifishthis.ca/?fit=a")).toThrow("is not a w,s,e,n box in degrees");
    expect(() => parseAppLink("https://canifishthis.ca/?regs=gnis:1&fit=-118,49,-114,52")).toThrow(/a box beside a water/);
    expect(() => parseAppLink("https://canifishthis.ca/water/x?fit=-118,49,-114,52")).toThrow(/a box beside a water/);
  });

  it("(T1034, T1035) the link's part and theme are carried to the open, never read and dropped; a bad one throws", async () => {
    const fetcher = async (_u: string) => new Response(page("gnis:3062"));
    expect(await resolveAppLink("https://canifishthis.ca/?regs=gnis:3062&view=map&part=2&theme=dark", fetcher))
      .toEqual({ item: "gnis:3062", view: "map", part: 2, theme: "dark" });
    expect(await resolveAppLink("https://canifishthis.ca/water/vedder-river?part=0&theme=light", fetcher))
      .toEqual({ item: "gnis:3062", view: "regs", part: 0, theme: "light" });
    expect(() => parseAppLink("https://canifishthis.ca/?regs=gnis:1&theme=blue")).toThrow(/theme=blue/);
    expect(() => parseAppLink("https://canifishthis.ca/?regs=gnis:1&part=-1")).toThrow(/part=-1/);
  });

  it("a water's item comes from its own page (cift:item), the same authority the web start link reads", () => {
    expect(itemOfWaterPage(page("gnis:3062"))).toBe("gnis:3062");
    expect(() => itemOfWaterPage("<head></head>")).toThrow(/names no item/);
    expect(() => itemOfWaterPage(page("garbage"))).toThrow(/names no item/);
  });

  it("resolves a slug through the page on the link's own host; ?regs= needs no fetch; the bare link opens nothing", async () => {
    const fetcher = vi.fn(async (_u: string) => new Response(page("gnis:3062")));
    expect(await resolveAppLink("https://staging.canifishthis.ca/water/vedder-river?view=conditions", fetcher))
      .toEqual({ item: "gnis:3062", view: "conditions", part: null, theme: null });
    expect(fetcher).toHaveBeenCalledWith("https://staging.canifishthis.ca/water/vedder-river");
    expect(await resolveAppLink("https://canifishthis.ca/?regs=gnis:5", fetcher)).toEqual({ item: "gnis:5", view: "regs", part: null, theme: null });
    expect(await resolveAppLink("https://canifishthis.ca/", fetcher)).toBeNull();
    expect(await resolveAppLink("https://elsewhere.test/water/x", fetcher)).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("a page that cannot be read throws, never opening some other water", async () => {
    await expect(resolveAppLink("https://canifishthis.ca/water/gone", async () => new Response("", { status: 404 }))).rejects.toThrow(/answered 404/);
  });
});
