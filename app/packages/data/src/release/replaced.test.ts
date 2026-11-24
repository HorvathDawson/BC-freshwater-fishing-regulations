/**
 * (T1016) WHICH SET A 404 BELONGS TO: the live pointer names it (a defect), only the page's opened manifest does (the
 * known state), or neither / not under the release root. The fake-origin walk is `@app/ui` replaced.test.ts.
 */
import { describe, expect, it } from "vitest";
import { createReplacedProbe, releasePath, replacedVerdict } from "./replaced";

const BASE = "https://data.test/release/";
const man = (id: string, tiles: string, basemap = "basemap-1") => ({
  data_release: id,
  files: { core: { url: `${id}/core-aaaa.sqlite.gz` }, regs: { url: `${id}/regs-3-bbbb.sqlite.gz` } },
  tiles: { current: { url: `tiles/${tiles}/atlas.pmtiles` }, mask: { url: `tiles/${tiles}/bc_outside.geojson` },
           assets: { url: `tiles/${tiles}/map-assets-1/`, pack: { url: `tiles/${tiles}/map-assets-1.tar.zlib` } } },
  basemap: { url: `basemap/${basemap}/${basemap}.pmtiles` },
  synopsis: { url: `${id}/synopsis-cccc.pdf` },
}) as never;

const A = man("r-a", "t.1"), C = man("r-c", "t.3", "basemap-2");
const v = (path: string, opened = A, live = C) => replacedVerdict(`${BASE}${path}`, BASE, opened, live);

describe("replacedVerdict", () => {
  it("every kind of path the opened manifest names and the live one does not is `replaced`", () => {
    expect(["r-a/regs-3-bbbb.sqlite.gz", "r-a/synopsis-cccc.pdf", "tiles/t.1/atlas.pmtiles", "tiles/t.1/bc_outside.geojson",
            "tiles/t.1/map-assets-1/sprites/light@2x.png", "tiles/t.1/map-assets-1.tar.zlib", "basemap/basemap-1/basemap-1.pmtiles"]
      .map((p) => [p, v(p)])).toEqual([
      ["r-a/regs-3-bbbb.sqlite.gz", "replaced"], ["r-a/synopsis-cccc.pdf", "replaced"], ["tiles/t.1/atlas.pmtiles", "replaced"],
      ["tiles/t.1/bc_outside.geojson", "replaced"], ["tiles/t.1/map-assets-1/sprites/light@2x.png", "replaced"],
      ["tiles/t.1/map-assets-1.tar.zlib", "replaced"], ["basemap/basemap-1/basemap-1.pmtiles", "replaced"]]);
  });

  it("a path the live manifest names is `live` (missing from what is served: a defect), whoever else names it", () => {
    expect(v("r-c/core-aaaa.sqlite.gz")).toBe("live");
    // a data-only release keeps the tile folder: its mask and sprites are still the live release's
    const dataOnly = man("r-c", "t.1");
    expect(v("tiles/t.1/bc_outside.geojson", A, dataOnly)).toBe("live");
    expect(v("tiles/t.1/map-assets-1/glyphs/0-255.pbf", A, dataOnly)).toBe("live");
    expect(v("r-a/regs-3-bbbb.sqlite.gz", A, A)).toBe("live");       // the page's own release is still the live one
  });

  it("neither names it, or it is off the release root: `not-ours`", () => {
    expect(v("r-a/never-named.bin")).toBe("not-ours");
    expect(v("tiles/t.1/map-assets-1-other/x.png")).toBe("not-ours");
    expect(replacedVerdict("https://data.test/feeds/live/index.json", BASE, A, C)).toBe("not-ours");
  });

  it("the path is read without its query or fragment", () => {
    expect(releasePath(`${BASE}r-a/core-aaaa.sqlite.gz?v=2#x`, BASE)).toBe("r-a/core-aaaa.sqlite.gz");
    expect(releasePath("https://elsewhere.test/release/r-a/core", BASE)).toBeNull();
  });
});

describe("createReplacedProbe", () => {
  it("reads the pointer fresh, once for every 404 asking at the same time; an unreadable pointer throws", async () => {
    const asked: unknown[] = [];
    let answer: Response = new Response(JSON.stringify(C));
    const probe = createReplacedProbe({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, opened: () => A,
      fetch: (async (u: RequestInfo | URL, i?: RequestInit) => { asked.push([String(u), i?.cache]); return answer; }) as typeof fetch });
    const both = await Promise.all([probe(`${BASE}r-a/core-aaaa.sqlite.gz`), probe(`${BASE}tiles/t.1/bc_outside.geojson`)]);
    expect(both).toEqual([{ verdict: "replaced", live: "r-c" }, { verdict: "replaced", live: "r-c" }]);
    expect(asked).toEqual([[`${BASE}manifest.json`, "no-store"]]);
    expect(probe.covers("https://data.test/feeds/live/x.json")).toBe(false);
    expect(await probe("https://data.test/feeds/live/x.json")).toEqual({ verdict: "not-ours", live: "r-a" });
    expect(asked).toHaveLength(1);
    answer = new Response("nope", { status: 503 });
    await expect(probe(`${BASE}r-a/core-aaaa.sqlite.gz`)).rejects.toThrow(`the release pointer could not be read: ${BASE}manifest.json: HTTP 503`);
  });
});
