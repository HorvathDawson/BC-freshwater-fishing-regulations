/**
 * (T299) NO BASEMAP NAME OUTSIDE BRITISH COLUMBIA, WHATEVER BASEMAP A PHONE HOLDS (owner, app 0.3.0: "Seattle" on the
 * map). Every basemap symbol layer of the composed style — the web's and the phone's — asks `within` BC's outline, and
 * the outline keeps the border towns while leaving out the neighbours' cities.
 */
import { describe, expect, it } from "vitest";
import { BC_OUTLINE } from "./bc-outline.generated";
import { runtimeStyle } from "./runtime-style";
import { nativeStyle } from "./native-style";

const AT = { atlas: "https://example.invalid/atlas.pmtiles", basemap: "https://example.invalid/basemap.pmtiles",
             outside: "https://example.invalid/outside.geojson", gauges: '{"type":"FeatureCollection","features":[]}' };

const ring = BC_OUTLINE.coordinates[0] as unknown as [number, number][];
const inside = ([x, y]: [number, number]) => {
  let c = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]!, [xj, yj] = ring[j]!;
    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) c = !c;
  }
  return c;
};

type Layer = { id: string; type: string; source?: string; filter?: unknown };
const confined = (l: Layer) => Array.isArray(l.filter) && JSON.stringify(l.filter).includes('["within",{"type":"Polygon"');

describe("the basemap names nothing outside BC (T299)", () => {
  it("the outline holds BC's border towns and none of its neighbours' cities", () => {
    const bc = { Vancouver: [-123.114, 49.261], Victoria: [-123.365, 48.428], "White Rock": [-122.80, 49.025],
                 Osoyoos: [-119.468, 49.032], "Prince Rupert": [-130.32, 54.315], "Fort Nelson": [-122.70, 58.806],
                 Atlin: [-133.69, 59.578], Fernie: [-115.06, 49.504] } as const;
    const not = { Seattle: [-122.33, 47.604], Bellingham: [-122.48, 48.75], Spokane: [-117.43, 47.66],
                  Calgary: [-114.07, 51.045], Juneau: [-134.42, 58.30], Whitehorse: [-135.06, 60.72],
                  Oroville: [-119.43, 48.94], Hyder: [-130.02, 55.917] } as const;
    for (const [n, p] of Object.entries(bc)) expect(inside(p as [number, number]), n).toBe(true);
    for (const [n, p] of Object.entries(not)) expect(inside(p as [number, number]), n).toBe(false);
  });

  for (const theme of ["light", "dark"]) {
    it(`every basemap symbol layer is confined, web and phone (${theme})`, () => {
      const web = (runtimeStyle(AT, theme).layers as Layer[]).filter((l) => l.source === "basemap" && l.type === "symbol");
      expect(web.length).toBeGreaterThan(5);
      expect(web.filter((l) => !confined(l)).map((l) => l.id)).toEqual([]);
      const phone = (nativeStyle({ at: AT, theme, basemap: "map", gauges: AT.gauges } as Parameters<typeof nativeStyle>[0])
        .style as { layers: Layer[] }).layers.filter((l) => l.source === "basemap" && l.type === "symbol");
      expect(phone.map((l) => l.id)).toEqual(web.map((l) => l.id));
      expect(phone.filter((l) => !confined(l)).map((l) => l.id)).toEqual([]);
      // no legacy filter rides under the expression `all` (it would parse as legacy and throw)
      for (const l of web) expect(JSON.stringify(l.filter), l.id).not.toMatch(/^\["all",\["within".*,\["(in|==|!=|!in)","/);
    });
  }
});
