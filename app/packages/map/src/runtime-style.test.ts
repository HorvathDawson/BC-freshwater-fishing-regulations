/**
 * P11: the map's glyphs and sprites come from the release's map assets when it names them (`releaseTiles` → `assets`:
 * the installed folder on a phone, the release url on the web), and from the Protomaps CDN only when it names none.
 */
import { describe, expect, it } from "vitest";
import { runtimeStyle } from "./runtime-style";

const AT = { atlas: "a.pmtiles", basemap: "b.pmtiles" };

describe("runtimeStyle: glyphs and sprites", () => {
  it("reads them from the release's assets folder, the sprite following the theme", () => {
    const at = { ...AT, assets: "file:///doc/release/tiles/v1/map-assets-1/" };
    const light = runtimeStyle(at, "light"), dark = runtimeStyle(at, "dark");
    expect(light.glyphs).toBe("file:///doc/release/tiles/v1/map-assets-1/fonts/{fontstack}/{range}.pbf");
    expect(light.sprite).toBe("file:///doc/release/tiles/v1/map-assets-1/sprites/v4/light");
    expect(dark.sprite).toBe("file:///doc/release/tiles/v1/map-assets-1/sprites/v4/dark");
  });

  it("falls back to the Protomaps CDN only when no assets folder is named", () => {
    const s = runtimeStyle(AT, "dark");
    expect(s.glyphs).toBe("https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf");
    expect(s.sprite).toBe("https://protomaps.github.io/basemaps-assets/sprites/v4/dark");
  });
});
