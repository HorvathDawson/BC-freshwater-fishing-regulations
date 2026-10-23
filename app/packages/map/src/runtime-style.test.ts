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

/*
 * T83: the temperature dots and their legend are painted from ONE table (@app/core `TEMP_BAND_TOKEN`), and the warm band
 * wears its own hue — it was `color.status.restricted`, so a warm river's dot read as a permit area.
 */
describe("the temperature dots", () => {
  it("paint each band with its own token, the warm one no status's, in every theme", async () => {
    const { gaugeDotColour } = await import("./runtime-style");
    const { resolveTheme, tempBandColour } = await import("./style");
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme) as Record<string, string>;
      expect([theme, gaugeDotColour(theme, "temperature")]).toEqual([theme,
        ["match", ["coalesce", ["get", "temperatureBand"], "unknown"],
         "cool", t["color.status.open"], "warm", t["color.temp.warm"], "critical", t["color.status.closed"],
         t["color.water.ungauged"]]]);
      expect([theme, tempBandColour(theme, "warm")]).toEqual([theme, t["color.temp.warm"]]);
      const statuses = ["open", "restricted", "closed", "own", "base"].map((s) => t[`color.status.${s}`]);
      expect([theme, statuses.includes(t["color.temp.warm"]!), /^#[0-9A-F]{6}$/i.test(t["color.temp.warm"] ?? "")])
        .toEqual([theme, false, true]);
    }
  });
});

/*
 * T126: the flow dots read `ramp` — the value already in the standing channel's units (`toRamp` in @app/ui) — against the
 * river's own stops, so a dot under Change sits in the change domain (200-300) beside the river it is on. Mutation: read
 * `percentile` x 100 again -> the input differs and the dot paints Change on the percentile ramp.
 */
describe("the flow dots", () => {
  it("interpolate the feature's ramp value over the stream's standing stops, Change's domain included", async () => {
    const { gaugeDotColour } = await import("./runtime-style");
    const e = gaugeDotColour("light", "standing") as unknown[];
    const stops = e.slice(3).filter((_, i) => i % 2 === 0) as number[];
    expect([e.slice(0, 3), stops.some((s) => s < 100), stops.some((s) => s >= 200)])
      .toEqual([["interpolate", ["linear"], ["coalesce", ["get", "ramp"], -1]], true, true]);
  });
});
