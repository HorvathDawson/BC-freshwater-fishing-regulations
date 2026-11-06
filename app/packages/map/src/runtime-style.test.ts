/**
 * P11: the map's glyphs and sprites come from the release's map assets when it names them (`releaseTiles` → `assets`:
 * the installed folder on a phone, the release url on the web), and from our own data host only when it names none.
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

  it("falls back to OUR data host (never a third-party CDN) only when no assets folder is named", () => {
    const s = runtimeStyle(AT, "dark");
    expect(s.glyphs).toBe("https://data-staging.canifishthis.ca/map-assets/v1/fonts/{fontstack}/{range}.pbf");
    expect(s.sprite).toBe("https://data-staging.canifishthis.ca/map-assets/v1/sprites/v4/dark");
  });
});

/*
 * T83: the temperature dots and their legend are painted from ONE table (@app/core `TEMP_BAND_TOKEN`), and the warm band
 * wears its own hue — it was `color.status.restricted`, so a warm river's dot read as a permit area.
 */
describe("the temperature dots", () => {
  it("paint each band with its own token, the warm one no status's, in every theme", async () => {
    const { gaugeDotColour } = await import("./gaugeDots");
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
    const { gaugeDotColour } = await import("./gaugeDots");
    const e = gaugeDotColour("light", "standing") as unknown[];
    const stops = e.slice(3).filter((_, i) => i % 2 === 0) as number[];
    expect([e.slice(0, 3), stops.some((s) => s < 100), stops.some((s) => s >= 200)])
      .toEqual([["interpolate", ["linear"], ["coalesce", ["get", "ramp"], -1]], true, true]);
  });
});

/* (T420) the ground's fills draw no antialiased outline (a second pass over the most detailed polygons zoomed out);
 * ours keep theirs */
describe("the basemap's fills (T420)", () => {
  it("draw without the antialias outline pass; our own fills keep the default", () => {
    const layers = runtimeStyle(AT, "light").layers as { id: string; type?: string; source?: string; paint?: Record<string, unknown> }[];
    const ground = layers.filter((l) => l.type === "fill" && l.source === "basemap");
    expect(ground.length).toBeGreaterThan(5);
    for (const l of ground) expect([l.id, l.paint?.["fill-antialias"]]).toEqual([l.id, false]);
    const ours = layers.filter((l) => l.type === "fill" && l.source === "atlas");
    expect(ours.length).toBeGreaterThan(5);
    for (const l of ours) expect([l.id, l.paint?.["fill-antialias"]]).toEqual([l.id, undefined]);
  });
});
