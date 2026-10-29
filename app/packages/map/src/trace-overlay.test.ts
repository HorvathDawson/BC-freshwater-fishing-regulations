/**
 * The route overlay's marks (owner 2026-10-21): upstream and downstream drawn apart, the spot
 * a different SHAPE from every gauge, and each gauge labelled with its share.
 */
import { describe, expect, it } from "vitest";
import { pinCollection, traceLayers, tracePaint, TRACE_LAYER_IDS } from "./trace-overlay";
import { runtimeStyle } from "./runtime-style";
import { hereImage, HERE_H, HERE_W } from "./here-mark";
import { SCALE } from "./raster";
import { colour } from "./chrome";
import { resolveTheme } from "./style";

const AT = { atlas: "a", basemap: "b", gauges: '{"type":"FeatureCollection","features":[]}' };

describe("the route overlay's layers", () => {
  it("draws last of all, over every river and gauge dot, in the order it declares", () => {
    const ids = (runtimeStyle(AT, "light").layers as { id: string }[]).map((l) => l.id);
    expect(ids.slice(-TRACE_LAYER_IDS.length)).toEqual([...TRACE_LAYER_IDS]);
    expect(traceLayers("light").map((l) => l.id)).toEqual([...TRACE_LAYER_IDS]);
  });

  it("draws the path as a solid line downstream and a dashed one upstream, with arrows on it (owner 2026-10-28)", () => {
    const layers = traceLayers("light");
    const ids = layers.map((l) => l.id);
    for (const gone of ["trace-area", "trace-casing", "trace-down", "trace-up"]) expect(ids).not.toContain(gone);
    const down = layers.find((l) => l.id === "trace-line-down")!, up = layers.find((l) => l.id === "trace-line-up")!;
    expect((down.paint as Record<string, unknown>)["line-dasharray"]).toBeUndefined();
    expect((up.paint as Record<string, unknown>)["line-dasharray"]).toBeTruthy();
    expect((up.paint as Record<string, unknown>)["line-color"]).toEqual(["get", "tone"]);
    // the line is under its arrows, and the picked gauge's is the heavier
    expect(ids.indexOf("trace-line-up")).toBeLessThan(ids.indexOf("trace-arrows"));
    expect(JSON.stringify((down.paint as Record<string, unknown>)["line-width"])).toContain('["get","on"]');
    const arrows = layers.find((l) => l.id === "trace-arrows")!;
    expect((arrows.paint as Record<string, unknown>)["text-color"]).toEqual(["get", "tone"]);
  });

  it("marks the spot with its own pin and every gauge with a dot, never the reverse", () => {
    const by = Object.fromEntries(traceLayers("light").map((l) => [l.id, l]));
    expect(by["trace-here"]!.filter).toEqual(["==", ["get", "kind"], "here"]);
    expect(by["trace-here"]!.type).toBe("symbol");
    expect(by["trace-gauge"]!.filter).toEqual(["==", ["get", "kind"], "gauge"]);
    expect(by["trace-gauge"]!.type).toBe("circle");
  });

  it("labels a gauge with its share on whichever side is free, heaviest first", () => {
    const label = traceLayers("light").find((l) => l.id === "trace-label")!;
    const layout = label.layout as Record<string, unknown>;
    expect(layout["text-field"]).toEqual(["get", "label"]);
    expect(layout["symbol-sort-key"]).toEqual(["get", "rank"]);
    expect(layout["text-variable-anchor-offset"]).toEqual(
      ["literal", ["left", [1.1, 0], "right", [-1.1, 0], "top", [0, 0.95], "bottom", [0, -0.95]]]);
  });

  /*
   * Review 2026-10-21: with overlap refused, a share that found no free side was DROPPED —
   * the gauge at the spot always lost its label to the pin, and zoomed out three of four
   * shares vanished. With overlap allowed and variable anchors, MapLibre tries every side
   * without overlap first (placement.ts, `placementPasses = 2`) and only then draws on the
   * first side: a free side when there is one, and never no label at all.
   */
  it("never drops a share: it takes a free side if there is one, and is drawn regardless", () => {
    const by = Object.fromEntries(traceLayers("light").map((l) => [l.id, l]));
    const layout = by["trace-label"]!.layout as Record<string, unknown>;
    expect(layout["text-allow-overlap"]).toBe(true);
    expect(layout["icon-allow-overlap"]).toBe(true);
    expect(layout["text-optional"]).not.toBe(true);
    // Variable anchors are what make the first, no-overlap pass look for a free side.
    expect(Array.isArray(layout["text-variable-anchor-offset"])).toBe(true);
    // The pill's padding is not tested in that pass (the icon allows overlap), so the text
    // box carries it: a free side is free of the whole pill, not just the digits.
    expect(layout["text-padding"]).toBeGreaterThanOrEqual(6);
    // The pin still TAKES its place, so the free-side pass moves a share off it.
    const here = by["trace-here"]!.layout as Record<string, unknown>;
    expect(here["icon-ignore-placement"]).toBe(false);
    expect(here["icon-allow-overlap"]).toBe(true);
  });

  it("takes its paper and ink from the theme, so a dark map gets a dark arrow halo", () => {
    for (const theme of ["light", "dark"]) {
      const t = resolveTheme(theme);
      expect(tracePaint(theme)["trace-arrows"]).toEqual({ "text-color": ["get", "tone"], "text-halo-color": t["color.outside"] });
      expect(tracePaint(theme)["trace-label"]).toEqual({ "text-color": t["color.ui.ink"] });
    }
    expect(tracePaint("light")["trace-arrows"]).not.toEqual(tracePaint("dark")["trace-arrows"]);
  });

  it("rings every gauge in the theme's ink, so a quiet (dark grey) gauge still shows on a dark map", () => {
    const by = Object.fromEntries(traceLayers("dark").map((l) => [l.id, l]));
    const halo = by["trace-gauge-halo"]!;
    expect(halo.type).toBe("circle");
    expect(halo.filter).toEqual(["==", ["get", "kind"], "gauge"]);
    expect(halo.paint).toEqual({ "circle-color": resolveTheme("dark")["color.ui.ink"],
                                 "circle-radius": 10.5, "circle-opacity": 0.6 });
    // Under the dot, never over it.
    const ids = traceLayers("dark").map((l) => l.id);
    expect(ids.indexOf("trace-gauge-halo")).toBe(ids.indexOf("trace-gauge") - 1);
    expect(tracePaint("light")["trace-gauge-halo"])
      .toEqual({ "circle-color": resolveTheme("light")["color.ui.ink"] });
  });
});

describe("the pins as GeoJSON", () => {
  it("carries kind, tone, rank and label, and leaves out a missing label", () => {
    expect(pinCollection([
      { lat: 49, lon: -122, kind: "here", tone: "tone-a", label: "You are here", rank: -1 },
      { lat: 49.1, lon: -121.9, kind: "gauge", tone: "tone-b", label: null, rank: 0 },
    ])).toEqual({ type: "FeatureCollection", features: [
      { type: "Feature", properties: { kind: "here", tone: "tone-a", rank: -1, label: "You are here" },
        geometry: { type: "Point", coordinates: [-122, 49] } },
      { type: "Feature", properties: { kind: "gauge", tone: "tone-b", rank: 0 },
        geometry: { type: "Point", coordinates: [-121.9, 49.1] } },
    ] });
  });
});

describe("the you-are-here pin", () => {
  const img = hereImage(colour("light", "color.ui.accent"), colour("light", "color.outside"));
  const px = (x: number, y: number) => {
    const i = (y * img.width + x) * 4;
    return [...img.data.slice(i, i + 4)];
  };
  const w = HERE_W * SCALE, h = HERE_H * SCALE;

  it("is a pin, not a dot: solid at the point, empty beside it", () => {
    expect([img.width, img.height]).toEqual([w, h]);
    expect(px(w / 2, h - 3)[3]).toBeGreaterThan(0);             // the point, near the bottom
    expect(px(2, h - 3)[3]).toBe(0);                            // nothing in the corner beside it
  });

  it("is the accent with a hollow, paper centre", () => {
    const hex = (c: number[]) => `#${c.slice(0, 3).map((v) => v.toString(16).padStart(2, "0")).join("")}`;
    const head = Math.round(w / 2 - 1 + 1);                     // the head's centre row
    expect(hex(px(w / 2, head)).toLowerCase())
      .toBe(colour("light", "color.outside").toLowerCase());
    expect(hex(px(w / 2 - Math.round(w * 0.3), head)).toLowerCase())
      .toBe(colour("light", "color.ui.accent").toLowerCase());
  });
});
