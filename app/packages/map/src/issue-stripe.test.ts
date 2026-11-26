/**
 * (T759, owner 2026-11-15) THE ISSUE STRIPE: dashes down a stream whose water has a DFO rule the feed could not place, in
 * the Salmon view ONLY. Two runtime layers (casing, ink) over the line; shown by the engine's `issue` label (`gi`) and the
 * stream's own mode; faded in with zoom like the gear marks. Each assertion is the WHOLE expression the map is handed.
 */
import { describe, expect, it } from "vitest";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { ENGINE_KEYS, issueInput } from "./colouring";
import { nativeStyle } from "./native-style";
import { maplibreDialect } from "./native/maplibre/dialect";
import { GEAR_RAMPS, GEAR_RAMP, ISSUE_LAYERS, ISSUE_MODE, issuePaint, issueShown } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { resolveTheme } from "./style";

const SHOWN = ["==", ["coalesce", ["feature-state", "gi"], ""], "1"];
const FADE = ["interpolate", ["linear"], ["zoom"], ...GEAR_RAMPS[GEAR_RAMP].opacity.flatMap(([z, o]) => [z, ["case", SHOWN, o, 0]])];
const PARTS = ["casing", "stripe"] as const;

function recorder() {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  return { h, paint };
}
const MODES = ["plain", "profile", "salmon", "standing", "stocked", "survey", "runs"];

describe("(T759) the issue stripe's layers", () => {
  it("in the Salmon view the expression is the label's test, faded in with zoom as the gear marks are; both tones one width, the dark dash shorter than the pale one", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme);
      const ink = issuePaint("stripe", "salmon", t), casing = issuePaint("casing", "salmon", t);
      expect([theme, ink["line-opacity"], casing["line-opacity"]]).toEqual([theme, FADE, FADE]);
      expect([ink["line-color"], casing["line-color"]]).toEqual([t["color.issue.stripe"], t["color.issue.casing"]]);
      expect([ink["line-dasharray"], casing["line-dasharray"]]).toEqual([t["dash.issue.stripe"], t["dash.issue.casing"]]);
      expect([ink["line-dasharray"], casing["line-dasharray"]]).toEqual([[3, 5], [5, 3]]);
      const sum = (a: unknown) => (a as number[]).reduce((x, y) => x + y, 0);
      // same period = in phase; the ink's dash is shorter than the casing's, so pale always shows after the dark
      expect(sum(ink["line-dasharray"])).toBe(sum(casing["line-dasharray"]));
      expect((ink["line-dasharray"] as number[])[0]!).toBeLessThan((casing["line-dasharray"] as number[])[0]!);
      // one width share on both layers: the same floored width, the same period
      expect(JSON.stringify(ink["line-width"])).toBe(JSON.stringify(casing["line-width"]));
      expect(JSON.stringify(ink["line-width"])).toContain("0.6");
      expect(issueShown("salmon")).toEqual(SHOWN);
    }
  });

  it("absent in every other view: opacity 0, whichever channel is labelled", () => {
    for (const mode of MODES.filter((m) => m !== ISSUE_MODE))
      for (const part of PARTS) expect([mode, part, issuePaint(part, mode, resolveTheme("light"))["line-opacity"]]).toEqual([mode, part, 0]);
    expect(issueShown("profile")).toBeNull();
  });

  it("through the adapter: painted in the Salmon mode, and taken back the moment the stream's mode is another", () => {
    const { h, paint } = recorder();
    const a = baseAdapter("web");
    a.setLayerMode(h, "stream", "salmon", "light");
    for (const part of PARTS) expect([part, paint[ISSUE_LAYERS[part]]!["line-opacity"]]).toEqual([part, FADE]);
    for (const m of ["plain", "profile", "standing"]) {
      a.setLayerMode(h, "stream", m, "light");
      for (const part of PARTS) expect([m, part, paint[ISSUE_LAYERS[part]]!["line-opacity"]]).toEqual([m, part, 0]);
    }
  });

  it("in the style the map is given: over the line and closed's stripe, casing under ink, none showing in a style built for another view", () => {
    for (const theme of ["light", "dark", "cvd"])
      for (const mode of ["salmon", "profile"]) {
        const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme, { stream: mode }) as
          unknown as { layers: { id: string; paint?: Record<string, unknown> }[] };
        const ids = s.layers.map((l) => l.id);
        expect(ids.indexOf(ISSUE_LAYERS.casing), mode).toBeGreaterThan(ids.indexOf("stream-closed"));
        expect(ids.indexOf(ISSUE_LAYERS.stripe), mode).toBe(ids.indexOf(ISSUE_LAYERS.casing) + 1);
        for (const part of PARTS)
          expect([mode, part, s.layers.find((l) => l.id === ISSUE_LAYERS[part])!.paint!["line-opacity"]])
            .toEqual([mode, part, mode === "salmon" ? FADE : 0]);
      }
  });
});

describe("(T759) the issue channel labels every section of an issue water, once, in feature-state `gi`", () => {
  it("labels the held sections '1' and no other", () => {
    const input = issueInput({ sections: new Set([5, 9]) });
    expect(input.labels!.key).toBe("gi");
    expect([5, 9, 6].map((id) => input.labels!.label(id))).toEqual(["1", "1", undefined]);
    expect(input.values()).toBeNull();
  });

  it("is an engine key, so it can only arrive through the one colouring pathway", () => {
    expect(ENGINE_KEYS).toContain("gi");
  });
});

describe("(T759) the native map draws the same stripe from the same style", () => {
  it("the native style JSON holds both layers with the web's paint, in the Salmon view only", () => {
    const AT = { atlas: "file:///d/a.pmtiles", basemap: "file:///d/b.pmtiles", outside: "file:///d/o.geojson", assets: "file:///d/m/" };
    const web = runtimeStyle(AT, "light", { stream: "salmon" }) as unknown as { layers: { id: string; paint: Record<string, unknown> }[] };
    for (const mode of ["salmon", "profile"]) {
      const n = nativeStyle({ at: AT, theme: "light", view: "plain", modes: { stream: mode }, hide: [], groups: {}, basemap: "map" as const,
                              gauges: '{"type":"FeatureCollection","features":[]}' }, maplibreDialect).style as unknown as
        { layers: { id: string; paint: Record<string, unknown> }[] };
      for (const part of PARTS) {
        const l = n.layers.find((x) => x.id === ISSUE_LAYERS[part])!;
        expect([mode, part, l.paint["line-opacity"]]).toEqual([mode, part, mode === "salmon" ? FADE : 0]);
        expect(l.paint["line-dasharray"]).toEqual(web.layers.find((x) => x.id === ISSUE_LAYERS[part])!.paint["line-dasharray"]);
      }
    }
  });
});
