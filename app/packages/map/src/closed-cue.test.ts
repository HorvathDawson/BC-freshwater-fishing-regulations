/**
 * (T377) CLOSED HAS A SECOND CUE BESIDES ITS COLOUR: a dashed stripe in the paper's colour down a closed stream, its own
 * runtime layer (`stream-closed`), shown exactly where the stream's mode and day paint the section closed — the Standard
 * status day, the Salmon day, never a second test of a rule. Each assertion is the WHOLE expression the map is handed.
 */
import { describe, expect, it } from "vitest";
import { isolateDay, type DayProfiles } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { CLOSED_CUE_LAYER, CLOSED_CUE_SHARE } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { closedCueColour, resolveTheme } from "./style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: [] };
const SALMON = { open: ["s0"], release: [], closed: ["s3"], check: [], none: [] };
const IS = (prop: string, l: string) => ["in", ["coalesce", ["feature-state", prop], ""], ["literal", [l]]];

function recorder() {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  return { h, paint };
}

describe("(T377) closed is striped as well as red", () => {
  it("Standard: the stripe shows on the day's closed labels, at a share of the stream's own width, in every theme", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme);
      const { h, paint } = recorder();
      const a = baseAdapter("web");
      a.setLayerMode(h, "stream", "profile", theme);
      a.setChannel(h, "status", DAY, theme);
      const p = paint[CLOSED_CUE_LAYER]!;
      expect([theme, p["line-color"], p["line-dasharray"], p["line-opacity"]])
        .toEqual([theme, t["color.cue.closed"], t["dash.cue.closed"], ["case", IS("p", "p1"), 1, 0]]);
      expect(p["line-color"]).toBe(closedCueColour(theme));
      // the stream's own width curve, each stop scaled
      expect(JSON.stringify(p["line-width"])).toContain(`["*",`);
      expect(JSON.stringify(p["line-width"])).toContain(String(CLOSED_CUE_SHARE));
    }
  });

  it("Salmon: on the salmon day's closed", () => {
    const { h, paint } = recorder();
    const a = baseAdapter("web");
    a.setLayerMode(h, "stream", "salmon", "light");
    a.setChannel(h, "salmon", SALMON, "light");
    expect(paint[CLOSED_CUE_LAYER]!["line-opacity"]).toEqual(["case", IS("sp", "s3"), 1, 0]);
  });

  it("off in every other mode, before a day, and where a legend tap greys closed out", () => {
    const { h, paint } = recorder();
    const a = baseAdapter("web");
    for (const m of ["plain", "standing"]) {
      a.setLayerMode(h, "stream", m, "light");
      expect([m, paint[CLOSED_CUE_LAYER]!["line-opacity"]]).toEqual([m, 0]);
    }
    a.setLayerMode(h, "stream", "profile", "light");
    expect(paint[CLOSED_CUE_LAYER]!["line-opacity"]).toBe(0);
    a.setChannel(h, "status", isolateDay(DAY, "own"), "light");
    expect(paint[CLOSED_CUE_LAYER]!["line-opacity"]).toBe(0);
  });

  it("is in the style the map is given, over the stream line, built with no stripe", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme, { stream: "profile" }) as
        unknown as { layers: { id: string; paint?: Record<string, unknown> }[] };
      const ids = s.layers.map((l) => l.id);
      expect(ids.indexOf(CLOSED_CUE_LAYER)).toBeGreaterThan(ids.indexOf("stream"));
      expect(s.layers.find((l) => l.id === CLOSED_CUE_LAYER)!.paint!["line-opacity"]).toBe(0);
    }
  });
});
