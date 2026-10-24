/**
 * (T165) NO PUBLIC ACCESS DRAWS DOTTED GREY, THROUGH THE ONE COLOURING PATHWAY (owner 2026-10-23: "maybe the stream
 * color through no access areas should be dotted grey? like under lakes but a little more visible?").
 *
 * A no-access stream section is hidden on the `stream` layer and drawn on `stream-noaccess` — its own layer, because
 * `line-dasharray` cannot vary per feature (the `stream-nobaseline` precedent) — and WHICH sections those are is the
 * colouring's own answer: the profile mode's day (@app/core `dayProfiles`, its `no_access` labels), never a second
 * test of access. Own keeps its amber on the map. Each assertion is the WHOLE expression the map is handed.
 */
import { describe, expect, it } from "vitest";
import type { DayProfiles } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { NOACCESS_LAYER, noaccessPaint, paintFor } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { resolveTheme } from "./style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: [] };
const IS_N = ["in", ["coalesce", ["feature-state", "p"], ""], ["literal", ["n"]]];

/** Does this stream opacity put a no-access section (the day's `n` label) at 0 — the gaps in the dots? */
const hiddenUnderDots = (o: unknown): boolean => JSON.stringify(o ?? null).includes(JSON.stringify(["case", IS_N, 0, 1]));

function recorder() {
  const paint: Record<string, Record<string, unknown>> = {};
  const h: MapHandle = { setVisibility: () => {}, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                         setFeatureState: () => {}, clearFeatureStates: () => {} };
  return { h, paint };
}

describe("no public access is a dotted grey line (T165)", () => {
  it("a no-access section draws on the dotted layer, and only there, on the day's labels", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme);
      const { h, paint } = recorder();
      const a = baseAdapter("web");
      a.setLayerMode(h, "stream", "profile", theme);
      a.setChannel(h, "status", DAY, theme);
      expect([theme, paint[NOACCESS_LAYER]]).toEqual([theme, {
        "line-color": t["color.status.no_access"],
        "line-width": ["interpolate", ["linear"], ["zoom"], 6, 1.2, 10, 1.6, 13, 2.2, 16, 2.8],
        "line-dasharray": t["dash.route"],
        "line-opacity": ["case", IS_N, 1, 0],
      }]);
      // the solid line is hidden under the dots (whatever opacity it had otherwise is kept for every other section)
      expect([theme, hiddenUnderDots(paint["stream"]!["line-opacity"])]).toEqual([theme, true]);
    }
  });

  it("the dots are off where the mode never says no_access, and before a day is installed", () => {
    const { h, paint } = recorder();
    const a = baseAdapter("web");
    a.setLayerMode(h, "stream", "plain", "light");
    expect(paint[NOACCESS_LAYER]!["line-opacity"]).toBe(0);
    a.setLayerMode(h, "stream", "standing", "light");
    expect(paint[NOACCESS_LAYER]!["line-opacity"]).toBe(0);
    a.setLayerMode(h, "stream", "profile", "light");   // no day yet: the plain map
    expect(paint[NOACCESS_LAYER]!["line-opacity"]).toBe(0);
    expect(JSON.stringify(paint["stream"]!["line-opacity"] ?? null)).not.toContain('"n"');
  });

  it("the categorical status mode dots the `no_access` member", () => {
    expect(noaccessPaint("status", resolveTheme("light"))["line-opacity"])
      .toEqual(["case", ["==", ["feature-state", "status"], "no_access"], 1, 0]);
  });

  it("is in the style the map is given, right after the no-baseline dash, built with no dots", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme, { stream: "profile" }) as
        unknown as { layers: { id: string; paint?: Record<string, unknown> }[] };
      const ids = s.layers.map((l) => l.id);
      expect([theme, ids[ids.indexOf("stream-nobaseline") + 1]]).toEqual([theme, NOACCESS_LAYER]);
      expect([theme, s.layers.find((l) => l.id === NOACCESS_LAYER)!.paint!["line-opacity"]]).toEqual([theme, 0]);
    }
  });

  it("the solid line is hidden exactly where the dots are, and the check fails a line left solid (mutation pin)", () => {
    const t = resolveTheme("light");
    expect(hiddenUnderDots(paintFor("stream", "profile", t, { status: DAY } as never)["line-opacity"])).toBe(true);
    // before T165 the stream's opacity said nothing of no access: a solid grey line ran under the dots, no gaps
    expect(hiddenUnderDots(paintFor("stream", "plain", t)["line-opacity"])).toBe(false);
    expect(hiddenUnderDots(["case", ["boolean", ["feature-state", "water"], false], 0.4, 1])).toBe(false);
  });

  it("own keeps its amber on the map: the day's own labels are painted `color.status.own`", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const c = paintFor("stream", "profile", resolveTheme(theme), { status: DAY } as never)["line-color"] as unknown[];
      const at = c.findIndex((x) => JSON.stringify(x) === JSON.stringify(["p2"]));
      expect([theme, c[at + 1]]).toEqual([theme, resolveTheme(theme)["color.status.own"]]);
      expect([theme, c[c.findIndex((x) => JSON.stringify(x) === JSON.stringify(["n"])) + 1]])
        .toEqual([theme, resolveTheme(theme)["color.status.no_access"]]);
    }
  });
});
