/**
 * MAP PARITY. The failure this prevents: a layer, colour or highlight is tweaked for
 * one platform and the two apps quietly render different maps. Nobody notices, because
 * nobody opens both at once.
 */
import { describe, expect, it } from "vitest";
import { nativeAdapter } from "./adapters/native";
import { webAdapter } from "./adapters/web";
import type { MapHandle } from "./adapters/contract";
import {
  MAP_STYLE, STYLE_META, colorExpression, isRuntimeLayer, layerIds, resolveTheme,
  toggleableGroups,
} from "./style";
import { runtimeStyle } from "./runtime-style";

/** Records every call, so two adapters can be compared on what they DO. */
function recorder() {
  const calls: string[] = [];
  const h: MapHandle = {
    setVisibility: (l, v) => calls.push(`vis ${l} ${v}`),
    setPaint: (l, p, v) => calls.push(`paint ${l} ${p} ${v}`),
    setFeatureState: (l, f, s) => calls.push(`state ${l} ${f} ${JSON.stringify(s)}`),
    clearFeatureStates: (l) => calls.push(`clear ${l}`),
  };
  return { h, calls };
}

describe("map parity", () => {
  const native = nativeAdapter();
  const web = webAdapter();

  it("both serve the identical style object", () => {
    expect(native.style()).toBe(web.style());
    expect(native.style()).toBe(MAP_STYLE);
  });

  it("both expose identical layer ids in the same order", () => {
    expect(native.layerIds()).toEqual(web.layerIds());
    expect(native.layerIds()).toEqual(layerIds());
  });

  it("layer ids are unique — a duplicate silently shadows on one renderer", () => {
    expect(new Set(layerIds()).size).toBe(layerIds().length);
  });

  it("a group toggle issues the identical calls on both", () => {
    const g = toggleableGroups()[0];
    if (!g) return;
    const a = recorder(), b = recorder();
    native.setGroupVisible(a.h, g.id, true);
    web.setGroupVisible(b.h, g.id, true);
    expect(a.calls).toEqual(b.calls);
    expect(a.calls.length).toBeGreaterThan(0);
  });

  it("the basemap switch issues the identical calls on both, and only on imagery", () => {
    for (const kind of ["satellite", "map"] as const) {
      const a = recorder(), b = recorder();
      native.setBasemap(a.h, kind);
      web.setBasemap(b.h, kind);
      expect(a.calls).toEqual(b.calls);
      expect(a.calls.length).toBeGreaterThan(0);
      for (const c of a.calls)
        expect(c, "setBasemap touched a layer outside the imagery group")
          .toBe(`vis imagery ${kind === "satellite"}`);
    }
  });

  it("every view x every theme produces the identical paint calls on both", () => {
    for (const view of STYLE_META.views)
      for (const theme of Object.keys(STYLE_META.themes)) {
        const a = recorder(), b = recorder();
        native.applyView(a.h, view.id, theme);
        web.applyView(b.h, view.id, theme);
        expect(a.calls, `view=${view.id} theme=${theme}`).toEqual(b.calls);
        expect(a.calls.length).toBeGreaterThan(0);
      }
  });

  it("a user palette override changes colour identically on both", () => {
    const a = recorder(), b = recorder();
    const o = { "color.status.closed": "#123456" };
    // The closed-area hatch (national parks, no-access land) draws in this token.
    native.applyView(a.h, "plain", "light", o);
    web.applyView(b.h, "plain", "light", o);
    expect(a.calls).toEqual(b.calls);
    expect(a.calls.join(" ")).toContain("#123456");
  });

  it("feature data pushes identically on both", () => {
    const a = recorder(), b = recorder();
    const v = { "380887781:11988": { standing: 42 } };
    native.setData(a.h, "stream", v);
    web.setData(b.h, "stream", v);
    expect(a.calls).toEqual(b.calls);
  });

  it("a highlight issues the identical feature-state calls on both", () => {
    const a = recorder(), b = recorder();
    native.highlight(a.h, ["356355331:50921", "356355331:67438"]);
    web.highlight(b.h, ["356355331:50921", "356355331:67438"]);
    expect(a.calls).toEqual(b.calls);
    expect(a.calls.length).toBeGreaterThan(0);
  });

  it("adapters differ ONLY in platform", () => {
    const { platform: _n, ...n } = native;
    const { platform: _w, ...w } = web;
    expect(Object.keys(n).sort()).toEqual(Object.keys(w).sort());
  });
});

describe("theme + toggle rules", () => {
  it("every theme supplies every themeable token", () => {
    const themeable = Object.entries(STYLE_META.tokens)
      .filter(([, d]) => d.themeable).map(([k]) => k);
    for (const [name, values] of Object.entries(STYLE_META.themes))
      for (const t of themeable)
        expect(values[t], `theme "${name}" missing "${t}"`).toBeDefined();
  });

  it("a user colour override must name a themeable token", () => {
    expect(() => resolveTheme("light", { "width.stream.base": 9 })).toThrow(/not themeable/);
    expect(() => resolveTheme("light", { "color.nope": "#fff" })).toThrow(/unknown token/);
  });

  it("a non-toggleable group cannot be hidden by an app", () => {
    const fixed = STYLE_META.groups.find((g) => !g.toggleable);
    if (!fixed) return;
    expect(() => nativeAdapter().setGroupVisible(recorder().h, fixed.id, false))
      .toThrow(/not toggleable/);
  });

  it("every colour mode a view names actually exists", () => {
    // Runtime layers excepted: the gauge dots are drawn from a live feed, so they have no
    // colour mode in the generated catalog — but a view still says whether they are about
    // flow or about water temperature, and that belongs beside every other view decision.
    for (const v of STYLE_META.views)
      for (const [layerId, mode] of Object.entries(v.modes)) {
        if (isRuntimeLayer(layerId)) continue;
        expect(STYLE_META.colorModes[layerId]?.[mode], `${v.id}/${layerId}`).toBeDefined();
      }
  });

  it("a runtime layer is declared, not just tolerated", () => {
    // The escape hatch above is only safe while the list is explicit. Without this, a
    // typo'd layer id in a view would be silently skipped instead of failing.
    expect(STYLE_META.runtimeLayers).toContain("gauges");
    for (const id of STYLE_META.runtimeLayers)
      expect(MAP_STYLE.layers.some((l) => l.id === id),
             `"${id}" is declared as a runtime layer but the style also defines it`).toBe(false);
  });

  it("a categorical mode over an enum colours EVERY member", () => {
    for (const [layerId, modes] of Object.entries(STYLE_META.colorModes))
      for (const [name, m] of Object.entries(modes))
        if (m.scale === "categorical" && m.enum)
          for (const member of STYLE_META.enums[m.enum] ?? [])
            expect(m.categories[member], `${layerId}/${name} missing "${member}"`).toBeDefined();
  });

  it("a continuous mode always defines a colour for missing data", () => {
    for (const modes of Object.values(STYLE_META.colorModes))
      for (const m of Object.values(modes))
        if (m.scale === "continuous") expect(m.missing).toBeDefined();
  });

  it("colour expressions read feature-state, so a view switch refetches nothing", () => {
    const t = resolveTheme("light");
    const expr = JSON.stringify(colorExpression("stream", "standing", t));
    expect(expr).toContain("feature-state");
    expect(expr).toContain("standing");
  });
});

describe("the flow ramp's units", () => {
  it("is a PERCENTAGE scale, so callers must not feed it 0-1", () => {
    // The bug this pins: percentiles are 0-1 everywhere in the app (`@app/core`'s
    // `standing()`, the feed, a saved spot) but this ramp stops at 0/50/100. Feeding 0.05
    // put every river on the bottom one percent of the scale — one flat colour, which
    // looks exactly like data that never arrived rather than like a unit mismatch.
    const mode = STYLE_META.colorModes.stream!.standing! as
      { scale: string; stops: [number, unknown][] };
    expect(mode.scale).toBe("continuous");
    const ats = mode.stops.map(([at]) => at).filter((a) => a >= 0);
    expect(Math.max(...ats)).toBe(100);
    expect(Math.min(...ats)).toBe(0);
  });

  it("reserves a stop below the scale for 'gauged, but no history'", () => {
    // A working gauge with no record to compare today against. It must not paint
    // identically to water nobody measures — that is the wrong claim in the other
    // direction. -1 is a sentinel, never a percentile: real values are 0..100, so nothing
    // interpolates across the gap.
    const mode = STYLE_META.colorModes.stream!.standing! as
      { stops: [number, { token: string }][] };
    const sentinel = mode.stops.filter(([at]) => at < 0);
    expect(sentinel).toHaveLength(1);
  });

  it("says 'no baseline' with a dash rather than with a hue", () => {
    /*
     * IT USED TO BE A PURPLE ON THE FLOW RAMP, and that is the one thing a sequential scale
     * must not carry: every other colour on it means a POSITION between low and high, and
     * this one means the axis does not exist here. A reader with any form of colour
     * blindness could not tell it from a value; a reader without had to learn a hue that
     * appears nowhere else.
     *
     * The stop is now the neutral ungauged grey and the distinction is a DASH —
     * `stream-nobaseline` in runtime-style.ts, its own layer because `line-dasharray`
     * cannot be varied per feature.
     */
    const mode = STYLE_META.colorModes.stream!.standing! as
      { stops: [number, { token: string }][] };
    const sentinel = mode.stops.find(([at]) => at < 0)!;
    expect(sentinel[1].token).toBe("color.water.ungauged");

    const dashed = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never,
                                "light", { stream: "standing" })
      .layers.find((l: { id: string }) => l.id === "stream-nobaseline") as
        { paint: Record<string, unknown> } | undefined;
    expect(dashed).toBeDefined();
    expect(dashed!.paint["line-dasharray"]).toBeDefined();
    // Shown only where the feature-state says -1, and invisible everywhere else.
    expect(JSON.stringify(dashed!.paint["line-opacity"]))
      .toContain('["feature-state","standing"]');
  });

  it("reads the value from feature-state, not from the tile", () => {
    // It changes every half hour; the tiles change once a build.
    const expr = JSON.stringify(colorExpression("stream", "standing", resolveTheme("light")));
    expect(expr).toContain('["feature-state","standing"]');
    expect(expr).not.toContain('["get","standing"]');
  });

  it("paints a reach with no reading as PALE water — visible, and quiet", () => {
    // 97.6% of BC has no gauge, so this is the colour most of the map wears. It used to be
    // `water.mapped`, a saturated teal DARKER than the low-flow end of the ramp, which
    // made ungauged water read as more prominent than measured water. It must stay
    // visible and stay quiet — and it must never be mistaken for "low for the date".
    const t = resolveTheme("light");
    const expr = colorExpression("stream", "standing", t) as unknown[];
    expect(expr[0]).toBe("case");
    // `water.ungauged`, NOT `water.unmapped`. They were one grey and they are two claims:
    // unmapped is about the atlas ("we hold no record of this water"), ungauged is about
    // measurement ("nothing is entitled to speak for this water"), and most of the province
    // is the second while very little is the first.
    expect(expr[2]).toBe(t["color.water.ungauged"]);
    expect(expr[2]).not.toBe(t["color.flow.f1"]);
    expect(expr[2]).not.toBe(t["color.water.unmapped"]);
  });

  it("a view's hidden layers are real layers, and not user toggles", () => {
    /*
     * WHY THIS EXISTS. The Conditions screen wanted the management-unit and region
     * boundaries off, and asked for it through `groups` — `{...activeGroups, admin: false}`.
     * The `admin` group is deliberately NOT toggleable, so the adapter refused the call: the
     * boundaries kept drawing and the console filled with "layer group admin is not
     * toggleable", seven times a render. The screen reported success and hid nothing.
     *
     * So a view states which layers it does not draw, and this holds that list to real
     * layers. The second assertion is the point: if a hidden layer's group were toggleable,
     * the view would be taking a decision that belongs to the user.
     */
    // `layerGroup` is the meta's own roster of every layer and the group it belongs to.
    const ids = new Set(Object.keys(STYLE_META.layerGroup));
    const byGroup = new Map(STYLE_META.groups.map((g) => [g.id, g]));
    for (const v of STYLE_META.views)
      for (const id of v.hide ?? []) {
        expect(ids.has(id), `view "${v.id}" hides unknown layer "${id}"`).toBe(true);
        const group = byGroup.get(STYLE_META.layerGroup[id] ?? "");
        expect(group?.toggleable,
               `"${id}" is in a toggleable group — that is the user's choice, not the view's`)
          .toBe(false);
      }
  });

  it("the Conditions view is the one that hides the boundaries", () => {
    // Named, so deleting it from the style fails here rather than silently restoring lines
    // over the flow field.
    const conditions = STYLE_META.views.find((v) => v.id === "conditions");
    expect([...(conditions?.hide ?? [])].sort()).toEqual(["mu", "region"]);
  });
});

describe("a water's status paints with the map's own colours", () => {
  it("every status resolves in every theme, through the one resolver", async () => {
    const { WATER_STATUS, WATER_STATUSES } = await import("@app/core");
    const { waterStatusColour, resolveTheme, themeNames } = await import("./style");
    for (const theme of themeNames())
      for (const s of WATER_STATUSES) {
        const hex = waterStatusColour(theme, s);
        expect(hex).toMatch(/^#[0-9A-Fa-f]{6}/);
        expect(hex).toBe(resolveTheme(theme)[WATER_STATUS[s].token]);
      }
  });

  /*
   * CLOSED IS ALSO WIDER, and only where the line answers by status. The width rides inside
   * the zoom curve (MapLibre allows one, outermost), multiplies by `width.status.closed` where
   * the feature-state says closed, and the plain mode's paint never sees it — the plain map's
   * water must not move when the status colouring does.
   */
  it("a closed line is drawn wider in the status mode, and only there", async () => {
    const { paintFor } = await import("./adapters/contract");
    const { resolveTheme } = await import("./style");
    const t = resolveTheme("light");
    for (const layer of ["stream", "lake__edge"]) {
      const status = JSON.stringify(paintFor(layer, "status", t)["line-width"]);
      expect(status, layer).toContain(
        `["match",["feature-state","status"],"closed",${t["width.status.closed"]},1]`);
      const plain = paintFor(layer, "plain", t);
      expect(JSON.stringify(plain), layer).not.toContain('"status"');
    }
    expect(paintFor("stream", "plain", t)["line-color"]).toContain(t["color.water.mapped"]);
  });

  it("closed is the crimson every closure on the map wears", async () => {
    const { waterStatusColour, resolveTheme } = await import("./style");
    expect(waterStatusColour("light", "closed")).toBe(resolveTheme("light")["color.status.closed"]);
  });

  /*
   * THE MAP LINE AND THE SEARCH DOT ARE ONE VOCABULARY. The `status` colour mode is authored in
   * layers.source.json and the dot resolves @app/core WATER_STATUS through `waterStatusColour`;
   * if the two ever named different tokens a row and the line beside it would disagree.
   * `missing` is a section with no status pushed (tidal, outside B.C., not yet asked): it
   * must paint a neutral `water.*` grey, never base — that would be an answer.
   */
  it("the status colour mode paints each status with WATER_STATUS's own token", async () => {
    const { WATER_STATUS, WATER_STATUSES } = await import("@app/core");
    const { STYLE_META } = await import("./style");
    for (const layer of ["stream", "lake"]) {
      const m = STYLE_META.colorModes[layer]!["status"] as
        { scale: string; property: string; categories: Record<string, { token: string }>;
          missing: { token: string } };
      expect(m.scale).toBe("categorical");
      expect(m.property).toBe("status");
      expect(Object.keys(m.categories).sort()).toEqual([...WATER_STATUSES].sort());
      for (const s of WATER_STATUSES) expect(m.categories[s]!.token).toBe(WATER_STATUS[s].token);
      expect(m.missing.token).toBe("color.water.unmapped");
    }
  });
});
