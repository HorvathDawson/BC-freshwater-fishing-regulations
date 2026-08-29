/**
 * MAP PARITY. The failure this prevents: a layer, colour or highlight is tweaked for
 * one platform and the two apps quietly render different maps. Nobody notices, because
 * nobody opens both at once.
 */
import { describe, expect, it } from "vitest";
import { nativeAdapter } from "./adapters/native.js";
import { webAdapter } from "./adapters/web.js";
import type { MapHandle } from "./adapters/contract.js";
import {
  MAP_STYLE, STYLE_META, colorExpression, layerIds, resolveTheme, toggleableGroups,
} from "./style.js";

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
    native.applyView(a.h, "regulations", "light", o);
    web.applyView(b.h, "regulations", "light", o);
    expect(a.calls).toEqual(b.calls);
    expect(a.calls.join(" ")).toContain("#123456");
  });

  it("feature data pushes identically on both", () => {
    const a = recorder(), b = recorder();
    const v = { "380887781:11988": { status: "closed" } };
    native.setData(a.h, "streams", v);
    web.setData(b.h, "streams", v);
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
    for (const v of STYLE_META.views)
      for (const [layerId, mode] of Object.entries(v.modes))
        expect(STYLE_META.colorModes[layerId]?.[mode], `${v.id}/${layerId}`).toBeDefined();
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
    const expr = JSON.stringify(colorExpression("streams", "closure", t));
    expect(expr).toContain("feature-state");
    expect(expr).toContain("status");
  });
});
