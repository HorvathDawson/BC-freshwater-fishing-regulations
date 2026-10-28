/**
 * MAP PARITY. The failure this prevents: a layer, colour or highlight is tweaked for
 * one platform and the two apps quietly render different maps. Nobody notices, because
 * nobody opens both at once.
 *
 * THIS USED TO COMPARE ONE OBJECT WITH ITSELF (audit F14): `nativeAdapter()` and `webAdapter()` were both
 * `baseAdapter()`, so "the two issue identical calls" could not fail. Since N2 the renderers are real
 * (`adapters/web.ts` over maplibre-gl, `adapters/native.ts` over the RN binding), and the first block below runs ONE
 * op script against both, each over a recorder shaped like its SDK — the web one answering ids as numbers, the native
 * one as Android does (strings, `""` for none, every call async) — and compares the two command logs.
 * The style half (`baseAdapter`) is platform-free by construction; it is tested once, for what it does.
 */
import { describe, expect, it } from "vitest";
import { nativeAdapter, nativeRenderer, type NativeRefs } from "./adapters/native";
import { webAdapter, webRenderer, type GlMapSlice } from "./adapters/web";
import { baseAdapter, type MapHandle, type MapRenderer } from "./adapters/contract";
import { CASING_PX, PART_HALO_PX, paintFor, SELECTION_CASING, SELECTION_RINGS } from "./paint";
import {
  MAP_STYLE, STYLE_META, colorExpression, isRuntimeLayer, layerIds, resolveTheme,
  toggleableGroups,
} from "./style";
import { runtimeStyle } from "./runtime-style";
import { colour } from "./chrome";

/** Records every call, so an adapter can be checked on what it DOES. */
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

// ---- the two renderers over recorders shaped like their SDKs ------------------------------------------------------
type Hit = { id: number | null; geometry?: unknown };
/** What the "tiles" hold: by layer (rendered queries) and by source-layer (source queries). */
const WORLD: { rendered: Record<string, Hit[]>; source: Record<string, Hit[]> } = {
  rendered: {
    stream: [{ id: null }, { id: 311274 }], lake: [{ id: 1566515 }], lake_flowing: [], wetland_flowing: [],
    wetland: [],
  },
  source: {
    stream: [{ id: 311274, geometry: { type: "LineString", coordinates: [[-122, 49], [-121.5, 49.2]] } },
             { id: 9, geometry: { type: "Point", coordinates: [-100, 40] } }],
    lake: [{ id: 1566515, geometry: { type: "Point", coordinates: [-121.9, 49.4] } }],
    wetland: [],
  },
};
const fmt = (id: unknown) => `${typeof id}:${String(id)}`;
const asked = (layers: readonly string[]) => layers.flatMap((l) => WORLD.rendered[l] ?? []);

function glRecorder(log: string[]): GlMapSlice {
  const layers = new Set(MAP_STYLE.layers.map((l) => l.id));
  return {
    getLayer: (id) => (layers.has(id) ? { id } : undefined),
    setFeatureState: (t, st) => log.push(`set ${t.source}/${t.sourceLayer} ${fmt(t.id)} ${JSON.stringify(st)}`),
    removeFeatureState: (t, key) => log.push(`remove ${t.source}/${t.sourceLayer} ${fmt(t.id)} ${key}`),
    queryRenderedFeatures: (...a: unknown[]) => {
      const box = Array.isArray(a[0]) ? a[0] : null;
      const ls = ((box ? a[1] : a[0]) as { layers: string[] }).layers;
      log.push(`query ${box ? JSON.stringify(box) : "viewport"} ${ls.join(",")}`);
      return asked(ls).map((h) => (h.id === null ? {} : { id: h.id }));     // GL JS: a number, or no id at all
    },
    querySourceFeatures: (_src, o) => (WORLD.source[o.sourceLayer] ?? []).map((h) => ({ ...h })),
    fitBounds: (b, o) => log.push(`fit ${JSON.stringify(b.flat())} pad ${o.padding} ${o.duration}ms`),
    easeTo: (o) => log.push(`ease ${JSON.stringify(o.center)} z${o.zoom} ${o.duration}ms`),
    getBounds: () => ({ getWest: () => -123, getSouth: () => 48, getEast: () => -121, getNorth: () => 50 }),
  };
}

function bindingRecorder(log: string[]): NativeRefs {
  const tick = () => Promise.resolve();
  return {
    map: () => ({
      queryRenderedFeatures: async (...a: unknown[]) => {
        await tick();
        const box = Array.isArray(a[0]) ? a[0] : null;
        const ls = ((box ? a[1] : a[0]) as { layers: string[] }).layers;
        log.push(`query ${box ? JSON.stringify(box) : "viewport"} ${ls.join(",")}`);
        return asked(ls).map((h) => ({ id: h.id === null ? "" : String(h.id) }));   // Android: strings, "" for none
      },
      getBounds: async () => [-123, 48, -121, 50],
    }),
    atlas: () => ({
      setFeatureState: async (f, st) => { log.push(`set atlas/${f.sourceLayer} ${fmt(f.id)} ${JSON.stringify(st)}`); },
      removeFeatureState: async (f, key) => { log.push(`remove atlas/${f.sourceLayer} ${fmt(f.id)} ${key}`); },
      querySourceFeatures: async (o) => {
        const ids = ((o.filter as unknown[])[2] as [string, number[]])[1];
        return (WORLD.source[o.sourceLayer] ?? []).filter((h) => ids.includes(h.id!)).map((h) => ({ ...h }));
      },
    }),
    camera: () => ({
      fitBounds: (b, o) => log.push(`fit ${JSON.stringify(b)} pad ${(o.padding as { top: number }).top} ${o.duration}ms`),
      easeTo: (o) => log.push(`ease ${JSON.stringify(o.center)} z${o.zoom} ${o.duration}ms`),
    }),
  };
}

/** THE OP SCRIPT: what a session does to a map, in the order it does it. Returns the log and the answers. */
async function script(r: MapRenderer, log: string[]) {
  const style = baseAdapter(r.platform);
  const out: unknown[] = [];
  // status for what is on screen, a stream and a lake section; data arrives keyed by STRINGS (JSON object keys)
  style.setData(r.states(() => []), "stream", { "311274": { status: "open" } });
  style.setData(r.states(() => []), "lake", { "1566515": { status: "closed", standing: 40 } });
  // select a river, then move the selection to a lake: only `selected` is cleared, per feature
  style.highlight(r.states(() => []), [311274]);
  style.highlight(r.states(() => [311274]), [1566515]);
  out.push(await r.press(100, 200));
  out.push(await r.visible());
  out.push(await r.waterBox([311274, 1566515]));
  out.push(await r.bounds());
  r.fitBounds([-123, 49, -122, 50], { padding: 24, duration: 450 });
  r.easeTo({ lon: -121.85, lat: 49.15, zoom: 9.4 }, { duration: 450 });
  await Promise.resolve();
  return { log, out };
}

describe("map parity: one op script, both renderers, the same commands", () => {
  it("issues the identical command log and answers identically (ids as NUMBERS on both)", async () => {
    const wlog: string[] = [];
    const w = await script(webRenderer(glRecorder(wlog), 14), wlog);
    const nlog: string[] = [];
    const n = await script(nativeRenderer(bindingRecorder(nlog), (e) => { throw e; }), nlog);
    expect(n.log).toEqual(w.log);
    expect(n.out).toEqual(w.out);
    // and the log is the one the contract describes, not merely the same on both: each feature addressed ONCE per
    // source-layer (lake_flowing draws the lake features, wetland_flowing the wetland ones: `onceSink`), the selection
    // moved by removing only `selected`, per feature
    const sel = (id: number, sl: string) => `set atlas/${sl} number:${id} {"selected":true}`;
    const unsel = (id: number, sl: string) => `remove atlas/${sl} number:${id} selected`;
    expect(w.log).toEqual([
      'set atlas/stream number:311274 {"status":"open"}',
      'set atlas/lake number:1566515 {"status":"closed","standing":40}',
      sel(311274, "wetland"), sel(311274, "lake"), sel(311274, "stream"),
      unsel(311274, "wetland"), unsel(311274, "lake"), unsel(311274, "stream"),
      sel(1566515, "wetland"), sel(1566515, "lake"), sel(1566515, "stream"),
      "query [[88,188],[112,212]] stream,lake_flowing,wetland_flowing",
      "query viewport stream,lake,lake_flowing,wetland_flowing",
      "fit [-123,49,-122,50] pad 24 450ms",
      "ease [-121.85,49.15] z9.4 450ms",
    ]);
    expect(w.out).toEqual([{ layer: "stream", id: 311274 }, [311274, 1566515], [-122, 49, -121.5, 49.4],
                           [-123, 48, -121, 50]]);
  });

  it("a tap with no stream under it falls through to still water, the same way on both", async () => {
    const saved = WORLD.rendered.stream!;
    WORLD.rendered.stream = [{ id: null }];
    try {
      const wlog: string[] = [], nlog: string[] = [];
      const w = await webRenderer(glRecorder(wlog), 14).press(10, 10);
      const n = await nativeRenderer(bindingRecorder(nlog), () => {}).press(10, 10);
      expect({ n, nlog }).toEqual({ n: w, nlog: wlog });
      expect(w).toEqual({ layer: "lake", id: 1566515 });
    } finally { WORLD.rendered.stream = saved; }
  });

  it("the phone pushes a feature-state once: the same value again is no call; forget() after a style swap re-sends", async () => {
    const log: string[] = [];
    const r = nativeRenderer(bindingRecorder(log), () => {});
    const push = () => baseAdapter("native").setData(r.states(() => []), "stream", { "5": { status: "open" } });
    push(); push();
    await Promise.resolve();
    expect(log).toEqual(['set atlas/stream number:5 {"status":"open"}']);
    r.forget(); push();
    await Promise.resolve();
    expect(log.length).toBe(2);
  });

  it("the web's whole-source-layer clear (the profile colouring's release swap) makes its sink forget that layer only",
     () => {
    // colour-ap review: the release swap clears every feature-state on stream/lake/wetland and then puts the selected
    // highlight back. Past the sink (`onceSink`) that re-set was swallowed as "already held" — the river went unlit.
    const log: string[] = [];
    const r = webRenderer(glRecorder(log), 14);
    const style = baseAdapter("web");
    style.highlight(r.states(() => []), [311274]);
    style.setData(r.states(() => []), "basin", { B1: { standing: 4 } });
    log.length = 0;
    for (const sl of ["stream", "lake", "wetland"]) r.clearSourceLayer("atlas", sl);
    style.highlight(r.states(() => []), [311274]);
    style.setData(r.states(() => []), "basin", { B1: { standing: 4 } });
    expect(log).toEqual([
      "remove atlas/stream undefined:undefined undefined", "remove atlas/lake undefined:undefined undefined",
      "remove atlas/wetland undefined:undefined undefined",
      'set atlas/wetland number:311274 {"selected":true}', 'set atlas/lake number:311274 {"selected":true}',
      'set atlas/stream number:311274 {"selected":true}',
      // basin was never cleared, so its value is still held and not re-sent
    ]);
  });

  it("an area layer (basin, by property) takes feature-state on the web only — a phone has no id for it", async () => {
    const wlog: string[] = [], nlog: string[] = [];
    baseAdapter("web").setData(webRenderer(glRecorder(wlog), 14).states(() => []), "basin", { B1: { standing: 4 } });
    baseAdapter("native").setData(nativeRenderer(bindingRecorder(nlog), () => {}).states(() => []), "basin",
                                  { B1: { standing: 4 } });
    await Promise.resolve();
    expect({ wlog, nlog }).toEqual({ wlog: ['set basins/basin string:B1 {"standing":4}'], nlog: [] });
  });

  it("each platform's style adapter is the one style adapter", () => {
    expect(Object.keys(nativeAdapter()).sort()).toEqual(Object.keys(baseAdapter("native")).sort());
    expect(Object.keys(webAdapter()).sort()).toEqual(Object.keys(baseAdapter("web")).sort());
  });
});

describe("the style adapter (platform-free: one implementation, tested for what it does)", () => {
  const a = baseAdapter("web");

  it("serves the generated style and its layer ids", () => {
    expect(a.style()).toBe(MAP_STYLE);
    expect(a.layerIds()).toEqual(layerIds());
  });

  it("layer ids are unique — a duplicate silently shadows on one renderer", () => {
    expect(new Set(layerIds()).size).toBe(layerIds().length);
  });

  it("a group toggle sets visibility on exactly that group's layers", () => {
    const g = toggleableGroups()[0];
    if (!g) return;
    const r = recorder();
    a.setGroupVisible(r.h, g.id, true);
    const want = Object.entries(STYLE_META.layerGroup).filter(([, gid]) => gid === g.id).map(([l]) => `vis ${l} true`);
    expect(r.calls).toEqual(want);
    expect(r.calls.length).toBeGreaterThan(0);
  });

  it("the basemap switch touches only imagery", () => {
    for (const kind of ["satellite", "map"] as const) {
      const r = recorder();
      a.setBasemap(r.h, kind);
      expect(r.calls.length).toBeGreaterThan(0);
      for (const c of r.calls)
        expect(c, "setBasemap touched a layer outside the imagery group")
          .toBe(`vis imagery ${kind === "satellite"}`);
    }
  });

  it("every view x every theme paints, and the same call twice paints the same", () => {
    for (const view of STYLE_META.views)
      for (const theme of Object.keys(STYLE_META.themes)) {
        const r1 = recorder(), r2 = recorder();
        a.applyView(r1.h, view.id, theme);
        baseAdapter("native").applyView(r2.h, view.id, theme);
        expect(r1.calls, `view=${view.id} theme=${theme}`).toEqual(r2.calls);
        expect(r1.calls.length).toBeGreaterThan(0);
      }
  });

  it("a user palette override reaches the paint", () => {
    const r = recorder();
    // Any value that is not light's own: the dark theme's closed red will do.
    const other = colour("dark", "color.status.closed");
    // The closed-area hatch (national parks, no-access land) draws in this token.
    a.applyView(r.h, "plain", "light", { "color.status.closed": other });
    expect(r.calls.join(" ")).toContain(other);
  });

  it("feature data is one feature-state call per feature", () => {
    const r = recorder();
    a.setData(r.h, "stream", { "380887781": { standing: 42 } });
    expect(r.calls).toEqual(['state stream 380887781 {"standing":42}']);
  });

  it("P15: a water's part is `selected`, the rest of its water `water`, in the one highlight call", () => {
    const r = recorder();
    a.highlight(r.h, [60210], [1235781, 1235785]);
    expect(r.calls).toEqual([
      ...STYLE_META.highlightable.map(({ id }) => `clear ${id}`),
      ...STYLE_META.highlightable.flatMap(({ id }) => [
        `state ${id} 60210 {"selected":true}`, `state ${id} 1235781 {"water":true}`, `state ${id} 1235785 {"water":true}`])]);
  });

  it("T151: a WATER'S MAP VIEW (cue \"dim\") lights its part `part`, never `selected` — no casing, no ring, no thicker line", () => {
    const r = recorder();
    a.highlight(r.h, [60210], [1235781], "dim");
    expect(r.calls).toEqual([
      ...STYLE_META.highlightable.map(({ id }) => `clear ${id}`),
      ...STYLE_META.highlightable.flatMap(({ id }) => [`state ${id} 60210 {"part":true}`, `state ${id} 1235781 {"water":true}`])]);
    // and the default cue is the browse map's outline
    const o = recorder();
    a.highlight(o.h, [60210]);
    expect(o.calls.filter((c) => c.startsWith("state")).every((c) => c.endsWith('{"selected":true}'))).toBe(true);
  });

  it("P15 / T151: the stream's width floor — `selected` at the highlight width; a water's part and the rest of its water " +
     "at ONE width (the water width), so the legend maps 1:1", () => {
    const t = resolveTheme("light");
    const w = paintFor("stream", "plain", t)["line-width"] as unknown[];
    const floor = ["case", ["boolean", ["feature-state", "selected"], false], 3.2,
                   ["any", ["boolean", ["feature-state", "part"], false], ["boolean", ["feature-state", "water"], false]], 2.4, 0];
    // inside the zoom curve, on every output
    expect((w.slice(3) as unknown[]).filter((_, i) => i % 2 === 1).every((v) => JSON.stringify((v as unknown[])[2]) === JSON.stringify(floor)))
      .toBe(true);
    expect([t["width.stream.highlight"], t["width.stream.water"]]).toEqual([3.2, 2.4]);
  });

  it("T151: the water's OTHER parts are dimmed by opacity (`water` × opacity.water.others) — on every per-section line " +
     "and fill and the edges that follow them, nowhere else; no colour reads `part` or `water`", () => {
    const water = ["boolean", ["feature-state", "water"], false];
    const dimmed = (base: unknown, k: number) => ["case", water, ["*", base, k], base];
    const seen: Record<string, unknown> = {};
    const reads: string[] = [];
    for (const theme of ["light", "dark", "cvd"]) {
      const t = resolveTheme(theme), k = Number(t["opacity.water.others"]);
      expect([theme, k]).toEqual([theme, 0.25]);
      for (const l of MAP_STYLE.layers as { id: string; type: string }[]) {
        const modes = Object.keys(STYLE_META.colorModes[l.id] ?? {});
        if (!modes.length) continue;
        const p = paintFor(l.id, modes[0]!, t);
        const op = JSON.stringify(p[`${l.type}-opacity`] ?? null);
        if (op.includes('"water"')) seen[`${theme} ${l.id}`] = p[`${l.type}-opacity`];
        const colour = Object.entries(p).filter(([key]) => key.endsWith("-color"));
        if (/"(part|water)"/.test(JSON.stringify(colour))) reads.push(`${theme} ${l.id}`);
      }
    }
    expect(reads).toEqual([]);
    const lt = resolveTheme("light"), lake = Number(lt["opacity.lake"]), wet = Number(lt["opacity.wetland"]);
    const edge = (seen["light lake__edge"] as unknown[] | undefined)?.[3];      // the shoreline's own opacity, undimmed
    expect(Object.fromEntries(Object.entries(seen).filter(([k]) => k.startsWith("light ")))).toEqual({
      "light wetland": dimmed(wet, 0.25), "light wetland_flowing": dimmed(wet, 0.25),
      "light lake": dimmed(lake, 0.25), "light lake__edge": dimmed(edge, 0.25),
      "light lake_flowing": dimmed(lake, 0.25), "light stream": dimmed(1, 0.25),
    });
    expect(Object.keys(seen).length).toBe(18);                                  // the same six layers in each theme
  });

  it("OWNER 2026-10-21: the selected part KEEPS its regulation colour — no layer's colour reads `selected`", () => {
    const recoloured: string[] = [];
    for (const theme of ["light", "dark", "cvd"])
      for (const { id } of STYLE_META.highlightable)
        for (const mode of Object.keys(STYLE_META.colorModes[id]!)) {
          const p = paintFor(id, mode, resolveTheme(theme));
          const colour = Object.entries(p).filter(([k]) => k.endsWith("-color") || k.endsWith("-pattern"));
          if (JSON.stringify(colour).includes('"selected"')) recoloured.push(`${theme} ${id} ${mode}`);
        }
    expect(recoloured).toEqual([]);
    // the plain stream, whole: exactly its mode's colour
    expect(paintFor("stream", "plain", resolveTheme("light"))["line-color"])
      .toEqual(colorExpression("stream", "plain", resolveTheme("light")));
  });

  it("…and is OUTLINED: a casing under the stream line, a ring on a lake or wetland, in `color.highlight`, by feature-state", () => {
    const style = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, "light", { stream: "profile" });
    const ids = style.layers.map((l: { id: string }) => l.id);
    const at = (id: string) => ids.indexOf(id);
    // under the stream line (its colour on top), over the lakes; each ring over its own polygons' last layer (the lake's
    // is its shoreline, drawn over the boundaries since T83)
    expect([at(SELECTION_CASING) === at("stream") - 1, at(SELECTION_CASING) > at("lake__edge"),
            at(SELECTION_RINGS.lake) === at("lake__edge") + 1, at(SELECTION_RINGS.wetland) === at("wetland_flowing") + 1])
      .toEqual([true, true, true, true]);
    const layer = (id: string) => style.layers.find((l: { id: string }) => l.id === id) as unknown as
      { type: string; "source-layer": string; paint: Record<string, unknown> };
    // (T203) the part of a water's Map ("dim") is haloed too — (T214) at the full outline, the same as a selection
    const shown = ["case", ["boolean", ["feature-state", "selected"], false], 1,
                   ["boolean", ["feature-state", "part"], false], 1, 0];
    for (const theme of ["light", "dark", "cvd"]) {
      const tk = resolveTheme(theme);
      const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme, {});
      const casing = s.layers.find((l: { id: string }) => l.id === SELECTION_CASING) as unknown as { paint: Record<string, unknown> };
      expect([theme, casing.paint["line-color"], casing.paint["line-opacity"]]).toEqual([theme, tk["color.highlight"], shown]);
    }
    expect([SELECTION_CASING, SELECTION_RINGS.lake, SELECTION_RINGS.wetland].map((id) =>
      [layer(id).type, layer(id)["source-layer"], layer(id).paint["line-opacity"]])).toEqual([
      ["line", "stream", shown], ["line", "lake", shown], ["line", "wetland", shown]]);
    // the casing is the stream's own width (with its floors) plus the outline on each side, inside the zoom curve
    const w = paintFor("stream", "plain", resolveTheme("light"))["line-width"] as unknown[];
    const cw = layer(SELECTION_CASING).paint["line-width"] as unknown[];
    expect([cw.slice(0, 3), cw.slice(3).filter((_, i) => i % 2 === 1)]).toEqual([
      w.slice(0, 3), w.slice(3).filter((_, i) => i % 2 === 1).map((v) => ["+", v, 2 * CASING_PX])]);
    expect(PART_HALO_PX).toBe(CASING_PX);
  });

  it("a theme or mode change repaints the outline with the stream (one paint path)", () => {
    const r = recorder();
    a.setLayerMode(r.h, "stream", "plain", "dark");
    const t = resolveTheme("dark");
    expect(r.calls.filter((c) => c.startsWith(`paint ${SELECTION_CASING} line-color`)
                                 || c.startsWith(`paint ${SELECTION_RINGS.lake} line-color`)))
      .toEqual([`paint ${SELECTION_CASING} line-color ${t["color.highlight"]}`,
                `paint ${SELECTION_RINGS.lake} line-color ${t["color.highlight"]}`]);
  });

  it("a highlight clears and sets `selected` on every highlightable layer", () => {
    const r = recorder();
    a.highlight(r.h, [356355331, 356355332]);
    expect(r.calls).toEqual([
      ...STYLE_META.highlightable.map(({ id }) => `clear ${id}`),
      ...STYLE_META.highlightable.flatMap(({ id }) => [
        `state ${id} 356355331 {"selected":true}`, `state ${id} 356355332 {"selected":true}`])]);
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
    expect(() => resolveTheme("light", { "color.nope": colour("light", "color.ui.card") })).toThrow(/unknown token/);
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
    // 200 and up are the Change view's own scale (T52, `toRamp` in @app/ui), a domain no percentile reaches
    const ats = mode.stops.map(([at]) => at).filter((a) => a >= 0 && a <= 100);
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
    // over the flow field. AND THEIR EDGES AND NUMBERS: `hide` named the fills and the generated `__edge`/`__label`
    // layers drew anyway — a grey boundary and a "3" across the field (refinement pass, 2026-10-22).
    const conditions = STYLE_META.views.find((v) => v.id === "conditions");
    expect([...(conditions?.hide ?? [])].sort())
      .toEqual(["mu", "mu__edge", "mu__label", "region", "region__edge", "region__label"]);
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
   * EVERY STATUS AT ONE WIDTH. Closed was drawn 1.6x wider (`width.status.closed`); the user
   * found it hard to use and it was removed, so the status mode's width is the plain mode's
   * width exactly — colour alone tells the statuses apart (tools/water-status-cvd.test.ts).
   */
  it("a status line is the same width as the plain one", async () => {
    const { paintFor } = await import("./paint");
    const { resolveTheme } = await import("./style");
    const t = resolveTheme("light");
    for (const layer of ["stream", "lake__edge"]) {
      const status = paintFor(layer, "profile", t)["line-width"];
      expect(JSON.stringify(status), layer).not.toContain('"feature-state","p"');
      expect(status, layer).toEqual(paintFor(layer, "plain", t)["line-width"]);
    }
    expect(paintFor("stream", "plain", t)["line-color"]).toContain(t["color.water.mapped"]);
  });

  it("closed is the crimson every closure on the map wears", async () => {
    const { waterStatusColour, resolveTheme } = await import("./style");
    expect(waterStatusColour("light", "closed")).toBe(resolveTheme("light")["color.status.closed"]);
  });

  /*
   * THE MAP LINE AND THE SEARCH DOT ARE ONE VOCABULARY. The `profile` colour mode is authored in
   * layers.source.json and the dot resolves @app/core WATER_STATUS through `waterStatusColour`;
   * if the two ever named different tokens a row and the line beside it would disagree.
   * `missing` is a section with no status pushed (tidal, outside B.C., not yet asked): it
   * must paint a neutral `water.*` grey, never base — that would be an answer.
   */
  it("the status colour mode (the day's profile) paints each status with WATER_STATUS's own token", async () => {
    const { WATER_STATUS, WATER_STATUSES } = await import("@app/core");
    const { STYLE_META } = await import("./style");
    for (const layer of ["stream", "lake"]) {
      const m = STYLE_META.colorModes[layer]!["profile"] as
        { scale: string; property: string; categories: Record<string, { token: string }>;
          missing: { token: string } };
      expect(m.scale).toBe("profile");
      expect(m.property).toBe("p");
      expect(Object.keys(m.categories).sort()).toEqual([...WATER_STATUSES].sort());
      for (const s of WATER_STATUSES) expect(m.categories[s]!.token).toBe(WATER_STATUS[s].token);
      expect(m.missing.token).toBe("color.water.unmapped");
    }
  });
});
