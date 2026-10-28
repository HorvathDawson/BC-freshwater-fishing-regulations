/**
 * (T213, owner 2026-10-28) BAIT BAN AND FLY ONLY ARE MARKS ON THE STANDARD MAP, through the one colouring engine.
 *
 *   marks      an ink edge round a stream line (solid fly, dashed bait) and a ring round a lake, shown where the method
 *              day places the section's `gm` label — never where the status day closes it or marks it no access, never
 *              in any mode but the Standard status mode (`profile`), never with no method or no status installed
 *   colour     the status colour is untouched: the Standard line paints exactly as it did without the marks
 *   repaint    a new method day repaints the marks (one paint change; no relabel), a new status day too
 *   style      the first frame carries the four layers, hidden; the stream's edges sit under the line, over the
 *              selection casing; the lake's rings over its outline, under the selection ring
 */
import { describe, expect, it } from "vitest";
import { methodDayProfiles, type MethodFacts, type StatusIndex } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { Colouring, methodInput, statusInput, type LabelTarget, type TileRenderer, type TileSections } from "./colouring";
import { GEAR_EDGE_PX, GEAR_LAYERS, GEAR_RAMP, GEAR_RAMPS, gearPaint, paintFor, rampAt, SELECTION_CASING, SELECTION_RINGS } from "./paint";
import { runtimeStyle } from "./runtime-style";
import { resolveTheme, NO_VALUES } from "./style";

/** Profile 0 bait ban all year; profile 1 fly only Jan 1 → Jun 30, none after. Sections 100..104 profile 1, 7 profile 0. */
const FACTS: MethodFacts = {
  handles: "0".repeat(16), profiles: 2,
  profileOf: (sid) => (sid >= 100 && sid <= 104 ? 1 : sid === 7 ? 0 : null),
  profileClassOn: (p, day) => (p === 0 ? "bait" : day <= 182 ? "fly" : "none"),
};
const MAY1 = new Date(2026, 4, 1), AUG1 = new Date(2026, 7, 1);
const INDEX: StatusIndex = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1,
  profileOf: () => 0, profileCodeOn: () => "base", codeOn: () => "base", waterCodeOn: () => "base",
};
const t = resolveTheme("light");
const STATUS = { closed: ["p3"], own: [], base: ["b", "p0"], no_access: ["n"], none: [] };
const MAY = { ...NO_VALUES, status: STATUS, method: methodDayProfiles(FACTS, MAY1) };
const W = paintFor("stream", "profile", t, MAY)["line-width"];

describe("the gear marks' paint", () => {
  it("fly: a solid ink edge where the day says fly, off where the status day closes it or marks it no access", () => {
    const p = gearPaint("stream", "profile", "fly", t, MAY, W);
    expect(p["line-color"]).toBe(t["color.gear.edge"]);
    expect(p["line-dasharray"]).toBeUndefined();
    // zoom is only ever the input of a top-level interpolate, so the day's test sits inside each stop (T226)
    const op = p["line-opacity"] as unknown[];
    expect(op.slice(0, 3)).toEqual(["interpolate", ["linear"], ["zoom"]]);
    expect(op[4]).toEqual(["case", ["all",
      ["in", ["coalesce", ["feature-state", "gm"], ""], ["literal", ["m1"]]],
      ["!", ["in", ["coalesce", ["feature-state", "p"], ""], ["literal", ["p3", "n"]]]]], 0, 0]);
    expect(JSON.stringify(p["line-width"])).toContain(`["+",`);
    expect(JSON.stringify(p["line-width"])).toContain(`${2 * GEAR_EDGE_PX}`);
  });

  it("T226: the marks fade in with zoom — nothing at the far zooms, the T213 look at full strength, a longer dash when thin", () => {
    const ramp = GEAR_RAMPS[GEAR_RAMP];
    expect([rampAt(ramp.opacity, 5), rampAt(ramp.opacity, 8), rampAt(ramp.opacity, 22)]).toEqual([0, 0, 1]);
    expect(rampAt(ramp.edge, 22)).toBe(1);
    const mid = rampAt(ramp.opacity, 10);
    expect(mid).toBeGreaterThan(rampAt(ramp.opacity, 9));
    expect(mid).toBeLessThan(1);
    const p = gearPaint("stream", "profile", "bait", t, MAY, W);
    const dash = t["dash.gear.bait"] as number[];
    expect(p["line-dasharray"]).toEqual(["step", ["zoom"], ["literal", dash.map((n) => n * 2)], 11, ["literal", dash]]);
    const ring = gearPaint("lake", "profile", "fly", t, MAY)["line-width"] as unknown[];
    expect(ring.slice(0, 3)).toEqual(["interpolate", ["linear"], ["zoom"]]);
    expect(ring[ring.length - 1]).toBe(1.6);
  });

  it("bait: the same edge, dashed (`dash.gear.bait`); a lake gets a ring of the same ink", () => {
    const p = gearPaint("stream", "profile", "bait", t, MAY, W);
    expect([JSON.stringify(p["line-dasharray"]).includes(JSON.stringify(t["dash.gear.bait"])), JSON.stringify(p["line-opacity"]).includes(`["m0"]`)]).toEqual([true, true]);
    const ring = gearPaint("lake", "profile", "bait", t, MAY);
    expect([ring["line-color"], Array.isArray(ring["line-width"]), JSON.stringify(ring["line-dasharray"]).includes(JSON.stringify(t["dash.gear.bait"]))])
      .toEqual([t["color.gear.edge"], true, true]);
  });

  it("nowhere outside the Standard and Salmon modes, with no method or no status installed, or on a day with none", () => {
    for (const mode of ["plain", "standing"])
      expect([mode, gearPaint("stream", mode, "fly", t, MAY, W)["line-opacity"]]).toEqual([mode, 0]);
    expect(gearPaint("stream", "profile", "fly", t, { ...MAY, method: null }, W)["line-opacity"]).toBe(0);
    expect(gearPaint("stream", "profile", "fly", t, { ...MAY, status: null }, W)["line-opacity"]).toBe(0);
    // Aug 1: profile 1 is none, so nothing is fly
    expect(gearPaint("stream", "profile", "fly", t, { ...MAY, method: methodDayProfiles(FACTS, AUG1) }, W)["line-opacity"])
      .toBe(0);
  });

  it("(owner 2026-10-28) the Salmon map carries the same marks, hidden where the SALMON day closes the section", () => {
    const SALMON = { open: ["s1"], release: [], closed: ["s0", "s2"], check: [], none: [] };
    const p = gearPaint("stream", "salmon", "fly", t, { ...MAY, salmon: SALMON }, W);
    expect((p["line-opacity"] as unknown[])[4]).toEqual(["case", ["all",
      ["in", ["coalesce", ["feature-state", "gm"], ""], ["literal", ["m1"]]],
      ["!", ["in", ["coalesce", ["feature-state", "sp"], ""], ["literal", ["s0", "s2"]]]]], 0, 0]);
    expect(JSON.stringify(gearPaint("lake", "salmon", "bait", t, { ...MAY, salmon: SALMON })["line-dasharray"]))
      .toContain(JSON.stringify(t["dash.gear.bait"]));
    // no salmon day installed: nothing marked (the status day is not the Salmon map's read)
    expect(gearPaint("stream", "salmon", "fly", t, MAY, W)["line-opacity"]).toBe(0);
  });

  it("the status colour is the Standard map's own: the marks change no colour", () => {
    expect(paintFor("stream", "profile", t, MAY)).toEqual(paintFor("stream", "profile", t, { ...MAY, method: null }));
  });

  it("the ink stands in every theme", () => {
    for (const th of ["light", "dark", "cvd"]) expect(typeof resolveTheme(th)["color.gear.edge"], th).toBe("string");
  });
});

describe("the gear marks through the engine", () => {
  function rig() {
    const paints: string[] = [];
    const h: MapHandle = {
      setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
      setPaint: (l, p, v) => { if (p === "line-opacity") paints.push(`${l} ${JSON.stringify(v)}`); },
    };
    const states: string[] = [];
    const target: LabelTarget = { setState: (l, id, s) => states.push(`${l} ${id} ${JSON.stringify(s)}`), clearAll: () => {} };
    let tiles: TileSections[] = [];
    const renderer: TileRenderer = { loaded: () => tiles, restore: () => {} };
    const adapter = baseAdapter("web");
    return { h, target, renderer, adapter, c: new Colouring(adapter), paints, states,
             setTiles: (x: TileSections[]) => { tiles = x; } };
  }

  it("labels arrive with the status's in one call; a new method day or status day repaints the marks", () => {
    const r = rig();
    r.adapter.setLayerMode(r.h, "stream", "profile", "light");
    r.adapter.setLayerMode(r.h, "lake", "profile", "light");
    r.setTiles([{ stream: [100] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: MAY1 }), "light");
    r.paints.splice(0);
    r.c.set(r.h, r.target, r.renderer, "method", methodInput({ facts: FACTS, day: MAY1 }), "light");
    const shown = (l: string) => r.paints.filter((p) => p.startsWith(`${l} `)).at(-1);
    expect(shown(GEAR_LAYERS.stream.fly)).toContain(`"m1"`);
    expect(shown(GEAR_LAYERS.lake.bait)).toContain(`"m0"`);
    r.states.splice(0);
    expect(r.c.label(r.target, { stream: [101, 7, 9] })).toBe(3);
    expect(r.states).toEqual(['stream 101 {"p":"p0","gm":"m1"}', 'stream 7 {"p":"p0","gm":"m0"}',
                              'stream 9 {"p":"p0","gm":"m"}']);
    r.paints.splice(0);
    r.c.set(r.h, r.target, r.renderer, "method", methodInput({ facts: FACTS, day: AUG1 }), "light");
    expect(shown(GEAR_LAYERS.stream.fly)).toBe(`${GEAR_LAYERS.stream.fly} 0`);
    r.paints.splice(0);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: AUG1 }), "light");
    expect(shown(GEAR_LAYERS.stream.bait)).toBeDefined();
  });
});

describe("the gear layers in the first frame", () => {
  it("present, hidden, the stream's under its line and over the selection casing; the lake's under the selection ring", () => {
    const ids = runtimeStyle({ atlas: "a", basemap: "b" } as never, "light").layers.map((l) => l.id);
    const at = (id: string) => ids.indexOf(id);
    for (const id of [...Object.values(GEAR_LAYERS.stream), ...Object.values(GEAR_LAYERS.lake)]) expect(at(id), id).toBeGreaterThan(-1);
    expect(at(SELECTION_CASING)).toBeLessThan(at(GEAR_LAYERS.stream.fly));
    expect(at(GEAR_LAYERS.stream.bait)).toBeLessThan(at("stream"));
    expect(at(GEAR_LAYERS.lake.bait)).toBeLessThan(at(SELECTION_RINGS.lake));
    const s = runtimeStyle({ atlas: "a", basemap: "b" } as never, "light");
    for (const id of Object.values(GEAR_LAYERS.stream))
      expect((s.layers.find((l) => l.id === id) as unknown as { paint: Record<string, unknown> }).paint["line-opacity"]).toBe(0);
  });
});
