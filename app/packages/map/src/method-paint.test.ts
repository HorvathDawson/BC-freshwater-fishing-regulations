/**
 * (T213) THE BAIT & FLY PAINT, through the one colouring engine.
 *
 *   paint      the `method` mode paints each profile label in its day's class colour (fly only, bait ban, none); the
 *              status day's CLOSED labels win first (the method file carries no closures); with no method installed
 *              the mode is the plain map
 *   repaint    a new status day repaints the method layers too (their closed set is the status's)
 *   speed      labels arrive with the status's in ONE feature-state call; a toggle is paint only
 */
import { describe, expect, it } from "vitest";
import { methodDayProfiles, type MethodFacts, type StatusIndex } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { Colouring, methodInput, statusInput, type LabelTarget, type TileRenderer, type TileSections } from "./colouring";
import { paintFor } from "./paint";
import { resolveTheme, NO_VALUES } from "./style";

/** Profile 0 bait ban all year; profile 1 fly only Jan 1 → Jun 30, none after. Sections 100..104 profile 1, 7 profile 0. */
const FACTS: MethodFacts = {
  handles: "0".repeat(16), profiles: 2,
  profileOf: (sid) => (sid >= 100 && sid <= 104 ? 1 : sid === 7 ? 0 : null),
  profileClassOn: (p, day) => (p === 0 ? "bait" : day <= 182 ? "fly" : "none"),
};
const MAY1 = new Date(2026, 4, 1), AUG1 = new Date(2026, 7, 1);
const MODE = "method";
const INDEX: StatusIndex = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1,
  profileOf: () => 0, profileCodeOn: () => "base", codeOn: () => "base", waterCodeOn: () => "base",
};
const t = resolveTheme("light");
const STATUS = { closed: ["p3"], own: [], base: ["b", "p0"], no_access: ["n"], none: [] };

function rig() {
  const states: string[] = [], paints: string[] = [];
  const h: MapHandle = {
    setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
    setPaint: (l, p, v) => { if (p === "line-color" || p === "fill-color") paints.push(`${l} ${JSON.stringify(v)}`); },
  };
  const target: LabelTarget = { setState: (l, id, s) => states.push(`${l} ${id} ${JSON.stringify(s)}`), clearAll: () => {} };
  let tiles: TileSections[] = [];
  const renderer: TileRenderer = { loaded: () => tiles, restore: () => {} };
  const adapter = baseAdapter("web");
  const c = new Colouring(adapter);
  return { h, target, renderer, adapter, c, states, paints, setTiles: (x: TileSections[]) => { tiles = x; } };
}

describe("the Bait & fly mode's paint", () => {
  it("each label in its day's class colour, the status day's closures first", () => {
    const expr = JSON.stringify(paintFor("stream", MODE, t,
      { ...NO_VALUES, status: STATUS, method: methodDayProfiles(FACTS, MAY1) })["line-color"]);
    // closed: the status channel's own label `p`, matched before the method label `gm`
    expect(expr.startsWith(`["match",["coalesce",["feature-state","p"],""],["p3"],"${t["color.method.closed"]}"`)).toBe(true);
    expect(expr).toContain(`["m1"],"${t["color.method.fly"]}"`);
    expect(expr).toContain(`["m0"],"${t["color.method.bait"]}"`);
    expect(expr).toContain(`["m"],"${t["color.method.none"]}"`);
    expect(expr).toContain(`"gm"`);
    const aug = JSON.stringify(paintFor("stream", MODE, t,
      { ...NO_VALUES, status: STATUS, method: methodDayProfiles(FACTS, AUG1) })["line-color"]);
    expect(aug).toContain(`["m","m1"],"${t["color.method.none"]}"`);
  });

  it("fly, bait, none and closed are four different colours in every theme", () => {
    for (const th of ["light", "dark", "cvd"]) {
      const r = resolveTheme(th);
      const c = ["fly", "bait", "none", "closed"].map((k) => r[`color.method.${k}`]);
      expect(new Set(c).size, th).toBe(4);
    }
  });

  it("with no method installed the mode is the plain map", () => {
    expect(paintFor("stream", MODE, t, NO_VALUES)).toEqual(paintFor("stream", "plain", t));
    expect(paintFor("stream", MODE, t, { ...NO_VALUES, status: STATUS })).toEqual(paintFor("stream", "plain", t));
  });
});

describe("the Bait & fly channel", () => {
  it("labels arrive with the status's in one call; a toggle is paint only; a new status day repaints it", () => {
    const r = rig();
    r.adapter.setLayerMode(r.h, "stream", "profile", "light");
    r.setTiles([{ stream: [100] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: MAY1 }), "light");
    r.c.set(r.h, r.target, r.renderer, "method", methodInput({ facts: FACTS, day: MAY1 }), "light");
    r.states.splice(0);
    expect(r.c.label(r.target, { stream: [101, 7, 9] })).toBe(3);
    expect(r.states).toEqual(['stream 101 {"p":"p0","gm":"m1"}', 'stream 7 {"p":"p0","gm":"m0"}',
                              'stream 9 {"p":"p0","gm":"m"}']);
    r.states.splice(0);
    r.paints.splice(0);
    r.adapter.setLayerMode(r.h, "stream", MODE, "light");
    expect(r.states).toEqual([]);
    expect(r.paints.some((p) => p.includes(String(t["color.method.fly"])))).toBe(true);
    // the status day moves: the method layer is repainted with it (its closures are the status's)
    r.paints.splice(0);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: AUG1 }), "light");
    expect(r.paints.some((p) => p.startsWith("stream "))).toBe(true);
  });
});
