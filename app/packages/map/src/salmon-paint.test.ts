/**
 * (T211) THE SALMON PAINT, through the one colouring engine — and the owner's speed rule (2026-10-27: "toggling
 * Standard/Salmon should be instant — no re-fetch/re-tile").
 *
 *   paint      the `salmon` mode paints each chain label in its day's salmon colour; a section DFO does not list (the
 *              UNLISTED chain, `s0`) paints CLOSED, never `missing`; with no salmon installed the mode is the plain map
 *   speed      the salmon chains are labelled as the tiles arrive, in the SAME feature-state call as the status profile;
 *              a toggle Standard ⇄ Salmon is then only paint properties — zero feature-state calls, zero relabels — and
 *              is timed here against a tile set of thousands of sections
 */
import { describe, expect, it } from "vitest";
import { salmonDayProfiles, type SalmonFacts, type StatusIndex } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { Colouring, salmonInput, statusInput, type LabelTarget, type TileRenderer, type TileSections } from "./colouring";
import { paintFor } from "./paint";
import { resolveTheme, NO_VALUES } from "./style";

/** Chain 0 the unlisted water (closed), chain 1 open on Jul 20; sections 100..104 are chain 1. */
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2,
  chainOf: (sid) => (sid >= 100 && sid <= 104 ? 1 : 0),
  payload: {
    format: "salmon/1", credit: "DFO", default: 0, regions: {}, verdicts: [], gears: [], unmapped: [],
    chains: [{ r: [], w: "", l: [], sp: [], g: [0, -1], m: [0, 3] },
             { r: ["6"], w: "Kispiox River", l: [], sp: [], g: [0, -1], m: [0, 3, 196, 1, 236, 3] }],
  },
};
const JUL20 = new Date(2026, 6, 20);
const MODE = "salmon";
const INDEX: StatusIndex = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1,
  profileOf: () => 0, profileCodeOn: () => "base", codeOn: () => "base", waterCodeOn: () => "base",
};
const t = resolveTheme("light");

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
  for (const l of ["stream", "lake"]) adapter.setLayerMode(h, l, "profile", "light");
  const c = new Colouring(adapter);
  return { h, target, renderer, adapter, c, states, paints, setTiles: (x: TileSections[]) => { tiles = x; } };
}

describe("the salmon mode's paint", () => {
  it("each chain label in its day's colour; the unlisted chain (s0) is CLOSED, not missing", () => {
    const expr = JSON.stringify(paintFor("stream", MODE, t, { ...NO_VALUES, salmon: salmonDayProfiles(FACTS, JUL20) })["line-color"]);
    expect(expr).toContain(`["s1"],"${t["color.salmon.open"]}"`);
    expect(expr).toContain(`["s0"],"${t["color.salmon.closed"]}"`);
    expect(expr.indexOf(`"sp"`)).toBeGreaterThan(-1);
  });

  it("with no salmon installed the salmon mode is the plain map — not asked is never painted closed", () => {
    expect(paintFor("stream", MODE, t, NO_VALUES)).toEqual(paintFor("stream", "plain", t));
    // and the status profile does not stand in for it, nor it for the status
    expect(paintFor("stream", MODE, t, { ...NO_VALUES, status: { closed: [], own: [], base: ["b"], no_access: [], none: [] } }))
      .toEqual(paintFor("stream", "plain", t));
  });

});

describe("the toggle is instant: a mode change, nothing else (owner 2026-10-27)", () => {
  it("both channels label each section in ONE call as the tiles arrive; the toggle then sets no feature-state", () => {
    const r = rig();
    r.setTiles([{ stream: [100, 7] }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: JUL20 }), "light");
    r.c.set(r.h, r.target, r.renderer, "salmon", salmonInput({ facts: FACTS, day: JUL20 }), "light");
    // the salmon channel came second, so the loaded tile is labelled for it; a NEW tile takes both keys in one call
    r.states.splice(0);
    expect(r.c.label(r.target, { stream: [101] })).toBe(1);
    expect(r.states).toEqual(['stream 101 {"p":"p0","sp":"s1"}']);
    r.states.splice(0);
    r.paints.splice(0);
    // Standard → Salmon → Standard: paint only
    r.adapter.setLayerMode(r.h, "stream", MODE, "light");
    r.adapter.setLayerMode(r.h, "stream", "profile", "light");
    expect(r.states).toEqual([]);
    expect(r.paints.length).toBeGreaterThan(0);
    expect(r.paints[0]).toContain(String(t["color.salmon.open"]));
  });

  it("the same salmon feed again (a re-render, a toggle) is no engine work at all", () => {
    const r = rig();
    r.setTiles([{ stream: [100, 101, 7] }]);
    const feed = { facts: FACTS, day: JUL20 };
    r.c.set(r.h, r.target, r.renderer, "salmon", salmonInput(feed), "light");
    const calls = r.c.calls;
    r.states.splice(0);
    r.paints.splice(0);
    r.c.set(r.h, r.target, r.renderer, "salmon", salmonInput(feed), "light");
    expect([r.c.calls - calls, r.states, r.paints]).toEqual([0, [], []]);
  });

  it("a toggle over 20,000 labelled sections costs a handful of paint calls and well under a frame", () => {
    const r = rig();
    const ids = Array.from({ length: 20_000 }, (_, i) => i);
    r.setTiles([{ stream: ids, lake: ids }]);
    r.c.set(r.h, r.target, r.renderer, "status", statusInput({ index: INDEX, day: JUL20 }), "light");
    r.c.set(r.h, r.target, r.renderer, "salmon", salmonInput({ facts: FACTS, day: JUL20 }), "light");
    r.states.splice(0);
    r.paints.splice(0);
    const t0 = performance.now();
    for (const l of ["stream", "lake"]) r.adapter.setLayerMode(r.h, l, MODE, "light");
    const ms = performance.now() - t0;
    expect(r.states).toEqual([]);
    // the same paint calls as a toggle over an empty map: the cost does not grow with the sections labelled
    const empty = rig();
    empty.paints.splice(0);
    for (const l of ["stream", "lake"]) empty.adapter.setLayerMode(empty.h, l, MODE, "light");
    expect(r.paints.length).toBe(empty.paints.length);
    expect(ms).toBeLessThan(16);
  });
});
