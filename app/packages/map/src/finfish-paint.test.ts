/**
 * (T796 A6, owner 2026-11-16: "Closes the whole page") A DFO "FINFISH CLOSURE" PAINTS CLOSED ON THE STANDARD MAP — all
 * fishing is closed on the stretch, trout included, so a section whose salmon chain (`sp`) has a verdict carrying `f` on
 * the shown day paints the closed status colour ahead of its status profile `p`, as a book-closed section does (its
 * stripe and width, no gear marks, no no-access dots, no order fade). On a day with none the paint is IDENTICAL to the
 * status day's — no cost and no change on a normal day. Under a legend isolate other than Closed it greys with the rest.
 * MapLibre's own validator takes it: tools/style-valid.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { SalmonFacts, StatusIndex } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { Colouring, salmonInput, statusInput, type LabelTarget, type TileRenderer, type TileSections } from "./colouring";
import { closedCuePaint, gearShown, noAccessShown, paintFor } from "./paint";
import { NO_VALUES, STYLE_META, resolveTheme, shadeHex, type ChannelValues } from "./style";

/** Chain 0 the unlisted water; chain 1 (sections 100..104) closed by DFO's "Finfish closure" on Jul 19–20 (days 200–201)
 *  only — coho; every other species and day the plain "not opened" closure. */
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2,
  chainOf: (sid) => (sid >= 100 && sid <= 104 ? 1 : 0),
  payload: {
    format: "salmon/4", credit: "DFO", default: 0, regions: {}, gears: [], unmapped: [],
    verdicts: [{ s: "closed", t: "Not opened by DFO", d: 1 }, { s: "closed", t: "Finfish closure (Jul 19–20)", f: 1 }],
    chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
             { r: ["1"], w: "Stamp River", l: [], sp: [[0, 0], [0, 0, 200, 1, 202, 0], [0, 0], [0, 0], [0, 0]],
               g: [0, -1], m: [0, 3] }],
  },
};
const JUL20 = new Date(2026, 6, 20), JUL25 = new Date(2026, 6, 25);
/** every section `p0`, which the day places `own` (open under its own rules) */
const INDEX: StatusIndex = {
  handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 0, waters: 0, profiles: 1,
  profileOf: () => 0, profileCodeOn: () => "own", codeOn: () => "own", waterCodeOn: () => "own",
};
const t = resolveTheme("light");
const salmon = (day: Date) => ({ facts: FACTS, day });
const valuesOf = (day: Date, only: "own" | "closed" | null = null, withSalmon = true): ChannelValues =>
  ({ ...NO_VALUES, status: statusInput({ index: INDEX, day, only }, withSalmon ? salmon(day) : null).values() as ChannelValues["status"] });
const SP = ["coalesce", ["feature-state", "sp"], ""];

describe("the Standard map under a DFO finfish closure", () => {
  it("a day with none: the values, the key and every layer's paint are exactly the status day's", () => {
    for (const only of [null, "own", "closed"] as const) {
      const plain = statusInput({ index: INDEX, day: JUL25, only }), over = statusInput({ index: INDEX, day: JUL25, only }, salmon(JUL25));
      expect([over.valuesKey, over.values()]).toEqual([plain.valuesKey, plain.values()]);
      for (const l of ["stream", "lake", "lake__edge", "wetland_flowing"])
        expect(paintFor(l, "profile", t, valuesOf(JUL25, only))).toEqual(paintFor(l, "profile", t, valuesOf(JUL25, only, false)));
    }
  });

  it("the closed chain paints the closed status colour ahead of its profile, on lines, fills and outlines", () => {
    const day = valuesOf(JUL20).status!;
    expect([day.finfish, day.finfishNone]).toEqual([["s1"], undefined]);
    for (const [l, prop] of [["stream", "line-color"], ["lake", "fill-color"], ["lake__edge", "line-color"]] as const) {
      const byDay = paintFor(l, "profile", t, valuesOf(JUL20, null, false))[prop];
      // (T467) the lake's shoreline draws closed a shade off the token, as it draws every status
      const closed = l === "lake__edge" ? shadeHex(String(t["color.status.closed"]), t["shade.lake.edge"] as number) : t["color.status.closed"];
      expect(paintFor(l, "profile", t, valuesOf(JUL20))[prop])
        .toEqual(["case", ["in", SP, ["literal", ["s1"]]], closed, byDay]);
    }
    // and the key moves with it, so the engine repaints the day
    expect(statusInput({ index: INDEX, day: JUL20 }, salmon(JUL20)).valuesKey)
      .not.toEqual(statusInput({ index: INDEX, day: JUL20 }).valuesKey);
  });

  it("closed as the book's closed is: its stripe shows; gear marks and no-access dots do not", () => {
    const v = valuesOf(JUL20);
    expect(closedCuePaint("profile", t, v)["line-opacity"]).toEqual(["case", ["in", SP, ["literal", ["s1"]]], 1, 0]);
    const method = { fly: ["m1"], bait: [] } as unknown as ChannelValues["method"];
    expect(JSON.stringify(gearShown("stream", "profile", "fly", { ...v, method })))
      .toContain(JSON.stringify(["!", ["in", SP, ["literal", ["s1"]]]]));
    const noAccess = { ...v, status: { ...v.status!, no_access: ["p9"] } };
    expect(JSON.stringify(noAccessShown("stream", "profile", noAccess))).toContain(JSON.stringify(["!", ["in", SP, ["literal", ["s1"]]]]));
    // (T937) the Salmon mode takes its no-access dots from the status day, so a finfish closure holds them off there too
    expect(JSON.stringify(noAccessShown("stream", "salmon", noAccess))).toContain(JSON.stringify(["!", ["in", SP, ["literal", ["s1"]]]]));
  });

  it("(T377) a legend isolate: shown alone under Closed, the neutral grey under any other class", () => {
    expect(valuesOf(JUL20, "closed").status!.finfish).toEqual(["s1"]);
    const away = valuesOf(JUL20, "own").status!;
    expect([away.finfish, away.finfishNone]).toEqual([undefined, ["s1"]]);
    const expr = paintFor("stream", "profile", t, valuesOf(JUL20, "own"))["line-color"] as unknown[];
    const missing = (STYLE_META.colorModes.stream!.profile as { missing: { token: string } }).missing.token;
    expect(expr.slice(0, 3)).toEqual(["case", ["in", SP, ["literal", ["s1"]]], t[missing]]);
    expect(t[missing]).not.toEqual(t["color.status.closed"]);
    // greyed is not closed: no stripe
    expect(closedCuePaint("profile", t, valuesOf(JUL20, "own"))["line-opacity"]).toBe(0);
  });

  it("through the engine: the salmon arriving is one repaint of the status day, no feature-state call", () => {
    const states: string[] = [], paints: string[] = [];
    const h: MapHandle = {
      setVisibility: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
      setPaint: (l, p, v) => { if (l === "stream" && p === "line-color") paints.push(JSON.stringify(v)); },
    };
    const target: LabelTarget = { setState: (l, id, s) => states.push(`${l} ${id} ${JSON.stringify(s)}`), clearAll: () => {} };
    const tiles: TileSections[] = [{ stream: [100, 7] }];
    const renderer: TileRenderer = { loaded: () => tiles, restore: () => {} };
    const adapter = baseAdapter("web");
    adapter.setLayerMode(h, "stream", "profile", "light");
    const c = new Colouring(adapter);
    c.set(h, target, renderer, "status", statusInput({ index: INDEX, day: JUL20 }), "light");
    c.set(h, target, renderer, "salmon", salmonInput(salmon(JUL20)), "light");
    expect(states).toEqual(['stream 100 {"p":"p0"}', 'stream 7 {"p":"p0"}', 'stream 100 {"sp":"s1"}', 'stream 7 {"sp":"s0"}']);
    states.splice(0); paints.splice(0);
    c.set(h, target, renderer, "status", statusInput({ index: INDEX, day: JUL20 }, salmon(JUL20)), "light");
    expect(states).toEqual([]);
    expect(paints).toEqual([JSON.stringify(paintFor("stream", "profile", t, valuesOf(JUL20))["line-color"])]);
  });
});
