/**
 * (T1029, look B, owner 2026-11-24) THE ONE-BANK CUE in the style a map is handed: the stretch drawn wider and split down
 * the middle — the bank's half in the BANK's chain's answer that day (feature-state `bk` "Ls1": left of the drawn line,
 * chain s1), the other half in the line's own (`sp`) — and the closed cue on the bank's half only, only when the bank's
 * answer is closed. Checked by EVALUATING (MapLibre's own evaluator) the style `nativeStyle` builds with the day
 * installed — the adapter's own paint, the call every later day repaints with — in every theme; and the first frame
 * (`runtimeStyle`, no day) draws none. (T1029 Q6) The Standard map draws the rule book's one-half closures the same way
 * from its own label `bb`; each colouring reads only its own key.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { BANK_CUE_LAYERS, BANK_LAYERS, BANK_MIN_PX, BANK_MINZOOM, BANK_WIDEN, CLOSED_CUE_LAYER, GEAR_BANK_LAYERS, GEAR_EDGE_PX, GEAR_LAYERS, ISSUE_LAYERS,
         bankCuePaint, bankHalfPaint } from "../packages/map/src/paint";
import { profileLabel, type StatusIndex, type WaterStatus } from "@app/core";
import { statusInput } from "../packages/map/src/colouring";
import { nativeStyle } from "../packages/map/src/native-style";
import { maplibreDialect } from "../packages/map/src/native/maplibre/dialect";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { resolveTheme, type ChannelValues } from "../packages/map/src/style";

type Layer = { id: string; type: string; "source-layer"?: string; paint?: Record<string, unknown>; layout?: Record<string, unknown>; minzoom?: number };
const specOf = (prop: string) =>
  (latest as unknown as Record<string, Record<string, { default?: unknown }>>)[`paint_line`]![prop]!;
function value(layer: Layer, prop: string, zoom: number, props: Record<string, unknown>, state: Record<string, unknown> = {}) {
  const v = layer.paint?.[prop] ?? specOf(prop).default;
  const e = createPropertyExpression(Array.isArray(v) && typeof v[0] === "number" ? ["literal", v] : v, prop as never, specOf(prop) as never);
  if (e.result !== "success") throw new Error(`${layer.id} ${prop} does not parse: ${JSON.stringify(e.value)}`);
  return e.value.evaluate({ zoom } as never, { type: 2, properties: props } as never, state as never) as unknown;
}
const colour = (v: unknown) => String((v as { toString(): string }).toString());
const hex = (h: unknown) => colour(value({ id: "x", type: "line", paint: { "line-color": h } }, "line-color", 14, {}));

// the day: chain s0 open, the bank's chain s1 closed; the status day marks "n" no access
const SALMON = { open: ["s0"], closed: ["s1"], release: ["s2"], check: ["s3"], none: ["s4"] };
const STATUS = { closed: ["p0"], own: ["p1"], base: ["b"], no_access: ["n"], none: ["p2"] };
const VALUES = { status: STATUS, salmon: SALMON, method: null, standing: null, stocked: null, survey: null, runs: null,
                 issue: null, bank: null } as unknown as ChannelValues;
const AT = { atlas: "file:///d/a.pmtiles", basemap: "file:///d/b.pmtiles", outside: "file:///d/o.geojson", assets: "file:///d/m/" };
function handed(theme: string, mode: string, values: ChannelValues = VALUES): Layer[] {
  const { style, errors } = nativeStyle({ at: AT, theme, view: "plain", modes: { stream: mode }, hide: [], groups: {},
    basemap: "map" as const, gauges: '{"type":"FeatureCollection","features":[]}', values } as never, maplibreDialect);
  expect(errors).toEqual([]);
  return (style as unknown as { layers: Layer[] }).layers;
}
const firstFrame = (theme: string, mode: string) =>
  (runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles", outside: "o.geojson" } as never, theme, { stream: mode }) as { layers: Layer[] }).layers;

const WEST = { bk: "Ls1", sp: "s0", p: "p1" };
const ORD7 = { ord: 7 };
const ALL = [...Object.values(BANK_LAYERS), ...Object.values(BANK_CUE_LAYERS)];

describe("(T1029) one bank: a wider line split down the middle", () => {
  for (const theme of ["light", "dark", "cvd"]) {
    const all = handed(theme, "salmon");
    const get = (id: string) => all.find((l) => l.id === id)!;
    const stream = get("stream");
    const t = resolveTheme(theme);
    it(`${theme}: each half is half the widened stream, a quarter of it to its own side`, () => {
      for (const [side, sign] of [["right", 1], ["left", -1]] as const) {
        const half = get(BANK_LAYERS[side]), cue = get(BANK_CUE_LAYERS[side]);
        for (const z of [13, 14, 16]) for (const ord of [{ ord: 1 }, ORD7]) {
          const w = Number(value(stream, "line-width", z, ord));
          const full = Math.max(w * BANK_WIDEN, BANK_MIN_PX);
          expect(Number(value(half, "line-width", z, ord, WEST))).toBeCloseTo(full / 2, 4);
          expect(Number(value(half, "line-offset", z, ord, WEST))).toBeCloseTo(sign * full / 4, 4);
          expect(Number(value(cue, "line-offset", z, ord, WEST))).toBeCloseTo(sign * full / 4, 4);
        }
      }
      // the floor is reached: a small creek at z13 is drawn at the minimum, not 1.5 times its hairline
      expect(Number(value(stream, "line-width", 13, { ord: 1 })) * BANK_WIDEN).toBeLessThan(BANK_MIN_PX);
    });
    it(`${theme}: the bank's half wears the bank chain's answer, the other half the line's own; no bank, nothing`, () => {
      const left = get(BANK_LAYERS.left), right = get(BANK_LAYERS.right);
      const closed = hex(t["color.salmon.closed"]), open = hex(t["color.salmon.open"]);
      expect(closed).not.toBe(open);
      expect(colour(value(left, "line-color", 14, ORD7, WEST))).toBe(closed);          // s1, the bank's chain: closed
      expect(colour(value(right, "line-color", 14, ORD7, WEST))).toBe(open);           // s0, the line's own: open
      expect(colour(value(right, "line-color", 14, ORD7, WEST))).toBe(colour(value(stream, "line-color", 14, ORD7, WEST)));
      // the bank on the other side: the halves swap
      const east = { ...WEST, bk: "Rs1" };
      expect(colour(value(right, "line-color", 14, ORD7, east))).toBe(closed);
      expect(colour(value(left, "line-color", 14, ORD7, east))).toBe(open);
      // the bank's chain open that day: both halves open
      expect(colour(value(left, "line-color", 14, ORD7, { ...WEST, bk: "Ls0" }))).toBe(open);
      for (const half of [left, right]) {
        expect(value(half, "line-opacity", 14, ORD7, WEST)).toBe(1);
        expect(value(half, "line-opacity", 14, ORD7, { sp: "s0", p: "p1" })).toBe(0);   // no bank: nothing drawn
        expect(value(half, "line-opacity", 14, ORD7, { ...WEST, p: "n" })).toBe(0);     // no access: the dots alone
      }
    });
    it(`${theme}: the closed cue rides the bank's half only, and only when the bank's answer is closed`, () => {
      const left = get(BANK_CUE_LAYERS.left), right = get(BANK_CUE_LAYERS.right);
      expect(value(left, "line-opacity", 14, ORD7, WEST)).toBe(1);
      expect(value(right, "line-opacity", 14, ORD7, WEST)).toBe(0);
      expect(value(right, "line-opacity", 14, ORD7, { ...WEST, bk: "Rs1" })).toBe(1);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, bk: "Ls0" })).toBe(0);   // the bank open: no cue
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, sp: "s1", bk: "Ls0" })).toBe(0);   // the LINE closed is not the bank's
      expect(value(left, "line-opacity", 14, ORD7, { sp: "s1" })).toBe(0);
      expect(value(left, "line-opacity", 14, ORD7, { ...WEST, p: "n" })).toBe(0);
      expect(left.paint?.["line-dasharray"]).toEqual(get("stream-closed").paint?.["line-dasharray"]);
    });
  }
  /*
   * (T1029 Q6, owner 2026-11-24: "Yes, stripe it too") THE STANDARD MAP draws the rule book's one-half closures the same
   * way from its OWN label `bb` (the side of the drawn line the closed half is on): that half closed, the other the line's
   * own, the cue on the closed half. Each colouring reads only its own key: a section may carry both (Kitimat 1234098).
   */
  for (const theme of ["light", "dark", "cvd"]) {
    const all = handed(theme, "profile");
    const get = (id: string) => all.find((l) => l.id === id)!;
    const stream = get("stream");
    const t = resolveTheme(theme);
    const BOOK = { bb: "L", sp: "s0", p: "p1" };
    it(`${theme}: Standard draws the book's closed half from \`bb\`: closed there, the line's own across, the cue on it`, () => {
      const left = get(BANK_LAYERS.left), right = get(BANK_LAYERS.right);
      const cueL = get(BANK_CUE_LAYERS.left), cueR = get(BANK_CUE_LAYERS.right);
      const closed = hex(t["color.status.closed"]), own = colour(value(stream, "line-color", 14, ORD7, BOOK));
      expect(own).toBe(hex(t["color.status.own"]));
      expect(colour(value(left, "line-color", 14, ORD7, BOOK))).toBe(closed);
      expect(colour(value(right, "line-color", 14, ORD7, BOOK))).toBe(own);
      expect(colour(value(right, "line-color", 14, ORD7, { ...BOOK, bb: "R" }))).toBe(closed);
      expect(colour(value(left, "line-color", 14, ORD7, { ...BOOK, bb: "R" }))).toBe(own);
      for (const half of [left, right]) {
        expect(value(half, "line-opacity", 14, ORD7, BOOK)).toBe(1);
        expect(value(half, "line-opacity", 14, ORD7, { sp: "s0", p: "p1" })).toBe(0);     // no half: nothing drawn
        expect(value(half, "line-opacity", 14, ORD7, { ...BOOK, p: "n" })).toBe(0);       // no access: the dots alone
        expect(value(half, "line-opacity", 14, ORD7, WEST)).toBe(0);                      // a one-bank place's `bk`: not Standard's
      }
      expect([value(cueL, "line-opacity", 14, ORD7, BOOK), value(cueR, "line-opacity", 14, ORD7, BOOK)]).toEqual([1, 0]);
      expect([value(cueL, "line-opacity", 14, ORD7, { ...BOOK, bb: "R" }), value(cueR, "line-opacity", 14, ORD7, { ...BOOK, bb: "R" })]).toEqual([0, 1]);
      expect(value(cueL, "line-opacity", 14, ORD7, { ...BOOK, p: "n" })).toBe(0);
      expect(value(cueL, "line-opacity", 14, ORD7, WEST)).toBe(0);
      for (const id of ALL) expect([id, get(id).layout?.["visibility"]]).not.toEqual([id, "none"]);
    });
    it(`${theme}: a section with both keys (Kitimat 1234098): each colouring reads its own, the Salmon map never \`bb\``, () => {
      const both = { bk: "Ls1", bb: "R", sp: "s0", p: "p1" };
      const salmon = handed(theme, "salmon");
      const s = (id: string) => salmon.find((l) => l.id === id)!;
      // Salmon: the bank (left, chain s1 closed); the book's right half is not its business
      expect(colour(value(s(BANK_LAYERS.left), "line-color", 14, ORD7, both))).toBe(hex(t["color.salmon.closed"]));
      expect(colour(value(s(BANK_LAYERS.right), "line-color", 14, ORD7, both))).toBe(hex(t["color.salmon.open"]));
      expect([value(s(BANK_CUE_LAYERS.left), "line-opacity", 14, ORD7, both), value(s(BANK_CUE_LAYERS.right), "line-opacity", 14, ORD7, both)]).toEqual([1, 0]);
      for (const id of ALL) expect(value(s(id), "line-opacity", 14, ORD7, { bb: "L", sp: "s0", p: "p1" })).toBe(0);
      // Standard: the book's half (right) closed, the left the line's own
      expect(colour(value(get(BANK_LAYERS.right), "line-color", 14, ORD7, both))).toBe(hex(t["color.status.closed"]));
      expect(colour(value(get(BANK_LAYERS.left), "line-color", 14, ORD7, both))).toBe(hex(t["color.status.own"]));
      expect([value(get(BANK_CUE_LAYERS.left), "line-opacity", 14, ORD7, both), value(get(BANK_CUE_LAYERS.right), "line-opacity", 14, ORD7, both)]).toEqual([0, 1]);
    });
  }
  it("Standard under a legend isolate: the closed half follows closed — grey and no cue unless Closed is the class shown", () => {
    // the status day as the Map tab's status input makes it (`statusInput`, `only` the legend's tap): profiles 0 closed,
    // 1 own, 2 base
    const index = {
      handles: "0".repeat(16), reachDigest: "0".repeat(16), sections: 3, waters: 0, profiles: 3,
      profileOf: (s: unknown) => Number(s) - 10, profileCodeOn: (p: number) => (["closed", "own", "base"] as const)[p]!,
      codeOn: () => "base", waterCodeOn: () => "base",
    } as unknown as StatusIndex;
    const t = resolveTheme("light");
    const at = (only: WaterStatus | null) => {
      const status = statusInput({ index, day: new Date(2026, 5, 1), only }).values() as ChannelValues["status"];
      const all = handed("light", "profile", { ...VALUES, status });
      return (id: string) => all.find((l) => l.id === id)!;
    };
    const closedLabel = profileLabel(index, 10), ownLabel = profileLabel(index, 11);
    const book = { bb: "L", p: ownLabel, sp: "s0" };
    for (const only of [null, "closed"] as const) {
      const get = at(only);
      expect(colour(value(get(BANK_LAYERS.left), "line-color", 14, ORD7, book))).toBe(hex(t["color.status.closed"]));
      expect(value(get(BANK_CUE_LAYERS.left), "line-opacity", 14, ORD7, book)).toBe(1);
    }
    for (const only of ["own", "base"] as const) {
      const get = at(only);
      // what a closed line wears under this isolate: the mode's missing grey
      const greyed = colour(value(get("stream"), "line-color", 14, ORD7, { p: closedLabel }));
      expect(greyed).not.toBe(hex(t["color.status.closed"]));
      expect([only, colour(value(get(BANK_LAYERS.left), "line-color", 14, ORD7, book))]).toEqual([only, greyed]);
      expect([only, value(get(BANK_CUE_LAYERS.left), "line-opacity", 14, ORD7, book)]).toEqual([only, 0]);
      // the other half: the line's own under the isolate
      expect(colour(value(get(BANK_LAYERS.right), "line-color", 14, ORD7, book)))
        .toBe(colour(value(get("stream"), "line-color", 14, ORD7, book)));
    }
  });
  it("Standard with no status day, and its first frame: nothing drawn", () => {
    const t = resolveTheme("light");
    for (const side of ["left", "right"] as const) {
      expect(bankHalfPaint(side, "profile", t, { ...VALUES, status: null }, 2)["line-opacity"]).toBe(0);
      expect(bankCuePaint(side, "profile", t, { ...VALUES, status: null }, 2)["line-opacity"]).toBe(0);
    }
    const first = firstFrame("light", "profile");
    for (const id of ALL) expect(value(first.find((x) => x.id === id)!, "line-opacity", 14, ORD7, { bb: "L", p: "p1" })).toBe(0);
  });
  it("no salmon day installed, and the first frame: nothing drawn", () => {
    const noDay = handed("light", "salmon", { ...VALUES, salmon: null });
    for (const layers of [noDay, firstFrame("light", "salmon")])
      for (const id of ALL) expect(value(layers.find((x) => x.id === id)!, "line-opacity", 14, ORD7, WEST)).toBe(0);
  });
  it("drawn from zoom 13 up only (owner 2026-11-24): every bank layer starts there", () => {
    expect(BANK_MINZOOM).toBe(13);
    for (const id of ALL) expect(handed("light", "salmon").find((l) => l.id === id)?.minzoom).toBe(13);
  });
  it("hidden on the plain map (no bucket of its own there)", () => {
    for (const layers of [handed("light", "plain"), firstFrame("light", "plain")])
      for (const id of ALL) expect(layers.find((l) => l.id === id)?.layout?.["visibility"]).toBe("none");
  });
  it("no label test is an `in` scan over the bank's label (T903): a `match`, the same answer", () => {
    // a finfish closure that day puts a label test over the chain into the line's colour (the no-access grey held off it)
    const fin = { ...VALUES, status: { ...STATUS, finfish: ["s3"] } } as unknown as ChannelValues;
    const all = handed("light", "salmon", fin);
    const stream = all.find((l) => l.id === "stream")!, left = all.find((l) => l.id === BANK_LAYERS.left)!;
    expect(JSON.stringify(stream.paint?.["line-color"])).toMatch(/\["in",\["coalesce",\["feature-state","sp"\]/);
    for (const id of ALL) expect(JSON.stringify(all.find((l) => l.id === id)!.paint)).not.toMatch(/\["in",\["slice"/);
    // no access on a finfish-closed chain is not grey: the bank's half reads the BANK's chain for it
    const t = resolveTheme("light");
    expect(colour(value(left, "line-color", 14, ORD7, { bk: "Ls3", sp: "s0", p: "n" }))).not.toBe(hex(t["color.status.no_access"]));
    expect(colour(value(left, "line-color", 14, ORD7, { bk: "Ls1", sp: "s3", p: "n" }))).toBe(hex(t["color.status.no_access"]));
  });
});

/**
 * (T1029, owner 2026-11-25: "A: cues above the stripe") THE LINE'S OWN CUES ARE NEVER UNDER THE BANK STRETCH: the closed
 * cue (T377) and the issue stripe (T759) are drawn after the four bank layers, and the gear edge — a wider line UNDER the
 * stream elsewhere, which the widened stretch would hide — is drawn again after them, as two thin edges just outside the
 * stretch, on the sections and days the edge itself is shown and only where a bank half is drawn.
 */
describe("(T1029 A) the line's cues over the one-bank stretch", () => {
  const METHOD = { none: [], fly: ["mf"], bait: ["mb"] };
  const withGear = { ...VALUES, method: METHOD } as unknown as ChannelValues;
  const ABOVE = [CLOSED_CUE_LAYER, ISSUE_LAYERS.casing, ISSUE_LAYERS.stripe, ...Object.values(GEAR_BANK_LAYERS)];
  for (const theme of ["light", "dark", "cvd"]) for (const mode of ["salmon", "profile"]) {
    it(`${theme} ${mode}: the closed cue, the issue stripe and the gear edges draw after every bank layer`, () => {
      for (const layers of [handed(theme, mode, withGear), firstFrame(theme, mode)]) {
        const at = (id: string) => layers.findIndex((l) => l.id === id);
        const lastBank = Math.max(...ALL.map(at));
        expect(Math.min(...ALL.map(at))).toBeGreaterThan(at("stream"));
        for (const id of ABOVE) expect([id, at(id) > lastBank]).toEqual([id, true]);
        // the gear edge itself stays under the line (its colour stays the read)
        for (const id of Object.values(GEAR_LAYERS.stream)) expect(at(id)).toBeLessThan(at("stream"));
      }
    });
  }
  it("a fully closed river reads closed across both halves: the closed cue shows on the stretch, above the halves", () => {
    for (const theme of ["light", "cvd"]) {
      const all = handed(theme, "salmon");
      const cue = all.find((l) => l.id === CLOSED_CUE_LAYER)!;
      // the line's own chain closed (s1) and the bank's closed (s1): the line's own cue is drawn on the stretch too
      expect(Number(value(cue, "line-opacity", 16, ORD7, { ...WEST, sp: "s1" }))).toBeGreaterThan(0);
      expect(Number(value(cue, "line-opacity", 16, ORD7, WEST))).toBe(0);         // the line open: none
    }
  });
  for (const [mode, state, off] of [["profile", { bb: "L", p: "p1" }, { p: "p1" }], ["salmon", WEST, { sp: "s0", p: "p1" }]] as const) {
    it(`${mode}: the gear edge over the stretch sits just outside it, where the edge is shown and a half is drawn`, () => {
      const all = handed("light", mode, withGear);
      const stream = all.find((l) => l.id === "stream")!;
      for (const mark of ["bait", "fly"] as const) {
        const edge = all.find((l) => l.id === GEAR_BANK_LAYERS[mark])!;
        const gm = mark === "bait" ? "mb" : "mf";
        for (const z of [13, 14, 16]) {
          const full = Math.max(Number(value(stream, "line-width", z, ORD7)) * BANK_WIDEN, BANK_MIN_PX);
          expect(Number(value(edge, "line-gap-width", z, ORD7, { ...state, gm }))).toBeCloseTo(full, 4);
        }
        expect(Number(value(edge, "line-width", 16, ORD7, { ...state, gm }))).toBeCloseTo(GEAR_EDGE_PX, 4);
        expect(Number(value(edge, "line-opacity", 16, ORD7, { ...state, gm }))).toBe(1);
        expect(Number(value(edge, "line-opacity", 16, ORD7, { ...off, gm }))).toBe(0);        // no bank half: none
        expect(Number(value(edge, "line-opacity", 16, ORD7, { ...state, gm: "m0" }))).toBe(0); // no gear rule: none
        expect(edge.minzoom).toBe(BANK_MINZOOM);
        expect("line-dasharray" in (edge.paint ?? {})).toBe(mark === "bait");
      }
    });
  }
  it("no gear day, no bank day, the plain map and the first frame: no gear edge over a stretch", () => {
    for (const layers of [handed("light", "profile"), handed("light", "salmon", { ...withGear, salmon: null }), firstFrame("light", "profile")])
      for (const id of Object.values(GEAR_BANK_LAYERS))
        expect(value(layers.find((x) => x.id === id)!, "line-opacity", 16, ORD7, { bb: "L", bk: "Ls1", sp: "s0", p: "p1", gm: "mb" })).toBe(0);
    for (const id of Object.values(GEAR_BANK_LAYERS))
      expect(handed("light", "plain").find((l) => l.id === id)?.layout?.["visibility"]).toBe("none");
  });
});
