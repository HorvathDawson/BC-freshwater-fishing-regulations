/**
 * (T848) THE STREAM'S WIDTH NO LONGER READS FEATURE-STATE ON THE BUSY LAYERS — AND THE MAP LOOKS THE SAME.
 *
 * The web map lagged zooming out over Intata Reach: each labelled section re-evaluated every feature-state paint property
 * of every layer it is in, and the stream's width (floors for the highlight, T470's heavier closed line) was one, shared
 * by nine layers. Now `stream` is drawn at the width zoom and order give, `stream-heavy` draws the lit and closed sections
 * over it at the width they were drawn at, and each mark reads the drawn width only where it shows.
 *
 * The proof is by EVALUATION (MapLibre's own evaluator) over zooms, stream orders and feature-states, against `paintFor`'s
 * own stream paint (the one layer as it was): what is drawn is the same width, colour and opacity, nothing is drawn twice,
 * and every mark is the width it was wherever it can be seen.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import type { DayProfiles, MethodDay } from "@app/core";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { CLOSED_CUE_LAYER, GEAR_LAYERS, ISSUE_LAYERS, SALMON_CASING_LAYER, NOACCESS_LAYER, NOBASELINE_LAYER, RUNS_PASS_LAYER, SELECTION_CASING, SELECTION_GLOW,
         STREAM_HEAVY, casingPaint, closedCuePaint, gearPaint, issuePaint, markShownIn, markWidth, paintFor, runsPassPaint,
         splitStream, streamWidths, type StreamMark } from "../packages/map/src/paint";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { PMTILES } from "../packages/map/src/tile-access";
import { resolveTheme, type ChannelValues } from "../packages/map/src/style";

const DAY: DayProfiles = { closed: ["p1"], own: ["p2"], base: ["b"], no_access: ["n"], none: ["p3"] };
const METHOD: MethodDay = { none: ["m0"], bait: ["m1"], fly: ["m2"] };
const SALMON = { open: ["s0"], release: ["s1"], closed: ["s3"], check: [], none: [] };
const VALUES = { status: DAY, method: METHOD, salmon: SALMON } as unknown as ChannelValues;

const ZOOMS = [4, 6.5, 7, 8, 9.5, 11, 11.5, 12, 14, 16];
const ORDS = [1, 3, 5, 8];
const FLAGS = [{}, { selected: true }, { part: true }, { water: true }, { inseason: true }, { part: true, inseason: true }];
const LABELS = [{ p: "p1", sp: "s3" }, { p: "p2", sp: "s0" }, { p: "n", sp: "s1" }, { p: "", sp: "" }];
const STATES = LABELS.flatMap((l, i) => FLAGS.map((f, j) => ({ ...l, gm: ["m0", "m1", "m2"][(i + j) % 3], rt: "pass", gi: "1", ...f })));

const spec = (prop: string) =>
  (latest as unknown as Record<string, Record<string, unknown>>)[`paint_${prop.split("-")[0]}`]![prop]!;
function ev(prop: string, value: unknown) {
  const v = value ?? (spec(prop) as { default?: unknown }).default;
  const e = createPropertyExpression(Array.isArray(v) && typeof v[0] === "number" ? ["literal", v] : v, prop as never, spec(prop) as never);
  if (e.result !== "success") throw new Error(`${prop} does not parse: ${JSON.stringify(e.value)}`);
  return (zoom: number, ord: number, state: Record<string, unknown>) => {
    const out = e.value.evaluate({ zoom } as never, { type: 2, properties: { ord } } as never, state as never);
    return typeof out === "number" ? Math.round(out * 1e6) / 1e6 : JSON.stringify(out);
  };
}
const reads = (e: unknown) => String(JSON.stringify(e)).includes('"feature-state"');

const CASES = [["profile", VALUES], ["salmon", VALUES], ["runs", VALUES], ["standing", VALUES], ["plain", VALUES],
               ["profile", null]] as const;

describe("(T848) the stream split in two draws exactly what the one layer drew", () => {
  for (const theme of ["light", "dark", "cvd"]) for (const [mode, values] of CASES) {
    it(`${theme} ${mode}${values ? "" : " (no day)"}: same width, colour and opacity; never drawn twice`, () => {
      const t = resolveTheme(theme);
      const one = paintFor("stream", mode, t, values);
      const sw = streamWidths(mode, t, values);
      expect(JSON.stringify(sw.drawn)).toBe(JSON.stringify(one["line-width"]));
      const { stream, heavy } = splitStream(one, sw);
      expect(reads(stream["line-width"])).toBe(false);
      const f = Object.fromEntries(["line-width", "line-opacity", "line-color"].flatMap((p) =>
        [[`one ${p}`, ev(p, one[p])], [`s ${p}`, ev(p, stream[p])], [`h ${p}`, ev(p, heavy[p])]]));
      const bad: string[] = [];
      for (const z of ZOOMS) for (const o of ORDS) for (const s of STATES) {
        const so = f["s line-opacity"]!(z, o, s) as number, ho = f["h line-opacity"]!(z, o, s) as number;
        if (so > 0 && ho > 0) bad.push(`drawn twice z${z} ${JSON.stringify(s)}`);
        const by = ho > 0 ? "h" : "s";
        for (const p of ["line-width", "line-opacity", "line-color"]) {
          if (p !== "line-opacity" && Number(f["one line-opacity"]!(z, o, s)) === 0) continue;
          const [a, b] = [f[`one ${p}`]!(z, o, s), f[`${by} ${p}`]!(z, o, s)];
          if (a !== b) bad.push(`${p} z${z} ord ${o} ${JSON.stringify(s)}: ${a} -> ${b}`);
        }
      }
      expect(bad.slice(0, 5)).toEqual([]);
    });
  }

  const MARKS: [StreamMark, (mode: string, w: unknown) => Record<string, unknown>][] = [
    ["casing", (_m, w) => casingPaint(w, resolveTheme("light"))],
    ["gear", (m, w) => gearPaint("stream", m, "fly", resolveTheme("light"), VALUES, w)],
    ["closed", (m, w) => closedCuePaint(m, resolveTheme("light"), VALUES, w)],
    ["runs", (m, w) => runsPassPaint(m, resolveTheme("light"), w)],
    ["issue", (m, w) => issuePaint("stripe", m, resolveTheme("light"), w)],
  ];
  for (const [mark, paintOf] of MARKS) {
    it(`${mark}: the width it was wherever it shows, in every mode; no feature-state where its mode never shows it`, () => {
      const t = resolveTheme("light");
      for (const mode of ["profile", "salmon", "runs", "standing", "plain"]) {
        const sw = streamWidths(mode, t, VALUES);
        const was = paintOf(mode, sw.drawn), now = paintOf(mode, markWidth(mark, mode, sw));
        const [op, w0, w1] = [ev("line-opacity", was["line-opacity"]), ev("line-width", was["line-width"]), ev("line-width", now["line-width"])];
        const bad: string[] = [];
        for (const z of ZOOMS) for (const o of ORDS) for (const s of STATES)
          if (Number(op(z, o, s)) > 0 && w0(z, o, s) !== w1(z, o, s)) bad.push(`${mode} z${z} ord ${o} ${JSON.stringify(s)}`);
        expect(bad.slice(0, 3)).toEqual([]);
        if (mark !== "casing" && mark !== "glow" && !markShownIn(mark, mode)) expect([mode, reads(now["line-width"])]).toEqual([mode, false]);
      }
    });
  }
});

describe("(T848) what the adapter hands the map", () => {
  function painted(mode: string) {
    const paint: Record<string, Record<string, unknown>> = {}, shown: Record<string, boolean> = {};
    const h: MapHandle = { setVisibility: (l, v) => { shown[l] = v; }, setPaint: (l, p, v) => { (paint[l] ??= {})[p] = v; },
                           setFeatureState: () => {}, clearFeatureStates: () => {} };
    const a = baseAdapter("web");
    a.setLayerMode(h, "stream", mode, "light");
    a.setChannel(h, "status", DAY, "light");
    a.setChannel(h, "method", METHOD, "light");
    return { paint, shown };
  }

  it("Standard: the stream's width reads no feature-state; only the heavy line and the marks that can be lit do", () => {
    const { paint } = painted("profile");
    // the water copy's layers are on their own source: one water's sections, not the province's
    const stateful = Object.entries(paint).filter(([l, p]) => !l.startsWith("water-copy") && reads(p["line-width"]))
      .map(([l]) => l).sort();
    expect(reads(paint["stream"]!["line-width"])).toBe(false);
    expect(stateful).toEqual([CLOSED_CUE_LAYER, SELECTION_CASING, SELECTION_GLOW, GEAR_LAYERS.stream.bait,
                              GEAR_LAYERS.stream.fly, STREAM_HEAVY].sort());
  });

  it("(fix 2) a mark its mode never shows is not drawn at all; one it can show is", () => {
    const want = (mode: string) => ({
      [NOBASELINE_LAYER]: mode === "standing", [NOACCESS_LAYER]: mode === "profile" || mode === "salmon",
      [GEAR_LAYERS.stream.fly]: mode === "profile" || mode === "salmon", [RUNS_PASS_LAYER]: mode === "runs",
      [CLOSED_CUE_LAYER]: mode === "profile" || mode === "salmon", [ISSUE_LAYERS.stripe]: mode === "salmon",
      [SALMON_CASING_LAYER]: mode === "salmon" });
    for (const mode of ["profile", "salmon", "runs", "standing", "plain"]) {
      const { shown } = painted(mode);
      const got = Object.fromEntries(Object.keys(want(mode)).map((l) => [l, shown[l]]));
      expect([mode, got]).toEqual([mode, want(mode)]);
      // the first frame agrees with the adapter
      const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, "light", { stream: mode }, PMTILES) as
        { layers: { id: string; layout?: { visibility?: string } }[] };
      const first = Object.fromEntries(Object.keys(want(mode)).map((l) =>
        [l, s.layers.find((x) => x.id === l)?.layout?.visibility !== "none"]));
      expect([mode, "first frame", first]).toEqual([mode, "first frame", want(mode)]);
    }
  });

  it("the heavy line sits directly over the stream on the first frame, and follows its zoom floor", () => {
    const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, "light", {}, PMTILES) as { layers: { id: string }[] };
    const ids = s.layers.map((l) => l.id);
    expect(ids.indexOf(STREAM_HEAVY)).toBe(ids.indexOf("stream") + 1);
    const ranges: Record<string, number> = {};
    const h: MapHandle = { setVisibility: () => {}, setPaint: () => {}, setFeatureState: () => {}, clearFeatureStates: () => {},
                           setZoomRange: (l, z) => { ranges[l] = z; } };
    baseAdapter("web").setLayerMode(h, "stream", "profile", "light");
    expect(ranges[STREAM_HEAVY]).toBe(ranges["stream"]);
  });
});
