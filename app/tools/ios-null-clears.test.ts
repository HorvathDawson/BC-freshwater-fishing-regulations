/**
 * (T832) ON iOS A RIVER LETS GO OF ITS COLOUR. React Native's iOS bridge drops a null-valued key (T826), so a feature-state
 * write `{standing: null}` reached MapLibre as `{}` and the river kept the colour of the last horizon or feed. The engine
 * now writes each key's non-null sentinel (`sections.ts` `unsetValue`), the style reads it as `missing`, and NO write the
 * phone sends — a relabel, a clear, a basin — carries a null. Pinned through the phone's own queue (`StateQueue`) and an
 * iOS bridge that drops null keys, the colours EVALUATED by MapLibre's own evaluator.
 */
import { describe, expect, it } from "vitest";
import { createPropertyExpression, latest } from "@maplibre/maplibre-gl-style-spec";
import { StateQueue } from "../packages/map/src/native/useColouring";
import { Colouring, standingInput, type LabelTarget, type TileSections } from "../packages/map/src/colouring";
import { baseAdapter, type MapHandle } from "../packages/map/src/adapters/contract";
import { colorExpression, resolveTheme, type StandingValues } from "../packages/map/src/style";
import { nobaselineOpacity } from "../packages/map/src/paint";

type State = Record<string, unknown>;
const t = resolveTheme("light");
const spec = (prop: string) => (latest as unknown as Record<string, Record<string, unknown>>)["paint_line"]![prop]!;
const evaluate = (expr: unknown, prop: string) => {
  const e = createPropertyExpression(expr as never, prop as never, spec(prop) as never);
  if (e.result !== "success") throw new Error(`${prop} does not parse`);
  return (s: State) => e.value.evaluate({ zoom: 10 } as never, { type: 2, properties: {} } as never, s as never);
};
const colourOf = (mode: string, state: State) => JSON.stringify(evaluate(colorExpression("stream", mode, t), "line-color")(state));
const missing = (mode: string) => colourOf(mode, {});

/** MapLibre Native's state per feature: each write merged in (what iOS hands it). */
function atlas() {
  const cur = new Map<string, State>();
  return {
    write: (layer: string, ids: string[], states: State[]) =>
      ids.forEach((id, i) => cur.set(`${layer}/${id}`, { ...(cur.get(`${layer}/${id}`) ?? {}), ...states[i] })),
    raw: (layer: string, id: string): State => ({ ...(cur.get(`${layer}/${id}`) ?? {}) }),
  };
}
/** iOS's bridge: a null-valued key never arrives */
const bridged = (s: State) => Object.fromEntries(Object.entries(s).filter(([, v]) => v !== null));

/** A phone: the engine, its queue, the bridge, the map's state. Every write sent is kept. */
function iosPhone() {
  const m = atlas(), q = new StateQueue(), sent: State[] = [];
  const flush = () => {
    for (const [l, ids, st] of q.drain()) { sent.push(...st); m.write(l, ids, st.map(bridged)); }
  };
  const target: LabelTarget = { setState: (l, id, s) => q.set(l, String(id), s), clearAll: (l) => q.clear(l) };
  const tiles: TileSections[] = [{ stream: [10, 11] }];
  const h: MapHandle = { setVisibility: () => {}, setPaint: () => {}, clearFeatureStates: () => {},
                         setFeatureState: (_l, _id, s) => { sent.push(s); } };
  const c = new Colouring(baseAdapter("native"));
  const set = (ch: "standing" | "runs", input: Parameters<Colouring["set"]>[4]) => {
    c.set(h, target, { loaded: () => tiles, restore: () => {} }, ch, input, "light");
    flush();
  };
  return { m, sent, set };
}
const CLASSES = { classOf: (s: number) => ({ 10: 0, 11: 1 } as Record<number, number>)[s] ?? null };
const feed = (xs: number[], basins: Record<string, number> = {}): StandingValues =>
  ({ section: (k: number) => xs[k] ?? NaN, basins } as unknown as StandingValues);

describe("(T832) on iOS a cleared label clears", () => {
  it("a horizon change that leaves a river nothing to say: its standing colour goes (missing), never the old one", () => {
    const p = iosPhone();
    p.set("standing", standingInput({ labels: CLASSES, values: feed([80, 20], { "08MF": 12 }) }));
    expect(colourOf("standing", p.m.raw("stream", "11"))).not.toBe(missing("standing"));
    p.set("standing", standingInput({ labels: CLASSES, values: feed([80]) }));
    expect([colourOf("standing", p.m.raw("stream", "11")), colourOf("standing", p.m.raw("stream", "10"))])
      .toEqual([missing("standing"), colourOf("standing", { standing: 80 })]);
    // the basin that lost its value too; nothing sent anywhere is a null
    expect(p.sent.filter((s) => Object.values(s).includes(null))).toEqual([]);
    expect(p.sent).toContainEqual({ standing: "" });
  });

  it("MUTATION PIN: a null write, as before T832, never reaches iOS and the old colour stays", () => {
    const m = atlas();
    m.write("stream", ["11"], [{ standing: 20 }]);
    m.write("stream", ["11"], [bridged({ standing: null })]);
    expect(colourOf("standing", m.raw("stream", "11"))).toBe(colourOf("standing", { standing: 20 }));
  });

  it("a run label cleared, and the whole channel withdrawn (a clear): every key is written non-null and reads as missing", () => {
    const p = iosPhone();
    const runs = (on: Record<number, string | null>, key: string) => ({
      labels: { key: "rt", of: CLASSES, label: (id: number) => on[id] }, valuesKey: key, relabels: true, values: () => null });
    p.set("runs", runs({ 10: "pass", 11: "spawn" }, "a"));
    expect(colourOf("runs", p.m.raw("stream", "11"))).not.toBe(missing("runs"));
    p.set("runs", runs({ 10: "pass", 11: null }, "b"));
    expect(colourOf("runs", p.m.raw("stream", "11"))).toBe(missing("runs"));
    p.set("runs", null);
    expect(colourOf("runs", p.m.raw("stream", "10"))).toBe(missing("runs"));
    expect(p.sent.length).toBeGreaterThan(0);
    expect(p.sent.filter((s) => Object.values(s).includes(null))).toEqual([]);
  });

  it("the paint reads the sentinel as unset wherever a stream label is read; the no-baseline dash is not lit by it", () => {
    expect(colourOf("standing", { standing: "" })).toBe(missing("standing"));
    expect(colourOf("runs", { rt: "" })).toBe(missing("runs"));
    expect(colourOf("standing", { standing: 50 })).not.toBe(missing("standing"));
    const dash = evaluate(nobaselineOpacity("standing"), "line-opacity");
    expect([dash({ standing: "" }), dash({ standing: -1 })]).toEqual([0, 1]);
  });
});
