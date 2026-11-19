/**
 * (T876) Which layers the label hold takes, and its state machine on its own (the map's wiring is
 * `label-hold.render.test.tsx`).
 */
import { describe, expect, it } from "vitest";
import { HELD_WHILE_ZOOMING, heldLayers, HOLD_SOURCES, labelGroup, LabelHold, LOST_END_MS, SETTLE_MS, type HoldTimers } from "./label-hold";
import { LAB_DEFAULT, LABEL_SWITCH, labStyle } from "./lab";
import { nativeStyle } from "./native-style";

type L = { id: string; type: string; source?: string; layout?: Record<string, unknown> };
const AT = { atlas: "file:///d/a.pmtiles", basemap: "file:///d/b.pmtiles", outside: "file:///d/o.geojson", assets: "file:///d/m/" };
const style = (p: Record<string, unknown> = {}) => nativeStyle({
  at: AT, theme: "light", view: "plain", modes: {}, hide: [], groups: {}, basemap: "map",
  gauges: '{"type":"FeatureCollection","features":[]}', ...p } as never).style as unknown as { layers: L[]; sources: Record<string, unknown> };

/** A clock the test turns by hand. */
function clock() {
  let t = 0, n = 0;
  const due = new Map<number, { at: number; fn: () => void }>();
  const timers: HoldTimers = { set: (fn, ms) => { due.set(++n, { at: t + ms, fn }); return n; }, clear: (h) => { due.delete(h as number); } };
  const tick = (ms: number) => {
    const end = t + ms;
    for (;;) {
      const next = [...due.entries()].filter(([, d]) => d.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!next) break;
      due.delete(next[0]); t = next[1].at; next[1].fn();
    }
    t = end;
  };
  return { timers, tick, pending: () => due.size };
}

describe("the layers the hold takes", () => {
  it("every symbol layer of the phone's style is a known kind, except the route map's own overlay", () => {
    const s = style();
    const unknown = s.layers.filter((l) => l.type === "symbol" && !labelGroup(l)).map((l) => l.id);
    expect(unknown.every((id) => id.startsWith("trace-"))).toBe(true);
    expect(s.layers.filter((l) => l.type !== "symbol").every((l) => labelGroup(l) === null)).toBe(true);
  });

  it("holds names and marks, never a gauge's reading; each from a source the style declares", () => {
    const s = style(), held = heldLayers(s);
    const ids = HOLD_SOURCES.flatMap((src) => held[src].map((l) => l.id));
    expect(ids).toContain("stream__label");
    expect(ids).toContain("places_locality");
    expect(ids).toContain("route-arrows");
    expect(ids).not.toContain("gauge-label");
    expect(HELD_WHILE_ZOOMING).not.toContain("gauges");
    for (const src of HOLD_SOURCES) if (held[src].length) expect(s.sources[src]).toBeTruthy();
  });

  it("the same ids in every view, mode and ground (the phone adopts them once, for the life of the map)", () => {
    const ids = (p: Record<string, unknown>) => JSON.stringify(heldLayers(style(p)), (k, v) => (k === "visibility" ? undefined : v));
    const today = ids({});
    for (const p of [{ view: "regulations" }, { view: "conditions" }, { basemap: "satellite" }, { modes: { stream: "salmon" } }])
      expect(ids(p)).toBe(today);
    // the dark basemap flavour has no POI layer: a light-first map's `pois` adopter makes an empty layer at none there
    const flat = (p: Record<string, unknown>) => HOLD_SOURCES.flatMap((s) => heldLayers(style(p))[s].map((l) => l.id));
    expect(flat({}).filter((id) => !flat({ theme: "dark" }).includes(id))).toEqual(["pois"]);
    expect(flat({ theme: "dark" }).every((id) => flat({}).includes(id))).toBe(true);
  });

  it("each held layer carries the style's own visibility: a kind the lab turned off is restored to none", () => {
    const off = heldLayers(labStyle(style() as never, { ...LAB_DEFAULT, [LABEL_SWITCH.water]: false }) as never);
    expect(off.atlas.find((l) => l.id === "stream__label")!.visibility).toBe("none");
    expect(off.basemap.find((l) => l.id === "places_locality")!.visibility).toBe("visible");
    expect(heldLayers(style()).atlas.find((l) => l.id === "obstacle")!.visibility).toBe("none");
  });
});

describe("the hold's state machine", () => {
  const machine = () => {
    const c = clock(), said: boolean[] = [];
    return { ...c, said, h: new LabelHold((v) => said.push(v), c.timers) };
  };

  it("goes when the zoom leaves its start, comes back SETTLE_MS after the rest, and says each change once", () => {
    const { h, said, tick } = machine();
    h.start(9);
    h.moving(9.005);
    expect(said).toEqual([]);
    h.moving(9.5); h.moving(10); h.moving(10.5);
    expect(said).toEqual([true]);
    h.rest();
    tick(SETTLE_MS - 1);
    expect(h.isHeld()).toBe(true);
    tick(1);
    expect(said).toEqual([true, false]);
  });

  it("a start with no zoom takes the first zoom reported as where it started", () => {
    const { h, said } = machine();
    h.start(undefined);
    h.moving(11);
    h.moving(11);
    expect(said).toEqual([]);
    h.moving(11.2);
    expect(said).toEqual([true]);
  });

  it("a lost end: LOST_END_MS after the last report the labels are back, and no timer is left", () => {
    const { h, said, tick, pending } = machine();
    h.start(9); h.moving(10);
    tick(LOST_END_MS - 1);
    expect(said).toEqual([true]);
    tick(1);
    expect(said).toEqual([true, false]);
    expect(pending()).toBe(0);
  });

  it("reset gives them back at once and cancels every timer", () => {
    const { h, said, pending } = machine();
    h.start(9); h.moving(10); h.rest();
    h.reset();
    expect(said).toEqual([true, false]);
    expect(pending()).toBe(0);
    h.reset();
    expect(said).toEqual([true, false]);
  });

  it("a rest with nothing held arms nothing", () => {
    const { h, said, pending } = machine();
    h.start(9); h.moving(9); h.rest();
    expect(said).toEqual([]);
    expect(pending()).toBe(0);
  });
});
