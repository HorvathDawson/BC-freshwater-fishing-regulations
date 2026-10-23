/**
 * THE LAYER CHOICE, KEPT, and the spots every map is handed (owner 2026-10-22: "toggle our spots on and off in the
 * layer menu"). The hook reads the store once, writes every change back whole, and a second mount over the same store
 * (a reload) starts where the first left off. `spotPins` is what a map receives: every saved spot while the switch is
 * on, none while it is off.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { LAYER_DEFAULTS, chooseLayer, type LayerChoice } from "@app/core";
import type { Spot } from "@app/data/spots";
import { spotPins, useLayerChoice, type LayerStore } from "./layerChoice";

afterEach(cleanup);

const NONE = { carries: { stocked: false, surveyed: false }, online: true } as const;

/** A store that is a string in memory: what a phone's file and a browser's key both are. */
function memory(text: string | null = null): LayerStore & { text: string | null; writes: number } {
  const m = { text, writes: 0,
              read: () => m.text,
              write: (t: string) => { m.text = t; m.writes += 1; } };
  return m;
}

function mount(store: LayerStore | undefined) {
  const out: { c?: LayerChoice; set?: (f: (c: LayerChoice) => LayerChoice) => void } = {};
  function Probe() {
    const [c, set] = useLayerChoice(store);
    out.c = c; out.set = set;
    return null;
  }
  render(<Probe />);
  return out;
}

describe("useLayerChoice", () => {
  it("nothing stored: the defaults, and nothing written until the reader changes something", () => {
    const s = memory();
    const out = mount(s);
    expect([out.c, s.text, s.writes]).toEqual([LAYER_DEFAULTS, null, 0]);
  });

  it("SPOTS OFF SURVIVES A RELOAD: the change is written, and a fresh mount over the store starts with it off", () => {
    const s = memory();
    const first = mount(s);
    act(() => first.set!((c) => chooseLayer(c, "overlay.spots", NONE)));
    cleanup();
    const second = mount(s);
    expect([first.c!.on["overlay.spots"], s.writes, second.c]).toEqual(
      [false, 1, { ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.spots": false } }]);
  });

  it("a store that cannot write keeps the choice for the session (it is a preference, not a record)", () => {
    const s: LayerStore = { read: () => null, write: () => { throw new Error("quota"); } };
    const out = mount(s);
    act(() => out.set!((c) => chooseLayer(c, "overlay.spots", NONE)));
    expect(out.c!.on["overlay.spots"]).toBe(false);
  });

  it("no store at all (a test, the mini map): the defaults, and the choice still changes", () => {
    const out = mount(undefined);
    act(() => out.set!((c) => chooseLayer(c, "overlay.private", NONE)));
    expect(out.c).toEqual({ ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.private": true } });
  });
});

describe("spotPins: the saved spots a map is handed", () => {
  const spot = (id: string, lat: number, lon: number, title: string) =>
    ({ id, lat, lon, title, waterName: null, item: null }) as unknown as Spot;
  const spots = [spot("a", 49.1, -121.9, "Tamihi run"), spot("b", 50.2, -122.3, "")];

  it("on: every spot, by its id, at its place, under its own name; off: none", () => {
    expect([spotPins(spots, true), spotPins(spots, false), spotPins([], true)]).toEqual([
      [{ id: "a", lat: 49.1, lon: -121.9, title: "Tamihi run" }, { id: "b", lat: 50.2, lon: -122.3, title: "50.2000, -122.3000" }],
      [], []]);
  });
});
