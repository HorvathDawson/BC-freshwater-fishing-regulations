/**
 * T1211 — THE MAP KEY FLOATS OVER THE MAP (owner 2026-11-26: "Floating \"Map key\" over the map, near the bottom"). On a
 * phone the Map tab's regulations key is a chip, not a bar: named aloud, a thumb's target, the whole key behind one press;
 * and the two rule statuses are "Its own rules / Region rules only" everywhere they are named.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, useState } from "react";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MAP_KEY, WATER_STATUS } from "@app/core";
import { BackLayerContext, type BackLayers } from "@app/ui";
import { MapLegend } from "./shell/MapLegend";
import { KEY_TARGET, noteMapTap } from "./shell/MapKey";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (regsLayer: "standard" | "salmon", over: Record<string, unknown> = {}, obstacles = false) => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles } } }, ...over,
}) as never;
const show = (mode: "standard" | "salmon" = "standard", obstacles = false) =>
  render(<MapLegend col={col(mode, {}, obstacles)} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);
/** the key over a map whose isolate is real state (useMapColouring's toggle: the same class again shows all) */
function Isolating() {
  const [isolated, setIsolated] = useState<string | null>(null);
  const isolate = (c: string) => setIsolated((p) => (p === c ? null : c));
  return <MapLegend col={col("standard", { isolated, isolate })} conditions={false} palette={LIGHT} theme="light" mapDay={null} />;
}
const chipWord = () => screen.getByTestId("map-key-word").textContent;
const words = () => screen.queryAllByTestId("legend-entry").map((e) => e.textContent);

describe("(T1211) the floating Map key", () => {
  it("closed: a chip named \"Map key\", a 44 x 44 target, no bar and no entries", () => {
    show();
    const chip = screen.getByTestId("map-key-chip");
    expect([chip.getAttribute("role"), chip.getAttribute("aria-label"), chip.getAttribute("aria-expanded")])
      .toEqual(["button", MAP_KEY.chip.label, "false"]);
    expect(chip.textContent).toContain(MAP_KEY.chip.word);
    expect([parseFloat(chip.style.minHeight), parseFloat(chip.style.minWidth)]).toEqual([KEY_TARGET, KEY_TARGET]);
    expect(screen.queryByTestId("legend-fishery-bar")).toBeNull();
    expect(screen.queryByTestId("legend-strip")).toBeNull();
    expect(words()).toEqual([]);
  });

  it("a press opens the whole key in a card (the bar's seven in its two rows, then the region boundary under them); a second press closes it", () => {
    show();
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(screen.getByTestId("map-key-chip").getAttribute("aria-expanded")).toBe("true");
    expect(words()).toEqual(["Closed", "Its own rules", "Region rules only", "No access", "Fly only", "Bait ban", "Not fishable"]);
    expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
    expect(screen.getByTestId("map-key-lines").textContent).toBe(MAP_KEY.region);
    // (T1208) the boundary's swatch is the map's dash-dot: a dash, a gap, a dot, a gap
    const parts = [...screen.getByTestId("swatch-boundary").children].map((e) => [parseFloat((e as HTMLElement).style.width), (e as HTMLElement).style.backgroundColor !== ""]);
    expect(parts.slice(0, 4)).toEqual([[10, true], [4, false], [2, true], [4, false]]);
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(words()).toEqual([]);
  });

  it("a class shown alone is said on the chip, open or closed; one tap on Show all draws every class again", () => {
    render(<Isolating />);
    expect(screen.queryByTestId("map-key-show-all")).toBeNull();
    fireEvent.click(screen.getByTestId("map-key-chip"));
    fireEvent.click(screen.getAllByTestId("legend-entry").find((e) => e.textContent === "Its own rules")!);
    fireEvent.click(screen.getByTestId("map-key-chip"));                    // the key closed: the isolate stays
    expect(words()).toEqual([]);
    expect(chipWord()).toContain(MAP_KEY.chip.only("Its own rules"));
    expect(screen.getByTestId("map-key-chip").getAttribute("aria-label")).toContain("Its own rules only");
    const all = screen.getByTestId("map-key-show-all");
    expect([all.getAttribute("role"), parseFloat(all.style.minHeight)]).toEqual(["button", KEY_TARGET]);
    fireEvent.click(all);
    expect(chipWord()).toContain(MAP_KEY.chip.word);
    expect(screen.queryByTestId("map-key-show-all")).toBeNull();
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(screen.getAllByTestId("legend-entry").filter((e) => e.getAttribute("aria-pressed") === "true")).toEqual([]);
  });

  it("fish obstacles on: their marks are in the opened key (the bar had them; T300)", () => {
    show("standard", true);
    expect(screen.queryByTestId("legend-obstacles")).toBeNull();
    fireEvent.click(screen.getByTestId("map-key-chip"));
    const card = screen.getByTestId("map-key-card");
    expect(card.querySelector('[data-testid="legend-obstacles"]')?.textContent).toContain(MAP_KEY.obstacle.falls);
  });

  it("the open key is a back layer (Android's back, the browser's) and a tap on the map closes it", () => {
    const open: (() => void)[] = [];
    const layers: BackLayers = { register: (c) => { open.push(c); return () => { open.splice(open.indexOf(c), 1); }; },
                                 back: () => { const c = open.at(-1); if (!c) return false; c(); return true; } };
    render(<BackLayerContext.Provider value={layers}>
      <MapLegend col={col("standard")} conditions={false} palette={LIGHT} theme="light" mapDay={null} />
    </BackLayerContext.Provider>);
    expect(open).toHaveLength(0);
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(open).toHaveLength(1);
    act(() => { layers.back(); });
    expect([words(), open.length]).toEqual([[], 0]);
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(words().length).toBeGreaterThan(0);
    act(() => { noteMapTap(); });
    expect([words(), open.length]).toEqual([[], 0]);
  });

  it("Salmon's key is the same chip, its own entries behind it", () => {
    show("salmon");
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(words().slice(0, 3)).toEqual([MAP_KEY.salmon.open, MAP_KEY.salmon.release, MAP_KEY.salmon.closed]);
  });

  it("the chip's word is one line and grows with the text only to the cap (type.ts LABEL_PROPS)", () => {
    show();
    const chip = screen.getByTestId("map-key-chip");
    const word = [...chip.querySelectorAll("*")].find((e) => e.children.length === 0 && (e.textContent ?? "").startsWith(MAP_KEY.chip.word)) as HTMLElement;
    expect(getComputedStyle(word).whiteSpace).toBe("nowrap");
  });
});

describe("(T1211) the two rule statuses' words, everywhere", () => {
  it("the key and the status's own label say \"Its own rules\" and \"Region rules only\" (owner 2026-11-26)", () => {
    expect([MAP_KEY.status.own, MAP_KEY.status.base]).toEqual(["Its own rules", "Region rules only"]);
    expect([WATER_STATUS.own.label, WATER_STATUS.base.label]).toEqual(["Its own rules", "Region rules only"]);
  });
});
