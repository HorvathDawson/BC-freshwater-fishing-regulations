/**
 * T1211 — THE MAP KEY FLOATS OVER THE MAP (owner 2026-11-26: "Floating \"Map key\" over the map, near the bottom"). On a
 * phone the Map tab's regulations key is a chip, not a bar: named aloud, a thumb's target, the whole key behind one press;
 * and the two rule statuses are "Its own rules / Region rules only" everywhere they are named.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MAP_KEY, WATER_STATUS } from "@app/core";
import { MapLegend } from "./shell/MapLegend";
import { KEY_TARGET, RAIL_CLEAR } from "./shell/MapKey";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (regsLayer: "standard" | "salmon") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const show = (mode: "standard" | "salmon" = "standard") =>
  render(<MapLegend col={col(mode)} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);
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

  it("a press opens the whole key in a card (all seven, in two rows), clear of the rail; a second press closes it", () => {
    show();
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(screen.getByTestId("map-key-chip").getAttribute("aria-expanded")).toBe("true");
    expect(words()).toEqual(["Closed", "Its own rules", "Region rules only", "No access", "Fly only", "Bait ban", "Not fishable"]);
    expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
    expect(parseFloat(screen.getByTestId("map-key-card").style.maxWidth)).toBeLessThanOrEqual(window.innerWidth - RAIL_CLEAR);
    fireEvent.click(screen.getByTestId("map-key-chip"));
    expect(words()).toEqual([]);
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
