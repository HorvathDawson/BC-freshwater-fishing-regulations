/**
 * T904 (owner 2026-11-19: "map in background and remove big white bars on desktop") — the Conditions key and the
 * watershed day strip are a CARD floating over the map at the desktop class (window ≥ 1280 px), not a full-width bar
 * under it; at 1279 px and below (tablet, phone) the bar is as it was.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { LayoutWidthContext } from "@app/ui";
import { MapLegend } from "./shell/MapLegend";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = {
  flowParam: "discharge", horizon: 0, mm: { onConditions: true }, cond: { marks: [] }, gearMarked: false, regsLayer: "standard",
  salmonOffered: false, statusOk: true, salmonFacts: null, isolated: null, isolate: () => {}, basinDays: null, pickDay: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
} as never;
const at = (w: number) => render(
  <LayoutWidthContext.Provider value={w}>
    <MapLegend col={col} conditions palette={LIGHT} theme="light" mapDay={null} />
  </LayoutWidthContext.Provider>);

describe("(T904) the Conditions key at the desktop breakpoint", () => {
  it("1280 px: a card with its own border and margin, no bar (no top-border-only strip)", () => {
    at(1280);
    const card = screen.getByTestId("legend-card");
    const s = getComputedStyle(card);
    expect(s.borderTopWidth).toBe("1px");
    expect(s.borderBottomWidth).toBe("1px");
    expect(s.marginBottom).toBe("12px");
    expect(s.borderTopLeftRadius === "" || s.borderTopLeftRadius === "0px").toBe(true);
  });

  it("1279 px: the bar, as before (no card)", () => {
    at(1279);
    expect(screen.queryByTestId("legend-card")).toBeNull();
  });
});
