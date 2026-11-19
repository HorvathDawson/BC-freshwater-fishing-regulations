// @vitest-environment jsdom
/**
 * (T759) THE ISSUE STRIPE'S LEGEND ENTRY: "Check rule", a line wearing the stripe, in the Salmon key's second row, only
 * while the Salmon key is up AND the map has issue waters to mark (so only where the stripe can appear). The two rows still
 * fit a 390 dp phone (the width law of legend-rows.test.ts, restated against the font).
 */
import "./render-setup.ts";
import { createElement as h } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { MapLegend } from "../packages/ui-native/src/shell/MapLegend";
import { LIGHT } from "../packages/ui-native/src/theme";

afterEach(cleanup);
const col = (regsLayer: "standard" | "salmon", issueFeed: unknown) => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer, issueFeed,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const draw = (c: never) => render(h(MapLegend, { col: c, conditions: false, palette: LIGHT, theme: "light", mapDay: null }));
const FEED = { sections: new Set([1]) };

describe("(T759) the legend's issue entry", () => {
  it("Salmon key with issue waters: one entry, the stripe swatch, in the second row after the gear marks", () => {
    draw(col("salmon", FEED));
    const entries = screen.getAllByTestId("legend-entry").map((e) => e.textContent);
    expect(entries).toEqual(["Keep salmon", "Release only", "Closed", "TBD/unread", "Fly only", "Bait ban", "Check rule"]);
    expect(screen.getAllByTestId("legend-row").length).toBe(2);
    expect(screen.getAllByTestId("swatch-issue").length).toBe(1);
  });
  it("absent without issue waters, and absent from the Standard (Trout) key where the stripe is never drawn", () => {
    draw(col("salmon", null));
    expect(screen.queryByTestId("swatch-issue")).toBeNull();
    cleanup();
    draw(col("standard", FEED));
    expect(screen.queryByTestId("swatch-issue")).toBeNull();
    expect(screen.queryByText("Check rule")).toBeNull();
  });
});
