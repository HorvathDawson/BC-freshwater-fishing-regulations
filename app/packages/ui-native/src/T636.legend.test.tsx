/**
 * (T636, owner 2026-11-11) THE RING, NAMED ONCE in the Conditions key: one line, a grey dot in the ring's colour, in the
 * words of the quantity NOT shown ("Gauge reports level only" under Flow, "… flow only" under Level), only while the
 * gauge dots are drawn, never under Temperature.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { OTHER_QUANTITY_WORDS } from "@app/core";
import { gaugeOnlyColours } from "@app/map/colours";
import { GaugeOnlyKey, MapLegend } from "./shell/MapLegend";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (flowParam: string, gauges: boolean) => ({
  flowParam, horizon: 0, mm: { onConditions: true }, cond: { marks: [] }, regsLayer: "standard",
  layers: { water: { drawn: "regulations" }, effective: { groups: {}, layers: { gauges } } },
}) as never;
const legend = (flowParam: string, gauges: boolean) => render(
  <GaugeOnlyKey col={col(flowParam, gauges)} palette={LIGHT} theme="light" bottom={40} />);

describe("the Conditions key names the ring once (T636), on the map (T681)", () => {
  it("is a square-cornered card over the map above the foot, and no longer a line in the bottom bar", () => {
    legend("discharge", true);
    const card = getComputedStyle(screen.getByTestId("gauge-only-key"));
    expect([card.position, card.bottom, card.borderTopLeftRadius]).toEqual(["absolute", "76px", "0px"]);
    cleanup();
    render(<MapLegend col={col("discharge", true)} conditions palette={LIGHT} theme="light" mapDay={null} />);
    expect(screen.queryByTestId("legend-ring")).toBeNull();
  });
  it("under Flow: 'Gauge reports level only', the swatch a grey dot in the ring's colour", () => {
    legend("discharge", true);
    const ring = screen.getByTestId("legend-ring");
    expect(ring.parentElement!.textContent).toBe(OTHER_QUANTITY_WORDS.level.key);
    const { fill, ring: edge } = gaugeOnlyColours("light");
    const st = getComputedStyle(ring);
    expect([st.backgroundColor, st.borderTopColor].map((c) => c.replace(/\s/g, "")))
      .toEqual([fill, edge].map((h) => `rgb(${[1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16)).join(",")})`));
  });
  it("under Level: 'Gauge reports flow only'", () => {
    legend("level", true);
    expect(screen.getByTestId("legend-ring").parentElement!.textContent).toBe(OTHER_QUANTITY_WORDS.discharge.key);
  });
  it("not while the gauge dots are off, and not under Temperature", () => {
    legend("discharge", false);
    expect(screen.queryByTestId("legend-ring")).toBeNull();
    cleanup();
    legend("temperature", true);
    expect(screen.queryByTestId("legend-ring")).toBeNull();
  });
});
