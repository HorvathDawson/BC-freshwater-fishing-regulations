/**
 * ONE HORIZON, TWO CONTROLS (owner 2026-10-20).
 *
 * The map's corner control is back on the Conditions map beside the panel header's. P16
 * removed it because two copies could disagree; what stops that now is that there is one
 * state (`useHorizon`, held by the Shell) and both controls draw its chips and call its
 * `pick`. This mounts the two controls the way the Shell wires them and drives each:
 * choosing +10 on the map moves the panel and the map's colouring; choosing Now in the
 * panel moves the map's control back.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text, View } from "react-native";
import { useHorizon, useMapModes, type PanelAnswer } from "@app/ui";
import { DonorPanel } from "./DonorPanel";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: () => null,
}));
const { MapScreen } = await import("./MapScreen");

afterEach(cleanup);

const ANSWER: PanelAnswer = {
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close", spread: 6,
                               donors: 1 } },
  rows: [], areaKm2: 50, routesReady: false,
};
const GROUPS = {};

/** The Shell's wiring, and nothing else: one `useHorizon`, two controls, the map modes. */
function Wired() {
  const hz = useHorizon();
  const mm = useMapModes({ tab: "conditions", flowParam: "discharge", horizon: hz.horizon,
                           zoomedOut: false, paired: true, statusOk: true, perTile: true, water: "regulations",
                           groups: GROUPS, basemap: "map", imageryUp: true });
  return (
    <View>
      <View testID="map">
        <MapScreen at={{ atlas: "a", basemap: "b" } as never} palette={LIGHT} theme="light"
                   view="plain" camera={{ lon: -122, lat: 49, zoom: 8 } as never}
                   horizons={mm.horizonsShown ? { chips: hz.chips, onPick: hz.pick } : undefined} />
        {/* What the map is painted for: the horizon the standings are asked at. */}
        <Text testID="painted">{`painted at +${mm.standingHorizon}`}</Text>
      </View>
      <View testID="panel">
        <DonorPanel palette={LIGHT} value={ANSWER} day={new Date(2026, 8, 4, 12)}
                    horizon={hz.horizon} onHorizon={hz.pick} />
      </View>
    </View>
  );
}

const map = () => within(screen.getByTestId("map"));
const panel = () => within(screen.getByTestId("panel"));

describe("the map's horizon control and the panel's share one state", () => {
  it("opens on now, both controls saying so", () => {
    render(<Wired />);
    expect(map().getByLabelText("Conditions now, showing")).toBeTruthy();
    expect(panel().getByLabelText("Conditions now, showing")).toBeTruthy();
    expect(screen.getByText("painted at +0")).toBeTruthy();
    expect(panel().getByText("ESTIMATE FOR THIS SPOT")).toBeTruthy();
  });

  it("choosing +10 on the map repaints the map and moves the panel with it", () => {
    render(<Wired />);
    fireEvent.click(map().getByLabelText("Forecast 10 days ahead"));
    expect(map().getByLabelText("Forecast 10 days ahead, showing")).toBeTruthy();
    expect(panel().getByLabelText("Forecast 10 days ahead, showing")).toBeTruthy();
    expect(screen.getByText("painted at +10")).toBeTruthy();
    expect(panel().getByText("FORECAST · 10 DAYS AHEAD")).toBeTruthy();
  });

  it("choosing Now in the panel moves the map's control back", () => {
    render(<Wired />);
    fireEvent.click(map().getByLabelText("Forecast 3 days ahead"));
    fireEvent.click(panel().getByLabelText("Conditions now"));
    expect(map().getByLabelText("Conditions now, showing")).toBeTruthy();
    expect(map().getByLabelText("Forecast 3 days ahead")).toBeTruthy();
    expect(screen.getByText("painted at +0")).toBeTruthy();
  });
});
