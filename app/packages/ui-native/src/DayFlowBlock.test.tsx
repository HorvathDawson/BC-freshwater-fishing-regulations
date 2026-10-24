/**
 * T170: "Flow that day" on a saved spot, mounted in light and dark — the action, the filled line — and absent from a
 * draft never asked and from a spot whose reading is the day's own. Q161: a spot whose trip was on an earlier day than
 * it was saved never shows the save day's gauges: the trip day's panel (each gauge's daily mean), else a line saying
 * they are still to read; the draft of such a spot says the day's flow it read.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { dayFlowView } from "@app/ui";
import { answerFrom, type StationId } from "@app/data";
import type { Spot } from "@app/data/spots";
import { SpotScreen } from "./SpotScreen";
import { DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const VISIT = new Date(2024, 7, 15, 9, 20).getTime();
const SAVED = new Date(2026, 9, 23, 11).getTime();
const backdated: Spot = { id: "s1", createdAt: SAVED, visitedAt: VISIT, updatedAt: SAVED, lat: 49.0974, lon: -121.9675,
  item: null, waterName: "Chilliwack River", title: "Tamihi run", notes: "", photos: [], weather: null, trace: null,
  panel: null,
  reading: { station: "08MH001", discharge: 11.2, level: 1.38, percentile: 0.05, parameter: "discharge",
             at: new Date(2026, 9, 23, 10, 20).toISOString() } };
const filled: Spot = { ...backdated, dayFlow: { state: "filled", station: "08MH001", date: "2024-08-15",
  discharge: 19.6, percentile: 0.244, place: 0.244, source: "hydat", filledAt: SAVED } };

/** A one-gauge panel answered with that gauge at the 24th percentile — the shape a trip day's panel has. */
const TRIP_PANEL = answerFrom({ areaKm2: 1200, members: [{ station: "08MH001" as StationId, role: "up", areaKm2: 1230,
  years: { discharge: 60, level: 60 }, regulated: false, sameRiver: true, lead: 1 }] },
  { stations: { "08MH001": { percentile: 0.244, discharge: 0.244, parameter: "discharge" } } }, "discharge");

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe.each([["light", LIGHT], ["dark", DARK]] as [string, Palette][])("Flow that day (%s)", (_, palette) => {
  it("a backdated spot offers the action, and never shows the reading of the day it was saved", () => {
    const onAsk = vi.fn();
    const { getByLabelText, getByTestId, container } = render(
      <SpotScreen spot={backdated} palette={palette} onBack={() => {}}
                  dayFlow={{ view: dayFlowView(backdated, "idle"), onAsk }} />);
    expect(getByTestId("spot-gauges-flow").textContent).toBe(
      "This water's gauge reading for the day of the visit is under Flow that day.");
    expect(container.textContent).not.toContain("11.2");          // the save day's reading
    expect(container.textContent).not.toContain("was saved (");   // T170's label is gone
    fireEvent.click(getByLabelText("Get the flow for that day"));
    expect(onAsk).toHaveBeenCalledTimes(1);
  });

  it("a save-day panel on a backdated spot is not shown: the trip day's gauges are still to read", () => {
    const old: Spot = { ...backdated, panel: TRIP_PANEL };
    const { getByTestId, queryByTestId } = render(
      <SpotScreen spot={old} palette={palette} onBack={() => {}} dayFlow={{ view: dayFlowView(old, "idle"), onAsk: null }} />);
    expect(getByTestId("spot-gauges-pending").textContent).toBe(
      "The gauges for 15 Aug 2024 have not been read from their record yet. Refresh fills them in.");
    expect(queryByTestId("spot-gauges-day")).toBeNull();
  });

  it("the trip day's panel: each gauge's daily mean that day, said so", () => {
    const day: Spot = { ...backdated, panel: TRIP_PANEL, panelDay: { state: "day", date: "2024-08-15" } };
    const { getByTestId, queryByTestId } = render(
      <SpotScreen spot={day} palette={palette} onBack={() => {}} dayFlow={{ view: dayFlowView(day, "idle"), onAsk: null }} />);
    expect(getByTestId("spot-gauges-day").textContent).toBe("Each gauge's daily mean on 15 Aug 2024, the day of the visit.");
    expect(queryByTestId("spot-gauges-pending")).toBeNull();
  });

  it("a draft added for an earlier day says the day's flow it read", () => {
    const { getByTestId } = render(
      <SpotScreen spot={{ ...filled, createdAt: SAVED }} palette={palette} mode="draft" onBack={() => {}}
                  dayFlow={{ view: dayFlowView(filled, "idle"), onAsk: null }} />);
    expect(getByTestId("day-flow-line").textContent).toBe("19.6 m³/s, the 24th percentile for the date — filled in "
      + "from the day's record (HYDAT daily mean, 15 Aug 2024).");
  });

  it("filled: the day's flow said whole in the theme's ink, and no action", () => {
    const { getByTestId, queryByLabelText } = render(
      <SpotScreen spot={filled} palette={palette} onBack={() => {}}
                  dayFlow={{ view: dayFlowView(filled, "idle"), onAsk: null }} />);
    const line = getByTestId("day-flow-line");
    expect(line.textContent).toBe("19.6 m³/s, the 24th percentile for the date — filled in from the day's record "
      + "(HYDAT daily mean, 15 Aug 2024).");
    expect(line.style.color).toBe(rgb(palette.ink));
    expect(queryByLabelText("Get the flow for that day")).toBeNull();
  });

  it("offline: said under the action, which stays", () => {
    const { getByTestId, getByLabelText } = render(
      <SpotScreen spot={backdated} palette={palette} onBack={() => {}}
                  dayFlow={{ view: dayFlowView(backdated, "offline"), onAsk: () => {} }} />);
    expect(getByTestId("day-flow-status").textContent).toBe(
      "Offline: the flow for that day will be asked for when you are back online.");
    expect(getByLabelText("Get the flow for that day")).toBeTruthy();
  });

  it("absent from a draft, and from a spot whose reading is the day's own", () => {
    const same: Spot = { ...backdated, visitedAt: SAVED, createdAt: SAVED };
    const a = render(<SpotScreen spot={backdated} palette={palette} mode="draft" onBack={() => {}}
                                 dayFlow={{ view: dayFlowView(backdated, "idle"), onAsk: () => {} }} />);
    expect([a.queryByLabelText("Get the flow for that day"), a.queryByTestId("day-flow-line")]).toEqual([null, null]);
    a.unmount();
    const b = render(<SpotScreen spot={same} palette={palette} onBack={() => {}}
                                 dayFlow={{ view: dayFlowView(same, "idle"), onAsk: () => {} }} />);
    expect([b.queryByLabelText("Get the flow for that day"), b.queryByTestId("spot-gauges-flow")]).toEqual([null, null]);
  });
});
