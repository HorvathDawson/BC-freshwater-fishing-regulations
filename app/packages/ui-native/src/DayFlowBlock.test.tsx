/**
 * T170: "Flow that day" on a saved spot, mounted in light and dark — the action, the filled line, the note under a
 * frozen estimate from another day — and absent from a draft and from a spot whose reading is the day's own.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { dayFlowView } from "@app/ui";
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

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe.each([["light", LIGHT], ["dark", DARK]] as [string, Palette][])("Flow that day (%s)", (_, palette) => {
  it("a backdated spot offers the action, and says the estimate is from the day it was saved", () => {
    const onAsk = vi.fn();
    const { getByLabelText, getByTestId } = render(
      <SpotScreen spot={backdated} palette={palette} onBack={() => {}}
                  dayFlow={{ view: dayFlowView(backdated, "idle"), onAsk }} />);
    expect(getByTestId("spot-not-the-day").textContent).toBe(
      "The estimate above was read when this spot was saved (23 Oct 2026), not on the day of the visit.");
    fireEvent.click(getByLabelText("Get the flow for that day"));
    expect(onAsk).toHaveBeenCalledTimes(1);
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
    expect([a.queryByLabelText("Get the flow for that day"), a.queryByTestId("spot-not-the-day")]).toEqual([null, null]);
    a.unmount();
    const b = render(<SpotScreen spot={same} palette={palette} onBack={() => {}}
                                 dayFlow={{ view: dayFlowView(same, "idle"), onAsk: () => {} }} />);
    expect([b.queryByLabelText("Get the flow for that day"), b.queryByTestId("spot-not-the-day")]).toEqual([null, null]);
  });
});
