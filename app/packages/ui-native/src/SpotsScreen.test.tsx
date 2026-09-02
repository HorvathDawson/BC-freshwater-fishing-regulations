/**
 * Spots, mounted.
 *
 * The empty state matters more than the list here: it is what every new user sees, and it
 * is the only place the app explains what a spot IS. An empty screen that just says
 * "nothing here" gets a feature ignored forever.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { SpotsScreen } from "./SpotsScreen";
import type { Spot } from "@app/data/spots";
import { LIGHT } from "./theme";

afterEach(cleanup);

const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: Date.parse("2026-08-30T09:00:00Z"), visitedAt: Date.parse("2026-08-30T09:00:00Z"), updatedAt: 0,
  lat: 49.0974, lon: -121.9675, item: null, section: null, waterName: null,
  title: "Tamihi run", notes: "", photos: [],
  reading: null, weather: null, trace: null, regulation: null, ...over,
});

describe("<SpotsScreen>", () => {
  it("explains what a spot is when there are none", () => {
    const { getByText } = render(
      <SpotsScreen palette={LIGHT} spots={[]} onOpen={() => {}} onAdd={() => {}} />);
    expect(getByText("No spots yet")).toBeTruthy();
    expect(getByText(/records the gauge/)).toBeTruthy();
  });

  it("offers a way in from the empty state", () => {
    const onAdd = vi.fn();
    const { getByLabelText } = render(
      <SpotsScreen palette={LIGHT} spots={[]} onOpen={() => {}} onAdd={onAdd} />);
    getByLabelText("Add a spot").click();
    expect(onAdd).toHaveBeenCalled();
  });

  it("shows a spot with no photo, no gauge and no water as a normal spot", () => {
    // 97.6% of BC's water has no registry item, so a pin on unnamed water is the ORDINARY
    // case. If it rendered as degraded, most real pins would look broken.
    const { getByText } = render(
      <SpotsScreen palette={LIGHT} spots={[spot()]} onOpen={() => {}} onAdd={() => {}} />);
    expect(getByText("Tamihi run")).toBeTruthy();
    expect(getByText(/no gauge/)).toBeTruthy();
    expect(getByText("NO PHOTO")).toBeTruthy();
  });

  it("shows the reading that was recorded, not a live one", () => {
    const { getByText } = render(
      <SpotsScreen palette={LIGHT} onOpen={() => {}} onAdd={() => {}}
                   spots={[spot({ reading: { station: "08MH001", discharge: 15.7, level: null,
                                            percentile: 0.038, at: null },
                                  weather: { window: null, tempC: 14, windKph: null, windDir: null,
                                             rain3h: null, pressureHpa: null, code: null,
                                             at: null } })]} />);
    expect(getByText(/15\.7 m³\/s/)).toBeTruthy();
    expect(getByText(/p4/)).toBeTruthy();
    expect(getByText(/14°C/)).toBeTruthy();
  });

  it("always shows where the spot is, because that is the one thing it cannot lose", () => {
    const { getByText } = render(
      <SpotsScreen palette={LIGHT} spots={[spot()]} onOpen={() => {}} onAdd={() => {}} />);
    expect(getByText(/49\.0974, -121\.9675/)).toBeTruthy();
  });

  it("opens the spot you tapped", () => {
    const onOpen = vi.fn();
    const { getByLabelText } = render(
      <SpotsScreen palette={LIGHT} spots={[spot()]} onOpen={onOpen} onAdd={() => {}} />);
    getByLabelText("Tamihi run").click();
    expect(onOpen).toHaveBeenCalledWith("s1");
  });
});
