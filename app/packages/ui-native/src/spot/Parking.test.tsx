// @vitest-environment jsdom
/** (T1163 G6) the parking block under a refused location: "Open Settings" and it opens Settings; not under any other line. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking } from "react-native";
import { MY_LOCATION, SPOT_WORDS } from "@app/core";
import { LIGHT } from "../theme";
import { ParkingBlock } from "./Parking";

// the vitest alias resolves `.web` first (no Settings page); the phone's half is what is under test here
vi.mock("../OpenSettings", async () => await import("../OpenSettings.native"));

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const block = (notice: string | null) => (
  <ParkingBlock palette={LIGHT} parking={null} editable notice={notice} onHere={() => {}} onPick={() => {}} />
);

describe("<ParkingBlock> notice", () => {
  it("location refused: the line and an Open Settings button that opens Settings", () => {
    const open = vi.fn().mockResolvedValue(undefined);
    (Linking as unknown as { openSettings: () => Promise<void> }).openSettings = open;
    render(block(MY_LOCATION.denied));
    expect(screen.getByText("Location is off for this app. Turn it on in your device settings, then tap again.")).toBeTruthy();
    fireEvent.click(screen.getByText("Open Settings"));
    expect(open).toHaveBeenCalledTimes(1);
  });
  it("no fix, a rough fix, or no notice: no button", () => {
    for (const n of [SPOT_WORDS.parkingNoFix, "Your phone placed you only to about 3.2 km, so this pin may not be where you are.", null]) {
      render(block(n));
      expect(screen.queryByText("Open Settings")).toBeNull();
      cleanup();
    }
  });
});
