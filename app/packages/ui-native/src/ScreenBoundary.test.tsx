/**
 * (T1164) A screen that throws while it draws shows its own line and is reported; the shell around it keeps working.
 */
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { setFailureSink } from "@app/core";
import { ScreenBoundary } from "./ScreenBoundary";
import { LIGHT } from "./theme";

afterEach(cleanup);

const reports: unknown[] = [];
let restore: () => void;
beforeEach(() => {
  reports.length = 0;
  restore = setFailureSink((e) => { reports.push(e); });
  // React logs the caught render error to the console; the test reads the report instead
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});
afterEach(() => { restore(); vi.restoreAllMocks(); });

let broken = true;
function Screen({ name }: { name: string }) {
  if (broken && name === "Spots") throw new Error("spots fell over");
  return <Text testID={`body-${name}`}>{`${name} is drawn`}</Text>;
}

/** a shell: tabs outside, one screen inside its boundary keyed by the tab, as `Shell.tsx` mounts them */
function Shell({ goBack }: { goBack?: () => void }) {
  const [tab, setTab] = useState<"map" | "spots">("spots");
  return (
    <View>
      <Text testID="tab-map" onPress={() => setTab("map")}>Map tab</Text>
      <Text testID="tab-spots" onPress={() => setTab("spots")}>Spots tab</Text>
      <ScreenBoundary screen={tab} resetKey={tab} palette={LIGHT} goBack={goBack}>
        <Screen name={tab === "map" ? "Map" : "Spots"} />
      </ScreenBoundary>
    </View>
  );
}

describe("ScreenBoundary", () => {
  it("a screen that throws shows its line, reports the fault once, and the other tab still works", () => {
    broken = true;
    render(<Shell />);
    expect(screen.getByRole("alert").textContent).toBe("Spots hit a problem and could not be drawn.");
    expect(reports).toHaveLength(1);
    expect((reports[0] as Error).message).toBe("spots fell over");
    fireEvent.click(screen.getByTestId("tab-map"));
    expect(reports).toHaveLength(1);
    expect(screen.getByTestId("body-Map").textContent).toBe("Map is drawn");
    expect(screen.queryByTestId("screen-fault-retry")).toBeNull();
    // leaving and coming back draws the broken screen afresh: broken again, the line again, a second report
    fireEvent.click(screen.getByTestId("tab-spots"));
    expect(screen.getByRole("alert").textContent).toBe("Spots hit a problem and could not be drawn.");
    expect(reports).toHaveLength(2);
  });

  it("Try again draws the screen again: still broken, the line stays; once mended, the screen is back", () => {
    broken = true;
    render(<Shell />);
    fireEvent.click(screen.getByTestId("screen-fault-retry"));
    expect(screen.getByRole("alert").textContent).toBe("Spots hit a problem and could not be drawn.");
    expect(reports).toHaveLength(2);
    broken = false;
    fireEvent.click(screen.getByTestId("screen-fault-retry"));
    expect(screen.getByTestId("body-Spots").textContent).toBe("Spots is drawn");
    expect(screen.queryByRole("alert")).toBeNull();
    expect(reports).toHaveLength(2);
  });

  it("Go back is offered only where a screen has somewhere to go back to, and takes it", () => {
    broken = true;
    const { unmount } = render(<Shell />);
    expect(screen.queryByTestId("screen-fault-back")).toBeNull();
    unmount();
    const back = vi.fn();
    render(<Shell goBack={back} />);
    expect(screen.getByTestId("screen-fault-back").textContent).toBe("Go back");
    fireEvent.click(screen.getByTestId("screen-fault-back"));
    expect(back).toHaveBeenCalledTimes(1);
  });

  it("a screen that does not throw is drawn as it is, and reports nothing", () => {
    broken = false;
    render(<Shell />);
    expect(screen.getByTestId("body-Spots").textContent).toBe("Spots is drawn");
    expect(reports).toHaveLength(0);
  });

  it("a panel (placing bottom) is a band, not a screen: its line, Try again, and Close, which calls goBack and draws it again", () => {
    broken = true;
    const close = vi.fn(() => { broken = false; });
    render(
      <ScreenBoundary screen="sheet" resetKey="s" palette={LIGHT} goBack={close} placing="bottom"><Screen name="Spots" /></ScreenBoundary>);
    expect(screen.getByRole("alert").textContent).toBe("A panel hit a problem and could not be drawn.");
    expect(getComputedStyle(screen.getByTestId("screen-fault-sheet")).position).toBe("absolute");
    expect(screen.queryByTestId("screen-fault-back")).toBeNull();
    expect(reports).toHaveLength(1);
    fireEvent.click(screen.getByTestId("screen-fault-close"));
    expect(close).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("body-Spots").textContent).toBe("Spots is drawn");
    expect(reports).toHaveLength(1);
  });

  it("a panel with no way to close it offers Try again only", () => {
    broken = true;
    render(<ScreenBoundary screen="key" resetKey="k" palette={LIGHT} placing="bottom"><Screen name="Spots" /></ScreenBoundary>);
    expect(screen.getByRole("alert").textContent).toBe("The map key hit a problem and could not be drawn.");
    expect(screen.queryByTestId("screen-fault-close")).toBeNull();
    expect(screen.getByTestId("screen-fault-retry").textContent).toBe("Try again");
  });
});
