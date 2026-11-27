/**
 * (T1164) A fault outside every screen (the shell's chrome, the boot screen) shows the app's line and is reported once; a
 * fault inside a screen is the screen's own boundary's, and the root never sees it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { setFailureSink } from "@app/core";
import { ScreenBoundary, THEMES } from "@app/ui-native";
import { withRootBoundary } from "./rootBoundary";

afterEach(cleanup);
const reports: unknown[] = [];
let restore: () => void;
beforeEach(() => {
  reports.length = 0;
  restore = setFailureSink((e) => { reports.push(e); });
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});
afterEach(() => { restore(); vi.restoreAllMocks(); });

let chromeBroken = true;
let screenBroken = true;
/** a shell: chrome (the bars) outside the screen's own boundary, as `Shell.tsx` draws it */
function MiniApp() {
  const Chrome = () => { if (chromeBroken) throw new Error("the bar fell over"); return <Text testID="chrome">Bars</Text>; };
  const Page = () => { if (screenBroken) throw new Error("the page fell over"); return <Text testID="page">Page</Text>; };
  return (
    <View>
      <Chrome />
      <ScreenBoundary screen="map" resetKey="map" palette={THEMES.light}><Page /></ScreenBoundary>
    </View>
  );
}
const Root = withRootBoundary(MiniApp);

describe("the root boundary", () => {
  it("a throw in the chrome shows the app's line, reports once, and Try again draws it afresh", () => {
    chromeBroken = true; screenBroken = false;
    render(<Root />);
    expect(screen.getByRole("alert").textContent).toBe("The app hit a problem and could not be drawn.");
    expect(reports).toHaveLength(1);
    expect((reports[0] as Error).message).toBe("the bar fell over");
    chromeBroken = false;
    fireEvent.click(screen.getByTestId("screen-fault-retry"));
    expect(screen.getByTestId("chrome").textContent).toBe("Bars");
    expect(screen.getByTestId("page").textContent).toBe("Page");
    expect(reports).toHaveLength(1);
  });

  it("a throw in a screen stays that screen's: its own line, the chrome still drawn, one report", () => {
    chromeBroken = false; screenBroken = true;
    render(<Root />);
    expect(screen.getByRole("alert").textContent).toBe("The map hit a problem and could not be drawn.");
    expect(screen.getByTestId("chrome").textContent).toBe("Bars");
    expect(reports).toHaveLength(1);
  });
});
