/** (T723 G6) "Open Settings" under a refusal line: the phone's draws it and opens Settings; the browser's draws nothing. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking } from "react-native";
import { MY_LOCATION, SPOTS_MENU } from "@app/core";
import { LIGHT } from "./theme";
import { OpenSettings as Phone } from "./OpenSettings.native";
import { OpenSettings as Web } from "./OpenSettings.web";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("OpenSettings", () => {
  it("phone: a button under a refusal line opens Settings; none under another line", () => {
    const open = vi.fn().mockResolvedValue(undefined);
    (Linking as unknown as { openSettings: () => Promise<void> }).openSettings = open;
    render(<Phone palette={LIGHT} line={MY_LOCATION.denied} />);
    fireEvent.click(screen.getByText("Open Settings"));
    expect(open).toHaveBeenCalledTimes(1);
    cleanup();
    render(<Phone palette={LIGHT} line={SPOTS_MENU.noLocation} />);
    expect(screen.queryByText("Open Settings")).toBeNull();
  });
  it("web: nothing", () => {
    render(<Web palette={LIGHT} line={MY_LOCATION.denied} />);
    expect(screen.queryByText("Open Settings")).toBeNull();
  });
});
