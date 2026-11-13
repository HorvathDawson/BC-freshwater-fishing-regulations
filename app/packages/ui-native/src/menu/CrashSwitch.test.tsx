/** (T242) More → Settings → Privacy: "Send crash reports" — on by default, one press turns it off (and it is kept), not drawn with no reporter. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { CRASH_WORDS, type DataInput } from "@app/core";
import { CrashSettingRoot, createCrashSetting, LayoutWidthContext } from "@app/ui";
import { LIGHT } from "../theme";
import { MoreScreen } from "./MoreScreen";

afterEach(cleanup);
const NOW = Date.now();
const DATA: DataInput = {
  sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
};
const screenWith = (setting: ReturnType<typeof createCrashSetting> | null) => (
  <LayoutWidthContext.Provider value={null}>
    <CrashSettingRoot setting={setting}>
      <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}} onPick={() => {}} />
    </CrashSettingRoot>
  </LayoutWidthContext.Provider>
);

describe("Send crash reports (T242)", () => {
  it("is a switch, on by default; one press turns it off and the setting keeps it", () => {
    const mem = { v: null as string | null };
    const setting = createCrashSetting({ read: () => mem.v, write: (t) => { mem.v = t; } });
    render(screenWith(setting));
    fireEvent.click(screen.getByTestId("menu-item-settings"));  // (T703) the controls live in Settings
    const sw = screen.getByRole("switch", { name: CRASH_WORDS.label });
    expect(sw.getAttribute("aria-checked")).toBe("true");
    expect(sw.textContent).toContain("On");
    expect(screen.getByTestId("data-crash").textContent).toContain(CRASH_WORDS.hint);
    expect(screen.getByTestId("settings-privacy").contains(screen.getByTestId("data-crash"))).toBe(true);
    // the explanation, word for word
    expect(CRASH_WORDS.hint).toBe("When the app hits an error it sends the error, the app version, the kind of screen, the id of the water you had open and a random id made on this device to our own server. Never your location, spots or photos. Nothing else.");
    act(() => { fireEvent.click(sw); });
    expect(screen.getByRole("switch", { name: CRASH_WORDS.label }).getAttribute("aria-checked")).toBe("false");
    expect(setting.get()).toBe(false);
    expect(mem.v).toBe("off");
  });
  it("is not drawn where the app has no reporter", () => {
    render(screenWith(null));
    fireEvent.click(screen.getByTestId("menu-item-settings"));  // (T703) the controls live in Settings
    expect(screen.getByTestId("menu-page-settings")).toBeTruthy();
    expect(screen.queryByTestId("data-crash")).toBeNull();
  });
});
