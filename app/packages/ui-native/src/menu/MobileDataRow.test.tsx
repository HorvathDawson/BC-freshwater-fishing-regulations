/** (T298, owner 2026-10-29) More → Settings → Data: "Download over mobile data" — Always / Ask / Wi-Fi only, the choice shown and changed. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MOBILE_DATA_WORDS, type DataInput, type MobileDataSetting } from "@app/core";
import { LayoutWidthContext } from "@app/ui";
import { LIGHT } from "../theme";
import { MoreScreen } from "./MoreScreen";

afterEach(cleanup);
const NOW = Date.now();
const DATA: DataInput = {
  sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
  updates: { sources: [], release: null },
};

describe("Download over mobile data (T298)", () => {
  it("shows the three choices with the setting's checked, and a press changes it", () => {
    const picked: MobileDataSetting[] = [];
    render(
      <LayoutWidthContext.Provider value={null}>
        <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                    onPick={() => {}} metered={{ value: "ask", set: (s) => picked.push(s) }} />
      </LayoutWidthContext.Provider>);
    fireEvent.click(screen.getByTestId("menu-item-settings"));  // (T703) the controls live in Settings
    expect(screen.getByTestId("data-updates-metered").textContent).toContain(MOBILE_DATA_WORDS.label);
    const radios = screen.getAllByRole("radio").filter((r) => r.getAttribute("aria-label")?.startsWith(MOBILE_DATA_WORDS.label));
    expect(radios.map((r) => r.textContent)).toEqual(["Always", "Ask", "Wi-Fi only"]);
    expect(radios.map((r) => r.getAttribute("aria-checked"))).toEqual(["false", "true", "false"]);
    act(() => { fireEvent.click(screen.getByTestId("data-mobile-wifi")); });
    expect(picked).toEqual(["wifi"]);
  });
});
