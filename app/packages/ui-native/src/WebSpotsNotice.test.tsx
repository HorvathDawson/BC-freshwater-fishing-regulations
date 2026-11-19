/** (T302) The web's Spots: a notice in the tab's place and the capture's, with a link to the app; the phone's unchanged. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking } from "react-native";
import { PLAY_STORE_URL, WEB_SPOTS_WORDS } from "@app/core";
import { WebSpotsNotice, showWebSpotsNotice, webSpotsNotice } from "./WebSpotsNotice";
import { SpotsScreen } from "./SpotsScreen";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); showWebSpotsNotice(null); });

describe("the web's spots notice (T302)", () => {
  it("says spots live in the phone app, don't show here yet, accounts are coming", () => {
    render(<WebSpotsNotice palette={LIGHT} />);
    for (const t of [WEB_SPOTS_WORDS.lead, ...WEB_SPOTS_WORDS.body]) expect(screen.getByText(t)).toBeTruthy();
    expect(screen.queryByText(WEB_SPOTS_WORDS.back)).toBeNull();      // the tab: no way back needed
  });
  it("T901: Google Play opens the Play listing; the App Store is a plain 'iPhone app coming soon', disabled, no link, while its page is not public", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} />);
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.playButton));
    expect(open).toHaveBeenCalledWith(PLAY_STORE_URL);
    expect(PLAY_STORE_URL).toBe("https://play.google.com/store/apps/details?id=ca.canifishthis.app");
    expect(screen.queryByText(WEB_SPOTS_WORDS.appStoreButton)).toBeNull();
    const soon = screen.getByTestId("web-spots-app-store-soon");
    expect(soon.textContent).toBe(WEB_SPOTS_WORDS.appStoreSoon);
    expect(soon.getAttribute("aria-disabled")).toBe("true");
    open.mockClear();
    fireEvent.click(soon);
    expect(open).not.toHaveBeenCalled();
  });
  it("T901: once the App Store page is public (APP_STORE_PUBLIC), its button opens the App Store address", async () => {
    const core = await import("@app/core");
    const spy = vi.spyOn(core, "appStoreLink").mockReturnValue(core.APP_STORE_URL);
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} />);
    expect(screen.queryByTestId("web-spots-app-store-soon")).toBeNull();
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.appStoreButton));
    expect(open).toHaveBeenCalledWith("https://apps.apple.com/app/id6835000455");
    spy.mockRestore();
  });
  it("where a spot was being added, a way back to the map", () => {
    let back = 0;
    render(<WebSpotsNotice palette={LIGHT} onDone={() => { back++; }} />);
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.back));
    expect(back).toBe(1);
  });
  it("the Spots tab draws it on the web build only", () => {
    expect(webSpotsNotice()).toBe(false);                            // the tests draw the phone's spots
    const vm = { title: "Spots", rows: [], empty: null, error: null, stale: null, adds: [], note: null } as never;
    showWebSpotsNotice(true);
    render(<SpotsScreen palette={LIGHT} vm={vm} onOpen={() => {}} onShow={() => {}} onAdd={() => {}} />);
    expect(screen.getByTestId("web-spots-notice")).toBeTruthy();
    expect(screen.queryByTestId("spots-screen")).toBeNull();
  });
});
