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
  it("T901: while neither store page is public both buttons are disabled 'coming soon' words, with no link", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} />);
    for (const [id, words] of [["web-spots-play-soon", WEB_SPOTS_WORDS.playSoon], ["web-spots-app-store-soon", WEB_SPOTS_WORDS.appStoreSoon]] as const) {
      const soon = screen.getByTestId(id);
      expect(soon.textContent).toBe(words);
      expect(soon.getAttribute("aria-disabled")).toBe("true");
      fireEvent.click(soon);
    }
    expect(WEB_SPOTS_WORDS.playSoon).toBe("Android app coming soon");
    expect(screen.queryByText(WEB_SPOTS_WORDS.playButton)).toBeNull();
    expect(screen.queryByText(WEB_SPOTS_WORDS.appStoreButton)).toBeNull();
    expect(open).not.toHaveBeenCalled();
    expect(PLAY_STORE_URL).toBe("https://play.google.com/store/apps/details?id=ca.canifishthis.app");
  });
  it("T901: once each page is public (its flag), its button opens its own address", async () => {
    const core = await import("@app/core");
    const sp = vi.spyOn(core, "playStoreLink").mockReturnValue(core.PLAY_STORE_URL);
    const sa = vi.spyOn(core, "appStoreLink").mockReturnValue(core.APP_STORE_URL);
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} />);
    expect(screen.queryByTestId("web-spots-play-soon")).toBeNull();
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.playButton));
    expect(open).toHaveBeenLastCalledWith(PLAY_STORE_URL);
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.appStoreButton));
    expect(open).toHaveBeenLastCalledWith("https://apps.apple.com/app/id6835000455");
    sp.mockRestore(); sa.mockRestore();
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
