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
  it("T901: neither store public: one plain 'coming soon' line, no disabled buttons, a join button per platform", () => {
    render(<WebSpotsNotice palette={LIGHT} contact="support@canifishthis.ca" />);
    expect(screen.getByTestId("web-spots-coming").textContent).toBe("Coming soon to Google Play and the App Store.");
    expect(screen.getByText(WEB_SPOTS_WORDS.join.lead)).toBeTruthy();
    expect(screen.queryByText(WEB_SPOTS_WORDS.playButton)).toBeNull();
    expect(screen.queryByText(WEB_SPOTS_WORDS.appStoreButton)).toBeNull();
    for (const b of screen.getAllByRole("button")) expect(b.getAttribute("aria-disabled")).not.toBe("true");
    expect(PLAY_STORE_URL).toBe("https://play.google.com/store/apps/details?id=ca.canifishthis.app");
  });
  it("T901: once each page is public (its flag), its button is the store link, in the join button's place", async () => {
    const core = await import("@app/core");
    const sp = vi.spyOn(core, "playStoreLink").mockReturnValue(core.PLAY_STORE_URL);
    const sa = vi.spyOn(core, "appStoreLink").mockReturnValue(core.APP_STORE_URL);
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} contact="support@canifishthis.ca" />);
    expect(screen.queryByTestId("web-spots-coming")).toBeNull();
    expect(screen.queryByText(/Join the/)).toBeNull();
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.playButton));
    expect(open).toHaveBeenLastCalledWith(PLAY_STORE_URL);
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.appStoreButton));
    expect(open).toHaveBeenLastCalledWith("https://apps.apple.com/app/id6835000455");
    sp.mockRestore(); sa.mockRestore();
  });
  it("T901: no contact address: the press opens Report with that subject; no address and no Report: no press at all", () => {
    const asks: string[] = [];
    render(<WebSpotsNotice palette={LIGHT} onReport={(t) => asks.push(t)} />);
    fireEvent.click(screen.getByText("Join the Android test"));
    fireEvent.click(screen.getByText("Join the iPhone test"));
    expect(asks[0]!.startsWith("Join the Android test")).toBe(true);
    expect(asks[1]!.startsWith("Join the iPhone test")).toBe(true);
    cleanup();
    render(<WebSpotsNotice palette={LIGHT} />);
    expect(screen.queryByText(/Join the/)).toBeNull();
  });
  it("T901: one store live: only the other platform keeps its join button, and the line names only it", async () => {
    const core = await import("@app/core");
    const sp = vi.spyOn(core, "playStoreLink").mockReturnValue(core.PLAY_STORE_URL);
    render(<WebSpotsNotice palette={LIGHT} contact="support@canifishthis.ca" />);
    expect(screen.queryByText("Join the Android test")).toBeNull();
    expect(screen.getByText("Join the iPhone test")).toBeTruthy();
    expect(screen.getByTestId("web-spots-coming").textContent).toBe("Coming soon to the App Store.");
    sp.mockRestore();
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
