/** (T302) The web's Spots: a notice in the tab's place and the capture's, with a link to the app; the phone's unchanged. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking } from "react-native";
import { WEB_SPOTS_WORDS } from "@app/core";
import { WebSpotsNotice, showWebSpotsNotice, webSpotsNotice } from "./WebSpotsNotice";
import { SpotsScreen } from "./SpotsScreen";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); showWebSpotsNotice(null); });

describe("the web's spots notice (T302)", () => {
  it("says spots live in the phone app, don't show here yet, accounts are coming; Get the app opens the listing", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true as never);
    render(<WebSpotsNotice palette={LIGHT} />);
    for (const t of [WEB_SPOTS_WORDS.lead, ...WEB_SPOTS_WORDS.body]) expect(screen.getByText(t)).toBeTruthy();
    fireEvent.click(screen.getByText(WEB_SPOTS_WORDS.getApp));
    expect(open).toHaveBeenCalledWith("https://play.google.com/store/apps/details?id=ca.canifishthis.app");
    expect(screen.queryByText(WEB_SPOTS_WORDS.back)).toBeNull();      // the tab: no way back needed
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
