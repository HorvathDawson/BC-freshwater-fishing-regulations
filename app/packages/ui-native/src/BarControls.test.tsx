/**
 * (T909) THE WEB'S SEARCH BAR draws neither the bell nor the refresh button, and no walkthrough step points at either
 * (the phone's bar is `BarControls.native.test.tsx`). A store-shots web build (T319, `storeShots.ts`) draws the phone's
 * screens for the store's pictures, so its bar is the phone's: the bell and the refresh button.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { guideSteps, syncVM } from "@app/core";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";
import { drawPhoneScreensOnWeb } from "./storeShots";

afterEach(() => { cleanup(); drawPhoneScreensOnWeb(false); });
const NOW = Date.now();
const sync = syncVM({ feed: true, feedFetchedAt: new Date(NOW - 60_000).toISOString(), checkedAt: NOW - 60_000, checking: false,
  newRelease: null, now: NOW, update: { downloading: null, ready: false, waitingWifi: false, failed: [], done: false } });

describe("(T909) the web's search bar", () => {
  it("neither the bell nor the refresh button is drawn", () => {
    render(<TopBar palette={LIGHT} placeholder="p" open={false} query="" onQuery={() => {}} onOpen={() => {}} onClose={() => {}}
                   sync={sync} onSync={() => {}} notices={{ count: 2, label: "2 unread", onPress: () => {} }} />);
    expect(screen.getByTestId("search-field")).toBeTruthy();
    expect(screen.queryByTestId("inseason-bell")).toBeNull();
    expect(screen.queryByTestId("sync-button")).toBeNull();
  });
  it("(T319) a store-shots build draws the phone's bar: the bell with its count and the refresh button", () => {
    drawPhoneScreensOnWeb(true);
    render(<TopBar palette={LIGHT} placeholder="p" open={false} query="" onQuery={() => {}} onOpen={() => {}} onClose={() => {}}
                   sync={sync} onSync={() => {}} notices={{ count: 2, label: "2 unread", onPress: () => {} }} />);
    expect(screen.getByTestId("inseason-bell")).toBeTruthy();
    expect(screen.getByTestId("inseason-count").textContent).toBe("2");
    expect(screen.getByTestId("sync-button")).toBeTruthy();
  });
  it("no walkthrough step, on either platform, points at the bell or the refresh button", () => {
    for (const p of ["web", "app"] as const)
      for (const s of guideSteps(p)) {
        expect(s.targets, `${p}/${s.key}`).not.toContain("bell");
        expect(s.targets, `${p}/${s.key}`).not.toContain("sync");
      }
  });
});
