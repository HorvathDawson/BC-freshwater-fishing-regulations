/** (T909) THE PHONE'S SEARCH BAR still draws the bell (with its count) and the refresh button. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { syncVM } from "@app/core";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";

vi.mock("./BarControls", async () => await import("./BarControls.native"));
afterEach(cleanup);
const NOW = Date.now();
const sync = syncVM({ feed: true, feedFetchedAt: new Date(NOW - 60_000).toISOString(), checkedAt: NOW - 60_000, checking: false,
  newRelease: null, now: NOW, update: { downloading: null, ready: false, waitingWifi: false, failed: [], done: false } });

describe("(T909) the phone's search bar", () => {
  it("draws the bell with its count and the refresh button", () => {
    render(<TopBar palette={LIGHT} placeholder="p" open={false} query="" onQuery={() => {}} onOpen={() => {}} onClose={() => {}}
                   sync={sync} onSync={() => {}} notices={{ count: 2, label: "2 unread", onPress: () => {} }} />);
    expect(screen.getByTestId("inseason-bell")).toBeTruthy();
    expect(screen.getByTestId("inseason-count").textContent).toBe("2");
    expect(screen.getByTestId("sync-button")).toBeTruthy();
  });
});
