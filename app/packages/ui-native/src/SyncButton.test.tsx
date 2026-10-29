/**
 * (T231) The sync icon's states as drawn: checking, the ring while downloading (with the dot), the brief check, the red
 * error badge — and the bell's sync section (what failed, last success, Retry) beside the in-season rows.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { syncVM, type UpdateSourceInput } from "@app/core";
import { InSeasonNotice } from "./InSeasonNotice";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";

afterEach(cleanup);
const NOW = Date.now();
const base = { feed: true, feedFetchedAt: new Date(NOW - 60_000).toISOString(), checkedAt: NOW - 60_000, checking: false,
               newRelease: null, now: NOW };
const idle: { downloading: number | null; ready: boolean; waitingWifi: boolean; failed: string[]; done: boolean } =
  { downloading: null, ready: false, waitingWifi: false, failed: [], done: false };
const bar = (u: Partial<typeof idle>, onSync = () => {}, checking = false) => (
  <TopBar palette={LIGHT} placeholder="p" open={false} query="" onQuery={() => {}} onOpen={() => {}} onClose={() => {}}
          sync={syncVM({ ...base, checking, update: { ...idle, ...u } })} onSync={onSync} />);

describe("the sync icon (T231)", () => {
  it("draws each state, and says it", () => {
    const { rerender } = render(bar({}));
    expect([screen.queryByTestId("sync-dot"), screen.queryByTestId("sync-ring"), screen.queryByTestId("sync-error")])
      .toEqual([null, null, null]);
    rerender(bar({ downloading: 0.3 }));
    expect(screen.getByTestId("sync-ring")).toBeTruthy();
    expect(screen.getByTestId("sync-dot")).toBeTruthy();
    expect(screen.getByLabelText("Refresh data: downloading an update, 30%")).toBeTruthy();
    rerender(bar({ done: true }));
    expect(screen.getByTestId("sync-done")).toBeTruthy();
    rerender(bar({ failed: ["the DFO salmon feed"] }));
    expect(screen.getByTestId("sync-error").textContent).toBe("!");
    expect(screen.getByLabelText("Refresh data: could not update the DFO salmon feed")).toBeTruthy();
    rerender(bar({ ready: true }));
    expect(screen.getByTestId("sync-dot")).toBeTruthy();
  });

  it("a tap calls the Shell's handler (a check, or the failure sheet)", () => {
    let taps = 0;
    render(bar({ failed: ["the data release"] }, () => { taps++; }));
    fireEvent.click(screen.getByTestId("sync-button"));
    expect(taps).toBe(1);
  });
});

describe("the bell's sync section (owner 2026-10-28)", () => {
  it("lists what failed, when it last succeeded, and Retry, above the in-season rows", () => {
    const failed: UpdateSourceInput[] = [{ k: "release", checkedAt: null, updatedAt: null, error: "HTTP 503", failedAt: NOW,
                                           offline: false }];
    let retried = 0;
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}}
                           sync={{ failed, checking: false, onRetry: () => { retried++; } }} />);
    expect(screen.getByTestId("sync-error-release").textContent)
      .toBe("Regulations, map and salmon dataThe server did not answerNot reached from this device yet");
    fireEvent.click(screen.getByLabelText("Retry"));
    expect(retried).toBe(1);
  });
});
