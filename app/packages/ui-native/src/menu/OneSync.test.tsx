/**
 * (T428) ONE sync control in More → Data: the map's own sync button; what it checked sits right under it as plain lines.
 * No second thing that looks like a button (the old "Updates" row wore a sync icon and did nothing on a tap), and an
 * offline source is said in words, not left blank.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { syncVM, UPDATE_WORDS, type DataInput } from "@app/core";
import { LayoutWidthContext } from "@app/ui";
import { LIGHT } from "../theme";
import { MoreScreen } from "./MoreScreen";

afterEach(cleanup);
const NOW = Date.now();
const DATA: DataInput = {
  sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
  updates: {
    sources: [
      { k: "gauges", checkedAt: NOW - 60_000, updatedAt: NOW - 60_000, error: null, failedAt: null, offline: false },
      { k: "dfo", checkedAt: null, updatedAt: null, error: "network request failed", failedAt: NOW, offline: true },
      { k: "inseason", checkedAt: NOW - 60_000, updatedAt: null, error: null, failedAt: null, offline: false },
    ],
    release: null,
  },
};
const sync = syncVM({ feed: true, feedFetchedAt: DATA.feedFetchedAt!, checkedAt: null, checking: false, newRelease: null, now: NOW });

function more(onSync: () => void) {
  return render(
    <LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={sync} onSync={onSync} />
    </LayoutWidthContext.Provider>);
}

describe("More → Data has one sync control (T428)", () => {
  it("the button is the map's, a tap checks now, and the per-feed lines follow it with nothing tappable among them", () => {
    const onSync = vi.fn();
    more(onSync);
    const lines = screen.getByTestId("data-updates");
    // right under the button's row, not after the list of sources
    expect(lines.previousElementSibling?.contains(screen.getByTestId("more-sync-button"))).toBe(true);
    // no second sync icon, no button, no tap target inside the lines
    expect([lines.querySelectorAll("svg").length, lines.querySelectorAll("[role=button],button,[tabindex]").length]).toEqual([0, 0]);
    fireEvent.click(lines);
    fireEvent.click(screen.getByTestId("data-updates-gauges"));
    expect(onSync).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("more-sync-button"));
    expect(onSync).toHaveBeenCalledTimes(1);
    // the whole screen offers exactly one sync button
    expect(screen.getAllByRole("button").filter((b) => /refresh|update|sync/i.test(b.getAttribute("aria-label") ?? "")).length).toBe(1);
  });

  it("an offline source says so in words, beside the others' last checks", () => {
    more(() => {});
    expect(screen.getByTestId("data-updates-dfo").textContent).toContain(UPDATE_WORDS.sheet.offline);
    expect(screen.getByTestId("data-updates-gauges").textContent).toContain(UPDATE_WORDS.row.checked("1 min ago"));
  });
});
