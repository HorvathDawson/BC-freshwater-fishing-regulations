/**
 * (T428, T703) ONE sync control in More → Data: the map's own sync button. Under it only what the table of sources does not
 * say: a failure, a release the app cannot use. "Checked 3 min ago" and each feed's check time are gone (the table has the ages).
 * No second thing that looks like a button (the old "Updates" row wore a sync icon and did nothing on a tap), and an
 * offline source is said in words, not left blank.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SYNC_WORDS, syncVM, UPDATE_WORDS, type DataInput } from "@app/core";
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
const sync = syncVM({ feed: true, feedFetchedAt: DATA.feedFetchedAt!, checkedAt: NOW, checking: false, newRelease: null, now: NOW });

function more(onSync: () => void) {
  return render(
    <LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={sync} onSync={onSync} />
    </LayoutWidthContext.Provider>);
}

describe("More → Data has one sync control (T428, T703)", () => {
  it("the button is the map's and a tap checks now; all well, it says only its name and no per-feed lines repeat the table", () => {
    const onSync = vi.fn();
    more(onSync);
    // (T703) a source that checked fine has no line: its age is the table's. Nothing is drawn where nothing is wrong... but
    // the offline source here is said, so the lines hold exactly that one
    const lines = screen.getByTestId("data-updates");
    expect(screen.queryByTestId("data-updates-gauges")).toBeNull();
    expect(screen.queryByTestId("data-updates-line")).toBeNull();
    expect(screen.getByTestId("more-sync-label").textContent).toBe(SYNC_WORDS.name);
    // right under the button's row, not after the list of sources
    expect(lines.previousElementSibling?.contains(screen.getByTestId("more-sync-button"))).toBe(true);
    // no second sync icon, no button, no tap target inside the lines
    expect([lines.querySelectorAll("svg").length, lines.querySelectorAll("[role=button],button,[tabindex]").length]).toEqual([0, 0]);
    fireEvent.click(lines);
    expect(onSync).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("more-sync-button"));
    expect(onSync).toHaveBeenCalledTimes(1);
    // the whole screen offers exactly one sync button
    expect(screen.getAllByRole("button").filter((b) => /refresh|update|sync/i.test(b.getAttribute("aria-label") ?? "")).length).toBe(1);
  });

  it("a source that could not be updated says so in words (never hidden); the ones that checked fine say nothing", () => {
    more(() => {});
    expect(screen.getByTestId("data-updates-dfo").textContent).toContain(UPDATE_WORDS.sheet.offline);
    expect(screen.queryByTestId("data-updates-inseason")).toBeNull();
  });

  it("nothing above the table when nothing is wrong: no update lines, no 'Checked' words, no new-release banner beside the button", () => {
    const quiet: DataInput = { ...DATA, updates: { sources: DATA.updates!.sources.filter((x) => x.k !== "dfo"), release: null } };
    render(<LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={quiet} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={sync} />
    </LayoutWidthContext.Provider>);
    expect(screen.queryByTestId("data-updates")).toBeNull();
    expect(screen.getByTestId("more-data").textContent).not.toContain("Checked");
  });

  it("a release this app cannot use, or one it refused, is said above the table (the button's words never say it)", () => {
    const refused: DataInput = { ...DATA, updates: { sources: [], release: { phase: "current", progress: null, refused: "r-2" } } };
    const { unmount } = render(<LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={refused} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={sync} />
    </LayoutWidthContext.Provider>);
    expect(screen.getByTestId("data-updates-notice").textContent).toBe(UPDATE_WORDS.row.refused);
    unmount();
    const old: DataInput = { ...DATA, updates: { sources: [], release: { phase: "app-too-old", progress: null } } };
    render(<LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={old} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={sync} />
    </LayoutWidthContext.Provider>);
    expect(screen.getByTestId("data-updates-notice").textContent).toBe(UPDATE_WORDS.row.appTooOld);
  });

  it("the button's state shows in words when it asks something (a newer release; a failure), once — no banner repeats it", () => {
    const newer = syncVM({ feed: true, feedFetchedAt: DATA.feedFetchedAt!, checkedAt: NOW, checking: false, newRelease: "r-9", now: NOW });
    render(<LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={{ ...DATA, newRelease: "r-9" }} back="Map" onBack={() => {}} contact={null} theme="light"
                  onTheme={() => {}} onPick={() => {}} sync={newer} />
    </LayoutWidthContext.Provider>);
    expect(screen.getByTestId("more-sync-label").textContent).toBe(newer.label);
    expect(screen.queryByTestId("data-new-release")).toBeNull();
  });

  it("without a sync button the newer-release banner and a download line say it instead", () => {
    const dl: DataInput = { ...DATA, newRelease: "r-9", updates: { sources: [], release: { phase: "waiting-wifi", progress: null } } };
    render(<LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={dl} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} />
    </LayoutWidthContext.Provider>);
    expect(screen.getByTestId("data-new-release")).toBeTruthy();
    expect(screen.getByTestId("data-updates-release").textContent).toContain(UPDATE_WORDS.row.waitingWifi);
  });
});
