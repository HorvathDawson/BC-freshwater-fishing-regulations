/**
 * (T230) The in-season banner draws core's line whole; the notice lists the unmatched rows; the bell carries the count.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { bellLabel, inSeasonBanner, inSeasonNoticeRows, releaseNoticeVM } from "@app/core";
import { InSeasonBanners } from "./regs/InSeasonBanner";
import { InSeasonNotice } from "./InSeasonNotice";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";

afterEach(cleanup);
const SRC = "https://www2.gov.bc.ca/x";
const change = { id: "is-a", region: "8", water: "Okanagan River", from: "2026-10-15", to: "2026-11-30",
                 text: "No fishing July 30 to September 15.", source: SRC, extents: [], places: null };

describe("in-season", () => {
  it("<InSeasonBanners>: the owner's line, whole, then the note and the source link", () => {
    render(<InSeasonBanners banners={[inSeasonBanner({ change, partial: false })]} palette={LIGHT} />);
    expect(screen.getByTestId("inseason-banner").textContent).toBe(
      "In-season change (Oct 15–Nov 30): No fishing July 30 to September 15."
      + "Shown as posted; the answers on this page do not include it yet.source ›");
  });
  it("<InSeasonBanners>: nothing to draw, nothing drawn", () => {
    render(<InSeasonBanners banners={[]} palette={LIGHT} />);
    expect(screen.queryByTestId("inseason-banners")).toBeNull();
  });
  it("<InSeasonNotice>: each row's region · water · dates and the text", () => {
    render(<InSeasonNotice open rows={inSeasonNoticeRows([{ ...change, region: "4", water: "Kaslo River", to: null, from: "2025-04-04" }])}
                           palette={LIGHT} onClose={() => {}} />);
    expect(screen.getAllByTestId("inseason-notice-row").map((r) => r.textContent))
      .toEqual(["Region 4 · Kaslo River · since Apr 4, 2025No fishing July 30 to September 15.source ›"]);
  });
  it("<TopBar>: the bell with its count opens the notice; no count, the bell without a badge (owner 2026-10-29)", () => {
    let opened = 0;
    const bar = (count: number) => (
      <TopBar palette={LIGHT} placeholder="p" sync={null} open={false} query="" onQuery={() => {}} onOpen={() => {}}
              onClose={() => {}} notices={{ count, label: `${count} in-season changes not on the map yet`, onPress: () => { opened++; } }} />);
    const { rerender } = render(bar(3));
    expect(screen.getByTestId("inseason-count").textContent).toBe("3");
    fireEvent.click(screen.getByLabelText("3 in-season changes not on the map yet"));
    expect(opened).toBe(1);
    rerender(bar(0));
    expect(screen.getByTestId("inseason-bell")).toBeTruthy();
    expect(screen.queryByTestId("inseason-count")).toBeNull();
  });
  it("<InSeasonNotice>: opened with nothing pending, it says so", () => {
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}} sync={{ failed: [], checking: false, onRetry: () => {} }} />);
    expect(screen.getByTestId("notice-empty").textContent).toBe("No notifications");
    expect(screen.getByText("Notifications")).toBeTruthy();
    expect(bellLabel(null, 0)).toBe("No notifications");
  });
});

describe("T263: the bell's Data update row", () => {
  const MB = 1048576;
  const vm = (phase: "waiting-wifi" | "downloading" | "ready" | "app-too-old", progress: { bytes: number; of: number } | null = null, devBuild = false) =>
    releaseNoticeVM({ phase, progress }, { devBuild })!;
  const draw = (v: ReturnType<typeof vm>, onAction: (k: string) => void = () => {}) =>
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}} release={{ vm: v, onAction }} />);

  it("words and counting, per state", () => {
    expect(vm("waiting-wifi", { bytes: 0, of: 120 * MB }).text).toBe("New regulations ready to download — waiting for Wi-Fi (120 MB).");
    expect(vm("downloading", { bytes: 30, of: 100 }).text).toBe("Downloading new regulations… 30%");
    expect(vm("ready").text).toBe("New regulations downloaded — they'll apply next time you return to the map.");
    expect(vm("app-too-old").text).toBe("New regulations need a newer version of the app.");
    expect([vm("waiting-wifi").counts, vm("downloading").counts, vm("ready").counts, vm("app-too-old").counts])
      .toEqual([true, false, true, true]);
    expect(releaseNoticeVM({ phase: "current", progress: null })).toBeNull();
    expect(bellLabel("2 in-season changes", 1, vm("ready"))).toBe("New regulations ready to apply; 2 in-season changes; 1 update failed");
  });
  it("waiting for Wi-Fi: Download now asks first, then acts; Not now does not", () => {
    const seen: string[] = [];
    draw(vm("waiting-wifi", { bytes: 0, of: 120 * MB }), (k) => seen.push(k));
    fireEvent.click(screen.getByLabelText("Download now"));
    expect(screen.getByTestId("notice-release-text").textContent).toContain("Download 120 MB over mobile data?");
    fireEvent.click(screen.getByLabelText("Not now"));
    expect(seen).toEqual([]);
    fireEvent.click(screen.getByLabelText("Download now"));
    fireEvent.click(screen.getByLabelText("Download"));
    expect(seen).toEqual(["download"]);
  });
  it("downloading: progress, no button; ready: Apply now; too old: Update the app / the test build", () => {
    const { unmount } = draw(vm("downloading", { bytes: 1, of: 4 }));
    expect(screen.getByTestId("notice-release-text").textContent).toBe("Downloading new regulations… 25%");
    expect(screen.queryByTestId("notice-release-action")).toBeNull();
    unmount();
    const seen: string[] = [];
    const r = draw(vm("ready"), (k) => seen.push(k));
    fireEvent.click(screen.getByLabelText("Apply now"));
    r.unmount();
    draw(vm("app-too-old"), (k) => seen.push(k));
    fireEvent.click(screen.getByLabelText("Update the app"));
    expect(seen).toEqual(["apply", "update-app"]);
    cleanup();
    draw(vm("app-too-old", null, true));
    expect(screen.getByLabelText("Get the new test build")).toBeTruthy();
  });
});
