/**
 * (T230) The in-season banner draws core's line whole; the notice lists the unmatched rows; the bell carries the count.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { appUpdateVM, bellLabel, inSeasonBanner, inSeasonNoticeRows, releaseNoticeVM } from "@app/core";
import { InSeasonBanners } from "./regs/InSeasonBanner";
import { InSeasonNotice } from "./InSeasonNotice";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";

afterEach(cleanup);
const SRC = "https://www2.gov.bc.ca/x";
const change = { id: "is-a", region: "8", water: "Okanagan River", from: "2026-10-15", to: "2026-11-30",
                 text: "No fishing July 30 to September 15.", source: SRC, waters: null, words: {} };

describe("in-season", () => {
  it("<InSeasonBanners>: collapsed, one line with the dates and 'see'; the notice is not drawn (T472)", () => {
    render(<InSeasonBanners banners={[inSeasonBanner(change, { where: null })]} palette={LIGHT} />);
    expect(screen.getByTestId("inseason-summary").textContent).toBe("In-season change (Oct 15–Nov 30) · see ›");
    expect(screen.getByTestId("inseason-line").getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByTestId("inseason-banner")).toBeNull();
  });
  it("<InSeasonBanners>: several changes say the count; a press opens every notice, a second closes them", () => {
    const b = (id: string) => inSeasonBanner({ ...change, id }, { where: null });
    render(<InSeasonBanners banners={[b("a"), b("b"), b("c")]} palette={LIGHT} />);
    expect(screen.getByTestId("inseason-summary").textContent).toBe("3 in-season changes · see ›");
    fireEvent.click(screen.getByTestId("inseason-line"));
    expect(screen.getAllByTestId("inseason-banner")).toHaveLength(3);
    fireEvent.click(screen.getByTestId("inseason-line"));
    expect(screen.queryByTestId("inseason-banner")).toBeNull();
  });
  it("<InSeasonBanners>: expanded, the owner's line whole, then the note and the source link", () => {
    render(<InSeasonBanners banners={[inSeasonBanner(change, { where: null })]} palette={LIGHT} />);
    fireEvent.click(screen.getByTestId("inseason-line"));
    expect(screen.getByTestId("inseason-line").getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByTestId("inseason-banner").textContent).toBe(
      "In-season change (Oct 15–Nov 30): No fishing July 30 to September 15."
      + "Shown as posted; the answers on this page do not include it yet.source ›");
  });
  it("(T518) <InSeasonBanners>: a change on part of the water says where, in its own ends", () => {
    render(<InSeasonBanners banners={[inSeasonBanner(change, { where: "From Hell's Gate to Spuzzum Creek confluence" })]} palette={LIGHT} />);
    fireEvent.click(screen.getByTestId("inseason-line"));
    expect(screen.getByTestId("inseason-where").textContent).toBe("Where: From Hell's Gate to Spuzzum Creek confluence");
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
  const vm = (phase: "waiting-wifi" | "downloading" | "ready", progress: { bytes: number; of: number } | null = null) =>
    releaseNoticeVM({ phase, progress })!;
  const draw = (v: ReturnType<typeof vm>, onAction: (k: string) => void = () => {}) =>
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}} release={{ vm: v, onAction }} />);

  it("words and counting, per state", () => {
    expect(vm("waiting-wifi", { bytes: 0, of: 120 * MB }).text).toBe("New regulations ready to download — waiting for Wi-Fi (120 MB).");
    expect(vm("downloading", { bytes: 30, of: 100 }).text).toBe("Downloading new regulations… 30%");
    expect(vm("ready").text).toBe("New regulations downloaded — they'll apply next time you return to the map.");
    expect([vm("waiting-wifi").counts, vm("downloading").counts, vm("ready").counts]).toEqual([true, false, true]);
    expect(releaseNoticeVM({ phase: "current", progress: null })).toBeNull();
    // (T390) a release refused for an old app is the App update section's strong prompt, not this row
    expect(releaseNoticeVM({ phase: "app-too-old", progress: null })).toBeNull();
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
  it("downloading: progress, no button; ready: Apply now", () => {
    const { unmount } = draw(vm("downloading", { bytes: 1, of: 4 }));
    expect(screen.getByTestId("notice-release-text").textContent).toBe("Downloading new regulations… 25%");
    expect(screen.queryByTestId("notice-release-action")).toBeNull();
    unmount();
    const seen: string[] = [];
    const r = draw(vm("ready"), (k) => seen.push(k));
    fireEvent.click(screen.getByLabelText("Apply now"));
    r.unmount();
    expect(seen).toEqual(["apply"]);
  });
});

describe("T390: the bell's App update section", () => {
  const draw = (app: Parameters<typeof InSeasonNotice>[0]["app"], extra: Partial<Parameters<typeof InSeasonNotice>[0]> = {}) =>
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}} app={app} {...extra} />);

  it("the strong prompt: its words, the data kept, one Update button", () => {
    let pressed = 0;
    draw({ vm: appUpdateVM({ channel: "play", check: { state: "current" }, tooOld: true, start: "idle" })!,
           onUpdate: () => { pressed++; } });
    expect(screen.getByText("App update")).toBeTruthy();
    expect(screen.getByTestId("notice-app-text").textContent).toBe("New data needs the latest app.");
    expect(screen.getByText("The app keeps using the data it has until it is updated.")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Update"));
    expect(pressed).toBe(1);
  });
  it("the gentle prompt sits first, above the data update; a failed start is said under the button", () => {
    draw({ vm: appUpdateVM({ channel: "play", check: { state: "available", version: "0.6.0" }, tooOld: false, start: "failed" })!,
           onUpdate: () => {} },
         { release: { vm: releaseNoticeVM({ phase: "ready", progress: null })!, onAction: () => {} } });
    expect(screen.getByText("Notices")).toBeTruthy();
    expect(screen.getByTestId("notice-app-text").textContent).toBe("Version 0.6.0 of the app is available.");
    expect(screen.getByTestId("notice-app-said").textContent).toBe("The update could not start.");
    expect(screen.getByLabelText("Update")).toBeTruthy();
    const order = screen.getAllByTestId(/^notice-(app|release)$/).map((e) => e.getAttribute("data-testid"));
    expect(order).toEqual(["notice-app", "notice-release"]);
  });
  it("downloading: said, no button; the staging test build: its own label", () => {
    const r = draw({ vm: appUpdateVM({ channel: "play", check: { state: "available", version: null }, tooOld: false, start: "downloading" })!,
                     onUpdate: () => {} });
    expect(screen.queryByTestId("notice-app-action")).toBeNull();
    expect(screen.getByTestId("notice-app-said").textContent).toBe("Downloading the update. The app restarts when it is ready.");
    r.unmount();
    draw({ vm: appUpdateVM({ channel: "apk-page", check: { state: "current" }, tooOld: true, start: "idle" })!, onUpdate: () => {} });
    expect(screen.getByLabelText("Get the new test build")).toBeTruthy();
  });
});

describe("T234 v1.1: what's new in the incoming version, under the bell's App update", () => {
  const draw = (app: Parameters<typeof InSeasonNotice>[0]["app"]) =>
    render(<InSeasonNotice open rows={[]} palette={LIGHT} onClose={() => {}} app={app} />);
  it("the version file's bullets, before the update is installed", () => {
    draw({ vm: appUpdateVM({ channel: "apk-page", check: { state: "available", version: "0.6.0", notes: ["Gauge alerts", "A changelog"] },
                             tooOld: false, start: "idle" })!, onUpdate: () => {} });
    expect(screen.getByText("What's new in 0.6.0")).toBeTruthy();
    expect(screen.getAllByTestId("notice-app-note").map((e) => e.textContent)).toEqual(["• Gauge alerts", "• A changelog"]);
  });
  it("a version with no notes anywhere is said missing, never a blank list", () => {
    draw({ vm: appUpdateVM({ channel: "apk-page", check: { state: "available", version: "9.9.9" }, tooOld: false, start: "idle" })!,
           onUpdate: () => {} });
    expect(screen.getByTestId("notice-app-notes-missing").textContent).toBe("No notes were published for version 9.9.9.");
    expect(screen.queryAllByTestId("notice-app-note")).toEqual([]);
  });
});

describe("the in-season line on the page (T472, T565)", () => {
  it("is first on the Regulations page, collapsed, and not inside the Keep tab (order: RegsScreen.test)", async () => {
    const { KeepTab } = await import("./regs/KeepTab");
    const vedder = (await import("./regs/fixtures/vedder_1010.json")).default as unknown as { keep: import("@app/core").KeepVM };
    render(<KeepTab keep={vedder.keep} dateLabel="Oct 10" palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} } as never} />);
    expect(screen.queryByTestId("inseason-banners")).toBeNull();
  });
});
