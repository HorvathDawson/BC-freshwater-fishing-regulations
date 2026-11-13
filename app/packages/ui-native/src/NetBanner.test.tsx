/**
 * (T254) BAD CONNECTIONS ON SCREEN: the banner (offline / a slow line the app found; never the reader's own On), the
 * Data setting in More (three choices, its line, stored), a page's waiting line and a missing part's line, the rule
 * book offered with its size on a slow line (nothing fetched until Download), and a phone's NetInfo cellular generation.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { NET_WORDS, type DataInput } from "@app/core";
import { createNet, type Net } from "@app/data";
import { installCrashHandlers, NetContext, type NetState } from "@app/ui";
import { NetBanner } from "./NetBanner";
import { LIGHT } from "./theme";
import { MoreScreen } from "./menu/MoreScreen";
import { PartPending, RegsWaiting } from "./regs/NetWaiting";
import { SynopsisLoading } from "./regs/SynopsisLoading";
import { openBook } from "./regs/openBook";
import { getSynopsisLoad, cancelSynopsisLoad, declineSynopsisOffer } from "./regs/synopsisLoad";
import { setSynopsisNet, setSynopsisSource } from "./regs/synopsisSource";
import { hintsFrom, netInfoConnectivity, type NetInfoLike, type NetInfoStateLike } from "./netInfoConnectivity";

afterEach(() => { cleanup(); vi.restoreAllMocks(); setSynopsisNet(null); setSynopsisSource(null); declineSynopsisOffer(); cancelSynopsisLoad(); });

const state = (net: Net | null, mode: NetState["mode"], over: Partial<NetState> = {}): NetState =>
  ({ net, mode, setting: "auto", setSetting: () => {}, ...over });
const SLOW = { low: true, why: "measured", offline: false } as const;
const OFFLINE = { low: false, why: null, offline: true } as const;
const GOOD = { low: false, why: null, offline: false } as const;

describe("<NetBanner>", () => {
  const banner = (v: NetState) => render(<NetContext.Provider value={v}><NetBanner palette={LIGHT} onPress={() => {}} /></NetContext.Provider>);
  it("a slow line the app found: said; offline: said; a good line or the reader's own On: nothing", () => {
    banner(state(null, SLOW));
    expect(screen.getByTestId("net-status").textContent).toContain(NET_WORDS.banner.slow);
    cleanup();
    banner(state(null, OFFLINE));
    expect(screen.getByTestId("net-status").textContent).toContain(NET_WORDS.banner.offline);
    cleanup();
    banner(state(null, GOOD));
    expect(screen.queryByTestId("net-status")).toBeNull();
    cleanup();
    banner(state(null, { low: true, why: "setting", offline: false }));
    expect(screen.queryByTestId("net-status")).toBeNull();
  });
});

describe("More → Data: the low-bandwidth setting", () => {
  const DATA: DataInput = { sources: {}, validUntil: null, tiles: null, tilesPaired: true, feedFetchedAt: null,
                            now: Date.now(), newRelease: null, forecastIssuedAt: null } as unknown as DataInput;
  it("three choices, the one in force checked; a press sets it; the line says what it is doing", () => {
    const set = vi.fn();
    render(<NetContext.Provider value={state(createNet(), SLOW, { setting: "auto", setSetting: set })}>
      <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}} onPick={() => {}} />
    </NetContext.Provider>);
    fireEvent.click(screen.getByTestId("menu-item-settings"));  // (T703) the controls live in Settings
    const checked = ["auto", "on", "off"].map((k) => screen.getByTestId(`data-low-${k}`).getAttribute("aria-checked"));
    expect(checked).toEqual(["true", "false", "false"]);
    expect(screen.getByTestId("data-low-now").textContent).toMatch(/^On now — slow connection/);
    fireEvent.click(screen.getByTestId("data-low-off"));
    expect(set).toHaveBeenCalledWith("off");
  });
  it("no Net (a screen alone): no row", () => {
    render(<MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}} onPick={() => {}} />);
    fireEvent.click(screen.getByTestId("menu-item-settings"));  // (T703) the controls live in Settings
    expect(screen.queryByTestId("data-low")).toBeNull();
  });
});

describe("waiting, and a missing part", () => {
  it("under the spinner: how far the regulations' files have come (at once on a slow line)", () => {
    const net = createNet();
    net.activity.queued("water", 0, null);
    net.activity.started("water");
    net.activity.progress("water", 1_200_000, 3_400_000);
    net.activity.queued("salmon-2", 1, 1000);                 // an extra: not the regulations
    render(<NetContext.Provider value={state(net, SLOW)}><RegsWaiting palette={LIGHT} /></NetContext.Provider>);
    expect(screen.getByTestId("regs-slow").textContent).toBe("Loading the regulations… 1.2 MB of 3.4 MB");
    act(() => net.activity.retry("water", 2));
    expect(screen.getByTestId("regs-slow").textContent).toBe("Connection dropped — trying again (1)");
  });
  it("offline: it will load when the line is back", () => {
    render(<NetContext.Provider value={state(createNet(), OFFLINE)}><RegsWaiting palette={LIGHT} /></NetContext.Provider>);
    expect(screen.getByTestId("regs-slow").textContent).toBe(NET_WORDS.waiting.offline);
  });
  it("a good line: nothing for the first moments (a fast answer never flashes a line)", () => {
    const net = createNet();
    net.activity.queued("water", 0, null);
    render(<NetContext.Provider value={state(net, GOOD)}><RegsWaiting palette={LIGHT} /></NetContext.Provider>);
    expect(screen.queryByTestId("regs-slow")).toBeNull();
  });
  it("a part still coming / failed offline / failed online with Try again", () => {
    const retry = vi.fn();
    const part = (st: "loading" | "failed", m: NetState["mode"]) => render(
      <NetContext.Provider value={state(null, m)}><PartPending palette={LIGHT} what="Salmon rules" state={st} onRetry={retry} testID="salmon-pending" /></NetContext.Provider>);
    part("loading", SLOW);
    expect(screen.getByTestId("salmon-pending").textContent).toBe("Salmon rules: still loading (slow connection)");
    cleanup();
    part("failed", OFFLINE);
    expect(screen.getByTestId("salmon-pending").textContent).toBe("Salmon rules: not available offline");
    expect(screen.queryByText(NET_WORDS.part.retry)).toBeNull();
    cleanup();
    part("failed", GOOD);
    fireEvent.click(screen.getByText(NET_WORDS.part.retry));
    expect(retry).toHaveBeenCalled();
  });
});

describe("the rule book on a slow line: offered with its size, fetched only on Download", () => {
  it("low mode: the offer, nothing fetched; Download starts it; a good line goes straight on", async () => {
    setSynopsisSource({ url: "https://r2.test/synopsis.pdf", sha256: "a".repeat(64), bytes: 25_041_728 });
    let low = true;
    setSynopsisNet({ low: () => low, bps: () => 50_000 });
    const open = vi.fn(async () => {});
    render(<SynopsisLoading palette={LIGHT} />);
    await act(async () => { await openBook(17, [], "web", open); });
    expect(open).not.toHaveBeenCalled();
    expect(screen.getByTestId("synopsis-offer").textContent).toContain("The rule book is 25.0 MB — about 8 min on this connection.");
    await act(async () => { fireEvent.click(screen.getByTestId("synopsis-download")); await Promise.resolve(); });
    expect(open).toHaveBeenCalledTimes(1);
    // Not now: nothing fetched, the card goes
    act(() => cancelSynopsisLoad());
    await act(async () => { await openBook(17, [], "web", open); });
    fireEvent.click(screen.getByTestId("synopsis-decline"));
    expect(getSynopsisLoad().phase).toBe("idle");
    expect(open).toHaveBeenCalledTimes(1);
    // a good line: no offer
    low = false;
    await act(async () => { await openBook(17, [], "web", open); });
    expect(open).toHaveBeenCalledTimes(2);
  });
});

describe("a phone's NetInfo: the cellular generation", () => {
  it("cellular 3g → a hint; Wi-Fi → none; a change is said once", () => {
    expect(hintsFrom({ isConnected: true, type: "cellular", details: { cellularGeneration: "3g" } })).toEqual({ online: true, cellularGeneration: "3g" });
    expect(hintsFrom({ isConnected: true, type: "wifi", details: {} })).toEqual({ online: true, cellularGeneration: null });
    let listeners: ((s: NetInfoStateLike) => void)[] = [];
    const ni: NetInfoLike = { addEventListener(on) { listeners.push(on); return () => { listeners = listeners.filter((l) => l !== on); }; } };
    const c = netInfoConnectivity(() => ni);
    const heard: number[] = [];
    c.watchHints!(() => heard.push(1));
    const emit = (s: NetInfoStateLike) => listeners.forEach((l) => l(s));
    emit({ isConnected: true, type: "cellular", details: { cellularGeneration: "3g" } });
    emit({ isConnected: true, type: "cellular", details: { cellularGeneration: "3g" } });
    expect(heard).toHaveLength(1);
    expect(c.hints!()).toEqual({ cellularGeneration: "3g" });
  });
});

describe("the failed synopsis card closes without opening anything (T285)", () => {
  it("Close; the failure is said on the card AND reported (owner 2026-11-03)", async () => {
    setSynopsisSource({ url: "https://r2.test/synopsis.pdf", sha256: "a".repeat(64), bytes: 10 });
    setSynopsisNet(null);
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const reported: unknown[] = [];
    const undo = installCrashHandlers({ report: (e) => { reported.push(e); }, flush: async () => false, sendExit: async () => false }, {});
    render(<SynopsisLoading palette={LIGHT} floating />);
    const err = new Error("synopsis: this release ships no page index (synopsis.append)");
    await act(async () => { await openBook(5, [], "android", vi.fn().mockRejectedValue(err)); });
    expect(getSynopsisLoad()).toMatchObject({ phase: "error" });
    expect(screen.getByTestId("synopsis-loading").textContent).toContain("Couldn’t load the synopsis.");
    expect(reported).toEqual([err]);
    expect(screen.getByTestId("synopsis-fallback")).toBeTruthy();
    fireEvent.click(screen.getByTestId("synopsis-close"));
    expect(getSynopsisLoad().phase).toBe("idle");
    undo();
    setSynopsisSource(null);
  });
});
