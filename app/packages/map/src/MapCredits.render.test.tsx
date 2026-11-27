/**
 * (T424) THE MAP'S CREDITS: one list (`creditLines`), one component (`MapCredits`) on the web and the phone, and the
 * corners kept apart. The renderers' own attribution controls are off, so the two cannot list or look different; the
 * scale bar is bottom-left and the credits bottom-right, at the narrowest width and with Esri's badge showing.
 */
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Dimensions } from "react-native";
import { GEOMETRY, mapChrome } from "./chrome";
import { creditLines, lineText, partsHtml, MAP_CREDITS_DRAWN, ATLAS_CREDIT, BASEMAP_CREDIT } from "./credits";
import { MapCredits, STARTUP_CREDIT, STARTUP_MS, resetStartupCredit, startupMaxWidth } from "./MapCredits";

afterEach(cleanup);
const PLAIN = { view: "plain", modes: {}, groups: {}, basemap: "map" } as const;
const ids = (l: Parameters<typeof creditLines>[0]) => creditLines(l).map((c) => c.id);

describe("the one list of credits (T424)", () => {
  it("names the ground, the Province's atlas and the gauges, and only what a look turns on beyond that", () => {
    expect(ids(PLAIN)).toEqual(["basemap", "atlas", "gauges"]);
    expect(ids({ ...PLAIN, basemap: "satellite" })).toEqual(["imagery", "atlas", "gauges"]);
    expect(ids({ ...PLAIN, modes: { stream: "salmon" } })).toEqual(["basemap", "atlas", "gauges", "salmon"]);
    expect(ids({ ...PLAIN, modes: { stream: "runs" } })).toEqual(["basemap", "atlas", "gauges", "runs"]);
    expect(ids({ ...PLAIN, groups: { obstacles: true } })).toEqual(["basemap", "atlas", "gauges", "obstacles"]);
    expect(ids({ ...PLAIN, view: "stocked" })).toEqual(["basemap", "atlas", "gauges", "stocking"]);
  });

  it("carries each licence's own wording: ODbL's, Esri's, OGL-Canada's, with the publisher named", () => {
    const text = (l: Parameters<typeof creditLines>[0]) => creditLines(l).map(lineText).join("\n");
    // OpenStreetMap: ODbL asks for exactly "© OpenStreetMap contributors", linked to its copyright page
    expect(text(PLAIN)).toContain("Basemap: © OpenStreetMap contributors · © Protomaps");
    const osm = creditLines(PLAIN).flatMap((c) => c.parts).filter((p) => typeof p !== "string" && p.text === "© OpenStreetMap contributors");
    expect(osm.map((p) => (p as { href: string }).href)).toEqual(["https://www.openstreetmap.org/copyright", "https://www.openstreetmap.org/copyright"]);
    // satellite: the basemap is not drawn, so it is not named; OpenStreetMap stays (the names and waters) and Esri's wording is whole
    const sat = text({ ...PLAIN, basemap: "satellite" });
    expect(sat).not.toContain("Protomaps");
    expect(sat).toContain("© OpenStreetMap contributors");
    expect(sat).toContain("Imagery: Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community");
    expect(text(PLAIN)).toContain("Province of British Columbia (OGL-BC)");
    expect(text({ ...PLAIN, modes: { stream: "runs" } })).toContain(
      "Run timing: Pacific Salmon Foundation (with permission) · Fisheries and Oceans Canada (OGL-Canada)");
    expect(text(PLAIN)).toContain("Gauges: Environment and Climate Change Canada");
  });

  it("is what the style's sources carry too (their HTML is derived from the same parts)", () => {
    expect(BASEMAP_CREDIT).toBe(partsHtml(creditLines(PLAIN)[0]!.parts));
    expect(ATLAS_CREDIT).toBe(partsHtml(creditLines(PLAIN)[1]!.parts));
    for (const c of MAP_CREDITS_DRAWN) expect(c.html.startsWith(`${c.label}: `)).toBe(true);
  });
});

describe("the credits chip and card (T424)", () => {
  const chrome = mapChrome("light");
  it("is a square © chip; a tap opens a card naming each source on screen, a second closes it", () => {
    render(<MapCredits look={{ ...PLAIN, modes: { stream: "salmon" } }} chrome={chrome} />);
    expect(screen.queryByTestId("map-credits-card")).toBeNull();
    const chip = screen.getByTestId("map-credits-chip");
    expect(chip.textContent).toBe("©");
    expect(["", "0px"]).toContain(getComputedStyle(chip).borderRadius);   // square: none set
    fireEvent.click(chip);
    const card = screen.getByTestId("map-credits-card");
    expect(["", "0px"]).toContain(getComputedStyle(card).borderRadius);
    // one row per line, in the list's order, in the list's words: the card is exactly `creditLines`
    const rows = [...card.querySelectorAll("[data-testid^=map-credit-]")].map((e) => e.textContent);
    expect(rows).toEqual(creditLines({ ...PLAIN, modes: { stream: "salmon" } }).map(lineText));
    fireEvent.click(screen.getByTestId("map-credits-close"));
    expect(screen.queryByTestId("map-credits-card")).toBeNull();
  });

  it("a link in the card opens its licence through the app's one way; with none given it is named, not linked", () => {
    const open = vi.fn();
    const { unmount } = render(<MapCredits look={PLAIN} chrome={chrome} onOpenLink={open} />);
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    fireEvent.click(screen.getAllByText("© OpenStreetMap contributors")[0]!);
    expect(open).toHaveBeenCalledWith("https://www.openstreetmap.org/copyright");
    unmount();
    render(<MapCredits look={PLAIN} chrome={chrome} />);
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    expect(screen.queryAllByRole("link")).toEqual([]);
    expect(screen.getByTestId("map-credit-basemap").textContent).toContain("© OpenStreetMap contributors");
  });

  it("Esri's 'Powered by Esri' is on the map beside the chip while its imagery is, and only then", () => {
    const { rerender } = render(<MapCredits look={PLAIN} chrome={chrome} />);
    expect(screen.queryByTestId("esri-badge")).toBeNull();
    rerender(<MapCredits look={{ ...PLAIN, basemap: "satellite" }} chrome={chrome} />);
    expect(screen.getByTestId("esri-badge").textContent).toBe("Powered by Esri");
  });

  it("sits bottom-right, over the foot block: the 28 px chip 14 px in and 10 px up, in a 44 x 44 press (T1212)", () => {
    render(<MapCredits look={PLAIN} chrome={chrome} insetBottom={120} />);
    const s = getComputedStyle(screen.getByTestId("map-credits"));
    const press = getComputedStyle(screen.getByTestId("map-credits-chip")), face = getComputedStyle(screen.getByTestId("map-credits-face"));
    // the press reaches (44 - 28) / 2 = 8 px past the chip each way, so the chip itself stands on the grid
    expect([s.right, s.bottom, press.width, press.height, face.width, face.height])
      .toEqual(["6px", "122px", "44px", "44px", "28px", "28px"]);
    expect([GEOMETRY.right, GEOMETRY.bottom, GEOMETRY.target]).toEqual([14, 10, 44]);
  });

  it("(T1212) in the map's own corner when nothing crosses the foot: 10 px up from the map's bottom, level with the key chip", () => {
    render(<MapCredits look={PLAIN} chrome={chrome} insetBottom={0} />);
    expect(getComputedStyle(screen.getByTestId("map-credits")).bottom).toBe("2px");
  });

  it("(T1212) Esri's badge stands over the ©, not beside it: the corner is no wider than the badge", () => {
    render(<MapCredits look={{ ...PLAIN, basemap: "satellite" }} chrome={chrome} />);
    const box = screen.getByTestId("map-credits");
    expect([getComputedStyle(box).flexDirection, [...box.children].map((c) => c.getAttribute("data-testid"))])
      .toEqual(["column", ["esri-badge", "map-credits-chip"]]);
  });
});

describe("the open card is above the app's overlays (T729)", () => {
  it("is drawn in a modal layer, whole, with satellite's Esri text; a tap on the dim or Close shuts it", () => {
    const chrome = mapChrome("dark");
    render(<MapCredits look={{ ...PLAIN, basemap: "satellite" }} chrome={chrome} insetBottom={300} />);
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    const card = screen.getByTestId("map-credits-card");
    // not inside the map's own corner block (under the foot's legend): in a modal dialog layer
    expect(card.closest("[data-testid=map-credits]")).toBeNull();
    expect(card.closest("[aria-modal=true], [role=dialog]")).not.toBeNull();
    expect(screen.getByTestId("map-credit-imagery").textContent).toContain("Powered by Esri");
    fireEvent.click(screen.getByTestId("map-credits-scrim"));
    expect(screen.queryByTestId("map-credits-card")).toBeNull();
  });
});

describe("the open card goes with the look it was opened on (T729)", () => {
  it("a tab change closes it, scrim included: nothing of it is left to cover the map; no scrim without the card", () => {
    const chrome = mapChrome("light");
    const { rerender } = render(<MapCredits look={PLAIN} chrome={chrome} />);
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    expect(screen.getByTestId("map-credits-scrim")).toBeTruthy();
    expect(screen.getByTestId("map-credits-card")).toBeTruthy();
    rerender(<MapCredits look={{ ...PLAIN, modes: { stream: "runs" } }} chrome={chrome} />);
    expect(screen.queryByTestId("map-credits-scrim")).toBeNull();
    expect(screen.queryByTestId("map-credits-card")).toBeNull();
    rerender(<MapCredits look={PLAIN} chrome={chrome} />);
    expect(screen.queryByTestId("map-credits-scrim")).toBeNull();
    // and the scrim and the card are always together
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    expect([!!screen.queryByTestId("map-credits-scrim"), !!screen.queryByTestId("map-credits-card")]).toEqual([true, true]);
    fireEvent.click(screen.getByTestId("map-credits-close"));
    expect([!!screen.queryByTestId("map-credits-scrim"), !!screen.queryByTestId("map-credits-card")]).toEqual([false, false]);
  });
});

describe("the open card's back is the app's one back (T729)", () => {
  it("registers the open state with the back layer it is handed, and a back there closes the card", () => {
    const calls: boolean[] = [];
    let close: () => void = () => {};
    const layer = (open: boolean, c: () => void) => { calls.push(open); close = c; return c; };
    render(<MapCredits look={PLAIN} chrome={mapChrome("light")} useBackLayer={layer} />);
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    expect(calls.at(-1)).toBe(true);
    act(() => { close(); });
    expect(screen.queryByTestId("map-credits-card")).toBeNull();
    expect(calls.at(-1)).toBe(false);
  });

  it("(T723) the card sits inside the platform back it is handed (the iOS left-edge swipe), whose back is the card's one back", () => {
    const backs: (() => void)[] = [];
    function EdgeBack({ onBack, children }: { onBack: () => void; children: ReactNode }) {
      backs.push(onBack);
      return <div data-testid="edge-back">{children}</div>;
    }
    const calls: boolean[] = [];
    const layer = (open: boolean, c: () => void) => { calls.push(open); return c; };
    render(<MapCredits look={PLAIN} chrome={mapChrome("light")} useBackLayer={layer} modalBack={EdgeBack} />);
    expect(screen.queryByTestId("edge-back")).toBeNull();                     // nothing of it while the card is shut
    fireEvent.click(screen.getByTestId("map-credits-chip"));
    expect(screen.getByTestId("edge-back").contains(screen.getByTestId("map-credits-card"))).toBe(true);
    act(() => { backs.at(-1)!(); });                                         // the swipe in from the edge
    expect([screen.queryByTestId("map-credits-card"), calls.at(-1)]).toEqual([null, false]);
  });
});

describe("the credit is shown first, then folds (OSMF attribution guideline)", () => {
  const chrome = mapChrome("light");
  afterEach(() => { vi.useRealTimers(); resetStartupCredit(); });

  it("the first map of a session shows '© OpenStreetMap contributors' and folds it to the chip after five seconds", () => {
    vi.useFakeTimers();
    resetStartupCredit();
    render(<MapCredits look={PLAIN} chrome={chrome} />);
    expect(screen.getByTestId("map-credits-startup").textContent).toBe(STARTUP_CREDIT);
    act(() => { vi.advanceTimersByTime(STARTUP_MS - 1); });
    expect(screen.queryByTestId("map-credits-startup")).not.toBeNull();
    act(() => { vi.advanceTimersByTime(1); });
    expect(screen.queryByTestId("map-credits-startup")).toBeNull();
    expect(screen.getByTestId("map-credits-chip")).toBeTruthy();          // the licence is still one tap away
  });

  it("a tap folds it at once; a later map in the same session starts folded", () => {
    resetStartupCredit();
    const first = render(<MapCredits look={PLAIN} chrome={chrome} />);
    fireEvent.click(screen.getByTestId("map-credits-startup"));
    expect(screen.queryByTestId("map-credits-startup")).toBeNull();
    first.unmount();
    render(<MapCredits look={PLAIN} chrome={chrome} />);
    expect(screen.queryByTestId("map-credits-startup")).toBeNull();
  });
});

describe("T1212 the opening credit line never meets the key row or the scale (review, 320 wide)", () => {
  it("beside a floating key (plain, or with its isolate row) it stands over the foot block, its width stopping short of the scale", () => {
    const G = GEOMETRY;
    // at 320, 360 and 412 the widest the line may be leaves it clear of the scale bar's widest end (it wraps, never reaches)
    for (const width of [320, 360, 412])
      expect([width, width - G.right - startupMaxWidth(width) >= G.scaleLeft + G.scaleMax + G.gap]).toEqual([width, true]);
    const look = { view: "plain", modes: {}, groups: {}, basemap: "drawn" } as never;
    for (const foot of [54, 106]) {   // the key chip's row; with the isolate row ("Showing: … / Show all") over it
      resetStartupCredit();
      render(<MapCredits look={look} chrome={mapChrome("light")} insetBottom={0} footBottom={foot} />);
      const s = getComputedStyle(screen.getByTestId("map-credits-startup"));
      expect([foot, parseFloat(s.bottom) >= foot + G.bottom, parseFloat(s.maxWidth)])
        .toEqual([foot, true, startupMaxWidth(Dimensions.get("window").width)]);
      cleanup();
    }
  });
});
