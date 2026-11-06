/**
 * (T424) THE MAP'S CREDITS: one list (`creditLines`), one component (`MapCredits`) on the web and the phone, and the
 * corners kept apart. The renderers' own attribution controls are off, so the two cannot list or look different; the
 * scale bar is bottom-left and the credits bottom-right, at the narrowest width and with Esri's badge showing.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GEOMETRY, ESRI_BADGE, mapChrome } from "./chrome";
import { creditLines, lineText, partsHtml, MAP_CREDITS_DRAWN, ATLAS_CREDIT, BASEMAP_CREDIT } from "./credits";
import { MapCredits, STARTUP_CREDIT, STARTUP_MS, resetStartupCredit } from "./MapCredits";

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
    expect(screen.getByTestId("esri-badge").textContent).toBe(ESRI_BADGE);
  });

  it("sits bottom-right, over the foot block", () => {
    render(<MapCredits look={PLAIN} chrome={chrome} insetBottom={120} />);
    const s = getComputedStyle(screen.getByTestId("map-credits"));
    expect([s.right, s.bottom]).toEqual([`${GEOMETRY.right}px`, `${GEOMETRY.bottom + 120}px`]);
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
