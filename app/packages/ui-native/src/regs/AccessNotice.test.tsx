/**
 * T54 on screen: a section's access notice opens its Regulations page — the no-access banner, ruled in the map's fourth
 * colour, with "Show area on map"; or the gentle note. Words are core's (`accessNotice`), pinned whole there; this pins
 * where they land and that the link moves to the map.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { accessNotice, type RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { RegsScreen } from "./RegsScreen";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const page = { ...(vedder1010 as unknown as RegsPage), sheet: () => null } as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("T54: the access notice on the Regulations page", () => {
  it("the no-access banner leads the page, in the fourth colour, and its link shows the map", () => {
    const notice = accessNotice([{ area: { tier: "no_access", name: "Coquitlam Watershed" }, share: 1 }])!;
    let moved = 0;
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} access={notice}
                       onAccessMap={() => { moved++; }} />);
    const banner = screen.getByTestId("access-banner");
    expect(banner.textContent).toBe("No public accessThis section is inside Coquitlam Watershed. The public may not go "
      + "there, so you can't fish here. The regulations below are the Province's; they don't give you access."
      + "Show area on map");
    // (T147) a block in the fourth colour: its bar, its tint as the ground, the legend's swatch beside a large title
    expect([banner.style.borderLeftColor, banner.style.borderLeftWidth, banner.style.backgroundColor,
            screen.getByTestId("access-swatch").style.backgroundColor])
      .toEqual([rgb(LIGHT.waterStatus.no_access), "6px", rgb(LIGHT.noAccessBand), rgb(LIGHT.waterStatus.no_access)]);
    expect([screen.getByTestId("access-title").textContent, screen.getByTestId("access-title").getAttribute("role")])
      .toEqual(["No public access", "heading"]);
    // first in the scroll: before the Keep tab
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")).slice(0, 2))
      .toEqual(["access-banner", "keep-tab"]);
    fireEvent.click(screen.getByTestId("access-map"));
    expect(moved).toBe(1);
  });

  it("the gentle note for private land: small, no colour rule", () => {
    const notice = accessNotice([{ area: { tier: "private", name: "" }, share: 0.4 }])!;
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} access={notice} onAccessMap={() => {}} />);
    expect(screen.getByTestId("access-note").textContent)
      .toBe("Access may be restricted here: this section crosses private land. See the map.See the map");
    expect(screen.queryByTestId("access-banner")).toBeNull();
  });

  it("(T147) the block leads even an out-of-date edition's page: above the lapsed line", () => {
    const notice = accessNotice([{ area: { tier: "no_access", name: "Seymour Watershed" }, share: 1 }])!;
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} access={notice}
                       lapsed="These regulations ran to March 31, 2026." />);
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")).slice(0, 3))
      .toEqual(["access-banner", "regs-lapsed", "keep-tab"]);
  });

  it("permit-only land is the gentle note, never the block (T54 Q1 default)", () => {
    const notice = accessNotice([{ area: { tier: "permit", name: "UBC Malcolm Knapp Research Forest" }, share: 1 }])!;
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} access={notice} />);
    expect(screen.getByTestId("access-note").textContent).toBe("Access may be restricted here: this section crosses "
      + "permit-only land (UBC Malcolm Knapp Research Forest), where a permit is needed. See the map.");
    expect(screen.queryByTestId("access-banner")).toBeNull();
  });

  it("nothing to say: nothing drawn", () => {
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} />);
    expect(screen.queryByTestId("access-banner")).toBeNull();
    expect(screen.queryByTestId("access-note")).toBeNull();
  });
});
