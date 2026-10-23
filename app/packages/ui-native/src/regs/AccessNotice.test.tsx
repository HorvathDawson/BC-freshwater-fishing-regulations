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
    expect(banner.textContent).toBe("No public access: this section is inside Coquitlam Watershed. The regulations below "
      + "are the Province's, but you may not enter this area to fish.Show area on map");
    expect(banner.style.borderTopColor).toBe(rgb(LIGHT.waterStatus.no_access));
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

  it("nothing to say: nothing drawn", () => {
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} />);
    expect(screen.queryByTestId("access-banner")).toBeNull();
    expect(screen.queryByTestId("access-note")).toBeNull();
  });
});
