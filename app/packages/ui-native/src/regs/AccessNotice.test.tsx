/**
 * T54 on screen: a section's access notices on its Regulations page — (T367) slim strips pinned under the header, outside
 * the scroll: the no-access banner in the warning amber, the gentle note in the accent, both when both apply. Words are
 * core's (`accessNotices`), pinned whole there; this pins where they land, their colours, and that a tap opens the text.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { accessNotices, type RegsPage } from "@app/core";
import { LIGHT, NO_ACCESS_BAND, laid } from "../theme";
import { RegsScreen } from "./RegsScreen";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const page = { ...(vedder1010 as unknown as RegsPage), sheet: () => null } as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};
const COQ = { tier: "no_access", name: "Coquitlam Watershed" } as const;
const CAP5 = { tier: "reserve", name: "Capilano 5" } as const;

describe("T54/T367: the access strips on the Regulations page", () => {
  it("the no-access strip: one line, amber, pinned outside the scroll; a tap opens the sentence", () => {
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} access={accessNotices([{ area: COQ, share: 1 }])} />);
    const banner = screen.getByTestId("access-banner");
    expect(screen.getByTestId("access-title").textContent).toBe("No public access · Coquitlam Watershed");
    expect(screen.queryByTestId("access-text")).toBeNull();
    expect([banner.style.borderLeftColor, banner.style.backgroundColor]).toEqual([rgb(LIGHT.warning), rgb(LIGHT.noAccessBand)]);
    expect(LIGHT.warning).toBe(LIGHT.waterStatus.own);
    // (T367 round 4) no dotted swatch before the title: it read as "…"
    expect(screen.queryByTestId("access-swatch")).toBeNull();
    // pinned: not in the scroll's content
    expect(screen.getByTestId("regs-content").contains(banner)).toBe(false);
    expect(screen.getByTestId("regs-scroll").contains(banner)).toBe(false);
    fireEvent.click(banner);
    expect(screen.getByTestId("access-text").textContent).toBe("This section is in Coquitlam Watershed, closed to the "
      + "public. The regulations below don't give you access.");
    fireEvent.click(banner);
    expect(screen.queryByTestId("access-text")).toBeNull();
    expect(screen.queryByTestId("access-map")).toBeNull();
  });

  it("both apply: the banner, then the reserve's note in the accent — not the warning", () => {
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       access={accessNotices([{ area: COQ, share: 0.5 }, { area: CAP5, share: 0.2 }])} />);
    expect([...screen.getByTestId("access-notices").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["access-banner", "access-note"]);
    const note = screen.getByTestId("access-note");
    expect(note.textContent).toBe("Access may be restricted · Indian reserve (Capilano 5)");
    expect([note.style.borderLeftColor, note.style.backgroundColor])
      .toEqual([rgb(LIGHT.accent), rgb(laid(LIGHT.accent, LIGHT.card, NO_ACCESS_BAND))]);
    expect(LIGHT.accent).not.toBe(LIGHT.warning);
    fireEvent.click(note);
    expect(note.textContent).toBe("Access may be restricted · Indian reserve (Capilano 5)"
      + "This section crosses an Indian reserve (Capilano 5).");
  });

  it("nothing to say: nothing drawn", () => {
    render(<RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} />);
    expect(screen.queryByTestId("access-notices")).toBeNull();
  });
});
