/**
 * (T168) THE DONOR ROWS GROUPED, ON SCREEN: Nicomen Slough's shape — its own two gauges silent, the Fraser's reading
 * ones listed, the rest folded. The fold is closed by default and names the count; pressing it shows the rows and
 * says so to a screen reader (`aria-expanded`, the label's "Hide them"); a folded member's dot is the quiet grey.
 *
 * Mutations (each run 2026-10-24, each failed here and was reverted):
 *   - `GaugeGroups` opening by default (`useState(true)`): the closed-by-default assertions fail.
 *   - DonorPanel's `quietAt` built from `vm.rows` alone: the folded 08MH085's dot is drawn in a live tone.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { MapProps } from "@app/map";
import type { DonorRow, GaugeSides, PanelAnswer } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (_: MapProps) => null,
}));
const { DonorPanel } = await import("./DonorPanel");

afterEach(cleanup);

const row = (station: string, lat: number, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.4, weight: 0.5, areaRatio: 2.4, areaKm2: 120,
  trust: "close", years: 40, regulated: false, sameRiver: true, factors: { share: 0.42, role: 1, record: 1 },
  route: { station: station as never, name: null, lat, lon: -121.5, role: "up", path: [lat] as never }, ...over,
});
const silent = { percentile: null, weight: 0, realtime: false } as const;

const VALUE: PanelAnswer = {
  answer: { ok: true, value: { percentile: 0.4, plusMinus: 12, trust: "close", spread: 6, donors: 2 } },
  rows: [row("08MF005", 49.2), row("08KA004", 49.3), row("08MH085", 49.1, silent), row("08MH086", 49.15, silent),
         row("08ME019", 49.4, silent)],
  areaKm2: 50, routesReady: true,
};
const SIDES: GaugeSides = new Map([["08MF005", "donor"], ["08KA004", "donor"], ["08MH085", "own"], ["08MH086", "own"],
                                    ["08ME019", "donor"]]);

/** jsdom reports a colour as `rgb(r, g, b)`; the tokens are hex. */
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("(T168) the donor rows, grouped", () => {
  it("own first, the count line whole, the not-reporting folded; opening a fold shows its rows", () => {
    const count = "No gauge on this water reporting · 2 estimating from other waters · 3 not reporting";
    render(<DonorPanel palette={LIGHT} value={VALUE} day={new Date(2026, 9, 23, 12)} sides={SIDES} count={count} />);
    expect(screen.getByTestId("gauge-count").textContent).toBe(count);
    const own = screen.getByTestId("donor-groups-own"), donor = screen.getByTestId("donor-groups-donor");
    expect(own.textContent).toBe("ON THIS WATER2 not reporting");
    expect(donor.textContent?.startsWith("ESTIMATING FROM OTHER WATERS08MF005")).toBe(true);
    // own above the donors in the document
    expect(own.compareDocumentPosition(donor) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    // closed: the folded members are not rows, the silent line is not drawn (its members are the folds')
    expect(screen.queryByText("08MH085")).toBeNull();
    expect(screen.queryByText(/^Not reporting:/)).toBeNull();
    const fold = screen.getByTestId("donor-groups-own-fold");
    expect(fold.getAttribute("aria-label")).toBe("2 gauges on this water not reporting. Show them");
    expect(fold.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(fold);
    expect(fold.getAttribute("aria-expanded")).toBe("true");
    expect(fold.getAttribute("aria-label")).toBe("2 gauges on this water not reporting. Hide them");
    expect(screen.getByText("08MH085")).toBeTruthy();
    expect(screen.getByText("08MH086")).toBeTruthy();
    // a folded member is drawn quiet: grey, never a live tone
    expect((screen.getByTestId("donor-dot-08MH085") as HTMLElement).style.backgroundColor).toBe(rgb(LIGHT.quiet));
    // the donors' fold is its own, still closed
    expect(screen.queryByText("08ME019")).toBeNull();
    expect(screen.getByTestId("donor-groups-donor-fold").getAttribute("aria-label"))
      .toBe("1 gauge from other waters not reporting. Show them");
  });

  it("without sides (a saved spot) the rows are one run and the silent line names the silent", () => {
    render(<DonorPanel palette={LIGHT} value={VALUE} day={new Date(2026, 9, 23, 12)} />);
    expect(screen.queryByTestId("donor-groups")).toBeNull();
    expect(screen.queryByTestId("gauge-count")).toBeNull();
    expect(screen.getByText(/^Not reporting: 08MH085/)).toBeTruthy();
  });
});
