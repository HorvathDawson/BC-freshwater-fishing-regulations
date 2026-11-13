/**
 * (T521, owner 2026-11-09) The bell's DFO section: a DFO rule on a water no curated binding knows, naming DFO's region
 * and its words; a list the feed could not read, said; nothing to say, no section.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { dfoNoticeRows, type SalmonFeed } from "@app/core";
import { InSeasonNotice } from "./InSeasonNotice";
import { LIGHT } from "./theme";

afterEach(cleanup);

const FEED = { format: "dfo-feed/3", at: "2026-10-27T18:00:00Z", verdicts: [{ s: "closed", t: "x" }], gears: [], locs: {},
               regions: { "6": { name: "Skeena", url: "u", asof: null, checked: null, stale: false, held: null } } } as unknown as SalmonFeed;
const NEW_WATER = { fingerprint: "bb22", region: "6", items: [], water: "Brand New Creek", scope: "",
                    rows: ["Coho | Sep 1 to Oct 31 | 2 per day"] };

describe("the bell's DFO section (T521)", () => {
  it("a new water: DFO's region · the water, and DFO's words", () => {
    render(<InSeasonNotice open palette={LIGHT} onClose={() => {}}
                           dfo={dfoNoticeRows({ ...FEED, unplaced: 1, unplacedRows: [NEW_WATER] })} />);
    expect(screen.getAllByTestId("dfo-notice-row").map((r) => r.textContent))
      .toEqual(["DFO Region 6 Skeena · Brand New CreekCoho · Sep 1 to Oct 31 · 2 per day"]);
    expect(screen.queryByTestId("notice-empty")).toBeNull();
    expect(screen.getByText("New DFO waters")).toBeTruthy();
  });

  it("an unread list is said, never an empty bell", () => {
    render(<InSeasonNotice open palette={LIGHT} onClose={() => {}} dfo={dfoNoticeRows({ ...FEED, unplaced: 1 })} />);
    expect(screen.getByTestId("dfo-notice-unread").textContent)
      .toBe("1 DFO place not yet on the map could not be read. They will show when the next check reads them.");
  });

  it("zero unplaced: no section", () => {
    render(<InSeasonNotice open palette={LIGHT} onClose={() => {}} dfo={dfoNoticeRows({ ...FEED, unplaced: 0, unplacedRows: [] })} />);
    expect(screen.queryByTestId("notice-dfo")).toBeNull();
    expect(screen.getByTestId("notice-empty")).toBeTruthy();
  });
});
