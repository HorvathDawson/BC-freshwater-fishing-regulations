/**
 * (T870, owner 2026-11-18 on the Mamquam: "The source dfo here doesn't show the rows that actually produce the results
 * above. It should have all the rows that apply in source  Also the bait ban here is confusing idk where it is from")
 * The Source (DFO) fold, opened, lists DFO's rows behind the card's answers (each place with its rows as printed, then
 * the page header's notes); the bait ban DFO and the B.C. rules both set is ONE line naming both, and no B.C. bait line.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { salmonCard, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const DAYLIGHT = "You can only fish for salmon in Region 2 during daylight hours.";
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2, chainOf: () => 1,
  payload: {
    format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
    regions: { "2": { name: "Lower Mainland", asof: "2026-11-06", url: "https://example/2", shared: [], sizes: [],
                      hours: { says: "Salmon fishing in daylight only: one hour before sunrise to one hour after sunset", id: "2:daylight:a" },
                      notes: [{ id: "2:daylight:a", k: "daylight", t: DAYLIGHT, fold: "hours" }] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "open", t: "1 per day (Sep 15–Jan 31)" }],
    gears: [["Bait ban (all year)"]],
    chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
             { r: ["2"], w: "Mamquam River", l: ["2:mamquam-river:whole"], sp: [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]],
               g: [0, 0], m: [0, 1], a: [[0, -1], [0, 0], [0, -1], [0, -1], [0, -1]] }],
    unmapped: [],
    p: { "2:mamquam-river:whole": { r: "2", water: "Mamquam River" } },
    pr: { "2:mamquam-river:whole": ["All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day, hatchery marked only"] },
  } as unknown as SalmonFacts["payload"],
};

describe("(T870) the Mamquam's salmon card", () => {
  it("the bait ban is one line naming DFO and the B.C. rules; no B.C. bait line beside it", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [1], item: null, gear: ["bait"] }, new Date(2026, 10, 19))} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-card-block-gear").map((e) => e.textContent))
      .toEqual(["Bait ban: no bait of any kind — DFO (all year) and B.C. rules"]);
    expect(screen.queryByTestId("salmon-gear-provincial")).toBeNull();
  });
  it("Source (DFO), opened, lists the place's rows as printed and the header note behind the hours line", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [1], item: null, gear: ["bait"] }, new Date(2026, 10, 19))} palette={LIGHT} />);
    expect(screen.queryByTestId("salmon-sources")).toBeNull();                         // the fold is closed by default
    fireEvent.click(screen.getByTestId("salmon-source-summary"));
    const body = screen.getByTestId("salmon-source-body");
    expect(within(body).getAllByTestId("salmon-sources-group").map((g) => [
      within(g).getByTestId("salmon-sources-from").textContent, within(g).getAllByTestId("salmon-sources-line").map((l) => l.textContent)]))
      .toEqual([["Region 2 · Mamquam River", ["All | Apr 1 to Mar 31 | Bait ban", "Coho | Sep 15 to Jan 31 | 1 per day, hatchery marked only"]],
                ["Region 2 Lower Mainland · page header", [DAYLIGHT]]]);
    expect(within(body).getByTestId("salmon-sources").textContent!.startsWith("DFO's rows behind these answers")).toBe(true);
  });
});
