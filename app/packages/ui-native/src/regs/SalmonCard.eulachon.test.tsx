/**
 * (T770, owner 2026-11-16 19:09: "As a closure line (Recommended)") the Kitsumkalum card: DFO's eulachon row is a
 * CLOSURE LINE right under the species rows (core `closures`, Python's words), never one of the gear lines below them.
 * Words are core's (`salmonCard`, pinned in salmonEulachon.test.ts); this pins where they land.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import { salmonCard, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const EULACHON = "Eulachon closed all year: No fishing for eulachon";
const BAIT = "No natural bait allowed (Jan 1–Jun 15)";
const five = <T,>(x: T) => [x, x, x, x, x];
/** (T829) the closure line is the last row of the trout-table look's "✕ Closed" group, under the closed species */
const closedRows = () => within(screen.getByTestId("salmon-closed-group")).getAllByTestId("keep-name").map((e) => e.textContent);
// the final scratch release r-1603e71cc92c's chain 279 (Kitsumkalum River — downstream of railway bridge), cut down
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2, chainOf: (sid) => (sid === 1 ? 1 : 0), loaded: [],
  payload: {
    format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/6", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" }],
    gears: [[BAIT], [EULACHON]],
    chains: [
      { r: [], w: "", l: [], sp: five([0, 0]), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Kitsumkalum River — downstream of railway bridge.", l: [], sp: five([0, 1]), g: [0, 0, 167, -1], o: [0, 1], m: [0, 3] },
    ],
    unmapped: [], scopes: {},
  } as never,
};

describe("(T770) the Kitsumkalum salmon card: the eulachon closure is a closure line, not a gear line", () => {
  it("Jan 10: the closure line under the species rows, the bait line alone among the gear lines", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [1], item: null }, new Date(2026, 0, 10))} palette={LIGHT} />);
    expect(closedRows()).toEqual(["Chinook", "Coho", "Sockeye", "Pink", "Chum", EULACHON]);
    expect(screen.getAllByTestId("salmon-card-block-gear").map((e) => e.textContent)).toEqual([BAIT]);
    // right under the species rows, in their closed group: before the gear line, its picture the closed mark
    const group = screen.getByTestId("salmon-closed-group");
    expect(group.compareDocumentPosition(screen.getByTestId("salmon-card-block-gear")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    const line = within(group).getAllByTestId("keep-row").at(-1)!;
    expect([within(line).queryByTestId("keep-look"), line.textContent]).toEqual([null, `✕${EULACHON}`]);
  });

  it("Jul 15: no gear line that day, the closure line still stands", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [1], item: null }, new Date(2026, 6, 15))} palette={LIGHT} />);
    expect([closedRows().at(-1), screen.queryAllByTestId("salmon-card-block-gear")]).toEqual([EULACHON, []]);
  });

  it("an unlisted water draws no closure line", () => {
    render(<SalmonCard card={salmonCard(FACTS, { sids: [9], item: null }, new Date(2026, 0, 10))} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-card").textContent).not.toContain(EULACHON);
  });
});
