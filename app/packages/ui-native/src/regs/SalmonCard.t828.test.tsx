/**
 * (T828, owner 2026-11-16: "why do we have this section in salmon" → "Fold into Source") DFO's rows as printed — each
 * species' row words, an in-season notice number, DFO's words for this water, the region's rules — sit inside the
 * "Source (DFO)" fold, closed by default. Wording the app could not read ("DFO changed the wording here — read their
 * text") stays above the fold. No data change: every one of those strings is still on the card once the fold opens.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { salmonCard, salmonTable, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const n = (id: string, k: string, t: string, extra: object = {}) => ({ id, k, t, ...extra });
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2, chainOf: () => 1,
  payload: {
    format: "salmon/1", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/6", shared: [], sizes: [],
                      notes: [n("6:other:a", "other", "Reminder: There is no fishing for eulachon in Region 6.")] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },
               { s: "open", t: "4 per day (Jul 29–Aug 16)", n: "FN0787" }],
    gears: [],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Skeena River", l: ["6:k"], sp: [[0, 1], [0, 1], [0, 2], [0, 1], [0, 1]], g: [0, -1], m: [0, 3] },
    ],
    unmapped: [],
    x: { "6:k": [{ k: "new", sp: "Coho", d: "Jul 15 to Aug 23", t: "4 per day, fish from shore on Sundays" },
                 { k: "note", sp: "Sockeye, pink and chum", d: "Apr 1 to Mar 31", t: "No fishing for sockeye, pink or chum salmon, unless otherwise stated below" }] },
  } as unknown as SalmonFacts["payload"],
};
const AUG1 = new Date(2026, 7, 1);
const card = () => salmonCard(FACTS, { sids: [1], item: null }, AUG1);

describe("(T828) DFO's rows as printed fold into Source (DFO)", () => {
  it("the fold is closed by default; opened, it holds the species' row words, the notice, DFO's words for the water and the region rules", () => {
    const c = card();
    // the fixture really has every kind of row the ticket names
    expect([salmonTable(c).says.length > 0, c.dfoWords?.notes.length, c.regionRules.length]).toEqual([true, 1, 1]);
    render(<SalmonCard card={c} palette={LIGHT} />);
    const summary = screen.getByTestId("salmon-source-summary");
    expect([summary.textContent, summary.getAttribute("aria-expanded")]).toEqual(["Source (DFO)", "false"]);
    for (const id of ["salmon-says", "salmon-wording-notes", "salmon-region-rules", "salmon-source-body"])
      expect(screen.queryByTestId(id), id).toBeNull();
    expect(screen.getByTestId("salmon-card").textContent).not.toContain("FN0787");
    fireEvent.click(summary);
    expect(summary.getAttribute("aria-expanded")).toBe("true");
    const body = screen.getByTestId("salmon-source-body");
    expect(within(body).getAllByTestId("salmon-says-line").map((e) => e.textContent)).toContain("4 per day (Jul 29–Aug 16) · FN0787");
    expect(within(body).getByTestId("salmon-wording-notes").textContent)
      .toBe("DFO's words for this waterSockeye, pink and chum · Apr 1 to Mar 31No fishing for sockeye, pink or chum salmon, unless otherwise stated below");
    expect(within(body).getByTestId("salmon-region-rules").textContent).toContain("Reminder: There is no fishing for eulachon in Region 6.");
    // DFO's own source lines follow the rows they are the source of
    expect(within(body).getByTestId("salmon-says").compareDocumentPosition(within(body).getByTestId("salmon-source"))
      & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it("the wording DFO changed stays above the fold, visible while it is closed, and is not repeated inside it", () => {
    render(<SalmonCard card={card()} palette={LIGHT} />);
    const warn = screen.getByTestId("salmon-wording-changed");
    expect(warn.textContent).toBe("DFO changed the wording here — read their textCoho · Jul 15 to Aug 234 per day, fish from shore on Sundays");
    const fold = screen.getByTestId("salmon-source-fold");
    expect(fold.contains(warn)).toBe(false);
    expect(warn.compareDocumentPosition(fold) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    fireEvent.click(screen.getByTestId("salmon-source-summary"));
    expect(screen.getAllByTestId("salmon-wording-changed")).toHaveLength(1);
    expect(fold.contains(screen.getByTestId("salmon-wording-changed"))).toBe(false);
  });
});
