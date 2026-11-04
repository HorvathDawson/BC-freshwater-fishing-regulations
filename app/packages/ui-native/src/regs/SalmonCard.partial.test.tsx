/**
 * PARTIAL COVERAGE (the Kitimat; T372, owner 2026-11-03: the trout way): a water with a DFO place not drawn yet keeps
 * its drawn answer and lists the place in the trout card's "in part" caveat block, with DFO's words for it.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { salmonCard, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 3,
  chainOf: (sid) => (sid === 101 ? 1 : 0),
  payload: {
    format: "salmon/1", credit: "DFO", default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "release", t: "No retention of coho (Jan 1–Oct 31)" },
               { s: "open", t: "4 per day, only 2 over 50 cm (Apr 1–Oct 31)" }],
    gears: [],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Kitimat River", l: ["6:kitimat-river:whole"], u: ["6:kitimat-river:downstream"],
        sp: [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 4] },
      { r: ["6"], w: "Kitimat River — downstream of Highway # 37 bridge.", l: ["6:kitimat-river:downstream"],
        sp: [[0, 0], [0, 2], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 1], n: [0, 1] },
    ],
    unmapped: [{ items: ["gnis:3225"], r: "6", w: "Kitimat River — downstream of Highway # 37 bridge.", chain: 2 }],
  },
};

describe("the Salmon card on a partly mapped water", () => {
  it("keeps the drawn answer and lists the undrawn place in the trout card's caveat block, with DFO's words", () => {
    const card = salmonCard(FACTS, { sids: [101], item: "gnis:3225" }, new Date(2026, 9, 28));
    render(<SalmonCard card={card} palette={LIGHT} />);
    // the rows are the drawn answer: coho release only — never "check DFO"
    const rows = screen.getAllByTestId("salmon-species-row").map((r) => r.textContent!);
    expect(rows.some((r) => r.includes("Release only"))).toBe(true);
    expect(screen.getByTestId("salmon-card").getAttribute("aria-label")).not.toContain("Check DFO");
    // the place, folded under the trout card's head; opened: where it is and what DFO says for it
    const block = screen.getByTestId("keep-caveat");
    expect(block.textContent).toContain("Fishing is different in some spots today");
    fireEvent.click(within(block).getByTestId("caveat-fold"));
    expect(block.textContent).toContain("Where: Kitimat River — downstream of Highway # 37 bridge.");
    expect(within(block).getAllByTestId("caveat-detail").map((e) => e.textContent))
      .toContain("Coho: 4 per day, only 2 over 50 cm (Apr 1–Oct 31)");
  });
});
