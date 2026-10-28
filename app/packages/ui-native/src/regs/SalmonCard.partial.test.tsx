/**
 * PARTIAL COVERAGE (owner 2026-10-28, the Kitimat): a water with a DFO place not drawn yet leads with "Salmon rules on
 * parts of this water aren't mapped yet — check DFO" and DFO's page, and no row says a confident status.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
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
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6" } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "release", t: "No retention of coho (Jan 1–Oct 31)" },
               { s: "open", t: "4 per day, only 2 over 50 cm (Apr 1–Oct 31)" }],
    gears: [],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Kitimat River", l: ["6:kitimat-river:whole"], u: ["6:kitimat-river:downstream"],
        sp: [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 4] },
      { r: ["6"], w: "Kitimat River — downstream of Highway # 37 bridge.", l: ["6:kitimat-river:downstream"],
        sp: [[0, 0], [0, 2], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 1] },
    ],
    unmapped: [{ items: ["gnis:3225"], r: "6", w: "Kitimat River — downstream of Highway # 37 bridge.", chain: 2 }],
  },
};

describe("the Salmon card on a partly mapped water", () => {
  it("says the water is not fully mapped, links DFO, and names no confident status", () => {
    const card = salmonCard(FACTS, { sids: [101], item: "gnis:3225" }, new Date(2026, 9, 28));
    render(<SalmonCard card={card} palette={LIGHT} />);
    expect(screen.getByTestId("salmon-partial").textContent).toContain(
      "Salmon rules on parts of this water aren't mapped yet — check DFO");
    expect(screen.getByTestId("salmon-partial-link").getAttribute("role")).toBe("link");
    expect(card.partial?.url).toBe("https://example/region6");
    expect(screen.getByTestId("salmon-card").getAttribute("aria-label")).toBe("Salmon, DFO: Check DFO today");
    const rows = screen.getAllByTestId("salmon-card-block-row").map((r) => r.textContent!);
    expect(rows.some((r) => r.includes("Release only"))).toBe(false);
    // DFO's own words for the undrawn place still show, tagged
    expect(screen.getByTestId("salmon-unmapped").textContent).toContain("4 per day, only 2 over 50 cm");
  });
});
