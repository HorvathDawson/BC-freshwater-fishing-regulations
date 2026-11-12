/**
 * (T668 fix 4, owner 2026-11-11) each species row names where today's answer comes from, as DFO's page names the place
 * ("Region 6 · Skeena River · B(ii)"); a release that names no places shows none. The words are core's (`placeFrom`).
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { salmonCard, type SalmonFacts } from "@app/core";
import { LIGHT } from "../theme";
import { SalmonCard } from "./SalmonCard";

afterEach(cleanup);

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const facts = (named: boolean): SalmonFacts => ({
  handles: "0".repeat(16), chains: 2, chainOf: (sid) => (sid === 101 ? 1 : 0),
  payload: {
    format: "salmon/4", credit: "DFO", default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "open", t: "2 per day (Jul 1–Aug 31)" }, { s: "closed", t: "No fishing for salmon" }],
    gears: [],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Skeena River", l: ["6:skeena-river:mainstem-waters-downstream-of-cnr", "6:a:region"],
        sp: [[0, 2], [0, 1], [0, 2], [0, 2], [0, 2]], g: [0, -1], m: [0, 1],
        ...(named ? { a: [[0, 1], [0, 0], [0, 1], [0, 1], [0, 1]] } : {}) },
    ],
    unmapped: [],
    ...(named ? { p: { "6:skeena-river:mainstem-waters-downstream-of-cnr": { r: "6", water: "Skeena River", sec: "B(ii)" },
                       "6:a:region": { r: "6", sec: "A" } } } : {}),
  },
});

describe("the Salmon card names where each species' answer comes from", () => {
  it("coho from the Skeena's B(ii) water, the rest from Region 6's section A", () => {
    render(<SalmonCard card={salmonCard(facts(true), { sids: [101], item: null }, new Date(2026, 6, 20))} palette={LIGHT} />);
    expect(screen.getAllByTestId("salmon-from").map((e) => e.textContent)).toEqual(
      ["Region 6 · A", "Region 6 · Skeena River · B(ii)", "Region 6 · A", "Region 6 · A", "Region 6 · A"]);
  });

  it("a release that names no places (before T668 fix 4) shows none", () => {
    render(<SalmonCard card={salmonCard(facts(false), { sids: [101], item: null }, new Date(2026, 6, 20))} palette={LIGHT} />);
    expect(screen.queryAllByTestId("salmon-from")).toEqual([]);
  });
});
