/**
 * (T692, owner 2026-11-12: "Keep in search, tidal card only") A TIDAL PAGE IS THE TIDAL CARD ALONE. DFO lists no rules for
 * tidal water here, so a salmon card or bar asked for it read "SALMON (DFO) Closed" above the card (Deas Slough): the
 * page asks no salmon at all while it is tidal, and draws no salmon block, card or pending face.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { RegsInputs, SalmonFacts } from "@app/core";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

const TIDAL = {
  target: { kind: "part", item: "gnis:8634", partIx: 0 }, day: { month: 10, day: 10 }, weekday: "Saturday", calendarYear: 2026,
  item: { id: "gnis:8634", ord: 1, name: "Deas Slough", kind: "stream", sections: [], parts: [null], partSections: [[]], picker: null, steelhead: null },
  partIx: 0, place: null, key: null, tidalPart: true, s: null, cell: null, frames: null, year: null, ruleset: null,
  rules: { get: (rix: number) => ({ rix, missing: true }), has: () => false, size: 0 }, lics: new Map(), mergedParts: null,
  statics: { exportSpecies: { fish: {}, groups: {} }, fish: [], glossary: { terms: [] } },
} as unknown as RegsInputs;

const asked: string[] = [];
vi.mock("@app/ui", async (orig) => {
  const ui = await orig<typeof import("@app/ui")>();
  const ready = <T,>(value: T) => ({ state: "ready", value, error: null });
  return {
    ...ui,
    useRegsStore: () => ready({ navRows: async () => ({ sections: [] }) }),
    useRegsTarget: () => ready(TIDAL.target),
    useRegulations: () => ready(TIDAL),
  };
});

/** DFO lists nothing here: the salmon default is closed. */
const FACTS = {
  handles: "", chains: 1, chainOf: () => 0,
  payload: { format: "salmon/1", credit: "DFO", default: 0, regions: {}, verdicts: [{ s: "closed", t: "Not opened by DFO", d: 1 }], gears: [],
             chains: [{ r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] }], unmapped: [] },
} as unknown as SalmonFacts;

afterEach(cleanup);

describe("<WaterScreen> a tidal page (T692)", () => {
  it("is the tidal banner alone: no salmon asked, no Salmon (DFO) line, card or pending face", async () => {
    const loader = { release: "r-x", manifest: { files: { salmon: { url: "salmon.bin.gz" } } },
                     salmon: async () => { asked.push("salmon"); return FACTS; }, method: async () => null, access: async () => null,
                     paint: async () => null } as unknown as ReleaseLoader;
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} loader={loader}
                        day={new Date(2026, 9, 10, 12)} glance />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    expect(screen.getByTestId("keep-banner").textContent).toMatch(/^Tidal water/);
    expect([screen.queryByTestId("salmon-block"), screen.queryByTestId("salmon-card"), screen.queryByTestId("salmon-pending"),
            screen.queryByTestId("glance:salmon")]).toEqual([null, null, null, null]);
    expect(screen.queryByText(/SALMON \(DFO\)/i)).toBeNull();
    expect(asked).toEqual([]);
  }, 30_000);
});
