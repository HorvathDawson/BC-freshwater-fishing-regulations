/**
 * T42 (C) through the water screen itself: outside B.C. (no key) WaterScreen hands RegsScreen no Gear, Methods or
 * Licence slot — not a "not in the app yet" placeholder. The store hooks are replaced by the inputs T09
 * `loadRegsInputs` gives for a handle outside B.C.; the page is built by the real `useRegsPage` (core buildRegsPage).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor, within } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { RegsInputs } from "@app/core";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

const OUTSIDE = {
  target: { kind: "section", handle: 2_000_000_000 }, day: { month: 10, day: 10 }, weekday: "Saturday", calendarYear: 2026,
  item: null, partIx: 0, place: null, key: null, s: null, cell: null, frames: null, year: null, ruleset: null,
  rules: { get: (rix: number) => ({ rix, missing: true }), has: () => false, size: 0 }, lics: new Map(), mergedParts: null,
  statics: { exportSpecies: { fish: {}, groups: {} }, fish: [], glossary: { terms: [] } },
} as unknown as RegsInputs;

vi.mock("@app/ui", async (orig) => {
  const ui = await orig<typeof import("@app/ui")>();
  const ready = <T,>(value: T) => ({ state: "ready", value, error: null });
  return {
    ...ui,
    useRegsStore: () => ready({}),
    useRegsTarget: () => ready(OUTSIDE.target),
    useRegulations: () => ready(OUTSIDE),
  };
});

afterEach(cleanup);

describe("<WaterScreen> outside B.C. (T42)", () => {
  it("only the Keep tab: no Gear, Methods or Licence section, and no 'not in the app yet' placeholder", async () => {
    const loader = { release: "r-x", manifest: {} } as unknown as ReleaseLoader;
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} loader={loader}
                        day={new Date(2026, 9, 10, 12)} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy());
    expect(screen.getByTestId("keep-outside").textContent).toBe("This part of the water is outside B.C.: no B.C. rules apply.");
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-tab"]);
    expect(screen.queryByTestId("regs-tab-pending")).toBeNull();
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(["What you can keep"]);
  });
});
