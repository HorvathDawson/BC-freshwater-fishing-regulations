/**
 * (T387 review, owner 2026-11-04: "the Salmon card no longer shows next to trout") A RELEASE THAT SHIPS SALMON DRAWS THE
 * CARD ON EVERY WATER PAGE, OR ITS FACE: the card when it came, the pending / failed face in its place otherwise — in the
 * page and in the Keep tab's at-a-glance table alike. The glance used to fold the card into its table only when the
 * card's row was there, and drew nothing at all while the card was coming or had failed: a silent gap.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);
const LOADED = { timeout: 20_000 } as const;
const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
/** Every section of the fixture on one DFO chain (open to coho in the fall). */
const FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2, chainOf: () => 1,
  payload: {
    format: "salmon/1", credit: "Fisheries and Oceans Canada (DFO)", default: 0,
    regions: { "2": { name: "Lower Fraser", asof: "2026-08-28", url: "https://example/2", shared: [], sizes: [] } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "open", t: "4 per day (Sep 1–Dec 31)" }],
    gears: [],
    chains: [{ r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
             { r: ["2"], w: "Chilliwack River", l: [], sp: all(1), g: [0, -1], m: [0, 3] }],
    unmapped: [],
  },
} as unknown as SalmonFacts;

/** The fixture release as one that ships salmon (its manifest names the file), the salmon read as given. */
function withSalmon(salmon: () => Promise<SalmonFacts | null>): ReleaseLoader {
  const live = openRegsFixture();
  return Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
    manifest: { ...live.manifest, files: { ...live.manifest.files, salmon: { url: "salmon.bin.gz" } } },
    salmon, method: async () => null, access: async () => null,
  });
}

describe("<WaterScreen> the Salmon card on a water with salmon", () => {
  for (const glance of [false, true]) {
    it(`the card is drawn${glance ? " (the at-a-glance Keep tab)" : ""}`, async () => {
      render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} glance={glance}
                          loader={withSalmon(async () => FACTS)} day={new Date(2026, 9, 10, 12)} part={0} />);
      await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
      // the glance folds the card into its table: its own row
      await waitFor(() => expect(screen.getByTestId(glance ? "glance:salmon" : "salmon-card")).toBeTruthy(), LOADED);
      expect(screen.queryByTestId("salmon-pending")).toBeNull();
    }, 30_000);

    it(`a card that failed is said in its place, never a gap${glance ? " (the at-a-glance Keep tab)" : ""}`, async () => {
      render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} glance={glance}
                          loader={withSalmon(async () => { throw new Error("salmon: damaged"); })}
                          day={new Date(2026, 9, 10, 12)} part={0} />);
      await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
      await waitFor(() => expect(screen.getByTestId("salmon-pending")).toBeTruthy(), LOADED);
      expect(screen.queryByTestId("salmon-card")).toBeNull();
    }, 30_000);
  }
});
