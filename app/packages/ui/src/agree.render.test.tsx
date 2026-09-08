/**
 * THE MAP AND THE SHEET MUST ALWAYS MATCH. This is the test that says so.
 *
 * They have diverged three times now — the Harrison, the Fraser, the Skeena — and each time
 * the cause was a different one: a quantity flattened to discharge on one side and not the
 * other, a panel read through a cache on one side and straight from the source on the
 * other, a horizon threaded into one call and defaulted in the next. Each was fixed where
 * it was found. None of them would have been FOUND by a test, because every test asked one
 * side or the other and never both about the same water.
 *
 * So this asks both, over the same section, the same feed, every quantity and every
 * horizon, and demands the same number. It does not care how they get there — only that a
 * reader tapping a river is told what the river was coloured.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import type { Panel, RegsSource, SectionId } from "@app/data";
import { clearPanelCache, HORIZONS, usePanel, usePanelStandings, type Quantity } from "./panel";

const A = 111 as SectionId;
/**
 * A SECOND REACH, GAUGED ONLY IN METRES.
 *
 * 237 BC stations measure stage and never discharge. This is the shape that separates
 * "discharge" from "discharge" — under "discharge" it answers from its own level, under "discharge"
 * it has nothing to say — and without it the fixture cannot tell the two apart, so a side
 * that flattens "discharge" to discharge passes. That flattening is the exact bug that hid the
 * Harrison's disagreement for a week.
 */
const B = 222 as SectionId;

/** A panel with the shapes that have actually caused trouble, all at once. */
const PANEL: Panel = {
  areaKm2: 500,
  members: [
    // A same-river gauge reporting discharge, and a tributary reporting only level.
    { station: "FLOW" as never, role: "up", areaKm2: 520, years: 40,
      regulated: false, sameRiver: true },
    { station: "STAGE" as never, role: "down", areaKm2: 480, years: 90,
      regulated: true, sameRiver: false },
    { station: "QUIET" as never, role: "up", areaKm2: 510, years: 12,
      regulated: false, sameRiver: true },
  ],
};

const STAGE_ONLY: Panel = {
  areaKm2: 300,
  members: [
    { station: "STAGE" as never, role: "up", areaKm2: 310, years: 90,
      regulated: false, sameRiver: true },
  ],
};

const PANELS: Record<string, Panel> = { [A]: PANEL, [B]: STAGE_ONLY };

const source = {
  panelsFor: async (secs: readonly SectionId[]) =>
    new Map(secs.filter((s) => PANELS[s]).map((s) => [s, PANELS[s]!])),
  lakeStationsFor: async () => new Map(),
} as unknown as RegsSource;

const feed = {
  index: async () => ({
    stations: {
      FLOW: { percentile: 0.31, parameter: "discharge" as const, discharge: 0.31,
              ahead: { "1": { discharge: 0.4 }, "3": { discharge: 0.52 } } },
      STAGE: { percentile: 0.62, parameter: "level" as const, level: 0.62,
               ahead: { "1": { level: 0.6 } } },
      QUIET: { percentile: null, parameter: "discharge" as const },
    },
  }),
};

beforeEach(clearPanelCache);

describe("the map and the sheet agree", () => {
  const QUANTITIES: Quantity[] = ["discharge", "discharge", "level"];

  for (const quantity of QUANTITIES)
    for (const horizon of HORIZONS)
      it(`agrees for ${quantity} at +${horizon}`, async () => {
        const map = renderHook(() =>
          usePanelStandings(source, feed, [A, B], quantity, horizon));
        await waitFor(() => expect(map.result.current.size).toBe(2));

        for (const section of [A, B]) {
          const sheet = renderHook(() => usePanel(source, feed, section, quantity, horizon));
          /*
           * WAIT FOR THE FETCH, NOT FOR AN ANSWER.
           *
           * An unsettled hook reports `{ok: false, why: "no-station"}` — the same shape as
           * a genuine refusal — so "it refused" is not evidence that it has finished. This
           * cost a false failure the first time it ran: the map had answered and the sheet
           * had not started. `areaKm2` is null until the panel is in hand and non-null
           * after, for every section that has one.
           */
          await waitFor(() => expect(sheet.result.current.areaKm2).not.toBeNull());
          const painted = map.result.current.get(section)!;
          const said = sheet.result.current.answer;
          if (said.ok) {
            // The map stores 0–1; the same number the sheet reports.
            expect(painted).toBeCloseTo(said.value.percentile, 9);
          } else {
            // And when the sheet refuses, the map must paint the refusal — not a value.
            expect(painted).toBe(-0.01);
          }
        }
      });

  it("refuses on both sides when the feed is unreachable", async () => {
    const dead = { index: async () => null };
    const map = renderHook(() => usePanelStandings(source, dead, [A]));
    const sheet = renderHook(() => usePanel(source, dead, A));
    await waitFor(() => expect(sheet.result.current.answer.ok).toBe(false));
    expect(map.result.current.size).toBe(0);
  });
});
