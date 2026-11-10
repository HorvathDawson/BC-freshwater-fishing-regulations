/**
 * (T620 G1) A search row's status dot answers for the day the MAP is on, not for today: pick Saturday on the date
 * pill and the row, the line it lights and the water page say the same thing. Real status index (the pipeline's
 * fixture): gnis:1 is closed on 2025-03-30 and has its own regulations from 2025-03-31.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { decodeStatusIndex } from "@app/core";
import { SearchSheet } from "./SearchSheet";
import { LIGHT } from "./theme";
import { FIXTURE } from "../../core/src/statusIndex.fixture";

afterEach(cleanup);

const INDEX = decodeStatusIndex(Uint8Array.from(atob(FIXTURE.bytes), (c) => c.charCodeAt(0)), FIXTURE.handles,
                                FIXTURE.reach_digest);
const HIT = { item: "gnis:1", name: "Test Creek", kind: "stream", matchedAs: null, pieces: 1, size: null, areaHa: null,
              near: null } as never;
const SESSION = {
  results: { waters: [HIT], places: [], pointers: [] }, typed: true, busy: false, failed: false, none: false,
  focus: null, town: { state: "idle" }, size: "full",
} as never;
const NO_PICK = { selected: null, town: null };

function rowLabel(day: Date | null): string {
  render(<SearchSheet open top={0} palette={LIGHT} session={SESSION} pick={NO_PICK} onAct={() => {}}
                      onOpenWater={() => {}} onClose={() => {}} statusIndex={INDEX} day={day} />);
  return screen.getAllByRole("button").map((b) => b.getAttribute("aria-label") ?? "")
    .find((l) => l.includes("Test Creek"))!;
}

describe("the search row's status is the picked day's", () => {
  it("the closed day says Closed, the next day says it has its own regulations, whatever today is", () => {
    expect(rowLabel(new Date(2025, 2, 30))).toMatch(/Closed$/);
    cleanup();
    expect(rowLabel(new Date(2025, 2, 31))).toMatch(/Has its own regulations$/);
  });
});
