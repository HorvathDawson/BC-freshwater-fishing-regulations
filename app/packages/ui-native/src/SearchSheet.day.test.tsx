/**
 * (T620 G1) A search row's status dot answers for the day the MAP is on, not for today: pick Saturday on the date
 * pill and the row, the line it lights and the water page say the same thing. 
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { SearchSheet } from "./SearchSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

// the day decides: the 30th is closed, any other day the water has its own regulations (the real index is gated in tools/one-answer.test.ts)
const INDEX = { waterCodeOn: (_i: string, on: Date) => (on.getDate() === 30 ? "closed" : "own") } as never;
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
