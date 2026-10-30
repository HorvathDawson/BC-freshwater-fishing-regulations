/**
 * (T292) The not-fishing-water sheet draws `notFishingSheet`'s words, whole, and is down when nothing is tapped.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { NotFishingSheet } from "./NotFishingSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<NotFishingSheet>", () => {
  it("a sewage lagoon: named, and not a fishing water", () => {
    render(<NotFishingSheet kind="sewage" palette={LIGHT} onClose={() => {}} />);
    expect(screen.getByText("Sewage lagoon — not a fishing water")).toBeTruthy();
    expect(screen.getByTestId("not-fishing-sheet").textContent).toBe(
      "The Freshwater Atlas records this as a sewage settling basin: man-made, and not a place to fish. It has no fishing regulations, and search does not list it.");
  });

  it("a tile with no kind: the sheet, unnamed", () => {
    render(<NotFishingSheet kind={null} palette={LIGHT} onClose={() => {}} />);
    expect(screen.getByText("Not a fishing water")).toBeTruthy();
  });

  it("nothing tapped: the sheet is down", () => {
    render(<NotFishingSheet kind={undefined} palette={LIGHT} onClose={() => {}} />);
    expect(screen.queryByTestId("not-fishing-sheet")).toBeNull();
  });
});
