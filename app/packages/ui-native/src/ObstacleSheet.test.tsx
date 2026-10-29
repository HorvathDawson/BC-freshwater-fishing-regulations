/**
 * (T173) The fish obstacle sheet draws `obstacleSheet`'s words, whole, and is down when nothing is tapped.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { OBSTACLE_WORDS } from "@app/core";
import { ObstacleSheet } from "./ObstacleSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<ObstacleSheet>", () => {
  it("a tapped dam: its name, every row, the note and the licence line", () => {
    render(<ObstacleSheet palette={LIGHT} onClose={() => {}}
                          props={{ obstacle_id: "wris:D120299-00", type: "dam", name: "Walker Lake Dam", height_m: 2,
                                   source: "wris", year: 1962 }} />);
    expect(screen.getByTestId("obstacle-sheet").textContent).toBe(
      "TypeDam" + "Height2 m" + "Commissioned1962" + "SourceBC Dams (provincial dam register) · WRIS D120299-00"
      + OBSTACLE_WORDS.note + OBSTACLE_WORDS.licence);
    expect(screen.getByText("Walker Lake Dam")).toBeTruthy();
  });

  it("the header wears the type's icon (a type it does not know wears the dot)", () => {
    const { rerender } = render(<ObstacleSheet palette={LIGHT} onClose={() => {}} props={{ type: "falls" }} />);
    expect(screen.getByTestId("obstacle-icon-falls")).toBeTruthy();
    rerender(<ObstacleSheet palette={LIGHT} onClose={() => {}} props={{ type: "weir" }} />);
    expect(screen.getByTestId("obstacle-icon-other")).toBeTruthy();
  });

  it("nothing tapped: the sheet is down", () => {
    render(<ObstacleSheet palette={LIGHT} onClose={() => {}} props={null} />);
    expect(screen.queryByTestId("obstacle-sheet")).toBeNull();
  });
});
