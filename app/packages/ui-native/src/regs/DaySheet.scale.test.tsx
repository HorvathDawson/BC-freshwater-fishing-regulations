/**
 * (T1203) "WHICH DAY?" AT THE DEFAULT TEXT SIZE IS 0.9.0's: the sheet is sized to its content and the calendar draws its own
 * gutter. Only at a larger text size does the body scroll under the title and Done (a scrolling body brings the gutter itself).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { DaySheet } from "./DaySheet";
import { dayRange } from "@app/core";
import { LIGHT } from "../theme";

const NOW = new Date(2026, 10, 19, 9, 30);
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const draw = (scale: number) => {
  vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(scale);
  render(<DaySheet open onClose={() => {}} palette={LIGHT} day={null} onDay={() => {}} range={dayRange(NOW, null)} now={NOW} />);
};

describe("(T1203) the day sheet", () => {
  it("1x: the body does not scroll (the sheet of 0.9.0)", () => {
    draw(1);
    expect(screen.queryByTestId("sheet-scroll")).toBeNull();
  });
  it("1.3x: the body scrolls under the title and Done", () => {
    draw(1.3);
    expect(screen.queryByTestId("sheet-scroll")).not.toBeNull();
  });
});
