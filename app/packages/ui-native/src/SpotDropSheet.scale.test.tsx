/**
 * (T1203) THE SPOT SHEET'S BUTTONS AT THE DEFAULT TEXT SIZE ARE TODAY'S: Continue takes the row's share with no minimum
 * (nothing moves at 1x, whatever the window); only at a larger text size does it keep `CONTINUE_MIN` of room, so the row
 * wraps (Continue to its own line) instead of squeezing it off the sheet.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { SpotDropSheet, CONTINUE_MIN } from "./SpotDropSheet";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const minWidthOfContinueSlot = (): string => {
  let el: HTMLElement | null = screen.getByText("Continue");
  const seen: string[] = [];
  for (let i = 0; el && i < 6; i++, el = el.parentElement) seen.push(getComputedStyle(el).minWidth);
  return seen.join(" ");
};

describe("(T1203) the spot sheet's buttons", () => {
  it("1x: no minimum on Continue's slot (the layout of 0.9.0)", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(1);
    render(<SpotDropSheet at={{ lat: 49.1, lon: -122 }} palette={LIGHT} onCancel={() => {}} onContinue={() => {}} />);
    expect(minWidthOfContinueSlot()).not.toContain(`${CONTINUE_MIN}px`);
  });
  it("1.3x: Continue keeps its room (the row wraps rather than squeezing it)", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(1.3);
    render(<SpotDropSheet at={{ lat: 49.1, lon: -122 }} palette={LIGHT} onCancel={() => {}} onContinue={() => {}} />);
    expect(minWidthOfContinueSlot()).toContain(`${CONTINUE_MIN}px`);
  });
});
