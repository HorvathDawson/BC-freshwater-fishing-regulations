/**
 * (T1203) A SHEET'S HEADER AT THE DEFAULT TEXT SIZE IS 0.9.0's: Done carries no margin and keeps its shrink, the title is its own
 * Text (no wrapper). Only at a larger text size does Done keep its place (a 12 px gap, no shrink) so a long title wraps beside it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { PixelRatio } from "react-native";
import { Sheet } from "./Sheet";
import { LIGHT } from "./theme";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const doneBox = () => screen.getAllByText("Done")[0]!.closest("[role=button]") as HTMLElement;
const draw = (scale: number) => {
  vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(scale);
  render(<Sheet open onClose={() => {}} title="A newer version of the app is ready" palette={LIGHT}><span>body</span></Sheet>);
};

describe("(T1203) the sheet header", () => {
  it("1x: Done has no margin, the title is not wrapped in a box of its own", () => {
    draw(1);
    expect(getComputedStyle(doneBox()).marginLeft).not.toBe("12px");
    const title = screen.getByRole("heading", { name: /newer version/ });
    expect(getComputedStyle(title).minWidth).not.toBe("0px");
  });
  it("1.3x: Done keeps a 12 px gap and the title may shrink and break", () => {
    draw(1.3);
    expect(getComputedStyle(doneBox()).marginLeft).toBe("12px");
  });
});
