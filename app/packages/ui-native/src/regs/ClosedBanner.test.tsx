/**
 * (owner 2026-11-04: "the all closed sections list is collapsed") A WATER CLOSED ALL YEAR ON SEVERAL STRETCHES says the
 * count and folds the stretches under it: closed on open, each with its level, on a press.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { BannerVM } from "@app/core";
import { LIGHT } from "../theme";
import { ClosedBanner } from "./KeepBlocks";

afterEach(cleanup);

const BANNER = {
  kind: "closed", big: ["Closed all year"], paras: [], chain: [], reopen: null,
  merged: { label: "Closed all year on 2 stretches (8 sections)", items: [
    { text: "From the signs up to the Ecological Reserve · 7 sections", level: "This water", rule: "r1", book: [] },
    { text: "From the Ecological Reserve up to the top of the river · 1 section", level: "Federal / parks", rule: "r2", book: [] },
  ] },
} as unknown as BannerVM;
const actions = { onSheet: vi.fn(), onDay: vi.fn() } as never;

describe("the closed banner's stretches", () => {
  it("are folded under their count, and open on a press", () => {
    render(<ClosedBanner banner={BANNER} palette={LIGHT} actions={actions} />);
    const fold = screen.getByTestId("merged-fold");
    expect(fold.textContent).toContain("Closed all year on 2 stretches (8 sections)");
    expect(fold.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryByText(/top of the river/)).toBeNull();
    fireEvent.click(fold);
    expect(screen.getByTestId("merged-fold").getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByText(/top of the river/)).toBeTruthy();
    expect(screen.getAllByTestId("chip:level").map((c) => c.textContent)).toEqual(["This water", "Federal / parks"]);
  });
});
