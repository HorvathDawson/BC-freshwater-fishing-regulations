/**
 * (T166) The tidal water sheet draws `tidalSheet`'s words, whole, and is down when nothing is tapped.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { TidalSheet } from "./TidalSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<TidalSheet>", () => {
  it("open: the title and both paragraphs", () => {
    render(<TidalSheet open palette={LIGHT} onClose={() => {}} />);
    expect(screen.getByTestId("tidal-sheet").textContent).toBe(
      "Tidal water: a different regulation system. The B.C. freshwater fishing regulations do not apply here: no provincial rule, quota, closure, gear rule, licence or stamp."
      + "See the federal (DFO) tidal waters sport fishing regulations or the Fishing BC app; a federal Tidal Waters Sport Fishing Licence is required.");
    expect(screen.getByText("Tidal water — DFO rules")).toBeTruthy();
  });

  it("closed: the sheet is down", () => {
    render(<TidalSheet open={false} palette={LIGHT} onClose={() => {}} />);
    expect(screen.queryByTestId("tidal-sheet")).toBeNull();
  });
});
