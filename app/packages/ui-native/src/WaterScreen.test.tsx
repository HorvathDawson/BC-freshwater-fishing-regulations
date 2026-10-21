/**
 * A water's sheet, mounted — with regulations NOT integrated.
 *
 * The sheet keeps its regulations face and says, in words, that regulations are coming. A
 * blank space there would read as "no rules apply", which is never true of any BC water.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<WaterScreen>", () => {
  it("names the water and holds the place where its regulations will go", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId}
                        palette={LIGHT} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    // The panel's words are what a screen reader reads: the title as a heading, and the sentence
    // under it — not one container label standing in for both (which hid the sentence).
    const title = screen.getByRole("heading", { name: "Regulations are coming" });
    expect(title).toBeTruthy();
    expect(screen.getByText(/Check the current BC Freshwater Fishing Regulations Synopsis/))
      .toBeTruthy();
    expect(screen.queryByLabelText("Regulations are coming")).toBeNull();
    // Nothing on the sheet claims an outcome.
    for (const word of [/\bCLOSED\b/, /\bRESTRICTED\b/, /\bOPEN\b/])
      expect(screen.queryByText(word)).toBeNull();
  });

  it("has no door to the water's first reach: the header strip is the way to its conditions", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    expect(screen.queryByLabelText("and the conditions: what the water is doing")).toBeNull();
    expect(screen.getAllByRole("tab").map((t) => [t.getAttribute("aria-label"), t.getAttribute("aria-selected")]))
      .toEqual([["Regulations", "true"], ["Conditions", "false"]]);
  });

  it("says so when the bundle has no such water", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:nope" as ItemId}
                        palette={LIGHT} />);
    await waitFor(() =>
      expect(screen.getByText("No such water in this bundle.")).toBeTruthy());
  });
});
