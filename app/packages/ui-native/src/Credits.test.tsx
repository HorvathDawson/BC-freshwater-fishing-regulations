/**
 * The credit block that replaced the map's attribution control.
 *
 * The route map under an estimate is now bare — no zoom stack, no scale, no attribution
 * button — so the notice it would have carried has to be on the page. The Province requires
 * its forecast wording VERBATIM, which makes "renders whatever it is given, unaltered" the
 * property worth testing.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { Credits } from "./Credits";
import { LIGHT } from "./theme";

afterEach(cleanup);

const PROVINCE =
  "Forecast data provided by the BC River Forecast Centre, Province of British Columbia, "
  + "and used under the Province's copyright terms.";

describe("<Credits>", () => {
  it("renders every line it is given, unaltered", () => {
    render(<Credits palette={LIGHT} lines={["Basemap © OpenStreetMap contributors",
                                            PROVINCE]} />);
    expect(screen.getByText("Basemap © OpenStreetMap contributors")).toBeTruthy();
    // Character for character: this string is a legal requirement, not copy.
    expect(screen.getByText(PROVINCE)).toBeTruthy();
  });

  it("renders nothing at all when there is nothing to credit", () => {
    // An empty "SOURCES" heading over blank space reads as a bug, not as an absence.
    const { container } = render(<Credits palette={LIGHT} lines={[]} />);
    expect(container.textContent).toBe("");
  });

  it("renders nothing when the caller passed no list", () => {
    const { container } = render(<Credits palette={LIGHT} />);
    expect(container.textContent).toBe("");
  });

  it("heads the block so the small print is identifiable as small print", () => {
    render(<Credits palette={LIGHT} lines={["x"]} />);
    expect(screen.getByText("SOURCES")).toBeTruthy();
  });
});
