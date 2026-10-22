/**
 * The app's one icon set (N1): each icon draws its Lucide elements on the 24 grid at the owner's 1.75 round stroke,
 * in the colour it is given, hidden from a screen reader (the button around it speaks). The house scan that refuses a
 * glyph drawn anywhere else is `tools/one-icon-set.test.ts`.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { ICONS, ICON_STROKE, Icon, IconText, LUCIDE_NAME, type IconName } from "./icons";

afterEach(cleanup);

describe("<Icon>", () => {
  const names = Object.keys(ICONS) as IconName[];

  it("every icon names the Lucide icon it copies; the stroke is the owner's 1.75", () => {
    expect(new Set(Object.keys(LUCIDE_NAME))).toEqual(new Set(names));
    expect(ICON_STROKE).toBe(1.75);
    // the bar's four, the rail's one, and the chrome the owner named (decisions.md 2026-10-22)
    expect([LUCIDE_NAME.map, LUCIDE_NAME.conditions, LUCIDE_NAME.spots, LUCIDE_NAME.more, LUCIDE_NAME.layers,
            LUCIDE_NAME.search, LUCIDE_NAME.back, LUCIDE_NAME.close])
      .toEqual(["map", "waves", "map-pin", "ellipsis", "layers", "search", "chevron-left", "x"]);
  });

  it.each(names)("%s: its elements on the 24 grid, round 1.75, unfilled, in the colour given, aria-hidden", (n) => {
    const { container, getByTestId } = render(<Icon name={n} colour="currentColor" size={20} />);
    expect(getByTestId(`icon-${n}`).getAttribute("aria-hidden")).toBe("true");
    const svg = container.querySelector("svg")!;
    expect([svg.getAttribute("viewBox"), svg.getAttribute("width")]).toEqual(["0 0 24 24", "20"]);
    const marks = [...svg.querySelectorAll("path, circle, rect")];
    expect(marks.map((m) => m.tagName.toLowerCase()))
      .toEqual(ICONS[n].map((s) => ("path" in s ? "path" : "circle" in s ? "circle" : "rect")));
    for (const m of marks) {
      expect([m.getAttribute("stroke"), m.getAttribute("stroke-width"), m.getAttribute("stroke-linecap"),
              m.getAttribute("stroke-linejoin"), m.getAttribute("fill")])
        .toEqual(["currentColor", "1.75", "round", "round", "none"]);
    }
  });

  it("<IconText>: the icon before the words, the words alone as text", () => {
    const { container, getByTestId } = render(<IconText name="back" text="Map" style={{ color: "currentColor" }} />);
    expect(getByTestId("icon-back")).toBeTruthy();
    expect(container.textContent).toBe("Map");
  });
});
