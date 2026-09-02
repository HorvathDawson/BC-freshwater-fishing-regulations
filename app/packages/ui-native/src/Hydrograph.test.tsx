/**
 * Mounted through react-native-web — the mobile-web target — so these assert a real SVG
 * tree, not a description of one.
 *
 * The arithmetic is `@app/ui`'s and tested there. What is only provable here is that the
 * component puts those numbers on screen: that the shape reaches the DOM at all, that the
 * axis labels render as text rather than as coordinates, and that nothing draws on top of
 * the data. Those are the failures a unit test on `buildHydrograph` cannot see.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import type { Band } from "@app/core";
import { buildHydrograph } from "@app/ui";
import { Hydrograph } from "./Hydrograph";
import { LIGHT } from "./theme";

afterEach(cleanup);

const BAND: Band = [8, 12, 18, 26, 40];
const flat = <T,>(n: number, v: T): T[] => Array.from({ length: n }, () => v);

const shape = (over: Partial<Parameters<typeof buildHydrograph>[0]> = {}) =>
  buildHydrograph({
    values: flat(72, 15.7), bands: flat(72, BAND),
    xLabels: ["3d", "2d", "1d", "now"], ...over,
  });

const draw = (over = {}) =>
  render(<Hydrograph shape={shape(over)} palette={LIGHT} colour="#1f7a8c" />);

describe("<Hydrograph>", () => {
  it("draws every path the shape describes, and no more", () => {
    const { container } = draw();
    const s = shape();
    const ds = [...container.querySelectorAll("path")].map((p) => p.getAttribute("d"));
    for (const e of s.envelopes) expect(ds).toContain(e.d);
    expect(ds).toContain(s.line);
    expect(ds).toContain(s.median);
    expect(ds).toHaveLength(s.envelopes.length + 2);
  });

  it("labels the axes with the tick labels, not the raw values", () => {
    const { container } = draw();
    const words = [...container.querySelectorAll("text")].map((t) => t.textContent);
    for (const t of shape().yTicks) expect(words).toContain(t.label);
    for (const t of shape().xTicks) expect(words).toContain(t.label);
  });

  it("puts the gridlines behind the data, so a rule never sits on the reading", () => {
    const { container } = draw();
    const kinds = [...container.querySelectorAll("line, path")].map((n) => n.tagName.toLowerCase());
    expect(kinds.lastIndexOf("line")).toBeLessThan(kinds.indexOf("path"));
  });

  it("marks today's reading once", () => {
    const { container } = draw();
    expect(container.querySelectorAll("circle")).toHaveLength(1);
  });

  it("omits the today marker when there is no reading, rather than drawing it at zero", () => {
    const { container } = draw({ values: flat(72, null), nowIndex: -1 });
    expect(container.querySelectorAll("circle")).toHaveLength(0);
  });

  it("carries a description, because a chart with no text is invisible to a screen reader", () => {
    const { getByLabelText } = render(
      <Hydrograph shape={shape()} palette={LIGHT} colour="#1f7a8c" label="Chilliwack River flow" />,
    );
    expect(getByLabelText("Chilliwack River flow")).toBeTruthy();
  });
});
