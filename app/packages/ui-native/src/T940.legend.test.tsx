/**
 * T940 (owner 2026-11-20: "Satellite should maybe just be its own colors? Best for satellite?") — over satellite the map
 * draws the water in one colour set, the same in every theme (`imagery.json`). The key shows what the map draws: while
 * satellite is the ground its swatches are that set's; on the drawn map they are the theme's, as before. The words and
 * the card stay the theme's.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen } from "@testing-library/react";
import { LayoutWidthContext } from "@app/ui";
import { resolveTheme, STYLE_META } from "@app/map";
import { MapLegend, RunsKey } from "./shell/MapLegend";
import { DARK, LIGHT } from "./theme";

afterEach(cleanup);

/** "#RRGGBB" as the DOM writes it */
const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;
const SET = STYLE_META.imagery as Record<string, string>;

const col = (basemap: "map" | "satellite") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false, basemap }, cond: { marks: [] }, gearMarked: false,
  regsLayer: "standard", salmonOffered: false, statusOk: true, salmonFacts: null, isolated: null, isolate: () => {},
  basinDays: null, pickDay: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const key = (theme: "light" | "dark", basemap: "map" | "satellite") => {
  const { container } = render(
    <LayoutWidthContext.Provider value={390}>
      <MapLegend col={col(basemap)} conditions={false} palette={theme === "light" ? LIGHT : DARK} theme={theme} mapDay={null} />
    </LayoutWidthContext.Provider>);
  const html = container.innerHTML;
  cleanup();
  return html;
};

describe("(T940) the key follows the ground", () => {
  it("the regulations key: the satellite set's closed over satellite, in light and dark; the theme's on the drawn map", () => {
    for (const theme of ["light", "dark"] as const) {
      const own = resolveTheme(theme)["color.status.closed"] as string;
      const [sat, map] = [key(theme, "satellite"), key(theme, "map")];
      expect([theme, sat.includes(rgb(SET["color.status.closed"]!)), sat.includes(rgb(own))]).toEqual([theme, true, false]);
      expect([theme, map.includes(rgb(own)), map.includes(rgb(SET["color.status.closed"]!))]).toEqual([theme, true, false]);
    }
  });

  it("the Runs bar: one set over satellite whatever the theme; the theme's on the drawn map", () => {
    const bars = (theme: string, ground?: "map" | "satellite") => {
      render(<RunsKey theme={theme} palette={theme === "light" ? LIGHT : DARK} bottom={0} ground={ground} />);
      const el = screen.getByTestId("runs-key");
      // the bar: its four bands (the label ticks after it are the theme's ink, like the words)
      const out = [...el.querySelectorAll("div")].map((d) => d.style.backgroundColor).filter((c) => c.startsWith("rgb")).slice(0, 4);
      cleanup();
      return out;
    };
    const runs = ["spawn", "pass", "off", "none"].map((s) => rgb(SET[`color.run.${s}`]!));
    for (const r of runs) expect(bars("light", "satellite")).toContain(r);
    expect(bars("dark", "satellite")).toEqual(bars("light", "satellite"));
    expect(bars("dark")).not.toEqual(bars("dark", "satellite"));
    expect(bars("dark", "map")).toEqual(bars("dark"));
  });
});
