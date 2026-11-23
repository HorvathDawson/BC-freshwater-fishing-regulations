/**
 * (T940) THE KEY FOLLOWS THE GROUND: over satellite the regulations key's swatches are the satellite set (the same in every theme), and
 * the ones that would fall under 3:1 on the card carry an ink outline; on the drawn map the key is as it was (the theme's own colours,
 * no outline). `MapLegend` reads `col.mm.basemap`.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, render, screen, within } from "@testing-library/react";
import { MAP_KEY } from "@app/core";
import { resolveTheme } from "@app/map";
import { MapLegend } from "./shell/MapLegend";
import { CVD, DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const col = (basemap: "map" | "satellite") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false, basemap }, cond: { marks: [] }, gearMarked: true, regsLayer: "standard",
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const show = (basemap: "map" | "satellite", palette: Palette, theme: string) =>
  render(<MapLegend col={col(basemap)} conditions={false} palette={palette} theme={theme} mapDay={null} />);
const entry = (label: string) => screen.getAllByTestId("legend-entry").find((e) => e.textContent === label)!;
/** The colour of the status entry's swatch line: its first painted descendant. */
const swatchColour = (label: string): string => {
  const all = [entry(label), ...Array.from(entry(label).querySelectorAll("*"))] as HTMLElement[];
  const hit = all.map((e) => getComputedStyle(e).backgroundColor).find((c) => /^rgb\(/.test(c));
  return hit ?? "";
};
const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;
const THEMES = [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const;

describe("(T940) the satellite key", () => {
  it("over satellite the swatches are the satellite set in every theme; on the drawn map they are the theme's", () => {
    for (const [th, palette] of THEMES) {
      const t = resolveTheme(th);
      show("satellite", palette, th);
      expect([th, swatchColour(MAP_KEY.status.base)]).toEqual([th, rgb(String(t["color.imagery.status.base"]))]);
      expect([th, swatchColour(MAP_KEY.status.own)]).toEqual([th, rgb(String(t["color.imagery.status.own"]))]);
      cleanup();
      show("map", palette, th);
      expect([th, swatchColour(MAP_KEY.status.base)]).toEqual([th, rgb(String(t["color.status.base"]))]);
      expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
      cleanup();
    }
  });

  it("over satellite the pale swatches wear an ink outline on the light card; dark's need none", () => {
    show("satellite", LIGHT, "light");
    expect(within(entry(MAP_KEY.status.base)).getAllByTestId("legend-outline")).toHaveLength(1);
    cleanup();
    show("satellite", DARK, "dark");
    expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
  });
});
